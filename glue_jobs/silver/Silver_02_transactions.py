"""
silver_02_transactions  -  SILVER stage: sales, returns, procurement, dock receipts, batches

Incremental: reads only Bronze rows newer than what each silver table already holds, then MERGEs (upsert).
Header tables that carry values derived from their lines/receipts (sales_order, purchase_order) are recomputed
for every order/PO touched by the new batch (new header, new line or new receipt).

Partitioning (hidden Iceberg partitions, by month of the business date):
  sales_order, sales_order_line, sales_return, purchase_order, purchase_order_line, goods_receipt_line

Business rules: silver_rules.py.  Silver quarantine (written to quarantine.rejected_rows): returns whose order does
not exist, returns dated before their order or before the shipment of their line.

NOTE: new Silver columns were added (flags, *_raw columns). Drop the old silver.* tables once before the first
run with this version (MERGE ... UPDATE SET * needs identical schemas).
"""
from pyspark.sql import functions as F

from common_utils import (QBin, cairo_date, dedup_latest, init_job, is_empty, publish_silver, read_new, table_exists,
                          table_or_empty, tbl)

import silver_rules as R

ORDER_DERIVED = ["total_net_amount_lines_egp", "total_allocated_amount_lines_egp", "total_ordered_amount_lines_egp",
                 "total_mismatch_flag", "is_cancelled"]
PO_DERIVED = ["receipt_count", "qty_received_total_base", "line_count", "lines_closed", "qty_ordered_total_base",
              "total_lines_cost_egp", "has_receipts", "po_status_derived", "cancelled_with_receipts_flag",
              "status_lag_flag", "total_mismatch_flag"]
GR_DDL = ("receipt_id string, receipt_line_no int, purchase_order_id string, po_line_no int, product_id string, "
          "received_ts timestamp, qty_received_uom int, updated_ts timestamp, _ingested_at timestamp")


def main():
    args, glue_context, spark, job = init_job()
    q = QBin()

    def S(t):
        return tbl("silver", t)

    def B(t):
        return spark.table(tbl("bronze", t))

    def month(col):
        return [F.months(col)]

    def ids_frame(col):
        return spark.createDataFrame([], f"{col} string")

    # ======================================================================= sales_order_line
    so_line_ids = ids_frame("sales_order_id")
    raw, wm = read_new(spark, "sales_order_lines", S("sales_order_line"))
    if not is_empty(raw):
        hdr_ts = (dedup_latest(B("sales_orders"), ["sales_order_id"], "source_updated_ts")
                  .select("sales_order_id", F.col("order_ts").alias("_order_ts")))
        lines = R.order_line(raw, hdr_ts)
        so_line_ids = lines.select("sales_order_id").distinct()
        publish_silver(spark, lines, "sales_order_line", ["sales_order_id", "line_no"], "upsert", q, wm,
                       order_col="source_updated_ts", partition=month("order_date"))

    # ======================================================================= sales_order
    raw, wm = read_new(spark, "sales_orders", S("sales_order"))
    hdr = None
    if not is_empty(raw):
        hdr = (dedup_latest(raw, ["sales_order_id"], "source_updated_ts")      # newest version wins (stale re-sends)
               .withColumn("order_date", cairo_date("order_ts")))
    ids = so_line_ids if hdr is None else so_line_ids.unionByName(hdr.select("sales_order_id"))
    ids = ids.distinct()
    base = hdr
    if table_exists(spark, S("sales_order")):
        existing = spark.table(S("sales_order")).drop(*ORDER_DERIVED)
        need = ids if hdr is None else ids.join(hdr.select("sales_order_id"), "sales_order_id", "left_anti")
        old = existing.join(need, "sales_order_id", "left_semi")
        base = old if hdr is None else hdr.unionByName(old)
    if base is not None:
        sums = None
        if table_exists(spark, S("sales_order_line")):
            sums = (spark.table(S("sales_order_line")).join(ids, "sales_order_id", "left_semi")
                    .groupBy("sales_order_id")
                    .agg(F.sum("line_net_amount_egp").alias("total_net_amount_lines_egp"),
                         F.sum("line_allocated_amount_egp").alias("total_allocated_amount_lines_egp"),
                         F.sum("line_ordered_amount_egp").alias("total_ordered_amount_lines_egp")))
        publish_silver(spark, R.sales_order(base, sums), "sales_order", ["sales_order_id"], "upsert", q, wm,
                       order_col="source_updated_ts", partition=month("order_date"))

    # ======================================================================= sales_return
    raw, wm = read_new(spark, "sales_returns", S("sales_return"))
    if not is_empty(raw):
        orders_ref = table_or_empty(spark, S("sales_order"), "sales_order_id string, order_ts timestamp")
        lines_ref = table_or_empty(spark, S("sales_order_line"),
                                   "sales_order_id string, line_no int, qty_shipped int, shipped_ts timestamp")
        r = R.sales_return(raw, orders_ref, lines_ref, q)
        publish_silver(spark, r, "sales_return", ["return_line_id"], "upsert", q, wm,
                       order_col="updated_ts", partition=month("return_date"))

    # ======================================================================= purchase_order_line
    po_line_ids = ids_frame("purchase_order_id")
    raw, wm = read_new(spark, "purchase_order_lines", S("purchase_order_line"))
    if not is_empty(raw):
        po_hdr = (dedup_latest(B("purchase_orders"), ["purchase_order_id"], "source_updated_ts")
                  .select("purchase_order_id", "supplier_id", F.col("po_created_ts").alias("_po_ts")))
        sp_current = (table_or_empty(spark, S("supplier_product"),
                                     "supplier_id string, product_id string, lead_time_days_effective int, "
                                     "moq_cases int, order_multiple_cases int, effective_to date")
                      .filter(F.col("effective_to").isNull()))
        pol = R.po_line(raw, po_hdr, sp_current)
        po_line_ids = pol.select("purchase_order_id").distinct()
        publish_silver(spark, pol, "purchase_order_line", ["purchase_order_id", "po_line_no"], "upsert", q, wm,
                       order_col="source_updated_ts", partition=month("po_date"))

    # ======================================================================= goods_receipt_line
    po_receipt_ids = ids_frame("purchase_order_id")
    raw, wm = read_new(spark, "goods_receipt_lines", S("goods_receipt_line"))
    if not is_empty(raw):
        new_keys = raw.select("receipt_id", "receipt_line_no").distinct()
        po_receipt_ids = raw.filter(F.col("purchase_order_id").isNotNull()).select("purchase_order_id").distinct()
        # earlier receipts of the same PO lines are needed for the cumulative over-delivery check
        prior = (table_or_empty(spark, S("goods_receipt_line"), GR_DDL)
                 .join(po_receipt_ids, "purchase_order_id", "left_semi"))
        pol_all = table_or_empty(spark, S("purchase_order_line"),
                                 "purchase_order_id string, po_line_no int, product_id string, "
                                 "units_per_case int, qty_ordered_uom int")
        upc_po = pol_all.groupBy("purchase_order_id", "product_id").agg(F.max("units_per_case").alias("_upc_po"))
        upc_prod = (dedup_latest(B("products"), ["product_id"])
                    .select("product_id", F.col("units_per_case").alias("_upc_prod")))
        both = raw.unionByName(prior.select(*[c for c in prior.columns if c in raw.columns]), allowMissingColumns=True)
        gr = R.goods_receipt(both, upc_po, upc_prod, pol_all.select("purchase_order_id", "po_line_no", "qty_ordered_uom"))
        gr = gr.join(new_keys, ["receipt_id", "receipt_line_no"], "left_semi")
        publish_silver(spark, gr, "goods_receipt_line", ["receipt_id", "receipt_line_no"], "upsert", q, wm,
                       order_col="updated_ts", partition=month("received_date"))

    # ======================================================================= purchase_order
    raw, wm = read_new(spark, "purchase_orders", S("purchase_order"))
    hdr = None
    if not is_empty(raw):
        hdr = (dedup_latest(raw, ["purchase_order_id"], "source_updated_ts")
               .withColumn("po_date", cairo_date("po_created_ts")))
    ids = po_line_ids.unionByName(po_receipt_ids)
    ids = (ids if hdr is None else ids.unionByName(hdr.select("purchase_order_id"))).distinct()
    base = hdr
    if table_exists(spark, S("purchase_order")):
        existing = spark.table(S("purchase_order")).drop(*PO_DERIVED)
        need = ids if hdr is None else ids.join(hdr.select("purchase_order_id"), "purchase_order_id", "left_anti")
        old = existing.join(need, "purchase_order_id", "left_semi")
        base = old if hdr is None else hdr.unionByName(old)
    if base is not None:
        rc = (table_or_empty(spark, S("goods_receipt_line"),
                             "purchase_order_id string, receipt_id string, qty_received_base int")
              .join(ids, "purchase_order_id", "left_semi").groupBy("purchase_order_id")
              .agg(F.countDistinct("receipt_id").alias("receipt_count"),
                   F.sum("qty_received_base").alias("qty_received_total_base")))
        pl = (table_or_empty(spark, S("purchase_order_line"),
                             "purchase_order_id string, line_status string, qty_ordered_base int, "
                             "line_total_cost_egp decimal(14,2)")
              .join(ids, "purchase_order_id", "left_semi").groupBy("purchase_order_id")
              .agg(F.count(F.lit(1)).alias("line_count"),
                   F.sum((F.col("line_status") == "CLOSED").cast("int")).alias("lines_closed"),
                   F.sum("qty_ordered_base").alias("qty_ordered_total_base"),
                   F.sum("line_total_cost_egp").alias("total_lines_cost_egp")))
        publish_silver(spark, R.purchase_order(base, rc, pl), "purchase_order", ["purchase_order_id"], "upsert", q, wm,
                       order_col="source_updated_ts", partition=month("po_date"))

    # ======================================================================= product_batch
    raw, wm = read_new(spark, "product_batches", S("product_batch"))
    if not is_empty(raw):
        gr_dates = table_or_empty(spark, S("goods_receipt_line"),
                                  "receipt_id string, receipt_line_no int, received_date date")
        all_numbers = B("product_batches").select("batch_number", "product_id").distinct()
        publish_silver(spark, R.product_batch(raw, gr_dates, all_numbers), "product_batch", ["batch_id"], "upsert",
                       q, wm, order_col="updated_ts")

    job.commit()


if __name__ == "__main__":
    main()
