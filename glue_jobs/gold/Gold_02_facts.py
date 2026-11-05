"""
Gold_02_facts  -  GOLD stage: Star Schema Granular Analytical Fact Tables

Builds and incrementally updates the analytical fact tables in the Gold Iceberg catalog:
  - gold.fact_sales                   : Granular order-line sales fact, financial margin & fulfillment rates.
  - gold.fact_sales_returns           : Customer return events linked to sales lines & cost loss impact.
  - gold.fact_procurement_receipts    : Dock receiving lines, PO delivery OTIF, lead times & quality rejections.
  - gold.fact_inventory_daily_balance : Daily warehouse-product stock valuation (EGP) and stockout tracking.

Partitioned by month (hidden Iceberg partitioning) for fast BI slice-and-dice queries.
All business rules reside in gold_rules.py as pure DataFrame transformations.
"""

from pyspark.sql import functions as F

from common_utils import init_job, publish_gold, table_exists, tbl
import gold_rules as R


def main():
    args, glue_context, spark, job = init_job()

    print("[Gold_02_facts] Starting analytical fact tables generation...")

    # Ensure prerequisite dimensions from Gold_01 exist
    required_dims = ["dim_product", "dim_customer", "dim_supplier", "dim_warehouse"]
    missing_dims = [d for d in required_dims if not table_exists(spark, tbl("gold", d))]
    if missing_dims:
        raise RuntimeError(
            f"[Gold_02_facts] Prerequisite dimension tables {missing_dims} do not exist in glue_catalog.gold! "
            "Please ensure 'Gold_01_dimensions' runs and succeeds before running 'Gold_02_facts'."
        )

    def S(table_name):

        return spark.table(tbl("silver", table_name))

    def G(table_name):
        return spark.table(tbl("gold", table_name))

    def month(col_name):
        return [F.months(col_name)]

    # 1. Fact Sales (Order Line grain)
    print("  -> Generating fact_sales...")
    fs = R.fact_sales(
        order_lines_df=S("sales_order_line"),
        orders_df=S("sales_order"),
        product_dim_df=G("dim_product"),
        customer_dim_df=G("dim_customer"),
        cost_df=S("product_cost"),
    )
    publish_gold(
        spark,
        df=fs,
        table="fact_sales",
        keys=["sales_order_id", "line_no"],
        mode="upsert",
        partition=month("order_date"),
    )

    # 2. Fact Sales Returns
    print("  -> Generating fact_sales_returns...")
    fr = R.fact_sales_returns(
        returns_df=S("sales_return"),
        fact_sales_df=fs,
    )
    publish_gold(
        spark,
        df=fr,
        table="fact_sales_returns",
        keys=["return_line_id"],
        mode="upsert",
        partition=month("return_date"),
    )

    # 3. Fact Procurement Receipts & Vendor OTIF
    print("  -> Generating fact_procurement_receipts...")
    fp = R.fact_procurement_receipts(
        receipts_df=S("goods_receipt_line"),
        po_lines_df=S("purchase_order_line"),
        po_df=S("purchase_order"),
        suppliers_df=G("dim_supplier"),
    )
    publish_gold(
        spark,
        df=fp,
        table="fact_procurement_receipts",
        keys=["receipt_id", "receipt_line_no"],
        mode="upsert",
        partition=month("received_date"),
    )

    # 4. Fact Daily Inventory Balance
    print("  -> Generating fact_inventory_daily_balance...")
    fi = R.fact_inventory_daily_balance(
        snapshots_df=S("inventory_snapshot"),
        product_dim_df=G("dim_product"),
    )
    publish_gold(
        spark,
        df=fi,
        table="fact_inventory_daily_balance",
        keys=["snapshot_date", "warehouse_id", "product_id"],
        mode="upsert",
        partition=month("snapshot_date"),
    )

    print("[Gold_02_facts] All 4 fact tables successfully published to Gold layer.")
    job.commit()


if __name__ == "__main__":
    main()

