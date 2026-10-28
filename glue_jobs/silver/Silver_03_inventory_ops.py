"""
silver_03_inventory_ops  -  SILVER stage: inventory ledger, daily snapshots, stockout episodes, weather

Incremental (reads only new Bronze rows). Partitioned by month: inventory_transaction (event_date),
inventory_snapshot (snapshot_date), stockout_event (start_date). weather_daily + gap tables are tiny.

Business rules: silver_rules.py. Highlights
  * ledger: duplicates dropped (first posting wins), sign corrected by txn_type / reason, standard reason codes,
    clock skew flagged
  * snapshot: reconciled against the CUMULATIVE CLEAN LEDGER (on_hand_qty = ledger, on_hand_qty_raw = source);
    stale copies / negatives / lost movements show up as ledger_mismatch_flag
  * stockout: overlapping episodes merged (all ids kept), episodes with no end although stock came back flagged
  * weather: Fahrenheit detected against the local climate (also in winter), raw values kept
  * silver.snapshot_gap / silver.weather_gap list days with no data

NOTE: drop the old silver.* tables once before the first run with this version (new columns, MERGE needs same schema).
"""
from pyspark.sql import functions as F

from common_utils import (QBin, dedup_latest, ensure_cols, init_job, is_empty, publish_silver, read_new, table_exists,
                          tbl)

import silver_rules as R


def main():
    args, glue_context, spark, job = init_job()
    q = QBin()

    def S(t):
        return tbl("silver", t)

    def B(t):
        return spark.table(tbl("bronze", t))

    # ======================================================================= inventory_transaction
    raw, wm = read_new(spark, "inventory_transactions", S("inventory_transaction"))
    if not is_empty(raw):
        raw = ensure_cols(raw, {"reference_id": "string", "reference_type": "string", "reason_code": "string"})
        publish_silver(spark, R.inventory_transaction(raw), "inventory_transaction", ["txn_id"], "insert_only", q, wm,
                       partition=[F.months("event_date")])

    # ======================================================================= inventory_snapshot
    raw, wm = read_new(spark, "inventory_snapshots", S("inventory_snapshot"))
    if not is_empty(raw) and table_exists(spark, S("inventory_transaction")):
        keys = ["snapshot_date", "warehouse_id", "product_id"]
        snap = dedup_latest(raw, keys, "snapshot_ts").withColumn("_ctx", F.lit(False))
        min_date = snap.agg(F.min("snapshot_date")).first()[0]
        if table_exists(spark, S("inventory_snapshot")) and min_date is not None:
            from pyspark.sql.window import Window
            w_last = Window.partitionBy("warehouse_id", "product_id").orderBy(F.col("snapshot_date").desc())
            ctx = (spark.table(S("inventory_snapshot")).filter(F.col("snapshot_date") < F.lit(min_date))
                   .withColumn("_rn", F.row_number().over(w_last)).filter(F.col("_rn") == 1)
                   .select("warehouse_id", "product_id", "snapshot_date", F.col("on_hand_qty_raw").alias("on_hand_qty"))
                   .withColumn("_ctx", F.lit(True)))                       # previous day's RAW balance, for the lag
            snap = snap.unionByName(ctx, allowMissingColumns=True)
        ledger_daily = (spark.table(S("inventory_transaction"))
                        .groupBy("warehouse_id", "product_id", F.col("event_date").alias("ledger_date"))
                        .agg(F.sum("qty_change_clean").cast("long").alias("net_qty")))
        publish_silver(spark, R.inventory_snapshot(snap, ledger_daily), "inventory_snapshot", keys, "upsert", q, wm,
                       order_col="snapshot_ts", partition=[F.months("snapshot_date")])

    # ======================================================================= stockout_event
    raw, wm = read_new(spark, "stockout_events", S("stockout_event"))
    if not is_empty(raw) and table_exists(spark, S("inventory_transaction")):
        affected = raw.select("warehouse_id", "product_id").distinct()
        full = dedup_latest(B("stockout_events").join(affected, ["warehouse_id", "product_id"], "left_semi"),
                            ["stockout_id"])
        restocks = (spark.table(S("inventory_transaction")).join(affected, ["warehouse_id", "product_id"], "left_semi")
                    .filter(F.col("txn_type").isin("RECEIPT", "RETURN_RESTOCK") & (F.col("qty_change_clean") > 0)
                            & (F.col("reference_type") != "INITIAL_LOAD"))
                    .select("warehouse_id", "product_id", F.col("event_ts").alias("restock_ts")))
        publish_silver(spark, R.stockout_event(full, restocks), "stockout_event", ["stockout_id"], "overwrite_keys", q, wm,
                       key_col="entity_key", partition=[F.months("start_date")])

    # ======================================================================= weather_daily
    raw, wm = read_new(spark, "weather_daily", S("weather_daily"))
    if not is_empty(raw):
        wd = R.weather_daily(raw, B("weather_daily"))                      # full history -> local climatology
        publish_silver(spark, wd, "weather_daily", ["obs_date", "location_code"], "upsert", q, wm,
                       order_col="_ingested_at")

    # ======================================================================= gap reports (small, replaced each run)
    if table_exists(spark, S("inventory_snapshot")):
        publish_silver(spark, R.missing_dates(spark.table(S("inventory_snapshot")), "snapshot_date"),
                       "snapshot_gap", None, "replace", q)
    if table_exists(spark, S("weather_daily")):
        publish_silver(spark, R.missing_dates(spark.table(S("weather_daily")), "obs_date", ["location_code"]),
                       "weather_gap", None, "replace", q)

    job.commit()


if __name__ == "__main__":
    main()
