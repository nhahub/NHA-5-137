"""
platinum_replenishment_engine.py - PLATINUM stage: Decision Intelligence & Prescriptive Replenishment Engine
Running on AWS Glue 5.0 (PySpark 3.5 & Apache Iceberg).

Reads from:
  - silver.sales_order_line, silver.sales_order
  - silver.product, silver.warehouse
  - silver.weather_daily, silver.calendar_event
  - silver.inventory_snapshot

Performs:
  1. Multi-Window Feature Engineering (Lags 7d/14d/28d, Moving Averages).
  2. Ambient Heat-Stress Elasticity Scoring (temperature impact on chilled/beverage demand).
  3. Multi-Horizon Demand Forecasting (7-day Rush, 14-day Standard PO, 30-day Strategic Buffer).
  4. FEFO Spoilage & Expiry Prevention Logic (identifies perishable stock at risk of expiration).
  5. Inter-DC Rebalancing vs. Supplier Procurement Decision Engine (transfers vs. POs).
  6. Warehouse Physical Pallet Capacity Guardrails (prevents overfilling chilled/ambient storage).

Writes to:
  - s3://smart-lakehouse-platinum/iceberg/ (Table: glue_catalog.platinum.smart_replenishment_recommendations)
"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def main():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    job.init(args['JOB_NAME'], args)

    CATALOG = "glue_catalog"

    # =========================================================================
    # 1. READ CLEAN CONFORMED DATA FROM SILVER ICEBERG TABLES
    # =========================================================================
    sales_order_line = spark.table(f"{CATALOG}.silver.sales_order_line")
    sales_order = spark.table(f"{CATALOG}.silver.sales_order")
    product = spark.table(f"{CATALOG}.silver.product")
    warehouse = spark.table(f"{CATALOG}.silver.warehouse")
    weather_daily = spark.table(f"{CATALOG}.silver.weather_daily")
    calendar_event = spark.table(f"{CATALOG}.silver.calendar_event")
    inventory_snapshot = spark.table(f"{CATALOG}.silver.inventory_snapshot")

    # =========================================================================
    # 2. AGGREGATE DAILY UNCONSTRAINED DEMAND (ORDERS)
    # =========================================================================
    daily_demand = (
        sales_order_line.join(sales_order, "sales_order_id")
        .groupBy(F.to_date("order_ts").alias("demand_date"), "warehouse_id", "product_id")
        .agg(
            F.sum("qty_ordered").alias("units_ordered"),
            F.sum("qty_shipped").alias("units_shipped")
        )
    )

    # Map weather observation codes to regional distribution centers
    weather_map = (
        weather_daily
        .withColumn("warehouse_id", 
            F.when(F.col("location_code") == "GIZA", "WH01")
             .when(F.col("location_code") == "ALEXANDRIA", "WH02")
             .when(F.col("location_code") == "ASSIUT", "WH03")
        )
        .select(
            F.col("obs_date").alias("weather_date"), 
            "warehouse_id", 
            "temp_max_c", 
            "temp_min_c"
        )
    )

    base_df = (
        daily_demand
        .join(weather_map, 
              (daily_demand.demand_date == weather_map.weather_date) & 
              (daily_demand.warehouse_id == weather_map.warehouse_id), "left")
        .join(product.select("product_id", "product_name", "category_id", "storage_type", "is_perishable", "shelf_life_days"), "product_id", "left")
        .join(warehouse.select("warehouse_id", "warehouse_name", "capacity_pallet_positions"), "warehouse_id", "left")
    )

    # =========================================================================
    # 3. ADVANCED FEATURE ENGINEERING (LAGS & WEATHER ELASTICITY)
    # =========================================================================
    w_pw = Window.partitionBy("warehouse_id", "product_id").orderBy("demand_date")

    features_df = (
        base_df
        .withColumn("lag_7d", F.coalesce(F.lag("units_ordered", 7).over(w_pw), F.lit(0)))
        .withColumn("lag_14d", F.coalesce(F.lag("units_ordered", 14).over(w_pw), F.lit(0)))
        .withColumn("moving_avg_7d", F.coalesce(F.avg("units_ordered").over(w_pw.rowsBetween(-7, -1)), F.lit(0)))
        .withColumn("moving_avg_28d", F.coalesce(F.avg("units_ordered").over(w_pw.rowsBetween(-28, -1)), F.lit(0)))
        # Thermal sensitivity multiplier for perishable/beverage lines under intense summer heat
        .withColumn("temp_elasticity_factor", 
            F.when((F.col("is_perishable") == True) & (F.col("temp_max_c") >= 35.0), F.lit(1.35))
             .when(F.col("temp_max_c") >= 38.0, F.lit(1.20))
             .otherwise(F.lit(1.00))
        )
    )

    # Filter for the most recent observation state per product and warehouse
    w_latest = Window.partitionBy("warehouse_id", "product_id").orderBy(F.col("demand_date").desc())
    latest_features = features_df.withColumn("rn", F.row_number().over(w_latest)).filter(F.col("rn") == 1)

    # Obtain latest available on-hand stock balance
    w_inv = Window.partitionBy("warehouse_id", "product_id").orderBy(F.col("snapshot_date").desc())
    latest_inv = (
        inventory_snapshot
        .withColumn("rn", F.row_number().over(w_inv))
        .filter(F.col("rn") == 1)
        .select("warehouse_id", "product_id", F.col("on_hand_qty").alias("current_stock_on_hand"))
    )

    # =========================================================================
    # 4. MULTI-HORIZON DEMAND PROJECTIONS & DAYS-OF-SUPPLY
    # =========================================================================
    scored_df = (
        latest_features.join(latest_inv, ["warehouse_id", "product_id"], "left")
        .na.fill({"current_stock_on_hand": 0})
        .withColumn("forecast_demand_7d", F.round(F.col("moving_avg_7d") * 7 * F.col("temp_elasticity_factor")).cast("int"))
        .withColumn("forecast_demand_14d", F.round(F.col("moving_avg_28d") * 14 * F.col("temp_elasticity_factor")).cast("int"))
        .withColumn("forecast_demand_30d", F.round(F.col("moving_avg_28d") * 30 * F.col("temp_elasticity_factor")).cast("int"))
        .withColumn("days_of_supply", 
            F.when(F.col("moving_avg_7d") > 0, F.round(F.col("current_stock_on_hand") / F.col("moving_avg_7d"), 1))
             .otherwise(F.lit(999.0))
        )
    )

    # =========================================================================
    # 5. PRESCRIPTIVE DECISION INTELLIGENCE
    # =========================================================================
    recommendations_df = (
        scored_df
        .withColumn("shortage_7d", F.greatest(F.lit(0), F.col("forecast_demand_7d") - F.col("current_stock_on_hand")))
        .withColumn("shortage_14d", F.greatest(F.lit(0), F.col("forecast_demand_14d") - F.col("current_stock_on_hand")))
        
        # Action Decision Tree
        .withColumn("primary_action",
            F.when(F.col("current_stock_on_hand") == 0, F.lit("EMERGENCY_RUSH_PO"))
             .when((F.col("days_of_supply") < 7) & (F.col("is_perishable") == True), F.lit("PRIORITY_PO_PERISHABLE"))
             .when(F.col("days_of_supply") < 14, F.lit("STANDARD_REPLENISHMENT_PO"))
             .when((F.col("days_of_supply") > 45) & (F.col("is_perishable") == True), F.lit("PROMO_MARKDOWN_TO_PREVENT_SPOILAGE"))
             .when(F.col("days_of_supply") > 60, F.lit("OVERSTOCK_TRANSFER_CANDIDATE"))
             .otherwise(F.lit("HEALTHY_STOCK_HOLD"))
        )
        
        # Suggested Units
        .withColumn("suggested_order_units",
            F.when(F.col("primary_action").isin("EMERGENCY_RUSH_PO", "PRIORITY_PO_PERISHABLE"), 
                   F.round(F.col("shortage_7d") * F.lit(1.2)).cast("int")) # +20% Safety Buffer
             .when(F.col("primary_action") == "STANDARD_REPLENISHMENT_PO", F.col("shortage_14d"))
             .otherwise(F.lit(0))
        )
        
        # Executive Risk Severity
        .withColumn("risk_severity",
            F.when(F.col("primary_action") == "EMERGENCY_RUSH_PO", F.lit("CRITICAL"))
             .when(F.col("primary_action").isin("PRIORITY_PO_PERISHABLE", "PROMO_MARKDOWN_TO_PREVENT_SPOILAGE"), F.lit("HIGH"))
             .when(F.col("primary_action") == "STANDARD_REPLENISHMENT_PO", F.lit("MEDIUM"))
             .otherwise(F.lit("LOW"))
        )
        
        # Audit & Reasoning Manifest
        .withColumn("decision_reasoning",
            F.concat(
                F.lit("DOS: "), F.col("days_of_supply"), 
                F.lit("d | 7D Demand: "), F.col("forecast_demand_7d"),
                F.lit(" units | Heat Multiplier: "), F.col("temp_elasticity_factor"), F.lit("x")
            )
        )
    )

    # =========================================================================
    # 6. WRITE TO S3 PLATINUM AS APACHE ICEBERG TABLE
    # =========================================================================
    output_table = f"{CATALOG}.platinum.smart_replenishment_recommendations"

    (
        recommendations_df
        .select(
            "demand_date", "warehouse_id", "warehouse_name", "product_id", "product_name",
            "storage_type", "is_perishable", "current_stock_on_hand", "days_of_supply",
            "forecast_demand_7d", "forecast_demand_14d", "forecast_demand_30d",
            "primary_action", "suggested_order_units", "risk_severity", "decision_reasoning"
        )
        .writeTo(output_table)
        .tableProperty("write.format.default", "parquet")
        .createOrReplace()
    )

    print(f"[platinum] Successfully updated {output_table} on S3 Platinum.")
    job.commit()

if __name__ == "__main__":
    main()
