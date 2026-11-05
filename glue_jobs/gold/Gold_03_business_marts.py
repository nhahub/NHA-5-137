"""
Gold_03_business_marts  -  GOLD stage: Executive & Operational Data Marts

Builds pre-aggregated, highly optimized business data marts for direct BI, dashboarding,
and executive KPI consumption (Amazon QuickSight, Power BI, Tableau, Athena):
  - gold.mart_sales_and_margin_daily    : Daily commercial sales, net revenue, COGS, margin % & fulfillment.
  - gold.mart_supplier_performance     : Supplier scorecards, On-Time In-Full (OTIF) %, quality rejections, spend.
  - gold.mart_inventory_health_and_risk : Stock velocity, Days of Supply (DOH), stockout duration, FEFO expiry risk.
  - gold.mart_warehouse_operations_daily: Warehouse DC throughput (inbound/outbound), pallet utilization & weather impact.

All calculations reside in gold_rules.py as pure DataFrame transformations.
"""

from common_utils import init_job, publish_gold, table_exists, tbl
import gold_rules as R


def main():
    args, glue_context, spark, job = init_job()

    print("[Gold_03_business_marts] Starting business data marts generation...")

    # Ensure prerequisite fact and dimension tables exist
    required_tables = ["fact_sales", "fact_procurement_receipts", "fact_inventory_daily_balance", "dim_product", "dim_warehouse"]
    missing = [t for t in required_tables if not table_exists(spark, tbl("gold", t))]
    if missing:
        raise RuntimeError(
            f"[Gold_03_business_marts] Prerequisite tables {missing} do not exist in glue_catalog.gold! "
            "Please ensure 'Gold_01_dimensions' and 'Gold_02_facts' run and succeed before running 'Gold_03_business_marts'."
        )

    def S(table_name):

        return spark.table(tbl("silver", table_name))

    def G(table_name):
        return spark.table(tbl("gold", table_name))

    # 1. Commercial Sales & Margin Daily Mart
    print("  -> Generating mart_sales_and_margin_daily...")
    m_sales = R.mart_sales_and_margin_daily(
        fact_sales_df=G("fact_sales"),
        product_dim_df=G("dim_product"),
        warehouse_dim_df=G("dim_warehouse"),
        customer_dim_df=G("dim_customer"),
    )
    publish_gold(spark, m_sales, "mart_sales_and_margin_daily", mode="replace")

    # 2. Supplier Scorecard & OTIF Procurement Mart
    print("  -> Generating mart_supplier_performance...")
    m_sup = R.mart_supplier_performance(
        fact_receipts_df=G("fact_procurement_receipts"),
        suppliers_dim_df=G("dim_supplier"),
    )
    publish_gold(spark, m_sup, "mart_supplier_performance", mode="replace")

    # 3. Inventory Health, Stockout Velocity & FEFO Expiry Mart
    print("  -> Generating mart_inventory_health_and_risk...")
    m_inv = R.mart_inventory_health_and_risk(
        fact_inventory_df=G("fact_inventory_daily_balance"),
        fact_sales_df=G("fact_sales"),
        stockouts_df=S("stockout_event"),
        batches_df=S("product_batch"),
        product_dim_df=G("dim_product"),
        warehouse_dim_df=G("dim_warehouse"),
    )
    publish_gold(spark, m_inv, "mart_inventory_health_and_risk", mode="replace")

    # 4. Warehouse DC Operations & Environmental Weather Mart
    print("  -> Generating mart_warehouse_operations_daily...")
    m_wh = R.mart_warehouse_operations_daily(
        fact_sales_df=G("fact_sales"),
        fact_receipts_df=G("fact_procurement_receipts"),
        fact_returns_df=G("fact_sales_returns"),
        snapshots_df=S("inventory_snapshot"),
        warehouse_dim_df=G("dim_warehouse"),
        weather_df=S("weather_daily"),
    )
    publish_gold(spark, m_wh, "mart_warehouse_operations_daily", mode="replace")

    print("[Gold_03_business_marts] All 4 business data marts successfully published to Gold layer.")
    job.commit()


if __name__ == "__main__":
    main()

