"""
Gold_01_dimensions  -  GOLD stage: Star Schema Conformed Dimensions

Builds and atomically publishes the core conformed business dimensions to the Gold Iceberg catalog:
  - gold.dim_date       : Rich Egyptian retail calendar, working week (Sat-Thu), Friday weekends, Ramadan & Eid.
  - gold.dim_product    : Master product hierarchy (L1/L2), active status, packaging, latest list price & cost margin.
  - gold.dim_customer   : Customer master, standardized 27 governorates, retail/wholesale segments, assigned warehouse.
  - gold.dim_warehouse  : Central DC facilities (WH01 Cairo/Giza, WH02 Alexandria, WH03 Assiut), capacity & zone specs.
  - gold.dim_supplier   : Vendor dimension, domestic/import classification, payment terms, and geographic location.

All business transformations reside in gold_rules.py as pure DataFrame functions.
"""

from common_utils import init_job, publish_gold, tbl
import gold_rules as R


def main():
    args, glue_context, spark, job = init_job()

    print("[Gold_01_dimensions] Starting conformed dimensions build...")

    def S(table_name):
        return spark.table(tbl("silver", table_name))

    # 1. Date Dimension (enriched with Egyptian calendar events)
    print("  -> Building dim_date...")
    dim_d = R.dim_date(spark, min_date="2023-01-01", max_date="2027-12-31", calendar_df=S("calendar_event"))
    publish_gold(spark, dim_d, "dim_date", mode="replace")

    # 2. Product Dimension (enriched with categories, current list price and cost margins)
    print("  -> Building dim_product...")
    dim_p = R.dim_product(S("product"), S("category"), S("product_price"), S("product_cost"))
    publish_gold(spark, dim_p, "dim_product", mode="replace")

    # 3. Customer Dimension (enriched with standardized governorates and assigned warehouse)
    print("  -> Building dim_customer...")
    dim_c = R.dim_customer(S("customer"), S("warehouse"))
    publish_gold(spark, dim_c, "dim_customer", mode="replace")

    # 4. Warehouse Dimension
    print("  -> Building dim_warehouse...")
    dim_w = R.dim_warehouse(S("warehouse"))
    publish_gold(spark, dim_w, "dim_warehouse", mode="replace")

    # 5. Supplier Dimension
    print("  -> Building dim_supplier...")
    dim_s = R.dim_supplier(S("supplier"))
    publish_gold(spark, dim_s, "dim_supplier", mode="replace")

    print("[Gold_01_dimensions] All 5 conformed dimensions successfully published to Gold layer.")
    job.commit()


if __name__ == "__main__":
    main()

