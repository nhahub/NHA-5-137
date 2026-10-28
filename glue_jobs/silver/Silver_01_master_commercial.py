"""
silver_01_master_commercial  -  SILVER stage: dimensions + commercial reference tables

bronze -> silver (Iceberg, unpartitioned: these tables are small). Runs only when Bronze received new data.
The tables are small and several rules need the whole picture (duplicate SKUs, orphan parents, overlapping
prices/promotions/supplier terms, gaps), so they are recomputed from Bronze and replaced atomically.

ALL business rules live in silver_rules.py (pure DataFrame functions, tested locally against the generator's
defect_ground_truth.json). This job only wires reads and writes.
"""
from common_utils import QBin, has_new_data, init_job, publish_silver, tbl

import silver_rules as R

PAIRS = [("product_categories", "category"), ("products", "product"), ("warehouses", "warehouse"),
         ("suppliers", "supplier"), ("supplier_products", "supplier_product"), ("customers", "customer"),
         ("product_prices", "product_price"), ("product_costs", "product_cost"),
         ("promotions", "promotion"), ("calendar_events", "calendar_event")]


def main():
    args, glue_context, spark, job = init_job()
    if not any(has_new_data(spark, b, s) for b, s in PAIRS):
        print("[silver_01] no new bronze data")
        job.commit()
        return

    q = QBin()

    def bronze(t):
        return spark.table(tbl("bronze", t))

    def publish(df, name):
        publish_silver(spark, df, name, None, "replace", q)

    publish(R.category(bronze("product_categories")), "category")
    publish(R.product(bronze("products")), "product")
    publish(R.warehouse(bronze("warehouses")), "warehouse")
    publish(R.supplier(bronze("suppliers")), "supplier")
    publish(R.supplier_product(bronze("supplier_products")), "supplier_product")
    publish(R.customer(bronze("customers")), "customer")

    price = R.product_price(bronze("product_prices"))
    publish(price, "product_price")
    publish(R.product_cost(bronze("product_costs"), price), "product_cost")
    publish(R.promotion(bronze("promotions")), "promotion")
    publish(R.calendar_event(bronze("calendar_events")), "calendar_event")

    job.commit()


if __name__ == "__main__":
    main()
