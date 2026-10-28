"""
Bronze configuration: one entry per raw table (schema from the data catalog v2.0).

fmt       csv | json
snap      True  -> raw folder is snapshot_date=YYYY-MM-DD (kept as source_snapshot_date)
part      True  -> bronze table partitioned by day of ingestion (hidden partition, used by Silver incremental reads)
schema    typed schema (what Bronze stores)
required  columns that must be NOT NULL  (a failed cast also becomes NULL, so bad values are caught here)
rules     extra Glue Data Quality (DQDL) rules - single column checks
checks    cross-column checks: Spark SQL boolean expression, TRUE = OK.  Stored as chk_* (1/0) and
          evaluated by Glue DQ as  ColumnValues "chk_*" >= 1

Rows that fail ANY rule go to quarantine.rejected_rows (original text in row_json); the rest go to bronze.<table>.
Business "defects" that can be repaired (duplicates, casing, orphan parents, sign errors ...) are NOT rejected
here - Silver fixes and flags them.
"""
import re


def parse_ddl(ddl):
    """'a string, b decimal(8,3)'  ->  [('a','string'), ('b','decimal(8,3)')]"""
    return re.findall(r"(\w+)\s+(decimal\(\d+\s*,\s*\d+\)|\w+)", ddl)


def build_ruleset(cfg):
    rules = [f'IsComplete "{c}"' for c in cfg.get("required", [])]
    rules += cfg.get("rules", [])
    rules += [f'ColumnValues "{c}" >= 1' for c in cfg.get("checks", {})]
    return "Rules = [\n  " + ",\n  ".join(rules) + "\n]"


TABLES = {
    # ------------------------------------------------------------ reference / master / commercial
    "product_categories": dict(
        fmt="csv", snap=True, part=False,
        schema="category_id string, parent_category_id string, category_name string, category_level int, "
               "default_storage_type string, is_perishable_default boolean, updated_ts timestamp",
        required=["category_id", "category_name", "category_level"],
        rules=['ColumnValues "category_level" in [1,2]'],
        checks={"chk_category_id_not_blank": "trim(category_id) <> ''"},
    ),
    "products": dict(
        fmt="csv", snap=True, part=False,
        schema="product_id string, sku_code string, product_name string, brand string, category_id string, "
               "base_uom string, units_per_case int, case_weight_kg decimal(8,3), storage_type string, "
               "is_perishable boolean, shelf_life_days int, status string, launch_date date, "
               "discontinued_date date, barcode_ean13 string, updated_ts timestamp",
        required=["product_id", "sku_code", "category_id", "units_per_case", "is_perishable", "updated_ts"],
        rules=['ColumnValues "units_per_case" > 0'],
    ),
    "warehouses": dict(
        fmt="csv", snap=True, part=False,
        schema="warehouse_id string, warehouse_name string, governorate string, city string, timezone string, "
               "capacity_pallet_positions int, has_chilled_zone boolean, operating_days string, "
               "order_cutoff_local string, opened_date date, status string, updated_ts timestamp",
        required=["warehouse_id", "capacity_pallet_positions"],
        rules=['ColumnValues "capacity_pallet_positions" > 0'],
    ),
    "suppliers": dict(
        fmt="csv", snap=True, part=False,
        schema="supplier_id string, supplier_name string, supplier_type string, governorate string, "
               "payment_terms_days int, is_imported boolean, status string, onboarded_date date, "
               "updated_ts timestamp",
        required=["supplier_id", "supplier_name", "payment_terms_days"],
        rules=['ColumnValues "payment_terms_days" >= 0'],
    ),
    "supplier_products": dict(
        fmt="csv", snap=True, part=False,
        schema="supplier_id string, product_id string, supplier_sku string, units_per_case int, moq_cases int, "
               "order_multiple_cases int, contractual_lead_time_days int, cost_per_case_egp decimal(10,2), "
               "is_primary boolean, effective_from date, effective_to date, updated_ts timestamp",
        required=["supplier_id", "product_id", "units_per_case", "moq_cases", "order_multiple_cases",
                  "cost_per_case_egp", "effective_from"],
        rules=['ColumnValues "units_per_case" > 0', 'ColumnValues "moq_cases" > 0',
               'ColumnValues "order_multiple_cases" > 0'],
    ),
    "customers": dict(
        fmt="csv", snap=True, part=False,
        schema="customer_id string, customer_name string, segment string, governorate string, city string, "
               "assigned_warehouse_id string, credit_terms_days int, status string, created_date date, "
               "updated_ts timestamp",
        required=["customer_id", "customer_name", "segment", "credit_terms_days"],
        rules=['ColumnValues "credit_terms_days" >= 0'],
    ),
    "product_prices": dict(
        fmt="csv", snap=True, part=False,
        schema="product_id string, price_list string, list_price_egp decimal(10,2), effective_from date, "
               "effective_to date, updated_ts timestamp",
        required=["product_id", "price_list", "list_price_egp", "effective_from"],
        rules=['ColumnValues "list_price_egp" > 0'],
        checks={"chk_price_dates_ordered": "effective_to IS NULL OR effective_to >= effective_from"},
    ),
    "product_costs": dict(
        fmt="csv", snap=True, part=False,
        schema="product_id string, cost_type string, cost_per_base_unit_egp decimal(10,4), "
               "effective_from date, effective_to date, updated_ts timestamp",
        required=["product_id", "cost_type", "cost_per_base_unit_egp", "effective_from"],
        rules=['ColumnValues "cost_per_base_unit_egp" > 0'],
    ),
    "promotions": dict(
        fmt="csv", snap=True, part=False,
        schema="promo_id string, product_id string, warehouse_id string, promo_name string, promo_type string, "
               "discount_pct decimal(5,2), start_date date, end_date date, funded_by string, "
               "supplier_id string, supplier_funding_pct decimal(5,2), updated_ts timestamp",
        required=["promo_id", "product_id", "discount_pct", "start_date", "end_date"],
        rules=['ColumnValues "discount_pct" >= 0', 'ColumnValues "discount_pct" <= 100'],
        checks={"chk_promo_dates_ordered": "end_date >= start_date"},
    ),
    "calendar_events": dict(
        fmt="csv", snap=True, part=False,
        schema="event_id string, event_name string, event_type string, start_date date, end_date date, "
               "closes_warehouse boolean, expected_demand_effect string, source_note string, "
               "updated_ts timestamp",
        required=["event_id", "event_name", "start_date", "end_date"],
    ),
    # ------------------------------------------------------------ sales / procurement transactions
    "sales_orders": dict(
        fmt="csv", snap=False, part=True,
        schema="sales_order_id string, customer_id string, warehouse_id string, order_ts timestamp, "
               "requested_delivery_date date, channel string, order_status string, cancelled_ts timestamp, "
               "cancel_reason string, currency_code string, total_net_amount_egp decimal(12,2), "
               "source_updated_ts timestamp",
        required=["sales_order_id", "customer_id", "warehouse_id", "order_ts", "order_status",
                  "total_net_amount_egp", "source_updated_ts"],
        rules=['ColumnValues "total_net_amount_egp" >= 0'],
    ),
    "sales_order_lines": dict(
        fmt="csv", snap=False, part=True,
        schema="sales_order_id string, line_no int, product_id string, warehouse_id string, qty_ordered int, "
               "qty_allocated int, qty_shipped int, uom string, list_price_egp decimal(10,2), "
               "discount_pct decimal(5,2), unit_net_price_egp decimal(10,2), promo_id string, "
               "line_status string, shortage_action string, shipped_ts timestamp, source_updated_ts timestamp",
        required=["sales_order_id", "line_no", "product_id", "qty_ordered", "qty_shipped", "list_price_egp",
                  "discount_pct", "unit_net_price_egp", "source_updated_ts"],
        rules=['ColumnValues "qty_ordered" >= 0', 'ColumnValues "qty_shipped" >= 0',
               'ColumnValues "discount_pct" >= 0', 'ColumnValues "discount_pct" <= 100'],
    ),
    "sales_returns": dict(
        fmt="csv", snap=False, part=True,
        schema="return_line_id string, sales_order_id string, line_no int, product_id string, "
               "warehouse_id string, return_ts timestamp, qty_returned int, return_reason string, "
               "disposition string, refund_amount_egp decimal(10,2), updated_ts timestamp",
        required=["return_line_id", "sales_order_id", "line_no", "product_id", "return_ts", "qty_returned"],
        rules=['ColumnValues "qty_returned" >= 0'],
    ),
    "purchase_orders": dict(
        fmt="csv", snap=False, part=True,
        schema="purchase_order_id string, supplier_id string, warehouse_id string, po_created_ts timestamp, "
               "requested_delivery_date date, po_status string, payment_terms_days int, "
               "total_amount_egp decimal(12,2), created_by string, source_updated_ts timestamp",
        required=["purchase_order_id", "supplier_id", "warehouse_id", "po_created_ts", "po_status",
                  "source_updated_ts"],
    ),
    "purchase_order_lines": dict(
        fmt="csv", snap=False, part=True,
        schema="purchase_order_id string, po_line_no int, product_id string, order_uom string, "
               "qty_ordered_uom int, units_per_case int, unit_cost_per_uom_egp decimal(10,2), "
               "promised_delivery_date date, line_status string, source_updated_ts timestamp",
        required=["purchase_order_id", "po_line_no", "product_id", "qty_ordered_uom", "units_per_case",
                  "unit_cost_per_uom_egp", "source_updated_ts"],
        rules=['ColumnValues "qty_ordered_uom" > 0', 'ColumnValues "units_per_case" > 0'],
    ),
    "goods_receipt_lines": dict(
        fmt="csv", snap=False, part=True,
        schema="receipt_id string, receipt_line_no int, purchase_order_id string, po_line_no int, "
               "product_id string, warehouse_id string, received_ts timestamp, supplier_delivery_note string, "
               "qty_received_uom int, qty_rejected_uom int, order_uom string, batch_number string, "
               "expiry_date date, updated_ts timestamp",
        required=["receipt_id", "receipt_line_no", "product_id", "warehouse_id", "received_ts",
                  "qty_received_uom", "qty_rejected_uom"],
        rules=['ColumnValues "qty_received_uom" >= 0', 'ColumnValues "qty_rejected_uom" >= 0'],
        checks={"chk_expiry_not_before_receipt":
                "expiry_date IS NULL OR expiry_date >= to_date(from_utc_timestamp(received_ts, 'Africa/Cairo'))"},
    ),
    "product_batches": dict(
        fmt="csv", snap=False, part=True,
        schema="batch_id string, warehouse_id string, product_id string, receipt_id string, "
               "receipt_line_no int, batch_number string, production_date date, expiry_date date, "
               "qty_received_base int, status string, updated_ts timestamp",
        required=["batch_id", "product_id", "production_date", "expiry_date"],
        checks={"chk_expiry_after_production": "expiry_date >= production_date"},
    ),
    # ------------------------------------------------------------ inventory / operations
    "inventory_transactions": dict(
        fmt="json", snap=False, part=True,
        schema="txn_id string, event_ts timestamp, posted_ts timestamp, warehouse_id string, product_id string, "
               "txn_type string, qty_change_base int, reference_type string, reference_id string, "
               "batch_id string, reason_code string, event_seq bigint, source_system string",
        required=["txn_id", "event_ts", "posted_ts", "warehouse_id", "product_id", "txn_type",
                  "qty_change_base"],
    ),
    "inventory_snapshots": dict(
        fmt="csv", snap=False, part=True,
        schema="snapshot_date date, warehouse_id string, product_id string, on_hand_qty int, "
               "allocated_qty int, blocked_qty int, snapshot_ts timestamp, source_batch_id string",
        required=["snapshot_date", "warehouse_id", "product_id", "on_hand_qty", "allocated_qty"],
    ),
    "stockout_events": dict(
        fmt="csv", snap=False, part=True,
        schema="stockout_id string, warehouse_id string, product_id string, start_ts timestamp, "
               "end_ts timestamp, detection_source string, trigger_ref string, updated_ts timestamp",
        required=["stockout_id", "warehouse_id", "product_id", "start_ts"],
    ),
    "weather_daily": dict(
        fmt="json", multiline=True, snap=False, part=False,
        schema="obs_date date, location_code string, temp_max_c double, temp_min_c double, "
               "precip_mm double, source string",
        required=["obs_date", "location_code", "temp_max_c", "temp_min_c"],
    ),
}

BRONZE_JOBS = {
    "bronze_01_reference": ["product_categories", "products", "warehouses", "suppliers", "supplier_products",
                            "customers", "product_prices", "product_costs", "promotions", "calendar_events"],
    "bronze_02_transactions": ["sales_orders", "sales_order_lines", "sales_returns", "purchase_orders",
                               "purchase_order_lines", "goods_receipt_lines", "product_batches"],
    "bronze_03_inventory_ops": ["inventory_transactions", "inventory_snapshots", "stockout_events",
                                "weather_daily"],
}
