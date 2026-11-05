"""
gold_rules  -  PURE analytical transformations for the Gold Layer (Lakehouse / Star Schema / Marts).

Design:
  - Dimensional Modeling (Kimball Star Schema):
      Dimensions: dim_date, dim_product, dim_customer, dim_warehouse, dim_supplier
      Facts     : fact_sales, fact_sales_returns, fact_procurement_receipts, fact_inventory_daily_balance
      Marts     : mart_sales_and_margin_daily, mart_supplier_performance,
                  mart_inventory_health_and_risk, mart_warehouse_operations_daily
  - Pure DataFrame functions (DataFrame -> DataFrame)
  - Zero hardcoded catalog / I/O here: Gold Glue jobs handle reading Silver & writing Iceberg tables.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def surrogate_key(*cols):
    """Always-positive 63-bit surrogate key from string / numeric attributes."""
    return F.pmod(F.xxhash64(*cols), F.lit(9223372036854775807))


# ==============================================================================
# 1. STAR SCHEMA CONFORMED DIMENSIONS
# ==============================================================================

def dim_date(spark, min_date="2023-01-01", max_date="2026-12-31", calendar_df=None):
    """
    Rich date dimension tailored for Egyptian FMCG retail & wholesale operations:
      - Egyptian working week: Saturday through Thursday (SAT-THU).
      - Official weekend: Friday (FRI).
      - Enriched with official Egyptian holidays & religious seasons (Ramadan, Eid al-Fitr, Eid al-Adha).
    """
    days_df = spark.sql(
        f"SELECT explode(sequence(TO_DATE('{min_date}'), TO_DATE('{max_date}'), INTERVAL 1 DAY)) AS full_date"
    )

    base = (
        days_df
        .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("full_date"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("quarter_name", F.concat(F.lit("Q"), F.quarter("full_date")))
        .withColumn("month", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("month_year", F.date_format("full_date", "yyyy-MM"))
        .withColumn("week_of_year", F.weekofyear("full_date"))
        .withColumn("day_of_month", F.dayofmonth("full_date"))
        .withColumn("day_of_week", F.dayofweek("full_date"))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
        # Spark dayofweek: 1=Sun, 2=Mon, 3=Tue, 4=Wed, 5=Thu, 6=Fri, 7=Sat
        .withColumn("is_weekend", F.col("day_of_week") == 6)
        .withColumn("is_egyptian_working_day", F.col("day_of_week") != 6)
    )

    if calendar_df is not None:
        cal = calendar_df.filter(F.col("event_name_std").isNotNull())
        joined = base.join(
            cal,
            (base.full_date >= cal.start_date) & (base.full_date <= cal.end_date),
            "left"
        )
        # In case of overlapping events on the same calendar day, prioritize warehouse closure / earliest start
        w_cal = Window.partitionBy("full_date").orderBy(
            F.coalesce(cal.closes_warehouse, F.lit(False)).cast("int").desc(),
            cal.start_date.asc()
        )
        dedup = joined.withColumn("_rn", F.row_number().over(w_cal)).filter(F.col("_rn") == 1).drop("_rn")
        return (
            dedup
            .withColumn("is_holiday", F.col("event_id").isNotNull())
            .withColumn("holiday_name", F.col("event_name_std"))
            .withColumn("expected_demand_effect", F.coalesce(F.col("expected_demand_effect"), F.lit("NORMAL")))
            .withColumn("closes_warehouse", F.coalesce(F.col("closes_warehouse"), F.lit(False)))
            .select(
                "date_key", "full_date", "year", "quarter", "quarter_name", "month", "month_name",
                "month_year", "week_of_year", "day_of_month", "day_of_week", "day_name",
                "is_weekend", "is_egyptian_working_day", "is_holiday", "holiday_name",
                "expected_demand_effect", "closes_warehouse"
            )
        )

    return (
        base
        .withColumn("is_holiday", F.lit(False))
        .withColumn("holiday_name", F.lit(None).cast("string"))
        .withColumn("expected_demand_effect", F.lit("NORMAL"))
        .withColumn("closes_warehouse", F.lit(False))
        .select(
            "date_key", "full_date", "year", "quarter", "quarter_name", "month", "month_name",
            "month_year", "week_of_year", "day_of_month", "day_of_week", "day_name",
            "is_weekend", "is_egyptian_working_day", "is_holiday", "holiday_name",
            "expected_demand_effect", "closes_warehouse"
        )
    )


def dim_product(product_df, category_df, price_df, cost_df):
    """
    Conformed product dimension with category taxonomy hierarchy (L1/L2),
    active status, storage specs, and latest commercial price & cost benchmark.
    """
    # 1. Resolve L1 Category Name
    l1_cats = (
        category_df
        .filter(F.col("category_level") == 1)
        .select(
            F.col("category_id").alias("_l1_id"),
            F.col("category_name").alias("l1_category_name")
        )
    )
    cat_enriched = (
        category_df
        .join(l1_cats, category_df.l1_category_id == l1_cats._l1_id, "left")
        .select("category_id", "category_name", "category_level", "l1_category_id", "l1_category_name")
    )

    # 2. Latest active list price per product
    w_pr = Window.partitionBy("product_id").orderBy(
        F.col("effective_to").isNull().desc(),
        F.col("effective_from").desc(),
        F.col("list_price_egp").desc()
    )
    cur_price = (
        price_df
        .withColumn("_rn", F.row_number().over(w_pr))
        .filter(F.col("_rn") == 1)
        .select("product_id", F.col("list_price_egp").alias("current_list_price_egp"))
    )

    # 3. Latest active cost per base unit
    w_cst = Window.partitionBy("product_id").orderBy(
        F.col("effective_to").isNull().desc(),
        F.col("effective_from").desc()
    )
    cur_cost = (
        cost_df
        .withColumn("_rn", F.row_number().over(w_cst))
        .filter(F.col("_rn") == 1)
        .select("product_id", F.col("cost_per_base_unit_egp").alias("current_cost_per_base_unit_egp"))
    )

    # 4. Join and assemble product dimension
    p = (
        product_df
        .join(cat_enriched, "category_id", "left")
        .join(cur_price, "product_id", "left")
        .join(cur_cost, "product_id", "left")
    )

    margin_egp = F.round(F.col("current_list_price_egp") - F.col("current_cost_per_base_unit_egp"), 2)
    margin_pct = F.when(
        F.col("current_list_price_egp") > 0,
        F.round((margin_egp / F.col("current_list_price_egp")) * 100, 2)
    )

    return (
        p
        .withColumn("product_key", surrogate_key("product_id"))
        .withColumn("sku_code", F.coalesce("sku_code_effective", "sku_code"))
        .withColumn("current_margin_egp", margin_egp)
        .withColumn("current_margin_pct", margin_pct)
        .select(
            "product_key", "product_id", "sku_code", "product_name", "brand",
            "category_id", "category_name", "category_level", "l1_category_id", "l1_category_name",
            "base_uom", "units_per_case", "case_weight_kg", "storage_type",
            "is_perishable", "shelf_life_days", "status", "barcode_ean13", "barcode_valid_flag",
            "is_active", "current_list_price_egp", "current_cost_per_base_unit_egp",
            "current_margin_egp", "current_margin_pct"
        )
    )


def dim_customer(customer_df, warehouse_df):
    """
    Conformed customer dimension with standardized Egyptian governorates,
    segments, assigned warehouse info, and account metrics.
    """
    wh_ref = warehouse_df.select(
        F.col("warehouse_id").alias("assigned_warehouse_id"),
        F.col("warehouse_name").alias("assigned_warehouse_name")
    )

    return (
        customer_df
        .join(wh_ref, "assigned_warehouse_id", "left")
        .withColumn("customer_key", surrogate_key("customer_id"))
        .withColumn("segment", F.col("segment_std"))
        .withColumn("governorate", F.col("governorate_std"))
        .withColumn("account_age_days", F.datediff(F.current_date(), F.col("created_date")))
        .select(
            "customer_key", "customer_id", "customer_name", "segment",
            "governorate", "city", "assigned_warehouse_id", "assigned_warehouse_name",
            "credit_terms_days", "status", "created_date", "account_age_days"
        )
    )


def dim_warehouse(warehouse_df):
    """Conformed warehouse distribution center dimension."""
    return (
        warehouse_df
        .withColumn("warehouse_key", surrogate_key("warehouse_id"))
        .select(
            "warehouse_key", "warehouse_id", "warehouse_name", "governorate",
            "city", "timezone", "capacity_pallet_positions", "has_chilled_zone",
            "operating_days", "order_cutoff_local", "opened_date", "status"
        )
    )


def dim_supplier(supplier_df):
    """Conformed supplier vendor dimension."""
    return (
        supplier_df
        .withColumn("supplier_key", surrogate_key("supplier_id"))
        .withColumn("governorate", F.col("governorate_std"))
        .select(
            "supplier_key", "supplier_id", "supplier_name", "supplier_type",
            "governorate", "payment_terms_days", "is_imported", "status", "onboarded_date"
        )
    )


# ==============================================================================
# 2. GRANULAR ANALYTICAL FACT TABLES
# ==============================================================================

def fact_sales(order_lines_df, orders_df, product_dim_df, customer_dim_df, cost_df):
    """
    Granular sales fact table (Order Line Grain).
    Enriched with product & customer keys, point-in-time cost, discount amount,
    net sales revenue, COGS, gross margin, and line fulfillment metrics.
    """
    # 1. Prepare orders header
    hdr = orders_df.select(
        "sales_order_id", "customer_id", "order_ts", "channel",
        F.col("order_status").alias("order_status_hdr"), "is_cancelled"
    )

    # 2. Join lines with headers
    f = order_lines_df.join(hdr, "sales_order_id", "inner")

    # 3. Join dimensions
    p_keys = product_dim_df.select(
        "product_id", "product_key", "category_id", "l1_category_id",
        F.col("current_cost_per_base_unit_egp").alias("_fallback_cost")
    )
    c_keys = customer_dim_df.select("customer_id", "customer_key")

    f = f.join(p_keys, "product_id", "left").join(c_keys, "customer_id", "left")

    # 4. Point-in-time cost lookup (fallback to current product cost)
    cst = (
        cost_df
        .select(
            F.col("product_id").alias("_c_prod"),
            F.col("effective_from").alias("_c_from"),
            F.col("effective_to").alias("_c_to"),
            F.col("cost_per_base_unit_egp").alias("_unit_cost")
        )
    )
    f = f.join(
        cst,
        (F.col("product_id") == cst._c_prod) &
        (F.col("order_date") >= cst._c_from) &
        (cst._c_to.isNull() | (F.col("order_date") <= cst._c_to)),
        "left"
    )
    w_cost = Window.partitionBy("sales_order_id", "line_no").orderBy(F.col("_c_from").desc_nulls_last())
    f = (
        f.withColumn("_rn_cst", F.row_number().over(w_cost))
        .filter(F.col("_rn_cst") == 1)
        .drop("_rn_cst", "_c_prod", "_c_from", "_c_to")
    )

    unit_cost = F.coalesce(F.col("_unit_cost"), F.col("_fallback_cost"), F.lit(0.0)).cast("decimal(10,4)")
    gross_rev = F.round(F.col("qty_shipped") * F.col("list_price_egp"), 2)
    net_rev = F.round(F.col("qty_shipped") * F.col("unit_net_price_egp"), 2)
    discount_amt = F.round(gross_rev - net_rev, 2)
    cogs = F.round(F.col("qty_shipped") * unit_cost, 2)
    gross_profit = F.round(net_rev - cogs, 2)
    margin_pct = F.when(net_rev > 0, F.round((gross_profit / net_rev) * 100, 2))

    shortage = F.greatest(F.lit(0), F.col("qty_ordered") - F.col("qty_shipped"))
    is_short = (F.col("qty_shipped") < F.col("qty_ordered")) & ~F.col("is_cancelled")
    is_promo = F.col("promo_id").isNotNull() & (F.col("promo_id") != "")

    return (
        f
        .withColumn("sales_order_line_key", surrogate_key("sales_order_id", "line_no"))
        .withColumn("order_date_key", F.date_format("order_date", "yyyyMMdd").cast("int"))
        .withColumn("warehouse_key", surrogate_key("warehouse_id"))
        .withColumn("unit_cost_egp", unit_cost)
        .withColumn("gross_revenue_egp", gross_rev)
        .withColumn("discount_amount_egp", discount_amt)
        .withColumn("net_revenue_egp", net_rev)
        .withColumn("cogs_egp", cogs)
        .withColumn("gross_profit_egp", gross_profit)
        .withColumn("gross_margin_pct", margin_pct)
        .withColumn("shortage_qty", shortage)
        .withColumn("is_fulfilled_in_full", F.col("qty_shipped") >= F.col("qty_ordered"))
        .withColumn("is_short_shipped", is_short)
        .withColumn(
            "line_fill_rate_pct",
            F.when(F.col("qty_ordered") > 0, F.round((F.col("qty_shipped") / F.col("qty_ordered")) * 100, 2))
        )
        .withColumn("is_promo_line", is_promo)
        .select(
            "sales_order_line_key", "sales_order_id", "line_no", "order_date_key", "order_date",
            "order_ts", "customer_key", "customer_id", "product_key", "product_id",
            "category_id", "l1_category_id", "warehouse_key", "warehouse_id", "channel",
            "order_status_hdr", "line_status", "qty_ordered", "qty_allocated", "qty_shipped",
            "shortage_qty", "is_fulfilled_in_full", "is_short_shipped", "line_fill_rate_pct",
            "uom", "list_price_egp", "discount_pct", "discount_source", "unit_net_price_egp",
            "unit_cost_egp", "gross_revenue_egp", "discount_amount_egp", "net_revenue_egp",
            "cogs_egp", "gross_profit_egp", "gross_margin_pct", "promo_id", "is_promo_line",
            "shipped_ts", "is_cancelled"
        )
    )


def fact_sales_returns(returns_df, fact_sales_df):
    """
    Granular sales returns fact table.
    Links returns to original sales lines to evaluate product defects,
    customer returns, refund values, and financial impact.
    """
    sales_ref = fact_sales_df.select(
        "sales_order_id", "line_no",
        F.col("order_date").alias("sales_order_date"),
        "customer_key", "warehouse_key", "product_key",
        "unit_net_price_egp", "unit_cost_egp"
    )

    r = returns_df.join(sales_ref, ["sales_order_id", "line_no"], "left")

    qty_ret = F.col("qty_returned_capped")
    cost_loss = F.when(
        F.col("disposition") != "RESTOCK",
        F.round(qty_ret * F.coalesce(F.col("unit_cost_egp"), F.lit(0.0)), 2)
    ).otherwise(F.lit(0.0).cast("decimal(12,2)"))

    return (
        r
        .withColumn("return_line_key", surrogate_key("return_line_id"))
        .withColumn("return_date_key", F.date_format("return_date", "yyyyMMdd").cast("int"))
        .withColumn("warehouse_key", F.coalesce(F.col("warehouse_key"), surrogate_key("warehouse_id")))
        .withColumn("product_key", F.coalesce(F.col("product_key"), surrogate_key("product_id")))
        .withColumn("qty_returned", qty_ret)
        .withColumn("cost_loss_egp", cost_loss)
        .withColumn("days_between_order_and_return", F.datediff("return_date", "sales_order_date"))
        .select(
            "return_line_key", "return_line_id", "return_date_key", "return_date", "return_ts",
            "sales_order_id", "line_no", "sales_order_date", "customer_key", "product_key",
            "product_id", "warehouse_key", "warehouse_id", "qty_returned", "refund_amount_egp",
            "cost_loss_egp", "return_reason", "disposition", "restockable_flag",
            "days_between_order_and_return"
        )
    )


def fact_procurement_receipts(receipts_df, po_lines_df, po_df, suppliers_df):
    """
    Granular dock receipts and PO line delivery fact table.
    Computes vendor On-Time In-Full (OTIF), dock quality rejection rate,
    and lead time variances.
    """
    # Exclude order_uom from pol select because it is already present in receipts_df
    pol = po_lines_df.select(
        "purchase_order_id", "po_line_no", "qty_ordered_base",
        "unit_cost_per_base_egp", "line_total_cost_egp", "po_date",
        "promised_date_effective", "promise_imputed_flag"
    )
    p_hdr = po_df.select("purchase_order_id", "supplier_id")
    sup = suppliers_df.select("supplier_id", "supplier_key", "payment_terms_days")

    gr = (
        receipts_df
        .join(pol, ["purchase_order_id", "po_line_no"], "left")
        .join(p_hdr, "purchase_order_id", "left")
        .join(sup, "supplier_id", "left")
    )

    actual_lt = F.datediff("received_date", "po_date")
    is_on_time = F.col("received_date") <= F.col("promised_date_effective")
    is_in_full = F.col("qty_received_base") >= F.col("qty_ordered_base")
    is_otif = is_on_time & is_in_full

    rej_rate = F.when(
        F.col("qty_received_base") > 0,
        F.round((F.col("qty_rejected_base") / F.col("qty_received_base")) * 100, 2)
    ).otherwise(F.lit(0.0))

    cost_base = F.coalesce(F.col("unit_cost_per_base_egp"), F.lit(0.0))
    tot_rec_cost = F.round(F.col("qty_received_base") * cost_base, 2)
    tot_acc_cost = F.round(F.col("qty_accepted_base") * cost_base, 2)

    return (
        gr
        .withColumn("receipt_line_key", surrogate_key("receipt_id", "receipt_line_no"))
        .withColumn("received_date_key", F.date_format("received_date", "yyyyMMdd").cast("int"))
        .withColumn("po_date_key", F.date_format("po_date", "yyyyMMdd").cast("int"))
        .withColumn("product_key", surrogate_key("product_id"))
        .withColumn("warehouse_key", surrogate_key("warehouse_id"))
        .withColumn("actual_lead_time_days", actual_lt)
        .withColumn("is_on_time", F.coalesce(is_on_time, F.lit(False)))
        .withColumn("is_in_full", F.coalesce(is_in_full, F.lit(False)))
        .withColumn("is_otif", F.coalesce(is_otif, F.lit(False)))
        .withColumn("rejection_rate_pct", rej_rate)
        .withColumn("total_received_cost_egp", tot_rec_cost)
        .withColumn("total_accepted_cost_egp", tot_acc_cost)
        .select(
            "receipt_line_key", "receipt_id", "receipt_line_no", "received_date_key",
            "received_date", "received_ts", "purchase_order_id", "po_line_no", "po_date_key",
            "po_date", "supplier_key", "supplier_id", "product_key", "product_id",
            "warehouse_key", "warehouse_id", "order_uom", "qty_ordered_base", "qty_received_base",
            "qty_rejected_base", "qty_accepted_base", "unit_cost_per_base_egp",
            "total_received_cost_egp", "total_accepted_cost_egp", "actual_lead_time_days",
            "promised_date_effective", "is_on_time", "is_in_full", "is_otif",
            "rejection_rate_pct", "batch_number", "expiry_date"
        )
    )


def fact_inventory_daily_balance(snapshots_df, product_dim_df, stockouts_df=None):
    """
    Daily warehouse product stock balance fact table.
    Enriched with product cost to track inventory asset valuation (EGP),
    allocated vs available balances, and stockout conditions.
    """
    prod_cost = product_dim_df.select(
        "product_id", "product_key",
        F.col("current_cost_per_base_unit_egp").alias("unit_cost_egp")
    )

    inv = snapshots_df.join(prod_cost, "product_id", "left")

    unit_cost = F.coalesce(F.col("unit_cost_egp"), F.lit(0.0))
    tot_val = F.round(F.col("on_hand_qty") * unit_cost, 2)
    is_oos = F.col("available_qty") <= 0

    return (
        inv
        .withColumn("snapshot_date_key", F.date_format("snapshot_date", "yyyyMMdd").cast("int"))
        .withColumn("warehouse_key", surrogate_key("warehouse_id"))
        .withColumn("product_key", F.coalesce("product_key", surrogate_key("product_id")))
        .withColumn("total_inventory_value_egp", tot_val)
        .withColumn("is_out_of_stock", is_oos)
        .select(
            "snapshot_date_key", "snapshot_date", "warehouse_key", "warehouse_id",
            "product_key", "product_id", "on_hand_qty", "allocated_qty", "available_qty",
            "blocked_qty", "unit_cost_egp", "total_inventory_value_egp", "is_out_of_stock",
            "negative_on_hand_flag", "ledger_mismatch_flag"
        )
    )


# ==============================================================================
# 3. EXECUTIVE & ANALYTICAL DATA MARTS
# ==============================================================================

def mart_sales_and_margin_daily(fact_sales_df, product_dim_df, warehouse_dim_df, customer_dim_df):
    """
    Daily aggregated sales & commercial profitability mart:
      - Slices by Date, Warehouse, Category Hierarchy, Customer Segment, and Order Channel.
      - Core KPIs: Total Orders, Units Ordered vs Shipped, Shortage, Gross Sales, Discounts,
        Net Sales, COGS, Gross Profit, Gross Margin %, Line Fill Rate %, and Promotional Lift.
    """
    p_info = product_dim_df.select("product_id", "category_id", "category_name", "l1_category_name")
    w_info = warehouse_dim_df.select("warehouse_id", "warehouse_name", "governorate")
    c_info = customer_dim_df.select("customer_id", "segment")

    s = (
        fact_sales_df
        .join(p_info, ["product_id", "category_id"], "left")
        .join(w_info, "warehouse_id", "left")
        .join(c_info, "customer_id", "left")
    )

    grp_cols = [
        "order_date", "warehouse_id", "warehouse_name", "governorate",
        "category_id", "category_name", "l1_category_name", "segment", "channel"
    ]

    agg_df = (
        s
        .groupBy(*grp_cols)
        .agg(
            F.countDistinct("sales_order_id").alias("total_orders"),
            F.count(F.lit(1)).alias("total_order_lines"),
            F.sum("qty_ordered").alias("total_ordered_units"),
            F.sum("qty_shipped").alias("total_shipped_units"),
            F.sum("shortage_qty").alias("total_shortage_units"),
            F.sum("gross_revenue_egp").alias("gross_revenue_egp"),
            F.sum("discount_amount_egp").alias("total_discount_egp"),
            F.sum("net_revenue_egp").alias("net_sales_revenue_egp"),
            F.sum("cogs_egp").alias("total_cogs_egp"),
            F.sum("gross_profit_egp").alias("gross_profit_egp"),
            F.sum(F.when(F.col("is_promo_line"), F.col("net_revenue_egp")).otherwise(0)).alias("promo_revenue_egp"),
            F.sum(F.when(F.col("is_fulfilled_in_full"), 1).otherwise(0)).alias("fulfilled_in_full_lines"),
        )
    )

    margin_pct = F.when(
        F.col("net_sales_revenue_egp") > 0,
        F.round((F.col("gross_profit_egp") / F.col("net_sales_revenue_egp")) * 100, 2)
    )
    aov = F.when(
        F.col("total_orders") > 0,
        F.round(F.col("net_sales_revenue_egp") / F.col("total_orders"), 2)
    )
    fill_rate = F.when(
        F.col("total_ordered_units") > 0,
        F.round((F.col("total_shipped_units") / F.col("total_ordered_units")) * 100, 2)
    )
    promo_share = F.when(
        F.col("net_sales_revenue_egp") > 0,
        F.round((F.col("promo_revenue_egp") / F.col("net_sales_revenue_egp")) * 100, 2)
    )

    return (
        agg_df
        .withColumn("order_date_key", F.date_format("order_date", "yyyyMMdd").cast("int"))
        .withColumn("gross_margin_pct", margin_pct)
        .withColumn("average_order_value_egp", aov)
        .withColumn("overall_fill_rate_pct", fill_rate)
        .withColumn("promo_revenue_share_pct", promo_share)
    )


def mart_supplier_performance(fact_receipts_df, suppliers_dim_df, po_lines_df=None):
    """
    Supplier scorecard & procurement performance mart:
      - Evaluates vendor On-Time In-Full (OTIF) rate %, quality acceptance vs rejection %,
        average lead time in days, total spend, and contract compliance.
    """
    s_info = suppliers_dim_df.select(
        "supplier_id", "supplier_name", "supplier_type",
        F.col("governorate").alias("supplier_governorate")
    )

    r = fact_receipts_df.join(s_info, "supplier_id", "left")

    r = (
        r
        .withColumn("year", F.year("received_date"))
        .withColumn("month", F.month("received_date"))
        .withColumn("year_month", F.date_format("received_date", "yyyy-MM"))
    )

    grp_cols = [
        "supplier_id", "supplier_name", "supplier_type", "supplier_governorate",
        "year", "month", "year_month"
    ]

    agg_df = (
        r
        .groupBy(*grp_cols)
        .agg(
            F.countDistinct("purchase_order_id").alias("total_pos"),
            F.countDistinct("receipt_id").alias("total_receipts"),
            F.count(F.lit(1)).alias("total_receipt_lines"),
            F.sum("qty_ordered_base").alias("total_ordered_units"),
            F.sum("qty_received_base").alias("total_received_units"),
            F.sum("qty_rejected_base").alias("total_rejected_units"),
            F.sum("qty_accepted_base").alias("total_accepted_units"),
            F.sum("total_received_cost_egp").alias("total_spend_egp"),
            F.sum("total_accepted_cost_egp").alias("accepted_spend_egp"),
            F.sum(F.when(F.col("is_on_time"), 1).otherwise(0)).alias("on_time_lines"),
            F.sum(F.when(F.col("is_in_full"), 1).otherwise(0)).alias("in_full_lines"),
            F.sum(F.when(F.col("is_otif"), 1).otherwise(0)).alias("otif_lines"),
            F.avg("actual_lead_time_days").alias("avg_lead_time_days"),
        )
    )

    lines = F.col("total_receipt_lines")
    rec_units = F.col("total_received_units")

    otif_rate = F.when(lines > 0, F.round((F.col("otif_lines") / lines) * 100, 2))
    on_time_rate = F.when(lines > 0, F.round((F.col("on_time_lines") / lines) * 100, 2))
    in_full_rate = F.when(lines > 0, F.round((F.col("in_full_lines") / lines) * 100, 2))
    quality_acc_rate = F.when(rec_units > 0, F.round((F.col("total_accepted_units") / rec_units) * 100, 2))
    rejection_rate = F.when(rec_units > 0, F.round((F.col("total_rejected_units") / rec_units) * 100, 2))

    return (
        agg_df
        .withColumn("otif_rate_pct", otif_rate)
        .withColumn("on_time_rate_pct", on_time_rate)
        .withColumn("in_full_rate_pct", in_full_rate)
        .withColumn("quality_acceptance_rate_pct", quality_acc_rate)
        .withColumn("rejection_rate_pct", rejection_rate)
        .withColumn("avg_lead_time_days", F.round("avg_lead_time_days", 1))
    )


def mart_inventory_health_and_risk(
    fact_inventory_df,
    fact_sales_df,
    stockouts_df,
    batches_df,
    product_dim_df,
    warehouse_dim_df
):
    """
    Inventory health, velocity, and FEFO expiry risk mart:
      - Current on-hand, allocated, and available balances per Warehouse and Product.
      - 30-day sales velocity and Days of Inventory on Hand (DOH).
      - Stock status classification: OUT_OF_STOCK, CRITICAL_LOW (<7 days), HEALTHY, OVERSTOCKED (>60 days).
      - Stockout frequency & estimated lost revenue (EGP).
      - FEFO Expiry Risk: quantities and value of stock expiring in 30, 60, and 90 days.
    """
    # 1. Latest snapshot per warehouse/product
    w_latest = Window.partitionBy("warehouse_id", "product_id").orderBy(F.col("snapshot_date").desc())
    latest_inv = (
        fact_inventory_df
        .withColumn("_rn", F.row_number().over(w_latest))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select(
            "snapshot_date", "warehouse_id", "product_id", "on_hand_qty",
            "allocated_qty", "available_qty", "unit_cost_egp", "total_inventory_value_egp"
        )
    )

    # 2. 30-day sales velocity
    max_order_date = fact_sales_df.agg(F.max("order_date")).first()[0]
    cutoff_date = F.date_sub(F.lit(max_order_date), 30) if max_order_date else F.current_date()

    sales_30d = (
        fact_sales_df
        .filter(F.col("order_date") >= cutoff_date)
        .groupBy("warehouse_id", "product_id")
        .agg(
            F.sum("qty_shipped").alias("total_shipped_30d"),
            F.countDistinct("order_date").alias("selling_days_30d")
        )
        .withColumn("avg_daily_sales_units_30d", F.round(F.col("total_shipped_30d") / 30.0, 2))
    )

    # 3. Stockout incidents & duration in last 30 days
    stockout_stats = (
        stockouts_df
        .withColumn(
            "_duration_hours",
            (F.col("end_ts").cast("long") - F.col("start_ts").cast("long")) / 3600.0
        )
        .groupBy("warehouse_id", "product_id")
        .agg(
            F.count(F.lit(1)).alias("stockout_events_count_total"),
            F.round(F.sum("_duration_hours"), 1).alias("total_stockout_hours"),
        )
    )

    # 4. FEFO Expiry risk from active product batches
    asof_date = F.current_date()
    batch_exp = (
        batches_df
        .filter(F.col("expiry_date") >= asof_date)
        .withColumn("_days_to_exp", F.datediff("expiry_date", asof_date))
        .groupBy("warehouse_id", "product_id")
        .agg(
            F.sum(F.when(F.col("_days_to_exp") <= 30, F.col("qty_received_base")).otherwise(0)).alias("units_expiring_30d"),
            F.sum(F.when(F.col("_days_to_exp") <= 60, F.col("qty_received_base")).otherwise(0)).alias("units_expiring_60d"),
            F.sum(F.when(F.col("_days_to_exp") <= 90, F.col("qty_received_base")).otherwise(0)).alias("units_expiring_90d"),
        )
    )

    # 5. Join all components with dimension descriptors
    p_info = product_dim_df.select(
        "product_id", "product_name", "sku_code", "brand",
        "category_id", "category_name", "storage_type", "is_perishable",
        "current_list_price_egp"
    )
    w_info = warehouse_dim_df.select("warehouse_id", "warehouse_name", "governorate")

    m = (
        latest_inv
        .join(sales_30d, ["warehouse_id", "product_id"], "left")
        .join(stockout_stats, ["warehouse_id", "product_id"], "left")
        .join(batch_exp, ["warehouse_id", "product_id"], "left")
        .join(p_info, "product_id", "left")
        .join(w_info, "warehouse_id", "left")
    )

    daily_sales = F.coalesce(F.col("avg_daily_sales_units_30d"), F.lit(0.0))
    avail_qty = F.col("available_qty")

    doh = F.when(
        daily_sales > 0,
        F.round(avail_qty / daily_sales, 1)
    ).otherwise(F.when(avail_qty > 0, F.lit(999.0)).otherwise(F.lit(0.0)))

    stock_status = (
        F.when(avail_qty <= 0, "OUT_OF_STOCK")
        .when(doh < 7.0, "CRITICAL_LOW")
        .when(doh > 60.0, "OVERSTOCKED")
        .otherwise("HEALTHY")
    )

    lost_sales_est = F.round(
        F.coalesce(F.col("total_stockout_hours"), F.lit(0.0)) *
        (daily_sales / 24.0) *
        F.coalesce(F.col("current_list_price_egp"), F.lit(0.0)),
        2
    )

    cost = F.coalesce(F.col("unit_cost_egp"), F.lit(0.0))
    exp_val_30 = F.round(F.coalesce(F.col("units_expiring_30d"), F.lit(0)) * cost, 2)
    exp_val_60 = F.round(F.coalesce(F.col("units_expiring_60d"), F.lit(0)) * cost, 2)
    exp_val_90 = F.round(F.coalesce(F.col("units_expiring_90d"), F.lit(0)) * cost, 2)

    return (
        m
        .withColumn("avg_daily_sales_units_30d", daily_sales)
        .withColumn("days_of_supply", doh)
        .withColumn("stock_status", stock_status)
        .withColumn("stockout_events_count_total", F.coalesce("stockout_events_count_total", F.lit(0)))
        .withColumn("total_stockout_hours", F.coalesce("total_stockout_hours", F.lit(0.0)))
        .withColumn("units_expiring_30d", F.coalesce("units_expiring_30d", F.lit(0)))
        .withColumn("units_expiring_60d", F.coalesce("units_expiring_60d", F.lit(0)))
        .withColumn("units_expiring_90d", F.coalesce("units_expiring_90d", F.lit(0)))
        .withColumn("estimated_lost_sales_egp", lost_sales_est)
        .withColumn("value_expiring_30d_egp", exp_val_30)
        .withColumn("value_expiring_60d_egp", exp_val_60)
        .withColumn("value_expiring_90d_egp", exp_val_90)
    )


def mart_warehouse_operations_daily(
    fact_sales_df,
    fact_receipts_df,
    fact_returns_df,
    snapshots_df,
    warehouse_dim_df,
    weather_df=None
):
    """
    Daily warehouse throughput, capacity utilization, and environmental intelligence mart:
      - Inbound received units & rejection rate.
      - Outbound shipped units & shortage rate.
      - Reverse logistics / return receipts.
      - Pallet capacity utilization %.
      - Weather extremes (temperatures > 40°C triggering thermal degradation alerts).
    """
    # 1. Outbound activity
    outbound = (
        fact_sales_df
        .groupBy(F.col("order_date").alias("op_date"), "warehouse_id")
        .agg(
            F.countDistinct("sales_order_id").alias("outbound_orders_count"),
            F.sum("qty_ordered").alias("outbound_ordered_units"),
            F.sum("qty_shipped").alias("outbound_shipped_units"),
            F.sum("shortage_qty").alias("outbound_shortage_units"),
        )
    )

    # 2. Inbound activity
    inbound = (
        fact_receipts_df
        .groupBy(F.col("received_date").alias("op_date"), "warehouse_id")
        .agg(
            F.countDistinct("receipt_id").alias("inbound_receipts_count"),
            F.sum("qty_received_base").alias("inbound_received_units"),
            F.sum("qty_rejected_base").alias("inbound_rejected_units"),
            F.sum("qty_accepted_base").alias("inbound_accepted_units"),
        )
    )

    # 3. Returns activity
    returns = (
        fact_returns_df
        .groupBy(F.col("return_date").alias("op_date"), "warehouse_id")
        .agg(
            F.count(F.lit(1)).alias("return_lines_count"),
            F.sum("qty_returned").alias("returned_units"),
            F.sum("refund_amount_egp").alias("refund_amount_egp"),
        )
    )

    # 4. End-of-day inventory balance
    inv_daily = (
        snapshots_df
        .groupBy(F.col("snapshot_date").alias("op_date"), "warehouse_id")
        .agg(
            F.sum("on_hand_qty").alias("closing_on_hand_units"),
            F.sum("allocated_qty").alias("closing_allocated_units"),
            F.sum("available_qty").alias("closing_available_units"),
        )
    )

    # Combine operations across dates and warehouses
    w_info = warehouse_dim_df.select(
        "warehouse_id", "warehouse_name", "governorate", "city", "capacity_pallet_positions"
    )

    # Union all distinct (op_date, warehouse_id) pairs
    keys_df = (
        outbound.select("op_date", "warehouse_id")
        .unionByName(inbound.select("op_date", "warehouse_id"))
        .unionByName(returns.select("op_date", "warehouse_id"))
        .unionByName(inv_daily.select("op_date", "warehouse_id"))
        .distinct()
    )

    ops = (
        keys_df
        .join(w_info, "warehouse_id", "left")
        .join(outbound, ["op_date", "warehouse_id"], "left")
        .join(inbound, ["op_date", "warehouse_id"], "left")
        .join(returns, ["op_date", "warehouse_id"], "left")
        .join(inv_daily, ["op_date", "warehouse_id"], "left")
    )

    # 5. Weather telemetry integration (case-insensitive join on location_code vs governorate)
    if weather_df is not None:
        w_tele = weather_df.select(
            F.col("obs_date").alias("_w_date"),
            F.upper(F.trim(F.col("location_code"))).alias("_w_loc"),
            "temp_max_c", "temp_min_c", "precip_mm"
        )
        ops = (
            ops
            .withColumn("_gov_norm", F.upper(F.trim(F.col("governorate"))))
            .join(
                w_tele,
                (ops["op_date"] == w_tele["_w_date"]) & (F.col("_gov_norm") == w_tele["_w_loc"]),
                "left"
            )
            .drop("_w_date", "_w_loc", "_gov_norm")
        )
    else:
        ops = (
            ops
            .withColumn("temp_max_c", F.lit(None).cast("double"))
            .withColumn("temp_min_c", F.lit(None).cast("double"))
            .withColumn("precip_mm", F.lit(None).cast("double"))
        )

    # Capacity utilization estimation (assuming average 50 units per pallet position)
    closing_on_hand = F.coalesce(F.col("closing_on_hand_units"), F.lit(0))
    est_pallets = F.round(closing_on_hand / 50.0, 1)
    cap = F.col("capacity_pallet_positions")
    util_pct = F.when(
        cap > 0,
        F.round((est_pallets / cap) * 100, 2)
    )

    extreme_heat = F.col("temp_max_c") >= 40.0

    return (
        ops
        .withColumn("outbound_orders_count", F.coalesce("outbound_orders_count", F.lit(0)))
        .withColumn("outbound_ordered_units", F.coalesce("outbound_ordered_units", F.lit(0)))
        .withColumn("outbound_shipped_units", F.coalesce("outbound_shipped_units", F.lit(0)))
        .withColumn("outbound_shortage_units", F.coalesce("outbound_shortage_units", F.lit(0)))
        .withColumn("inbound_receipts_count", F.coalesce("inbound_receipts_count", F.lit(0)))
        .withColumn("inbound_received_units", F.coalesce("inbound_received_units", F.lit(0)))
        .withColumn("inbound_rejected_units", F.coalesce("inbound_rejected_units", F.lit(0)))
        .withColumn("inbound_accepted_units", F.coalesce("inbound_accepted_units", F.lit(0)))
        .withColumn("return_lines_count", F.coalesce("return_lines_count", F.lit(0)))
        .withColumn("returned_units", F.coalesce("returned_units", F.lit(0)))
        .withColumn("refund_amount_egp", F.coalesce("refund_amount_egp", F.lit(0.0)))
        .withColumn("closing_on_hand_units", closing_on_hand)
        .withColumn("closing_allocated_units", F.coalesce("closing_allocated_units", F.lit(0)))
        .withColumn("closing_available_units", F.coalesce("closing_available_units", F.lit(0)))
        .withColumn("operation_date_key", F.date_format("op_date", "yyyyMMdd").cast("int"))
        .withColumn("operation_date", F.col("op_date"))
        .withColumn("estimated_pallet_positions_used", est_pallets)
        .withColumn("capacity_utilization_pct", util_pct)
        .withColumn("extreme_heat_alert_flag", F.coalesce(extreme_heat, F.lit(False)))
        .drop("op_date")
    )

