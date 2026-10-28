"""
Master Data Generators:
R02 products, R03 warehouses (clean control), R04 suppliers,
R05 supplier_products, R06 customers.
Defects go through the DefectRegistry only (random rows, logged, rate-based).
"""
from datetime import date, timedelta
import pandas as pd
from data_generator.config import (
    WAREHOUSES, EGYPTIAN_GOVERNORATES, CUSTOMER_SEGMENTS, CUSTOMER_SEGMENT_VARIANTS,
    BRANDS, GOVERNORATE_WAREHOUSE, GOVERNORATE_WEIGHT
)
from data_generator.core.registry import DefectSpec, set_value, messy_case, to_null, typo
from data_generator.defects import generate_valid_ean13
from data_generator.tables.reference_data import CLEAN_L2_NAME


def _rand_date(rng, lo: date, hi: date) -> date:
    return lo + timedelta(days=rng.randint(0, (hi - lo).days))


def generate_warehouses() -> pd.DataFrame:
    return pd.DataFrame(WAREHOUSES)


def _shelf_life(rng, storage: str, perishable: bool) -> int:
    # every product has a shelf life (FMCG needs expiry/batch traceability)
    if perishable and storage == "FROZEN":
        return rng.choice([180, 270, 365])
    if perishable:
        return rng.choice([7, 14, 21, 30, 60, 90])
    return rng.choice([365, 540, 720, 1095])


def generate_products(num_products, categories_df, rng, reg) -> pd.DataFrame:
    """R02. Defects: duplicate sku (2 rows min), perishable w/o shelf life (3 rows min),
    messy brand (15%), invalid EAN check digit (1%)."""
    l2 = categories_df[categories_df["category_level"] == 2].to_dict("records")
    rows = []
    for i in range(1, num_products + 1):
        cat = rng.choice(l2)
        units_per_case = rng.choice([6, 12, 24, 48])
        brand = rng.choice(BRANDS)
        status = "ACTIVE" if rng.random() < 0.95 else "DISCONTINUED"
        rows.append({
            "product_id": f"P{i:06d}", "sku_code": f"SKU-{i:06d}",
            "product_name": f"{brand} {CLEAN_L2_NAME[cat['category_id']]} Pack-{i}",
            "brand": brand, "category_id": cat["category_id"], "base_uom": "EA",
            "units_per_case": units_per_case,
            "case_weight_kg": round(units_per_case * rng.uniform(0.25, 1.5), 3),
            "storage_type": cat["default_storage_type"],
            "is_perishable": bool(cat["is_perishable_default"]),
            "shelf_life_days": _shelf_life(rng, cat["default_storage_type"], bool(cat["is_perishable_default"])),
            "status": status,
            "launch_date": _rand_date(rng, date(2020, 1, 1), date(2023, 12, 31)).isoformat(),
            "discontinued_date": _rand_date(rng, date(2024, 3, 1), date(2025, 9, 30)).isoformat()
                                 if status == "DISCONTINUED" else None,
            "barcode_ean13": generate_valid_ean13(i), "updated_ts": "2024-01-01 00:00:00+02:00"})
    df = pd.DataFrame(rows)
    key = ["product_id"]

    df = reg.mutate(df, DefectSpec("DEF-PRD-001", "products", "sku_code",
                                   "duplicate sku_code across two products", 0.01, "reject", min_rows=2), key,
                    lambda v, row, r: df.at[r.choice([j for j in df.index if df.at[j, "sku_code"] != v]), "sku_code"])
    df = reg.mutate(df, DefectSpec("DEF-PRD-002", "products", "shelf_life_days",
                                   "perishable product without shelf_life_days", 0.02, "flag", min_rows=3), key,
                    to_null, mask=df["is_perishable"])
    df = reg.mutate(df, DefectSpec("DEF-PRD-003", "products", "brand",
                                   "mixed case / whitespace in brand", 0.15, "fix"), key, messy_case)

    def bad_ean(v, row, r):
        return v[:-1] + str((int(v[-1]) + r.randint(1, 9)) % 10)
    df = reg.mutate(df, DefectSpec("DEF-PRD-004", "products", "barcode_ean13",
                                   "invalid EAN-13 check digit", 0.01, "flag"), key, bad_ean)
    return df


def generate_suppliers(num_suppliers, rng, reg) -> pd.DataFrame:
    """R04. Defects: messy supplier_name (20%), null governorate (1 row min)."""
    types = ["MANUFACTURER", "DISTRIBUTOR", "IMPORTER"]
    rows = []
    for i in range(1, num_suppliers + 1):
        stype = rng.choice(types)
        rows.append({
            "supplier_id": f"SUP{i:03d}", "supplier_name": f"Supplier {i:03d} Trading & Distribution Co",
            "supplier_type": stype, "governorate": rng.choice(EGYPTIAN_GOVERNORATES),
            "payment_terms_days": rng.choice([15, 30, 45, 60, 90]),
            "is_imported": True if stype == "IMPORTER" else (rng.random() < 0.2),
            "status": "ACTIVE" if rng.random() < 0.9 else rng.choice(["ON_HOLD", "INACTIVE"]),
            "onboarded_date": _rand_date(rng, date(2021, 1, 1), date(2023, 6, 30)).isoformat(),
            "updated_ts": "2024-01-01 00:00:00+02:00"})
    df = pd.DataFrame(rows)
    key = ["supplier_id"]
    df = reg.mutate(df, DefectSpec("DEF-SUP-001", "suppliers", "supplier_name",
                                   "whitespace / case issues in supplier_name", 0.20, "fix"), key, messy_case)
    df = reg.mutate(df, DefectSpec("DEF-SUP-002", "suppliers", "governorate",
                                   "null governorate", 0.01, "flag"), key, to_null)
    return df


def generate_supplier_products(products_df, suppliers_df, catalog, rng, reg) -> pd.DataFrame:
    """
    R05. Supplier cost per case = product landed cost x units_per_case x supplier factor (0.92-1.12)
    x small line noise -> realistic spread (~+-15%) instead of 10x random prices.
    Defects: null lead time (4%), overlapping range (0.5%), two primaries (0.3%).
    """
    sup_ids = suppliers_df["supplier_id"].tolist()
    sup_factor = {s: rng.uniform(0.92, 1.12) for s in sup_ids}   # stable per supplier
    rows = []
    for _, prod in products_df.iterrows():
        pid, upc = prod["product_id"], int(prod["units_per_case"])
        shelf = prod["shelf_life_days"] if pd.notna(prod["shelf_life_days"]) else 30
        # short-life goods are ordered in small, frequent lots; durable goods in bigger MOQs
        moq_pool = [1, 2, 3, 5] if shelf <= 30 else [2, 3, 5, 10] if shelf <= 90 else [5, 10, 20, 50]
        mult_pool = [1] if shelf <= 90 else [1, 2, 5]
        lt_pool = [1, 2, 3] if shelf <= 30 else [2, 3, 5, 7] if shelf <= 90 else [3, 5, 7, 10, 14, 21]
        for idx, sup_id in enumerate(rng.sample(sup_ids, k=min(len(sup_ids), rng.choice([1, 2, 3])))):
            invoice_unit = catalog[pid]["c1"] * rng.uniform(0.88, 0.95)      # landed cost incl. freight/duty
            rows.append({
                "supplier_id": sup_id, "product_id": pid, "supplier_sku": f"SSKU-{sup_id}-{pid}",
                "units_per_case": upc, "moq_cases": rng.choice(moq_pool),
                "order_multiple_cases": rng.choice(mult_pool),
                "contractual_lead_time_days": rng.choice(lt_pool),
                "cost_per_case_egp": round(invoice_unit * upc * sup_factor[sup_id] * rng.uniform(0.97, 1.03), 2),
                "is_primary": idx == 0, "effective_from": "2024-01-01", "effective_to": None,
                "updated_ts": "2024-01-01 00:00:00+02:00"})
    df = pd.DataFrame(rows)
    key = ["supplier_id", "product_id", "effective_from"]

    df = reg.mutate(df, DefectSpec("DEF-SPR-001", "supplier_products", "contractual_lead_time_days",
                                   "null contractual lead time", 0.04, "flag"), key, to_null)
    multi = df.groupby("product_id")["supplier_id"].transform("count") > 1
    df = reg.mutate(df, DefectSpec("DEF-SPR-003", "supplier_products", "is_primary",
                                   "two primary suppliers for one product", 0.003, "flag"), key,
                    set_value(True), mask=multi & ~df["is_primary"])
    df = reg.duplicate(df, DefectSpec("DEF-SPR-002", "supplier_products", None,
                                      "overlapping effective range with different cost", 0.005, "flag"), key,
                       {"effective_from": lambda v, row, r: "2024-06-01",
                        "effective_to": lambda v, row, r: "2024-12-31",
                        "cost_per_case_egp": lambda v, row, r: round(v * 1.1, 2)})
    return df


def generate_customers(num_customers, warehouses_df, rng, reg) -> pd.DataFrame:
    """R06. Customers are served by the DC of their governorate.
    Defects: segment label variants (75% non-canonical), governorate typos (1%)."""
    govs = list(GOVERNORATE_WAREHOUSE)
    weights = [GOVERNORATE_WEIGHT.get(g, 2) for g in govs]
    rows = []
    for i in range(1, num_customers + 1):
        gov = rng.choices(govs, weights=weights)[0]
        rows.append({
            "customer_id": f"C{i:06d}", "customer_name": f"Customer {i:06d}",
            "segment": rng.choice(CUSTOMER_SEGMENTS), "governorate": gov, "city": f"{gov} City Center",
            "assigned_warehouse_id": GOVERNORATE_WAREHOUSE[gov],
            "credit_terms_days": rng.choice([0, 7, 14, 30, 45]),
            "status": "ACTIVE" if rng.random() < 0.96 else rng.choice(["BLOCKED", "CLOSED"]),
            "created_date": _rand_date(rng, date(2021, 1, 1), date(2023, 12, 31)).isoformat(),
            "updated_ts": "2024-01-01 00:00:00+02:00"})
    df = pd.DataFrame(rows)
    key = ["customer_id"]
    df = reg.mutate(df, DefectSpec("DEF-CUS-001", "customers", "segment",
                                   "segment label variants (non-canonical)", 0.75, "fix"), key,
                    lambda v, row, r: r.choice([x for x in CUSTOMER_SEGMENT_VARIANTS[v] if x != v]))
    df = reg.mutate(df, DefectSpec("DEF-CUS-002", "customers", "governorate",
                                   "governorate misspelling", 0.01, "fix"), key, typo)
    return df
