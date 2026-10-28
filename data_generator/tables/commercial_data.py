"""
Commercial & Pricing Data Generators:
- R15: product_prices (CSV)
- R16: product_costs (CSV)
- R17: promotions (CSV)

All numbers derive from ONE clean price catalog (build_price_catalog), so
prices, costs, supplier costs and promo funding are mutually consistent.
Defects are injected afterwards through the DefectRegistry only.
"""
import math
from datetime import date, timedelta
import pandas as pd
from data_generator.core.registry import DefectSpec, set_value

# EGP per base unit, by L1 category (log-uniform inside the band)
CAT_PRICE_BAND = {
    "CAT-01": (15, 90), "CAT-02": (12, 80), "CAT-03": (5, 60), "CAT-04": (60, 320),
    "CAT-05": (8, 70), "CAT-06": (25, 140), "CAT-07": (20, 180), "CAT-08": (25, 200),
}
# H2-2024 price increase per category (different pass-through per sector)
CAT_INFLATION = {
    "CAT-01": 1.18, "CAT-02": 1.22, "CAT-03": 1.12, "CAT-04": 1.20,
    "CAT-05": 1.14, "CAT-06": 1.25, "CAT-07": 1.10, "CAT-08": 1.11,
}
P1_FROM, P1_TO, P2_FROM = "2024-01-01", "2024-06-30", "2024-07-01"


def _l1(category_id: str) -> str:
    return str(category_id)[:6]


def build_price_catalog(products_df: pd.DataFrame, rng) -> dict:
    """pid -> clean {p1, p2, c1, c2}: sell price & landed cost for period 1 / period 2."""
    cat = {}
    for pid, cid in zip(products_df["product_id"], products_df["category_id"]):
        l1 = _l1(cid)
        lo, hi = CAT_PRICE_BAND.get(l1, (10, 100))
        infl = CAT_INFLATION.get(l1, 1.15)
        p1 = round(math.exp(rng.uniform(math.log(lo), math.log(hi))), 2)
        p2 = round(p1 * infl * (1 + rng.uniform(-0.03, 0.03)), 2)
        c1 = round(p1 * rng.uniform(0.65, 0.82), 4)
        c2 = round(c1 * infl * rng.uniform(0.93, 1.03), 4)
        cat[pid] = {"p1": p1, "p2": p2, "c1": c1, "c2": c2}
    return cat


def generate_product_prices(products_df, catalog, reg) -> pd.DataFrame:
    """R15. Defects: price=0 (0.1%), effective_to<from (0.1%), conflicting overlap (0.5%)."""
    rows = []
    for pid in products_df["product_id"]:
        c = catalog[pid]
        rows.append({"product_id": pid, "price_list": "B2B_STANDARD", "list_price_egp": c["p1"],
                     "effective_from": P1_FROM, "effective_to": P1_TO,
                     "updated_ts": f"{P1_FROM} 00:00:00+02:00"})
        rows.append({"product_id": pid, "price_list": "B2B_STANDARD", "list_price_egp": c["p2"],
                     "effective_from": P2_FROM, "effective_to": None,
                     "updated_ts": f"{P2_FROM} 00:00:00+02:00"})
    df = pd.DataFrame(rows)
    key = ["product_id", "effective_from"]

    df = reg.mutate(df, DefectSpec("DEF-PRC-001", "product_prices", "list_price_egp",
                                   "list price = 0", 0.001, "reject"), key, set_value(0.0))
    df = reg.mutate(df, DefectSpec("DEF-PRC-002", "product_prices", "effective_to",
                                   "effective_to before effective_from", 0.001, "reject"), key,
                    lambda v, row, rng: (date.fromisoformat(row["effective_from"])
                                         - timedelta(days=rng.randint(1, 60))).isoformat(),
                    mask=df["effective_to"].isna())
    df = reg.duplicate(df, DefectSpec("DEF-PRC-003", "product_prices", None,
                                      "overlapping validity window with conflicting price", 0.005, "flag"), key,
                       {"effective_from": lambda v, row, rng: "2024-05-01",
                        "effective_to": lambda v, row, rng: "2024-08-31",
                        "list_price_egp": lambda v, row, rng: round(v * 1.05, 2)})
    return df


def generate_product_costs(products_df, catalog, reg) -> pd.DataFrame:
    """R16. Defects: cost above selling price (0.4%), gap between periods (1%)."""
    rows = []
    for pid in products_df["product_id"]:
        c = catalog[pid]
        rows.append({"product_id": pid, "cost_type": "STANDARD_LANDED", "cost_per_base_unit_egp": c["c1"],
                     "effective_from": P1_FROM, "effective_to": P1_TO,
                     "updated_ts": f"{P1_FROM} 00:00:00+02:00"})
        rows.append({"product_id": pid, "cost_type": "STANDARD_LANDED", "cost_per_base_unit_egp": c["c2"],
                     "effective_from": P2_FROM, "effective_to": None,
                     "updated_ts": f"{P2_FROM} 00:00:00+02:00"})
    df = pd.DataFrame(rows)
    key = ["product_id", "effective_from"]

    df = reg.mutate(df, DefectSpec("DEF-CST-001", "product_costs", "cost_per_base_unit_egp",
                                   "cost above selling price (margin anomaly)", 0.004, "flag"), key,
                    lambda v, row, rng: round(catalog[row["product_id"]]["p1"] * rng.uniform(2.0, 3.0), 4))
    df = reg.mutate(df, DefectSpec("DEF-CST-002", "product_costs", "effective_to",
                                   "gap between consecutive cost periods", 0.01, "flag"), key,
                    set_value("2024-05-31"), mask=df["effective_to"] == P1_TO)
    return df


def generate_promotions(products_df, supplier_products_df, start_date, end_date, rng, reg) -> pd.DataFrame:
    """
    R17. Funding is internally consistent:
      SELF -> no supplier, SUPPLIER -> primary supplier 100%, SHARED -> 30/50/70%.
    Defects: end<start (0.3%), overlapping promo on same product (3%).
    """
    pids = products_df["product_id"].tolist()
    wh_ids = ["WH01", "WH02", "WH03"]
    primary = (supplier_products_df.sort_values("is_primary", ascending=False)
               .drop_duplicates("product_id").set_index("product_id")["supplier_id"].to_dict())

    span_days = max(1, (end_date - start_date).days)
    n_campaigns = max(8, round(20 * span_days / 366))
    rows = []
    for c_idx in range(1, n_campaigns + 1):
        s = start_date + timedelta(days=rng.randint(0, max(0, span_days - 7)))
        e = s + timedelta(days=rng.choice([7, 14, 21]))
        promo_id = f"PRM-{s.year}-{c_idx:02d}"
        name = f"Campaign Promotion {s.year} #{c_idx}"
        for pid in rng.sample(pids, k=min(len(pids), rng.choice([3, 5, 8]))):
            funded = rng.choice(["SUPPLIER", "SELF", "SHARED"])
            sup_pct = {"SUPPLIER": 100.0, "SELF": None, "SHARED": rng.choice([30.0, 50.0, 70.0])}[funded]
            rows.append({
                "promo_id": promo_id, "product_id": pid,
                "warehouse_id": rng.choice([None, rng.choice(wh_ids)]),
                "promo_name": name, "promo_type": "PCT_DISCOUNT",
                "discount_pct": rng.choice([5.0, 10.0, 15.0, 20.0, 25.0]),
                "start_date": s.isoformat(), "end_date": e.isoformat(),
                "funded_by": funded,
                "supplier_id": primary.get(pid) if funded != "SELF" else None,
                "supplier_funding_pct": sup_pct,
                "updated_ts": f"{start_date.isoformat()} 00:00:00+02:00"})
    df = pd.DataFrame(rows)
    key = ["promo_id", "product_id"]

    df = reg.mutate(df, DefectSpec("DEF-PRM-001", "promotions", "end_date",
                                   "end_date before start_date", 0.003, "reject"), key,
                    lambda v, row, rng: (date.fromisoformat(row["start_date"])
                                         - timedelta(days=rng.randint(3, 15))).isoformat())
    df = reg.duplicate(df, DefectSpec("DEF-PRM-002", "promotions", None,
                                      "overlapping promotion on same product (higher discount)", 0.03, "flag"), key,
                       {"promo_id": lambda v, row, rng: f"PRM-OVL-{rng.randint(100, 999)}",
                        "promo_name": lambda v, row, rng: "Overlapping Super Promo",
                        "discount_pct": lambda v, row, rng: min(35.0, v + 10.0)})
    return df
