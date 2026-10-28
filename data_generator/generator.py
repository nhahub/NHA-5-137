"""
Main Generator Orchestrator for Smart Warehouse Synthetic Data Platform.
Generates all 21 raw datasets (R01-R21) with full injected defects into target directory.
Supports deterministic random seeding and end-to-end supply chain integration.
"""

import os
import json
import gzip
import random
import argparse
import numpy as np
import pandas as pd
from datetime import datetime

from data_generator.config import PROFILES, RAW_DATA_DIR, DATA_DIR
from data_generator.core import rng as rnglib
from data_generator.core.registry import DefectRegistry
from data_generator.tables.reference_data import (
    generate_product_categories, generate_calendar_events, generate_weather_daily
)
from data_generator.tables.master_data import (
    generate_warehouses, generate_products, generate_suppliers,
    generate_supplier_products, generate_customers
)
from data_generator.tables.commercial_data import (
    build_price_catalog, generate_product_prices, generate_product_costs, generate_promotions
)
from data_generator.tables.simulation import run_simulation
from data_generator.tables.sim_defects import inject_simulation_defects


def run_generation(
    profile_name: str = "test",
    output_dir: str = None,
    inject_defects: bool = True,
    seed: int = None
) -> dict:
    """
    Executes end-to-end data generation for all 21 raw tables.
    Fulfills AUD-001, AUD-002, AUD-003, AUD-004, AUD-005, AUD-006, AUD-007, and AUD-008.
    """
    hub = rnglib.init(seed)
    seed = hub.seed                      # resolved seed (random if None) is always recorded
    random.seed(seed)                    # legacy tables (orders/procurement/inventory) still use global RNG
    np.random.seed(seed % (2**32))
    print(f"[*] Master seed: {seed}")
    reg = DefectRegistry(hub.py("defects"), enabled=inject_defects)

    if output_dir is None:
        output_dir = RAW_DATA_DIR
    os.makedirs(output_dir, exist_ok=True)

    cfg = PROFILES.get(profile_name, PROFILES["test"])
    print(f"\n=======================================================")
    print(f"[*] Launching Synthetic Data Generation | Profile: {profile_name.upper()}")
    print(f"[-] Output Directory: {output_dir}")
    print(f"[!] Defect Injection: {'ENABLED' if inject_defects else 'DISABLED'}")
    if seed is not None:
        print(f"[*] Random Seed:     {seed}")
    print(f"=======================================================\n")

    summary = {"generated_files": {}, "defects_summary": []}

    # 1. Reference Data
    print("[-] [1/7] Generating Reference Data (Categories, Calendar, Weather)...")
    df_categories = generate_product_categories(reg)
    df_categories.to_csv(os.path.join(output_dir, "product_categories.csv"), index=False)
    summary["generated_files"]["product_categories.csv"] = len(df_categories)

    df_calendar = generate_calendar_events(cfg["start_date"].year, cfg["end_date"].year, reg)
    df_calendar.to_csv(os.path.join(output_dir, "calendar_events.csv"), index=False)
    summary["generated_files"]["calendar_events.csv"] = len(df_calendar)

    weather_records = generate_weather_daily(cfg["start_date"], cfg["end_date"], hub.py("weather"), reg)
    with open(os.path.join(output_dir, "weather_daily.json"), "w", encoding="utf-8") as f:
        json.dump(weather_records, f, indent=2)
    summary["generated_files"]["weather_daily.json"] = len(weather_records)

    # 2. Master Data + Commercial (price catalog first: costs & supplier prices derive from it)
    print("[-] [2/7] Generating Master Data, Pricing, Costs, Promotions...")
    df_warehouses = generate_warehouses()
    df_warehouses.to_csv(os.path.join(output_dir, "warehouses.csv"), index=False)
    summary["generated_files"]["warehouses.csv"] = len(df_warehouses)

    df_products = generate_products(cfg["num_products"], df_categories, hub.py("products"), reg)
    df_products.to_csv(os.path.join(output_dir, "products.csv"), index=False)
    summary["generated_files"]["products.csv"] = len(df_products)

    catalog = build_price_catalog(df_products, hub.py("catalog"))
    df_prices = generate_product_prices(df_products, catalog, reg)
    df_prices.to_csv(os.path.join(output_dir, "product_prices.csv"), index=False)
    summary["generated_files"]["product_prices.csv"] = len(df_prices)

    df_costs = generate_product_costs(df_products, catalog, reg)
    df_costs.to_csv(os.path.join(output_dir, "product_costs.csv"), index=False)
    summary["generated_files"]["product_costs.csv"] = len(df_costs)

    df_suppliers = generate_suppliers(cfg["num_suppliers"], hub.py("suppliers"), reg)
    df_suppliers.to_csv(os.path.join(output_dir, "suppliers.csv"), index=False)
    summary["generated_files"]["suppliers.csv"] = len(df_suppliers)

    df_sup_products = generate_supplier_products(df_products, df_suppliers, catalog, hub.py("supplier_products"), reg)
    df_sup_products.to_csv(os.path.join(output_dir, "supplier_products.csv"), index=False)
    summary["generated_files"]["supplier_products.csv"] = len(df_sup_products)

    df_customers = generate_customers(cfg["num_customers"], df_warehouses, hub.py("customers"), reg)
    df_customers.to_csv(os.path.join(output_dir, "customers.csv"), index=False)
    summary["generated_files"]["customers.csv"] = len(df_customers)

    df_promos = generate_promotions(df_products, df_sup_products, cfg["start_date"], cfg["end_date"],
                                    hub.py("promotions"), reg)
    df_promos.to_csv(os.path.join(output_dir, "promotions.csv"), index=False)
    summary["generated_files"]["promotions.csv"] = len(df_promos)

    # 3. Supply-chain simulation: orders, procurement, batches, ledger, snapshots, stockouts
    #    (one daily loop over real stock; purchase-order and ledger volumes are emergent)
    print("[-] [3/7] Simulating orders, procurement, inventory ledger (daily loop)...")
    df_calendar_for_sim = df_calendar
    sim = run_simulation(cfg, df_products, df_customers, df_warehouses, df_suppliers, df_sup_products,
                         catalog, df_promos, df_calendar_for_sim, hub)
    print("[-] [4/7] Injecting transactional defects (logged to ground truth)...")
    sim = inject_simulation_defects(sim, df_sup_products, reg)

    print("[-] [5/7] Writing transactional files...")
    for name, frame in sim.items():
        if name == "inventory_transactions":
            continue
        frame.to_csv(os.path.join(output_dir, f"{name}.csv"), index=False)
        summary["generated_files"][f"{name}.csv"] = len(frame)
    led = sim["inventory_transactions"].astype(object)
    led = led.where(led.notna(), None)
    with open(os.path.join(output_dir, "inventory_transactions.jsonl"), "w", encoding="utf-8") as f:
        for rec in led.to_dict("records"):
            f.write(json.dumps(rec, default=lambda o: o.item() if hasattr(o, "item") else str(o)) + "\n")
    summary["generated_files"]["inventory_transactions.jsonl"] = len(led)

    # 7. Metadata Report
    print("[-] [7/7] Writing Generation Manifest & Quality Log...")
    manifest_path = os.path.join(output_dir, "generation_manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump({
            "generated_at": datetime.now().isoformat(),
            "profile": profile_name,
            "seed": seed,
            "inject_defects": inject_defects,
            "files": summary["generated_files"]
        }, f, indent=2)

    reg.save(os.path.join(output_dir, "defect_ground_truth.json"),
             {"profile": profile_name, "seed": seed, "note": "Phase 2: all 21 tables"})
    print("\n[OK] All 21 Raw Datasets successfully generated!")
    for fname, count in summary["generated_files"].items():
        print(f"  * {fname:<30}: {count:>6} rows/records")

    summary["files"] = summary["generated_files"]
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic raw warehouse data with injected defects.")
    parser.add_argument("--profile", choices=["test", "medium", "full"], default="test", help="Generation profile size")
    parser.add_argument("--output-dir", default=None, help="Directory to save raw CSV/JSON files")
    parser.add_argument("--no-defects", action="store_true", help="Disable intentional defect injection")
    parser.add_argument("--seed", type=int, default=None, help="Deterministic random seed")
    args = parser.parse_args()

    run_generation(
        profile_name=args.profile,
        output_dir=args.output_dir,
        inject_defects=not args.no_defects,
        seed=args.seed
    )
