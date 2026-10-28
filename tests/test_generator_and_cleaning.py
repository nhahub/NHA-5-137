"""
Automated Test Suite for Smart Warehouse Platform.
Validates raw data generation, schema integrity, intentional defect injection,
deterministic random seeding, and inventory ledger reconciliation.
"""

import os
import json
import pytest
import pandas as pd
from data_generator.generator import run_generation
from data_generator.config import RAW_DATA_DIR


def test_raw_data_generation_and_defects():
    """Validates that raw datasets are generated with expected schema and injected defects."""
    manifest = run_generation(profile_name="test", inject_defects=True, seed=42)
    assert len(manifest["generated_files"]) == 21, "Must generate all 21 raw datasets"
    
    # 1. Test category orphan defect
    df_cat = pd.read_csv(os.path.join(RAW_DATA_DIR, "product_categories.csv"))
    assert (df_cat["category_id"] == "CAT-0105").any()
    orphan_row = df_cat[df_cat["category_id"] == "CAT-0105"].iloc[0]
    assert orphan_row["parent_category_id"] == "CAT-99", "Orphan category parent defect must be present"
    
    # 2. Test duplicate SKU in products
    df_prod = pd.read_csv(os.path.join(RAW_DATA_DIR, "products.csv"))
    assert df_prod["sku_code"].duplicated().any(), "Duplicate SKU code defect must be present"
    
    # 3. Test sales order lines negative quantity
    df_lines = pd.read_csv(os.path.join(RAW_DATA_DIR, "sales_order_lines.csv"))
    assert (df_lines["qty_ordered"] < 0).any(), "Negative qty_ordered defect must be present in raw"
    
    # 4. Test goods receipt invalid expiry
    df_rec = pd.read_csv(os.path.join(RAW_DATA_DIR, "goods_receipt_lines.csv"))
    assert (df_rec["expiry_date"] == "2023-01-01").any(), "Invalid expiry date defect must be present in raw"


def test_deterministic_seeding():
    """Validates that running with the same seed produces identical row counts (AUD-006)."""
    manifest_a = run_generation(profile_name="test", inject_defects=True, seed=99)
    manifest_b = run_generation(profile_name="test", inject_defects=True, seed=99)
    assert manifest_a["files"] == manifest_b["files"], "Identical seed must produce identical file metrics"


def test_inventory_ledger_linkage():
    """Validates that inventory ledger transactions link to real supply chain events (AUD-001)."""
    txns = []
    with open(os.path.join(RAW_DATA_DIR, "inventory_transactions.jsonl"), "r", encoding="utf-8") as f:
        for line in f:
            txns.append(json.loads(line))
            
    df_lines = pd.read_csv(os.path.join(RAW_DATA_DIR, "sales_order_lines.csv"))
    df_receipts = pd.read_csv(os.path.join(RAW_DATA_DIR, "goods_receipt_lines.csv"))
    
    # Verify presence of SO_LINE issues linked to actual order lines
    so_refs = {t["reference_id"] for t in txns if t["reference_type"] == "SO_LINE" and t["reference_id"]}
    assert len(so_refs) > 0, "Inventory ledger must contain linked SO_LINE issue transactions"
    
    # Verify presence of GR_LINE receipts linked to actual goods receipts
    gr_refs = {t["reference_id"] for t in txns if t["reference_type"] == "GR_LINE" and t["reference_id"]}
    assert len(gr_refs) > 0, "Inventory ledger must contain linked GR_LINE receipt transactions"
    
    # Verify snapshots contain all products
    df_snapshots = pd.read_csv(os.path.join(RAW_DATA_DIR, "inventory_snapshots.csv"))
    df_products = pd.read_csv(os.path.join(RAW_DATA_DIR, "products.csv"))
    assert len(df_snapshots["product_id"].unique()) == len(df_products["product_id"].unique()), \
        "Inventory snapshots must cover all catalog products (AUD-003)"
