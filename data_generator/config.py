"""
Configuration and Master Constants for Smart Warehouse Synthetic Data Platform.
Covers Egyptian market specifics: currency (EGP), governorates, cities, calendar, and warehouse profiles.
"""

import os
from datetime import date, datetime

def _load_env():
    candidate_paths = [
        "/opt/airflow/.env",
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"),
        os.path.join(os.getcwd(), ".env"),
    ]
    for p in candidate_paths:
        if os.path.exists(p) and os.path.isfile(p):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith("#") and "=" in line:
                            k, v = line.split("=", 1)
                            os.environ.setdefault(k.strip(), v.strip().strip("'\""))
                break
            except Exception:
                pass

_load_env()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if os.path.exists("/opt/airflow"):
    DATA_DIR = os.environ.get("AIRFLOW_DATA_DIR", "/opt/airflow/data/raw")
    RAW_DATA_DIR = DATA_DIR if DATA_DIR.endswith("raw") else os.path.join(DATA_DIR, "raw")
else:
    raw_path_env = os.environ.get("RAW_DATA_PATH", "data/raw")
    RAW_DATA_DIR = raw_path_env if os.path.isabs(raw_path_env) else os.path.join(BASE_DIR, raw_path_env)
    DATA_DIR = os.path.dirname(RAW_DATA_DIR)

SILVER_DATA_DIR = os.path.join(DATA_DIR, "silver")
QUARANTINE_DATA_DIR = os.path.join(DATA_DIR, "quarantine")

TIMEZONE = "Africa/Cairo"
CURRENCY = "EGP"

# Warehouses (Clean control tables R03)
WAREHOUSES = [
    {
        "warehouse_id": "WH01",
        "warehouse_name": "Greater Cairo Central DC (6th of October)",
        "governorate": "Giza",
        "city": "6th of October",
        "timezone": "Africa/Cairo",
        "capacity_pallet_positions": 18000,
        "has_chilled_zone": True,
        "operating_days": "SAT-THU",
        "order_cutoff_local": "17:00",
        "opened_date": "2021-01-15",
        "status": "ACTIVE",
        "updated_ts": "2024-01-01 00:00:00+02:00"
    },
    {
        "warehouse_id": "WH02",
        "warehouse_name": "Alexandria & Coastal Distribution Hub",
        "governorate": "Alexandria",
        "city": "Borg El Arab",
        "timezone": "Africa/Cairo",
        "capacity_pallet_positions": 12000,
        "has_chilled_zone": True,
        "operating_days": "SAT-THU",
        "order_cutoff_local": "16:00",
        "opened_date": "2021-08-01",
        "status": "ACTIVE",
        "updated_ts": "2024-01-01 00:00:00+02:00"
    },
    {
        "warehouse_id": "WH03",
        "warehouse_name": "Upper Egypt Regional Logistics Hub",
        "governorate": "Assiut",
        "city": "Assiut Industrial Zone",
        "timezone": "Africa/Cairo",
        "capacity_pallet_positions": 8500,
        "has_chilled_zone": True,
        "operating_days": "SAT-THU",
        "order_cutoff_local": "15:00",
        "opened_date": "2022-03-10",
        "status": "ACTIVE",
        "updated_ts": "2024-01-01 00:00:00+02:00"
    }
]

EGYPTIAN_GOVERNORATES = [
    "Cairo", "Giza", "Alexandria", "Qalyubia", "Dakahlia", "Sharqia", "Gharbia",
    "Monufia", "Beheira", "Ismailia", "Suez", "Port Said", "Fayoum", "Beni Suef",
    "Minya", "Assiut", "Sohag", "Qena", "Luxor", "Aswan",
    "Damietta", "Kafr El Sheikh", "Matrouh", "Red Sea", "New Valley",
    "North Sinai", "South Sinai"
]

# Which DC serves which governorate (drives customer.assigned_warehouse_id)
GOVERNORATE_WAREHOUSE = {
    **{g: "WH01" for g in ["Cairo", "Giza", "Qalyubia", "Sharqia", "Monufia", "Ismailia",
                           "Suez", "Red Sea", "North Sinai", "South Sinai"]},
    **{g: "WH02" for g in ["Alexandria", "Beheira", "Gharbia", "Dakahlia", "Port Said",
                           "Damietta", "Kafr El Sheikh", "Matrouh"]},
    **{g: "WH03" for g in ["Fayoum", "Beni Suef", "Minya", "Assiut", "Sohag", "Qena",
                           "Luxor", "Aswan", "New Valley"]},
}
# Rough population weight for customer placement (default 2)
GOVERNORATE_WEIGHT = {"Cairo": 18, "Giza": 12, "Alexandria": 10, "Qalyubia": 8, "Dakahlia": 6,
                      "Sharqia": 6, "Gharbia": 4, "Monufia": 4, "Beheira": 4, "Minya": 3}

CUSTOMER_SEGMENTS = [
    "SUPERMARKET", "MINI_MARKET", "WHOLESALER", "HORECA", "ECOMMERCE"
]

CUSTOMER_SEGMENT_VARIANTS = {
    "SUPERMARKET": ["Supermarket", "SUPER_MARKET", "SuperMarket", "SUPERMARKET"],
    "MINI_MARKET": ["Mini Market", "mini_market", "Mini-Market", "MINI_MARKET"],
    "WHOLESALER": ["Wholesaler", "WHOLESALE", "wholesaler", "WHOLESALER"],
    "HORECA": ["Horeca", "Hotel & Restaurant", "horeca", "HORECA"],
    "ECOMMERCE": ["E-Commerce", "eCommerce", "Online Store", "ECOMMERCE"]
}

STORAGE_TYPES = ["AMBIENT", "CHILLED", "FROZEN"]

BRANDS = [
    "Nile Harvest", "El-Baraka Foods", "Delta Mills", "Pyramid Dairy",
    "Cleopatra Beverage", "Pharaoh Sweets", "Al-Ahram Spices", "Oasis Waters",
    "Sinai Herbs", "Alexandria Milling", "Giza Valley", "Nubia Gold", "Cairo Brew"
]

PROFILES = {
    "test": {
        "num_products": 60,
        "num_suppliers": 15,
        "num_customers": 80,
        "num_sales_orders": 350,
        "num_purchase_orders": 60,
        "num_inventory_txns": 800,
        "num_snapshot_days": 14,
        "start_date": date(2024, 1, 1),
        "end_date": date(2024, 1, 31)
    },
    "medium": {
        "num_products": 250,
        "num_suppliers": 35,
        "num_customers": 400,
        "num_sales_orders": 2500,
        "num_purchase_orders": 400,
        "num_inventory_txns": 8000,
        "num_snapshot_days": 60,
        "start_date": date(2024, 1, 1),
        "end_date": date(2024, 6, 30)
    },
    "full": {
        "num_products": 800,
        "num_suppliers": 40,
        "num_customers": 2400,
        "num_sales_orders": 25000,
        "num_purchase_orders": 2000,
        "num_inventory_txns": 60000,
        "num_snapshot_days": 365,
        "start_date": date(2024, 1, 1),
        "end_date": date(2025, 12, 31)
    }
}
