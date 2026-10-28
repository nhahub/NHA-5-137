"""
Defect Injection Engine for Smart Warehouse Raw Layer.
Simulates real-world ERP, WMS, and human operator data defects according to the project blueprint.
"""

import random
import numpy as np
import pandas as pd

def inject_whitespace_and_case(series: pd.Series, prob: float = 0.15) -> pd.Series:
    """Injects leading/trailing spaces and weird casing into text columns."""
    result = series.copy().astype(str)
    for idx in result.index:
        if random.random() < prob and result.at[idx] not in ["None", "nan", ""]:
            val = result.at[idx]
            case_choice = random.choice(["upper", "lower", "mixed", "spaces"])
            if case_choice == "upper":
                val = val.upper()
            elif case_choice == "lower":
                val = val.lower()
            elif case_choice == "mixed":
                val = "".join(c.upper() if i % 2 == 0 else c.lower() for i, c in enumerate(val))
            elif case_choice == "spaces":
                val = f"  {val}   "
            result.at[idx] = val
    return result

def calculate_ean13_checksum(code12: str) -> str:
    """Calculates valid EAN-13 check digit."""
    digits = [int(d) for d in code12]
    odd_sum = sum(digits[0::2])
    even_sum = sum(digits[1::2])
    total = odd_sum + (even_sum * 3)
    mod = total % 10
    return str((10 - mod) % 10)

def generate_valid_ean13(seq: int) -> str:
    """Generates a valid 13-digit EAN code."""
    prefix_12 = f"622{seq:09d}"[:12] # 622 is Egypt GS1 country code prefix!
    check_digit = calculate_ean13_checksum(prefix_12)
    return prefix_12 + check_digit

def corrupt_ean13(ean: str) -> str:
    """Corrupts the check digit to create an invalid barcode."""
    if len(ean) == 13:
        original_check = int(ean[-1])
        corrupted_check = (original_check + random.randint(1, 9)) % 10
        return ean[:-1] + str(corrupted_check)
    return ean

def inject_duplicates(df: pd.DataFrame, prob: float = 0.005) -> pd.DataFrame:
    """Duplicates a percentage of rows to simulate duplicate data extraction."""
    if len(df) == 0:
        return df
    num_dups = max(1, int(len(df) * prob))
    dup_rows = df.sample(n=num_dups, replace=True)
    return pd.concat([df, dup_rows], ignore_index=True)

def inject_orphan_foreign_keys(series: pd.Series, orphan_id: str, prob: float = 0.005) -> pd.Series:
    """Injects orphan foreign keys pointing to non-existent parent records."""
    result = series.copy()
    for idx in result.index:
        if random.random() < prob:
            result.at[idx] = orphan_id
    return result

def inject_nulls(series: pd.Series, prob: float = 0.04) -> pd.Series:
    """Injects null/None into a series."""
    result = series.copy()
    for idx in result.index:
        if random.random() < prob:
            result.at[idx] = None
    return result
