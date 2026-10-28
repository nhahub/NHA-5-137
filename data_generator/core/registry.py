"""
Defect Registry = single place to inject AND record defects.
Every injected defect is logged (ID, table, row key, column, before/after) to
defect_ground_truth.json, used later to score the Glue pipeline.

Rules: rows are chosen randomly by RATE (never df.loc[0]); no-op when disabled;
mutators receive (value, row, rng) and return the new value.
"""
from __future__ import annotations
import json
import math
import random
from collections import Counter
from dataclasses import dataclass, asdict
from typing import Any, Callable

import pandas as pd


@dataclass(frozen=True)
class DefectSpec:
    id: str                         # e.g. "DEF-PRC-001"
    table: str                      # raw table name
    column: str | None              # None for row-level defects
    description: str
    rate: float                     # target share of rows (0.001 = 0.1%)
    expected_action: str = "flag"   # reject | flag | fix  (what Glue SHOULD do)
    min_rows: int = 1               # guarantee existence on small profiles


Mutator = Callable[[Any, pd.Series, random.Random], Any]

# ---- reusable mutators ---------------------------------------------------
def to_null(v, row, rng): return None
def set_value(x): return lambda v, row, rng: x
def scale(f): return lambda v, row, rng: round(v * f, 4) if pd.notna(v) else v
def add(n): return lambda v, row, rng: v + n if pd.notna(v) else v

def messy_case(v, row, rng):
    if not isinstance(v, str):
        return v
    return rng.choice([v.upper(), v.lower(), f"  {v}   ", v.title().replace(" ", "  ")])

def typo(v, row, rng):
    """Realistic misspelling: drop / double / swap / vowel-replace one character."""
    if not isinstance(v, str) or len(v) < 4:
        return v
    for _ in range(10):
        i = rng.randrange(1, len(v) - 1)
        op = rng.choice(["drop", "double", "swap", "vowel"])
        if op == "drop":
            out = v[:i] + v[i + 1:]
        elif op == "double":
            out = v[:i] + v[i] + v[i:]
        elif op == "swap":
            out = v[:i] + v[i + 1] + v[i] + v[i + 2:]
        else:
            out = v[:i] + rng.choice("aeiou") + v[i + 1:]
        if out != v:
            return out
    return v + v[-1]


def _s(x):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    return str(x)


class DefectRegistry:
    def __init__(self, rng: random.Random, enabled: bool = True):
        self.rng = rng
        self.enabled = enabled
        self.events: list[dict] = []
        self.specs: dict[str, DefectSpec] = {}

    # -- selection ---------------------------------------------------------
    def pick_rows(self, df: pd.DataFrame, spec: DefectSpec, mask: pd.Series | None = None) -> list:
        pool = list(df.index[mask]) if mask is not None else list(df.index)
        if not pool:
            return []
        n = max(spec.min_rows, round(len(df) * spec.rate))
        return self.rng.sample(pool, k=min(n, len(pool)))

    @staticmethod
    def _key(row: pd.Series, key_cols: list[str]) -> str:
        return "|".join(str(row[c]) for c in key_cols)

    def log(self, spec: DefectSpec, key: str, before=None, after=None, kind: str = "mutate"):
        self.specs[spec.id] = spec
        self.events.append({"defect_id": spec.id, "table": spec.table, "row_key": key,
                            "column": spec.column, "kind": kind,
                            "before": _s(before), "after": _s(after)})

    # -- injection ops -----------------------------------------------------
    def mutate(self, df, spec, key_cols, mutator: Mutator, mask=None):
        """Change spec.column on a random subset of rows."""
        if not self.enabled or df.empty:
            return df
        df = df.copy()
        for idx in self.pick_rows(df, spec, mask):
            row = df.loc[idx]
            before = row[spec.column]
            after = mutator(before, row, self.rng)
            df.at[idx, spec.column] = after
            self.log(spec, self._key(row, key_cols), before, after)
        return df

    def mutate_multi(self, df, spec, key_cols, fn: Callable[[pd.Series, random.Random], dict], mask=None):
        """Change several columns at once; fn(row, rng) -> {col: new_value}."""
        if not self.enabled or df.empty:
            return df
        df = df.copy()
        for idx in self.pick_rows(df, spec, mask):
            row = df.loc[idx]
            changes = fn(row, self.rng)
            before = {c: _s(row[c]) for c in changes}
            for c, v in changes.items():
                df.at[idx, c] = v
            self.log(spec, self._key(row, key_cols), json.dumps(before), json.dumps({c: _s(v) for c, v in changes.items()}))
        return df

    def duplicate(self, df, spec, key_cols, mutator_cols: dict[str, Mutator] | None = None, mask=None):
        """Append copies of random rows (optionally altered, e.g. a conflicting version)."""
        if not self.enabled or df.empty:
            return df
        copies = []
        for idx in self.pick_rows(df, spec, mask):
            row = df.loc[idx].copy()
            for col, m in (mutator_cols or {}).items():
                row[col] = m(row[col], row, self.rng)
            copies.append(row)
            self.log(spec, self._key(row, key_cols), None, "duplicate_row", kind="duplicate")
        return pd.concat([df, pd.DataFrame(copies)], ignore_index=True) if copies else df

    def drop_rows(self, df, spec, key_cols, mask=None):
        """Delete random rows (missing days, lost records)."""
        if not self.enabled or df.empty:
            return df
        idxs = self.pick_rows(df, spec, mask)
        for idx in idxs:
            self.log(spec, self._key(df.loc[idx], key_cols), "row_present", "row_missing", kind="drop")
        return df.drop(index=idxs).reset_index(drop=True)

    # -- output ------------------------------------------------------------
    def summary(self) -> dict:
        c = Counter(e["defect_id"] for e in self.events)
        return {d: {"count": n, **asdict(self.specs[d])} for d, n in sorted(c.items())}

    def save(self, path: str, meta: dict | None = None):
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"meta": meta or {}, "summary": self.summary(), "events": self.events},
                      f, indent=2, ensure_ascii=False)
