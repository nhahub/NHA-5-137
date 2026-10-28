"""
Defect injection for the simulated transactional tables (orders, procurement, inventory).
Runs AFTER the simulation, so the clean data is internally consistent and every defect
is a deliberate, logged deviation (ground truth in defect_ground_truth.json).
Rates follow the original blueprint comments.
"""
from datetime import date, datetime, timedelta
import pandas as pd
from data_generator.core import timeutil as tu
from data_generator.core.registry import DefectSpec as S, set_value, to_null, add


def _shift(ts_str, **kw):
    return tu.fmt(datetime.fromisoformat(ts_str) + timedelta(**kw))


def inject_simulation_defects(f: dict, sup_products_df: pd.DataFrame, reg) -> dict:
    f = dict(f)

    # ------------------------------------------------------------------ orders
    so, k = f["sales_orders"], ["sales_order_id"]
    so = reg.mutate(so, S("DEF-SO-003", "sales_orders", "total_net_amount_egp",
                          "header total differs from sum of lines", 0.003, "flag"), k, add(500.0))
    so = reg.duplicate(so, S("DEF-SO-001", "sales_orders", None, "exact duplicate order header", 0.004, "fix"), k)
    so = reg.duplicate(so, S("DEF-SO-002", "sales_orders", None,
                             "stale older version of an order re-sent (keep latest source_updated_ts)", 0.03, "fix"), k,
                       {"order_status": lambda v, row, r: "ALLOCATED",
                        "source_updated_ts": lambda v, row, r: row["order_ts"]},
                       mask=so["order_status"].isin(["DELIVERED", "SHIPPED"]))
    f["sales_orders"] = so

    # ------------------------------------------------------------------ order lines
    sl, k = f["sales_order_lines"], ["sales_order_id", "line_no"]
    ship_ts = {(r.sales_order_id, r.line_no): r.shipped_ts for r in sl.itertuples() if isinstance(r.shipped_ts, str)}
    ship_qty = {(r.sales_order_id, r.line_no): r.qty_shipped for r in sl.itertuples()}
    sl = reg.mutate(sl, S("DEF-SOL-003", "sales_order_lines", "qty_shipped",
                          "qty_shipped greater than qty_ordered", 0.0005, "flag"), k,
                    lambda v, row, r: row["qty_ordered"] + r.randint(5, 20), mask=sl["qty_shipped"] > 0)
    sl = reg.mutate(sl, S("DEF-SOL-004", "sales_order_lines", "qty_ordered",
                          "negative ordered quantity", 0.0002, "reject"), k, lambda v, row, r: -abs(v))
    sl = reg.mutate(sl, S("DEF-SOL-005", "sales_order_lines", "unit_net_price_egp",
                          "net price inconsistent with list price and discount", 0.005, "flag"), k,
                    lambda v, row, r: round(row["list_price_egp"] * r.uniform(0.4, 0.8), 2),
                    mask=sl["discount_pct"] == 0)
    sl = reg.mutate(sl, S("DEF-SOL-006", "sales_order_lines", "promo_id",
                          "promo_id missing while discount_pct > 0", 0.01, "flag"), k, to_null,
                    mask=(sl["discount_pct"] > 0) & sl["promo_id"].notna())
    sl = reg.mutate(sl, S("DEF-SOL-007", "sales_order_lines", "line_status",
                          "line marked SHIPPED although qty_shipped < qty_ordered", 0.005, "flag"), k,
                    set_value("SHIPPED"), mask=sl["line_status"] == "SHORT_SHIPPED")
    sl = reg.duplicate(sl, S("DEF-SOL-001", "sales_order_lines", None, "duplicate order line", 0.003, "fix"), k)
    f["sales_order_lines"] = sl

    # ------------------------------------------------------------------ returns
    rt, k = f["sales_returns"], ["return_line_id"]
    if not rt.empty:
        rkey = lambda row: (row["sales_order_id"], row["line_no"])
        rt = reg.mutate(rt, S("DEF-RET-001", "sales_returns", "return_ts",
                              "return timestamp before shipment", 0.001, "reject"), k,
                        lambda v, row, r: _shift(ship_ts[rkey(row)], days=-r.randint(1, 10)),
                        mask=rt.apply(lambda x: rkey(x) in ship_ts, axis=1))
        rt = reg.mutate(rt, S("DEF-RET-002", "sales_returns", "qty_returned",
                              "qty_returned greater than qty_shipped", 0.002, "flag"), k,
                        lambda v, row, r: int(ship_qty.get(rkey(row), 1)) + r.randint(1, 50))
        rt = reg.mutate(rt, S("DEF-RET-004", "sales_returns", "disposition",
                              "EXPIRED/DAMAGED return marked RESTOCK", 0.05, "fix"), k, set_value("RESTOCK"),
                        mask=rt["return_reason"].isin(["EXPIRED", "DAMAGED"]) & (rt["disposition"] != "RESTOCK"))
        rt = reg.mutate(rt, S("DEF-RET-003", "sales_returns", "sales_order_id",
                              "return references a non-existent order", 0.003, "reject"), k,
                        lambda v, row, r: f"SO{9000000000 + r.randint(0, 999999):010d}")
    f["sales_returns"] = rt

    # ------------------------------------------------------------------ purchasing
    po, k = f["purchase_orders"], ["purchase_order_id"]
    po_sup = po.set_index("purchase_order_id")["supplier_id"].to_dict()
    received = po["po_status"].isin(["CLOSED", "PARTIALLY_RECEIVED"])
    po = reg.mutate(po, S("DEF-PO-002", "purchase_orders", "po_status",
                          "status lags reality (OPEN although goods received)", 0.01, "fix"), k,
                    set_value("OPEN"), mask=received)
    po = reg.mutate(po, S("DEF-PO-003", "purchase_orders", "po_status",
                          "CANCELLED PO that has receipts", 0.001, "flag"), k, set_value("CANCELLED"),
                    mask=po["po_status"].isin(["CLOSED", "PARTIALLY_RECEIVED"]))
    po = reg.duplicate(po, S("DEF-PO-001", "purchase_orders", None, "duplicate PO header", 0.005, "fix"), k)
    f["purchase_orders"] = po

    pl, k = f["purchase_order_lines"], ["purchase_order_id", "po_line_no"]
    moq = {(r.supplier_id, r.product_id): int(r.moq_cases) for r in sup_products_df.drop_duplicates(["supplier_id", "product_id"]).itertuples()}
    moq_of = lambda row: moq.get((po_sup.get(row["purchase_order_id"]), row["product_id"]), 1)
    pl = reg.mutate(pl, S("DEF-POL-001", "purchase_order_lines", "promised_delivery_date",
                          "promised delivery date missing", 0.06, "flag"), k, to_null)
    pl = reg.mutate(pl, S("DEF-POL-002", "purchase_order_lines", "qty_ordered_uom",
                          "ordered quantity below supplier MOQ", 0.003, "flag"), k,
                    lambda v, row, r: max(1, moq_of(row) - 1),
                    mask=pl.apply(lambda x: moq_of(x) > 1, axis=1))
    f["purchase_order_lines"] = pl

    gr, k = f["goods_receipt_lines"], ["receipt_id", "receipt_line_no"]
    gr = reg.mutate_multi(gr, S("DEF-GR-001", "goods_receipt_lines", "purchase_order_id|po_line_no",
                                "blind receipt (no PO reference)", 0.003, "flag"), k,
                          lambda row, r: {"purchase_order_id": None, "po_line_no": None})
    gr = reg.mutate(gr, S("DEF-GR-002", "goods_receipt_lines", "qty_received_uom",
                          "over-delivery versus ordered quantity", 0.02, "flag"), k,
                    lambda v, row, r: int(v * r.uniform(1.3, 2.0)) + 1)
    gr = reg.mutate(gr, S("DEF-GR-003", "goods_receipt_lines", "expiry_date",
                          "expiry date before received date", 0.001, "reject"), k,
                    lambda v, row, r: (date.fromisoformat(row["received_ts"][:10]) - timedelta(days=r.randint(10, 400))).isoformat())
    f["goods_receipt_lines"] = gr

    bt, k = f["product_batches"], ["batch_id"]
    bt = reg.mutate(bt, S("DEF-BAT-001", "product_batches", "expiry_date",
                          "expiry before production date", 0.001, "reject"), k,
                    lambda v, row, r: (date.fromisoformat(row["production_date"]) - timedelta(days=r.randint(1, 60))).isoformat())
    nums = bt[["batch_number", "product_id"]].to_dict("records")
    def reuse(v, row, r):
        other = [n for n in r.sample(nums, k=min(8, len(nums))) if n["product_id"] != row["product_id"]]
        return other[0]["batch_number"] if other else v
    bt = reg.mutate(bt, S("DEF-BAT-002", "product_batches", "batch_number",
                          "batch number reused across products/suppliers", 0.005, "flag"), k, reuse)
    f["product_batches"] = bt

    # ------------------------------------------------------------------ inventory ledger
    lg, k = f["inventory_transactions"], ["txn_id"]
    lg = reg.mutate(lg, S("DEF-INV-001", "inventory_transactions", "qty_change_base",
                          "sign error on quantity", 0.001, "flag"), k, lambda v, row, r: -v, mask=lg["qty_change_base"] != 0)
    lg = reg.mutate(lg, S("DEF-INV-002", "inventory_transactions", "reference_id",
                          "missing reference_id", 0.001, "flag"), k, to_null)
    lg = reg.mutate(lg, S("DEF-INV-003", "inventory_transactions", "reason_code",
                          "ADJUSTMENT without reason code", 0.001, "flag"), k, to_null, mask=lg["txn_type"] == "ADJUSTMENT")
    lg = reg.mutate(lg, S("DEF-INV-004", "inventory_transactions", "posted_ts",
                          "posted_ts earlier than event_ts (clock skew)", 0.001, "flag"), k,
                    lambda v, row, r: _shift(row["event_ts"], minutes=-r.randint(5, 600)))
    lg = reg.duplicate(lg, S("DEF-INV-005", "inventory_transactions", None, "duplicate ledger event", 0.003, "fix"), k)
    f["inventory_transactions"] = lg

    # ------------------------------------------------------------------ snapshots
    sn, k = f["inventory_snapshots"], ["snapshot_date", "warehouse_id", "product_id"]
    if not sn.empty:
        prev = {(r.snapshot_date, r.warehouse_id, r.product_id): (r.on_hand_qty, r.allocated_qty) for r in sn.itertuples()}
        first = sn["snapshot_date"].min()
        def stale(row, r):
            p = prev.get(((date.fromisoformat(row["snapshot_date"]) - timedelta(days=1)).isoformat(),
                          row["warehouse_id"], row["product_id"]))
            return {"on_hand_qty": p[0], "allocated_qty": p[1]} if p else {}
        sn = reg.mutate(sn, S("DEF-SNP-001", "inventory_snapshots", "on_hand_qty",
                              "negative on_hand_qty", 0.0005, "flag"), k, lambda v, row, r: -r.randint(1, 20))
        sn = reg.mutate_multi(sn, S("DEF-SNP-002", "inventory_snapshots", "on_hand_qty|allocated_qty",
                                    "stale copy of previous day's snapshot", 0.005, "flag"), k, stale,
                              mask=sn["snapshot_date"] > first)
        days = pd.DataFrame({"d": sorted(sn["snapshot_date"].unique())})
        spec = S("DEF-SNP-003", "inventory_snapshots", None, "entire snapshot day missing", 0.01, "flag")
        if reg.enabled:
            gone = [days.at[i, "d"] for i in reg.pick_rows(days, spec)]
            for d in gone:
                reg.log(spec, d, "day_present", "day_missing", kind="drop")
            sn = sn[~sn["snapshot_date"].isin(gone)].reset_index(drop=True)
    f["inventory_snapshots"] = sn

    # ------------------------------------------------------------------ stockouts
    st, k = f["stockout_events"], ["stockout_id"]
    if not st.empty:
        st = reg.mutate(st, S("DEF-STK-001", "stockout_events", "end_ts",
                              "episode never closed (end_ts missing)", 0.02, "flag"), k, to_null, mask=st["end_ts"].notna())
        st = reg.duplicate(st, S("DEF-STK-002", "stockout_events", None,
                                 "overlapping duplicate episode for same warehouse-product", 0.03, "fix"), k,
                           {"stockout_id": lambda v, row, r: f"STK9{r.randint(0, 999999):06d}"})
    f["stockout_events"] = st
    return f
