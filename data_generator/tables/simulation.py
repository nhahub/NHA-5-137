"""
Supply-chain simulation (Phase 2). ONE daily loop produces, from a real stock state:
  R07 sales_orders, R08 sales_order_lines, R09 sales_returns,
  R10 purchase_orders, R11 purchase_order_lines, R12 goods_receipt_lines, R19 product_batches,
  R13 inventory_transactions (ledger), R14 inventory_snapshots, R18 stockout_events.

Invariants of the CLEAN data (before defect injection):
  * ledger running balance per (warehouse, product) never goes negative
  * snapshot on_hand == cumulative ledger == sum of batch quantities
  * stockouts open only when a line is really short (ATP = 0) and close on replenishment
  * issues follow FEFO and carry the real batch_id (B000123), receipts create batches for ALL products
  * no orders/shipments/receipts on Fridays or warehouse-closing holidays
  * return disposition is derived from return_reason; expired batches can't be restocked
  * source_updated_ts follows the real lifecycle; batch status follows expiry/quarantine
Defects are injected afterwards via the DefectRegistry (ground-truth logged).
"""
from __future__ import annotations
import heapq
import math
from collections import defaultdict
from datetime import date, datetime, timedelta

import pandas as pd

from data_generator.config import WAREHOUSES, CUSTOMER_SEGMENT_VARIANTS
from data_generator.core import timeutil as tu
from data_generator.core.registry import DefectSpec, set_value, to_null, add

_SEG = {v: k for k, vs in CUSTOMER_SEGMENT_VARIANTS.items() for v in vs}
SEG_CASES = {   # order size in CASES by customer segment
    "WHOLESALER": [5, 10, 20, 50], "SUPERMARKET": [2, 5, 10, 20], "HORECA": [1, 2, 5, 10],
    "MINI_MARKET": [1, 2, 3, 5], "ECOMMERCE": [1, 1, 2, 3],
}
RETURN_REASONS = [("CUSTOMER_REMORSE", 0.30), ("DAMAGED", 0.25), ("WRONG_ITEM", 0.20),
                  ("QUALITY", 0.15), ("EXPIRED", 0.10)]
# disposition is DERIVED from the reason (expired/damaged goods are never restocked)
DISPOSITION = {
    "EXPIRED": [("WRITE_OFF", 0.7), ("SUPPLIER_RETURN", 0.3)],
    "DAMAGED": [("WRITE_OFF", 0.6), ("SUPPLIER_RETURN", 0.4)],
    "QUALITY": [("SUPPLIER_RETURN", 0.6), ("WRITE_OFF", 0.4)],
    "WRONG_ITEM": [("RESTOCK", 0.95), ("WRITE_OFF", 0.05)],
    "CUSTOMER_REMORSE": [("RESTOCK", 0.85), ("WRITE_OFF", 0.15)],
}
P1_END = date(2024, 6, 30)


def _wchoice(rng, pairs):
    return rng.choices([p[0] for p in pairs], weights=[p[1] for p in pairs])[0]


def run_simulation(cfg, products_df, customers_df, warehouses_df, suppliers_df, sup_products_df,
                   catalog, promos_df, calendar_df, hub):
    R_plan, R_ops, R_proc, R_post = hub.py("sim_plan"), hub.py("sim_ops"), hub.py("sim_proc"), hub.py("sim_post")
    start, end = cfg["start_date"], cfg["end_date"]
    horizon_end = datetime(end.year, end.month, end.day, 23, 59, 59, tzinfo=tu.CAIRO)
    all_days = [start + timedelta(i) for i in range((end - start).days + 1)]
    wh_ids = warehouses_df["warehouse_id"].tolist()
    wh_spec = {w["warehouse_id"]: w["operating_days"] for w in WAREHOUSES}
    holidays = tu.holiday_dates(calendar_df.to_dict("records"))

    def is_op(d, wh="WH01"): return tu.is_operating_day(d, wh_spec[wh], holidays)
    def next_op(d, wh="WH01"): return tu.next_operating_day(d, wh_spec[wh], holidays)
    def at(d, h, m=0): return datetime(d.year, d.month, d.day, h, m, tzinfo=tu.CAIRO)

    # ---------------- reference lookups ----------------
    pinfo = {}
    for r in products_df.itertuples():
        shelf = int(r.shelf_life_days) if pd.notna(r.shelf_life_days) else \
            {"CHILLED": 14, "FROZEN": 270}.get(r.storage_type, 30)   # fallback if defect nulled it
        disc = date.fromisoformat(r.discontinued_date) if isinstance(r.discontinued_date, str) else None
        pinfo[r.product_id] = {"upc": int(r.units_per_case), "shelf": shelf, "disc": disc}
    pids = list(pinfo)
    sup_status = suppliers_df.set_index("supplier_id")["status"].to_dict()
    sup_terms = suppliers_df.set_index("supplier_id")["payment_terms_days"].to_dict()

    # supplier choice per product: primary first, active suppliers only
    spo = sup_products_df.drop_duplicates(["supplier_id", "product_id"])
    sp_rows = defaultdict(list)
    for r in spo.sort_values("is_primary", ascending=False, kind="stable").itertuples():
        sp_rows[r.product_id].append(r)
    supplier_for = {}
    for pid in pids:
        rows = sp_rows.get(pid, [])
        act = [r for r in rows if sup_status.get(r.supplier_id) == "ACTIVE"]
        supplier_for[pid] = (act or rows or [None])[0]

    promo_by_pid = defaultdict(list)
    for r in promos_df.itertuples():
        promo_by_pid[r.product_id].append((r.start_date, r.end_date,
                                           r.warehouse_id if pd.notna(r.warehouse_id) else None,
                                           r.promo_id, float(r.discount_pct)))

    def find_promo(pid, ds, wh):
        for s, e, w, pr, dp in promo_by_pid.get(pid, []):
            if s <= ds <= e and (w is None or w == wh):
                return pr, dp
        return None, 0.0

    def list_price(pid, d):
        c = catalog[pid]
        return c["p1"] if d <= P1_END else c["p2"]

    # ---------------- demand plan (what customers WILL ask for) ----------------
    demand_mult = defaultdict(lambda: 1.0)
    for r in calendar_df.itertuples():
        if not r.closes_warehouse:
            s, e = date.fromisoformat(r.start_date), date.fromisoformat(r.end_date)
            m = 1.5 if "White Friday" in r.event_name else 1.25 if r.expected_demand_effect == "UP" else 1.0
            for i in range((e - s).days + 1):
                demand_mult[s + timedelta(i)] = max(demand_mult[s + timedelta(i)], m)
    op_days = [d for d in all_days if is_op(d)]
    dw = [demand_mult[d] * (1.2 if d.weekday() == 5 else 0.85 if d.weekday() == 3 else 1.0)
          * R_plan.uniform(0.8, 1.2) for d in op_days]

    popularity = {pid: R_plan.lognormvariate(0, 0.8) for pid in pids}
    pop_w = [popularity[p] for p in pids]
    # assortment: fast movers stocked in every DC, the long tail only in 1-2 DCs
    ranked = sorted(pids, key=lambda p: -popularity[p])
    fast = set(ranked[: max(1, int(len(ranked) * 0.45))])
    assort = {wh: {p for p in pids if p in fast or R_plan.random() < 0.3} for wh in wh_ids}
    active_cust = customers_df[customers_df["status"] == "ACTIVE"]
    cust_ids = active_cust["customer_id"].tolist()
    cust_wh = active_cust.set_index("customer_id")["assigned_warehouse_id"].to_dict()
    cust_seg = {c: _SEG.get(s, "MINI_MARKET") for c, s in
                active_cust.set_index("customer_id")["segment"].items()}

    n_target = cfg["num_sales_orders"]
    order_days = sorted(R_plan.choices(op_days, weights=dw, k=int(n_target * 1.25)))   # oversample, prune, trim
    plans = []
    n_days = len(all_days)
    for d in order_days:
        cid = R_plan.choice(cust_ids)
        wh = cust_wh[cid] if R_plan.random() < 0.95 else R_plan.choice(wh_ids)
        want, chosen = R_plan.randint(1, 5), []
        for _ in range(want * 12):
            p = R_plan.choices(pids, weights=pop_w)[0]
            if p in chosen or p not in assort[wh] or (pinfo[p]["disc"] and pinfo[p]["disc"] < d):
                continue
            chosen.append(p)
            if len(chosen) >= want:
                break
        if not chosen:
            continue
        cases = SEG_CASES[cust_seg[cid]]
        cancel = R_plan.choice(["CUSTOMER_REQUEST", "CREDIT_BLOCK", "DUPLICATE_ORDER"]) \
            if R_plan.random() < 0.04 else None
        plans.append({"day": d, "cust": cid, "wh": wh, "cancel": cancel,
                      "lines": [(p, pinfo[p]["upc"] * min(R_plan.choice(cases), 5 if pinfo[p]["shelf"] <= 30 else 99))
                                for p in chosen],     # short-life goods are bought in small lots
                      "ts": tu.business_ts(R_plan, d, 8, 20, peak_h=11),
                      "channel": R_plan.choice(["SALES_REP", "PORTAL", "PHONE"])})

    # Slow-moving short-life items are not stocked (a DC cannot hold them without waste):
    # drop keys whose expected demand within one shelf life is below a few cases.
    est = defaultdict(float)
    for p in plans:
        if not p["cancel"]:
            for pid, q in p["lines"]:
                est[(p["wh"], pid)] += q / n_days / 1.25
    def stockable(k):
        pi = pinfo[k[1]]
        need = 10 if pi["shelf"] <= 14 else 6 if pi["shelf"] <= 30 else 3 if pi["shelf"] <= 90 else 0
        return est[k] * pi["shelf"] >= need * pi["upc"]
    ok_key = {k: stockable(k) for k in est}
    pruned = []
    for p in plans:
        lines = [(pid, q) for pid, q in p["lines"] if ok_key.get((p["wh"], pid), True)]
        if lines:
            pruned.append({**p, "lines": lines})
    if len(pruned) > n_target:
        keep = sorted(R_plan.sample(range(len(pruned)), n_target))
        pruned = [pruned[i] for i in keep]
    plans = pruned
    rate, line_qtys = defaultdict(float), defaultdict(list)
    for i, p in enumerate(plans, start=1):
        p["so"] = f"SO{i:010d}"
        if not p["cancel"]:
            for pid, q in p["lines"]:
                rate[(p["wh"], pid)] += q / n_days
                line_qtys[(p["wh"], pid)].append(q)
    # demand is lumpy (case-multiple B2B orders): keep one typical large order (p90) as extra safety stock
    lumpy = {k: sorted(v)[min(len(v) - 1, int(0.9 * len(v)))] for k, v in line_qtys.items()}
    plan_by_day = defaultdict(list)
    for p in plans:
        plan_by_day[p["day"]].append(p)

    # ---------------- state ----------------
    keys = [(wh, p) for wh in wh_ids for p in pids if p in assort[wh] and ok_key.get((wh, p), False)]
    stock = {k: [] for k in keys}
    reserved = defaultdict(int)
    on_order = defaultdict(int)
    batches = {}
    expiry_heap, release_sched = [], defaultdict(list)
    ledger, led_n = [], [0]
    seq = {"batch": 0, "gr": 0, "po": 0, "ret": 0, "stk": 0}

    orders, order_lines, returns_rows = {}, [], []
    ship_sched, returns_sched = defaultdict(list), defaultdict(list)
    backorders, touched = defaultdict(list), set()   # keys that just got stock -> try to fill backorders
    ship_batches = {}
    pos, po_lines, receipts = {}, [], []
    receipt_plan = defaultdict(list)
    line_state = {}
    open_stk, stockouts = {}, []

    def led(ts, wh, pid, typ, qty, reftype, refid, batch_id, reason=None):
        led_n[0] += 1
        ledger.append({"ts": ts, "n": led_n[0], "wh": wh, "pid": pid, "typ": typ, "qty": qty,
                       "reftype": reftype, "refid": refid, "batch": batch_id, "reason": reason})

    def new_batch(wh, pid, number, prod_d, exp_d, qty, rid, rline, status, ts):
        seq["batch"] += 1
        b = {"id": f"B{seq['batch']:06d}", "wh": wh, "pid": pid, "number": number, "prod": prod_d, "exp": exp_d,
             "qty": qty, "recv": qty, "rid": rid, "rline": rline, "status": status, "upd": ts}
        batches[b["id"]] = b
        stock[(wh, pid)].append(b)
        heapq.heappush(expiry_heap, (exp_d, b["id"]))
        return b

    def onhand(k): return sum(b["qty"] for b in stock[k])
    def eligible(k, d): return [b for b in stock[k] if b["status"] == "ACTIVE" and b["exp"] > d and b["qty"] > 0]
    def physical(k, d): return sum(b["qty"] for b in eligible(k, d))
    def available(k, d): return physical(k, d) - reserved[k]

    def fefo(k, qty, d):
        taken = []
        for b in sorted(eligible(k, d), key=lambda x: (x["exp"], x["id"])):
            if qty <= 0:
                break
            t = min(qty, b["qty"])
            b["qty"] -= t
            qty -= t
            taken.append((b, t))
        return taken

    def close_stockout(k, d, ts):
        ep = open_stk.get(k)
        if ep and available(k, d) > 0:
            ep["end_ts"] = ts + timedelta(minutes=R_ops.randint(5, 40))
            ep["upd"] = ep["end_ts"]
            del open_stk[k]

    def open_stockout(k, ts, ref):
        if k in open_stk:
            return
        seq["stk"] += 1
        ep = {"id": f"STK{seq['stk']:07d}", "wh": k[0], "pid": k[1], "start_ts": ts, "end_ts": None,
              "ref": ref, "upd": ts}
        open_stk[k] = ep
        stockouts.append(ep)

    # ---------------- opening stock ----------------
    open_ts = at(start, 6)
    for k in keys:
        wh, pid = k
        pi = pinfo[pid]
        spr0 = supplier_for[pid]
        lt0 = int(spr0.contractual_lead_time_days) if spr0 is not None and pd.notna(spr0.contractual_lead_time_days) else 7
        cover = min(lt0 + min(10, 0.3 * pi["shelf"]) + R_ops.uniform(3, 20), max(3.0, pi["shelf"] * 0.6))   # starts above reorder point
        if rate[k] > 0:
            qty = max(pi["upc"], math.ceil(rate[k] * cover / pi["upc"]) * pi["upc"])
        else:
            qty = pi["upc"] * R_ops.choice([1, 2])
        age = R_ops.randint(1, max(2, int(pi["shelf"] * 0.4)))
        prod_d = start - timedelta(days=age)
        b = new_batch(wh, pid, f"LOT-{prod_d:%Y%m%d}-OPEN", prod_d, prod_d + timedelta(days=pi["shelf"]),
                      qty, None, None, "ACTIVE", open_ts)
        led(open_ts, wh, pid, "RECEIPT", qty, "INITIAL_LOAD", f"INIT-{wh}-{pid}", b["id"], "OPENING_BALANCE")

    # ---------------- daily loop ----------------
    snap_days = set(all_days[:min(cfg["num_snapshot_days"], len(all_days))])
    snapshots, adj_n = [], 0
    for d in all_days:
        # quarantine releases + expiry write-offs (system jobs, every calendar day)
        for bid in release_sched.pop(d, []):
            b = batches[bid]
            if b["status"] == "QUARANTINED":
                b["status"], b["upd"] = "ACTIVE", at(d, 8)
                close_stockout((b["wh"], b["pid"]), d, b["upd"])
                touched.add((b["wh"], b["pid"]))
        while expiry_heap and expiry_heap[0][0] < d:
            _, bid = heapq.heappop(expiry_heap)
            b = batches[bid]
            ts = at(d, 5, R_ops.randint(0, 30))
            if b["qty"] > 0:
                led(ts, b["wh"], b["pid"], "ADJUSTMENT", -b["qty"], "EXPIRY", f"EXP-{bid}", bid, "EXPIRY_WRITE_OFF")
                b["qty"] = 0
            b["status"], b["upd"] = "EXPIRED", ts

        if is_op(d):
            # --- goods receipts (early-morning dock window) ---
            for pr in sorted(receipt_plan.pop(d, []), key=lambda x: x["order"]):
                k = (pr["wh"], pr["pid"])
                pi = pinfo[pr["pid"]]
                seq["gr"] += 1
                rid = f"GR{seq['gr']:07d}"
                ts = tu.business_ts(R_proc, d, 6, 8)
                accepted = (pr["qty"] - pr["rej"]) * pi["upc"]
                prod_d = d - timedelta(days=R_proc.randint(1, max(2, int(pi["shelf"] * 0.35))))
                exp_d = prod_d + timedelta(days=pi["shelf"])
                number = f"LOT-{prod_d:%Y%m%d}-{R_proc.randint(100, 999)}"
                quarantined = R_proc.random() < 0.02
                b = new_batch(pr["wh"], pr["pid"], number, prod_d, exp_d, accepted, rid, 1,
                              "QUARANTINED" if quarantined else "ACTIVE", ts)
                if quarantined:
                    release_sched[next_op(d + timedelta(days=R_proc.randint(2, 5)))].append(b["id"])
                led(ts, pr["wh"], pr["pid"], "RECEIPT", accepted, "GR_LINE", f"{rid}:1", b["id"])
                on_order[k] -= pr["release"]
                receipts.append({"receipt_id": rid, "receipt_line_no": 1, "purchase_order_id": pr["po"],
                                 "po_line_no": pr["line"], "product_id": pr["pid"], "warehouse_id": pr["wh"],
                                 "received_ts": tu.fmt(ts), "supplier_delivery_note": f"DN-{R_proc.randint(10000, 99999)}",
                                 "qty_received_uom": pr["qty"], "qty_rejected_uom": pr["rej"], "order_uom": "CASE",
                                 "batch_number": number, "expiry_date": exp_d.isoformat(), "updated_ts": tu.fmt(ts)})
                st = line_state[(pr["po"], pr["line"])]
                st["done"] += 1
                st["last"] = ts
                pos[pr["po"]]["last"] = max(pos[pr["po"]]["last"], ts)
                close_stockout(k, d, ts)
                touched.add(k)

            # --- customer returns (restock only when the disposition says so) ---
            for rt in returns_sched.pop(d, []):
                ts = tu.business_ts(R_ops, d, 12, 16)
                b = batches[rt["batch"]]
                disp = rt["disp"]
                if disp == "RESTOCK" and (b["exp"] <= d or b["status"] != "ACTIVE"):
                    disp = "WRITE_OFF"
                seq["ret"] += 1
                rlid = f"RL{seq['ret']:09d}"
                if disp == "RESTOCK":
                    b["qty"] += rt["qty"]
                    led(ts, rt["wh"], rt["pid"], "RETURN_RESTOCK", rt["qty"], "RET_LINE", rlid, b["id"], rt["reason"])
                    close_stockout((rt["wh"], rt["pid"]), d, ts)
                    touched.add((rt["wh"], rt["pid"]))
                returns_rows.append({"return_line_id": rlid, "sales_order_id": rt["so"], "line_no": rt["line"],
                                     "product_id": rt["pid"], "warehouse_id": rt["wh"], "return_ts": tu.fmt(ts),
                                     "qty_returned": rt["qty"], "return_reason": rt["reason"], "disposition": disp,
                                     "refund_amount_egp": round(rt["qty"] * rt["net"], 2),
                                     "updated_ts": tu.fmt(ts + timedelta(hours=R_ops.randint(2, 5)))})

            # --- backorders: stock arrived -> allocate to the oldest open backorders first ---
            for k in list(backorders):
                for e in list(backorders[k]):
                    if (d - e["since"]).days > 14:               # supplier never delivered: give up
                        backorders[k].remove(e)
                        orders[e["so"]]["lines"][e["line"] - 1]["shortage_action"] = "CANCEL_REMAINDER"
                if not backorders[k]:
                    del backorders[k]
            for k in sorted(touched):
                for e in list(backorders.get(k, [])):
                    av = available(k, d)
                    if av <= 0:
                        break
                    q = min(e["rem"], av)
                    reserved[k] += q
                    orders[e["so"]]["lines"][e["line"] - 1]["qty_allocated"] += q
                    ship_sched[next_op(d + timedelta(days=1), k[0])].append(("BO", e["so"], e["line"], q))
                    e["rem"] -= q
                    if e["rem"] <= 0:
                        backorders[k].remove(e)
            touched.clear()

            # --- new orders (allocate against REAL availability) ---
            for pl in sorted(plan_by_day.pop(d, []), key=lambda x: x["ts"]):
                wh, so, ts = pl["wh"], pl["so"], pl["ts"]
                ds = d.isoformat()
                lines = []
                total_alloc = 0
                for ln, (pid, qty) in enumerate(pl["lines"], start=1):
                    k = (wh, pid)
                    alloc = 0 if pl["cancel"] else min(qty, max(0, available(k, d)))
                    reserved[k] += alloc
                    total_alloc += alloc
                    if not pl["cancel"] and alloc < qty:
                        open_stockout(k, ts, f"{so}:{ln}")
                    lp = list_price(pid, d)
                    promo, disc = find_promo(pid, ds, wh)
                    lines.append({"sales_order_id": so, "line_no": ln, "product_id": pid, "warehouse_id": wh,
                                  "qty_ordered": qty, "qty_allocated": alloc, "qty_shipped": 0, "uom": "EA",
                                  "list_price_egp": lp, "discount_pct": disc,
                                  "unit_net_price_egp": round(lp * (1 - disc / 100.0), 2), "promo_id": promo,
                                  "line_status": "ALLOCATED" if alloc > 0 else "SHORT_SHIPPED",
                                  "shortage_action": "NONE" if alloc >= qty else R_ops.choice(
                                      ["CANCEL_REMAINDER", "CANCEL_REMAINDER", "BACKORDER"]),
                                  "shipped_ts": None, "upd": ts})
                cancel_reason, cancelled_ts = pl["cancel"], None
                if not cancel_reason and total_alloc == 0:
                    cancel_reason = "OUT_OF_STOCK"
                if cancel_reason:
                    cancelled_ts = ts + timedelta(minutes=R_ops.randint(10, 300))
                    for l in lines:
                        reserved[(wh, l["product_id"])] -= l["qty_allocated"]
                        l.update(qty_allocated=0, line_status="CANCELLED", shortage_action="NONE", upd=cancelled_ts)
                orders[so] = {"so": so, "cust": pl["cust"], "wh": wh, "ts": ts, "channel": pl["channel"],
                              "req": (d + timedelta(days=R_ops.choice([1, 2]))).isoformat(),
                              "cancel_reason": cancel_reason, "cancelled_ts": cancelled_ts,
                              "status": "CANCELLED" if cancel_reason else "ALLOCATED",
                              "upd": cancelled_ts or ts, "lines": lines}
                if not cancel_reason:
                    ship_sched[next_op(d + timedelta(days=1), wh)].append(so)

            # --- shipments (FEFO, real batch ids) ---
            for so in ship_sched.pop(d, []):
                if isinstance(so, tuple):                          # second shipment of a backorder
                    _, bso, bln, bq = so
                    o = orders[bso]
                    l = o["lines"][bln - 1]
                    k = (o["wh"], l["product_id"])
                    reserved[k] -= bq
                    bts = tu.business_ts(R_ops, d, 11, 17)
                    taken = fefo(k, min(bq, physical(k, d)), d)
                    got = sum(t for _, t in taken)
                    for b, t in taken:
                        led(bts, o["wh"], l["product_id"], "ISSUE", -t, "SO_LINE", f"{bso}:{bln}", b["id"])
                    l["qty_shipped"] += got
                    if got:
                        l["shipped_ts"] = bts
                    l["upd"] = bts
                    l["line_status"] = "SHIPPED" if l["qty_shipped"] == l["qty_ordered"] else "SHORT_SHIPPED"
                    ship_batches[(bso, bln)] = ship_batches.get((bso, bln), []) + taken
                    if got < bq:                                    # expired meanwhile: stays on backorder
                        backorders[k].insert(0, {"so": bso, "line": bln, "rem": bq - got, "since": d})
                    delivered = bts + timedelta(hours=R_ops.uniform(2, 9))
                    o["status"], o["upd"] = ("DELIVERED", delivered) if delivered <= horizon_end else ("SHIPPED", bts)
                    continue
                o = orders[so]
                ship_ts = tu.business_ts(R_ops, d, 11, 17)
                for l in o["lines"]:
                    k = (o["wh"], l["product_id"])
                    reserved[k] -= l["qty_allocated"]
                    taken = fefo(k, min(l["qty_allocated"], physical(k, d)), d)
                    shipped = sum(t for _, t in taken)
                    lts = ship_ts + timedelta(seconds=30 * l["line_no"])
                    for b, t in taken:
                        led(lts, o["wh"], l["product_id"], "ISSUE", -t, "SO_LINE", f"{so}:{l['line_no']}", b["id"])
                    l["qty_shipped"] = shipped
                    l["line_status"] = "SHIPPED" if shipped == l["qty_ordered"] else "SHORT_SHIPPED"
                    l["shipped_ts"] = lts if shipped > 0 else None
                    l["upd"] = lts
                    ship_batches[(so, l["line_no"])] = taken
                    if taken and R_ops.random() < 0.03:
                        b0, t0 = taken[0]
                        reason = _wchoice(R_ops, RETURN_REASONS)
                        rd = next_op(d + timedelta(days=R_ops.randint(2, 7)), o["wh"])
                        returns_sched[rd].append({"so": so, "line": l["line_no"], "pid": l["product_id"],
                                                  "wh": o["wh"], "batch": b0["id"], "reason": reason,
                                                  "disp": _wchoice(R_ops, DISPOSITION[reason]),
                                                  "qty": R_ops.randint(1, max(1, min(t0, pinfo[l["product_id"]]["upc"] * 2))),
                                                  "net": l["unit_net_price_egp"]})
                    if shipped < l["qty_ordered"]:
                        if l["shortage_action"] == "NONE":          # short at pick time (e.g. batch expired)
                            l["shortage_action"] = "CANCEL_REMAINDER"
                        if l["shortage_action"] == "BACKORDER":
                            backorders[k].append({"so": so, "line": l["line_no"], "rem": l["qty_ordered"] - shipped, "since": d})
                        open_short = available(k, d) <= 0
                        if open_short:
                            open_stockout(k, lts, f"{so}:{l['line_no']}")
                delivered = ship_ts + timedelta(hours=R_ops.uniform(2, 9))
                if delivered <= horizon_end:
                    o["status"], o["upd"] = "DELIVERED", delivered
                else:
                    o["status"], o["upd"] = "SHIPPED", ship_ts

            # --- cycle counts / shrinkage (never below what is reserved) ---
            mean_adj = len(keys) / 200.0
            for _ in range(int(mean_adj * R_ops.uniform(0.5, 1.5) + R_ops.random())):
                k = R_ops.choice(keys)
                wh, pid = k
                adj_n += 1
                ts = at(d, 17, 30) + timedelta(minutes=R_ops.randint(0, 90))
                oh = onhand(k)
                if oh <= 0:
                    continue
                mag = max(1, round(oh * R_ops.uniform(0.005, 0.03)))
                if R_ops.random() < 0.6:
                    mag = min(mag, max(0, available(k, d)))
                    for b, t in fefo(k, mag, d):
                        led(ts, wh, pid, "ADJUSTMENT", -t, "COUNT", f"ADJ-{wh}-{d:%Y%m%d}-{adj_n:04d}", b["id"], "SHRINKAGE")
                else:
                    el = eligible(k, d)
                    if el:
                        b = max(el, key=lambda x: x["qty"])
                        b["qty"] += mag
                        led(ts, wh, pid, "ADJUSTMENT", mag, "COUNT", f"ADJ-{wh}-{d:%Y%m%d}-{adj_n:04d}", b["id"], "CYCLE_COUNT")

            # --- replenishment: reorder point -> purchase orders ---
            groups = defaultdict(list)
            for k in keys:
                r_ = rate[k]
                pid = k[1]
                spr = supplier_for[pid]
                if r_ <= 0 or spr is None or (pinfo[pid]["disc"] and pinfo[pid]["disc"] < d):
                    continue
                pi = pinfo[pid]
                lt = int(spr.contractual_lead_time_days) if pd.notna(spr.contractual_lead_time_days) else 7
                position = onhand(k) + on_order[k] - reserved[k]
                safety = min(10.0, 0.3 * pi["shelf"])           # short-life goods carry little safety stock
                lump = lumpy.get(k, 0) if pi["shelf"] > 30 else min(lumpy.get(k, 0), r_ * 0.4 * pi["shelf"])
                rop = r_ * (lt + safety) + lump
                if position <= rop:
                    target = max(rop + r_ * min(7.0, 0.2 * pi["shelf"]), r_ * min(30.0, 0.5 * pi["shelf"]) + lump)
                    cases = math.ceil((target - position) / pi["upc"])
                    mult, moq = max(1, int(spr.order_multiple_cases)), int(spr.moq_cases)
                    cases = max(moq, math.ceil(cases / mult) * mult)
                    groups[(k[0], spr.supplier_id)].append((pid, cases, spr, lt))
            for (wh, sid), items in sorted(groups.items()):
                seq["po"] += 1
                po_id = f"PO{seq['po']:07d}"
                po_ts = tu.business_ts(R_proc, d, 8, 15)
                pos[po_id] = {"po": po_id, "sid": sid, "wh": wh, "ts": po_ts, "last": po_ts,
                              "req": (d + timedelta(days=R_proc.choice([7, 10, 14]))).isoformat(),
                              "created_by": "BUYER" if R_proc.random() < 0.15 else "AUTO_REPLEN",
                              "total": 0.0, "lines": []}
                for ln, (pid, cases, spr, lt) in enumerate(items, start=1):
                    upc = pinfo[pid]["upc"]
                    cost = float(spr.cost_per_case_egp)
                    pos[po_id]["total"] += cases * cost
                    u = R_proc.random()
                    delay = 0 if R_proc.random() < 0.7 else R_proc.randint(1, 5) if R_proc.random() < 0.85 else R_proc.randint(6, 12)
                    arr = next_op(d + timedelta(days=max(1, lt + delay - (R_proc.randint(0, 1) if delay == 0 else 0))), wh)
                    rej = 1 if (R_proc.random() < 0.05 and cases > 1) else 0
                    if u < 0.06 and cases >= 2:       # two partial deliveries
                        q1 = max(1, int(cases * 0.6))
                        plan = [(arr, q1, rej, q1 * upc), (next_op(arr + timedelta(days=R_proc.randint(2, 6)), wh),
                                                           cases - q1, 0, (cases - q1) * upc)]
                    elif u < 0.12 and cases >= 2:     # short delivery, line closed short
                        q = max(1, int(cases * R_proc.uniform(0.6, 0.95)))
                        plan = [(arr, q, rej, cases * upc)]
                    else:
                        plan = [(arr, cases, rej, cases * upc)]
                    line_state[(po_id, ln)] = {"planned": len(plan), "done": 0, "last": po_ts}
                    for arr_d, q, rj, rel in plan:
                        receipt_plan[arr_d].append({"po": po_id, "line": ln, "wh": wh, "pid": pid, "qty": q,
                                                    "rej": rj, "release": rel, "order": (po_id, ln, arr_d)})
                    on_order[(wh, pid)] += cases * upc
                    promised = (d + timedelta(days=lt)).isoformat()
                    pos[po_id]["lines"].append({"purchase_order_id": po_id, "po_line_no": ln, "product_id": pid,
                                                "order_uom": "CASE", "qty_ordered_uom": cases, "units_per_case": upc,
                                                "unit_cost_per_uom_egp": cost, "promised_delivery_date": promised,
                                                "ts": po_ts})

        # --- end-of-day snapshot (every calendar day in the window, incl. Fridays) ---
        if d in snap_days:
            ts_s = tu.fmt(at(d, 23, 59))
            for k in keys:
                bl = sum(b["qty"] for b in stock[k] if b["status"] == "QUARANTINED")
                oh = onhand(k)
                snapshots.append({"snapshot_date": d.isoformat(), "warehouse_id": k[0], "product_id": k[1],
                                  "on_hand_qty": oh, "allocated_qty": min(max(0, reserved[k]), oh),
                                  "blocked_qty": bl, "snapshot_ts": ts_s, "source_batch_id": f"BATCH-{d:%Y%m%d}"})

    # ---------------- assemble frames ----------------
    F = tu.fmt
    order_rows, line_rows = [], []
    for so, o in orders.items():
        for l in o["lines"]:
            line_rows.append({**{c: l[c] for c in ["sales_order_id", "line_no", "product_id", "warehouse_id",
                                                  "qty_ordered", "qty_allocated", "qty_shipped", "uom",
                                                  "list_price_egp", "discount_pct", "unit_net_price_egp",
                                                  "promo_id", "line_status", "shortage_action"]},
                              "shipped_ts": F(l["shipped_ts"]), "source_updated_ts": F(l["upd"])})
        total = sum((l["qty_shipped"] if l["shipped_ts"] else l["qty_allocated"]) * l["unit_net_price_egp"]
                    for l in o["lines"])
        order_rows.append({"sales_order_id": so, "customer_id": o["cust"], "warehouse_id": o["wh"],
                           "order_ts": F(o["ts"]), "requested_delivery_date": o["req"], "channel": o["channel"],
                           "order_status": o["status"], "cancelled_ts": F(o["cancelled_ts"]),
                           "cancel_reason": o["cancel_reason"], "currency_code": "EGP",
                           "total_net_amount_egp": round(total, 2), "source_updated_ts": F(o["upd"])})
        # an order that is still pending at horizon end keeps its allocation-time state
    for l in line_rows:
        if l["line_status"] == "ALLOCATED" and l["shipped_ts"] is None:
            pass

    po_rows, pol_rows = [], []
    for po_id, p in pos.items():
        n_closed = n_started = 0
        for l in p["lines"]:
            st = line_state[(po_id, l["po_line_no"])]
            closed = st["done"] >= st["planned"]
            n_closed += closed
            n_started += st["done"] > 0
            pol_rows.append({**{c: l[c] for c in ["purchase_order_id", "po_line_no", "product_id", "order_uom",
                                                 "qty_ordered_uom", "units_per_case", "unit_cost_per_uom_egp",
                                                 "promised_delivery_date"]},
                             "line_status": "CLOSED" if closed else "OPEN", "source_updated_ts": F(st["last"])})
        status = "CLOSED" if n_closed == len(p["lines"]) else "PARTIALLY_RECEIVED" if n_started else "OPEN"
        po_rows.append({"purchase_order_id": po_id, "supplier_id": p["sid"], "warehouse_id": p["wh"],
                        "po_created_ts": F(p["ts"]), "requested_delivery_date": p["req"], "po_status": status,
                        "payment_terms_days": int(sup_terms.get(p["sid"], 30)), "total_amount_egp": round(p["total"], 2),
                        "created_by": p["created_by"], "source_updated_ts": F(p["last"])})

    batch_rows = []
    for b in batches.values():
        status = "EXPIRED" if b["exp"] < end else b["status"]
        batch_rows.append({"batch_id": b["id"], "warehouse_id": b["wh"], "product_id": b["pid"],
                           "receipt_id": b["rid"], "receipt_line_no": b["rline"], "batch_number": b["number"],
                           "production_date": b["prod"].isoformat(), "expiry_date": b["exp"].isoformat(),
                           "qty_received_base": b["recv"], "status": status, "updated_ts": F(b["upd"])})

    ledger.sort(key=lambda t: (t["ts"], t["n"]))
    led_rows = []
    for i, t in enumerate(ledger, start=1):
        led_rows.append({"txn_id": f"TX{i:09d}", "event_ts": F(t["ts"]), "posted_ts": F(tu.posting_lag(R_post, t["ts"])),
                         "warehouse_id": t["wh"], "product_id": t["pid"], "txn_type": t["typ"],
                         "qty_change_base": t["qty"], "reference_type": t["reftype"], "reference_id": t["refid"],
                         "batch_id": t["batch"], "reason_code": t["reason"], "event_seq": i, "source_system": "WMS"})

    stk_rows = [{"stockout_id": e["id"], "warehouse_id": e["wh"], "product_id": e["pid"],
                 "start_ts": F(e["start_ts"]), "end_ts": F(e["end_ts"]), "detection_source": "WMS_ATP_ZERO",
                 "trigger_ref": e["ref"], "updated_ts": F(e["upd"])} for e in stockouts]

    return {
        "sales_orders": pd.DataFrame(order_rows), "sales_order_lines": pd.DataFrame(line_rows),
        "sales_returns": pd.DataFrame(returns_rows), "purchase_orders": pd.DataFrame(po_rows),
        "purchase_order_lines": pd.DataFrame(pol_rows), "goods_receipt_lines": pd.DataFrame(receipts),
        "product_batches": pd.DataFrame(batch_rows), "inventory_transactions": pd.DataFrame(led_rows),
        "inventory_snapshots": pd.DataFrame(snapshots), "stockout_events": pd.DataFrame(stk_rows),
    }
