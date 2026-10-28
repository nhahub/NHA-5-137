"""
Regression validator for the CLEAN generated data.
Run:  python -m data_generator.generator --profile test --seed 7 --no-defects --output-dir out
      python -m data_generator.validate out
With defects enabled many of these checks fail BY DESIGN (that is the point of the defects).
"""
import json
import sys
import pandas as pd

results = []
def check(name, ok, detail=""):
    results.append((name, bool(ok), detail))


import functools, traceback
def _guard(fn):
    @functools.wraps(fn)
    def run(d):
        try:
            return fn(d)
        except Exception as e:            # dirty data may break a check: report it, do not crash
            check("validator crashed (data too broken to check)", False, f"{type(e).__name__}: {e}")
            return _report()
    return run


@_guard
def main(d):
    r = lambda n: pd.read_csv(f"{d}/{n}.csv")
    so, sl, rt = r("sales_orders"), r("sales_order_lines"), r("sales_returns")
    po, pl, gr, bt = r("purchase_orders"), r("purchase_order_lines"), r("goods_receipt_lines"), r("product_batches")
    sn, st, pr, cu = r("inventory_snapshots"), r("stockout_events"), r("products"), r("customers")
    led = pd.read_json(f"{d}/inventory_transactions.jsonl", lines=True, dtype=False)
    sup = r("suppliers")

    # uniqueness
    check("no duplicate order ids", so.sales_order_id.is_unique)
    check("no duplicate order lines", not sl.duplicated(["sales_order_id", "line_no"]).any())
    check("no duplicate ledger txn_id", led.txn_id.is_unique)
    check("sku_code unique", pr.sku_code.is_unique)

    # referential integrity
    check("lines -> orders", sl.sales_order_id.isin(so.sales_order_id).all())
    check("orders -> customers", so.customer_id.isin(cu.customer_id).all())
    check("returns -> orders", rt.sales_order_id.isin(so.sales_order_id).all())
    check("po lines -> po", pl.purchase_order_id.isin(po.purchase_order_id).all())
    check("po -> suppliers", po.supplier_id.isin(sup.supplier_id).all())
    check("receipts -> po lines", gr.set_index(["purchase_order_id", "po_line_no"]).index.isin(
        pl.set_index(["purchase_order_id", "po_line_no"]).index).all())
    check("ledger batch_id -> batches", led.batch_id.notna().all() and led.batch_id.isin(bt.batch_id).all())
    check("batches -> products", bt.product_id.isin(pr.product_id).all())

    # ledger physics
    L = led.sort_values(["event_ts", "event_seq"])
    bal = L.groupby(["warehouse_id", "product_id"]).qty_change_base.cumsum()
    check("ledger balance never negative", bal.min() >= 0, f"min={bal.min()}")
    L["d"] = L.event_ts.str[:10]
    dl = L.groupby(["warehouse_id", "product_id", "d"]).qty_change_base.sum().reset_index()
    dl["cum"] = dl.groupby(["warehouse_id", "product_id"]).qty_change_base.cumsum()
    m = sn.merge(dl.rename(columns={"d": "snapshot_date"}), on=["warehouse_id", "product_id", "snapshot_date"], how="left")
    m["cum"] = m.groupby(["warehouse_id", "product_id"]).cum.ffill().fillna(0)
    check("snapshot on_hand == cumulative ledger", (m.on_hand_qty == m.cum).all(), f"{(m.on_hand_qty != m.cum).sum()} mismatches")
    check("snapshot qty non-negative, allocated <= on_hand", (sn.on_hand_qty >= 0).all() and (sn.allocated_qty <= sn.on_hand_qty).all())
    iss = led[led.txn_type == "ISSUE"].groupby("reference_id").qty_change_base.sum().mul(-1)
    ref = sl.sales_order_id + ":" + sl.line_no.astype(str)
    check("qty_shipped == ledger issues per line", (sl.qty_shipped.values == iss.reindex(ref).fillna(0).values).all())
    upc = pr.set_index("product_id").units_per_case
    g = gr.assign(acc=(gr.qty_received_uom - gr.qty_rejected_uom) * gr.product_id.map(upc))
    rec = led[led.reference_type == "GR_LINE"].drop_duplicates("reference_id").set_index("reference_id").qty_change_base
    gk = g.receipt_id + ":" + g.receipt_line_no.astype(str)
    check("receipt accepted qty == ledger receipt", (g.acc.values == rec.reindex(gk).values).all())
    batch_sum = led.groupby("batch_id").qty_change_base.sum()
    check("batch balance >= 0 everywhere", batch_sum.min() >= 0)

    # calendar / time
    wd = lambda s: pd.to_datetime(s.str[:10]).dt.weekday
    ops = led[(led.reference_type != "INITIAL_LOAD") & led.txn_type.isin(["ISSUE", "RECEIPT", "RETURN_RESTOCK"])]
    check("no Friday orders/movements/returns", (wd(so.order_ts) != 4).all() and (wd(ops.event_ts) != 4).all() and (wd(rt.return_ts) != 4).all())
    check("posted_ts >= event_ts", (pd.to_datetime(led.posted_ts.str[:19]) >= pd.to_datetime(led.event_ts.str[:19])).all())
    check("event timestamps not frozen", pd.to_datetime(led.event_ts.str[:19]).dt.hour.nunique() >= 8)

    # orders & lines
    live = sl[sl.line_status != "CANCELLED"]
    check("qty_shipped <= qty_ordered, qty >= 0", (sl.qty_shipped <= sl.qty_ordered).all() and (sl.qty_ordered > 0).all())
    check("SHIPPED lines fully shipped", (sl[sl.line_status == "SHIPPED"].qty_shipped == sl[sl.line_status == "SHIPPED"].qty_ordered).all())
    check("discount>0 implies promo_id", sl[sl.discount_pct > 0].promo_id.notna().all())
    check("net price == list*(1-disc)", ((sl.list_price_egp * (1 - sl.discount_pct / 100)).round(2) - sl.unit_net_price_egp).abs().max() < 0.011)
    sl["amt"] = sl.apply(lambda x: (x.qty_shipped if isinstance(x.shipped_ts, str) else x.qty_allocated) * x.unit_net_price_egp, axis=1)
    line_sum = sl.groupby("sales_order_id").amt.sum().reindex(so.sales_order_id).fillna(0).values
    check("order total == sum of lines", (so.total_net_amount_egp.values - line_sum).__abs__().max() < 0.05)
    bo = sl[(sl.shortage_action == "BACKORDER") & (sl.qty_shipped < sl.qty_ordered)]
    last = pd.to_datetime(so.order_ts.str[:10]).max()
    odate = so.drop_duplicates("sales_order_id").set_index("sales_order_id").order_ts.str[:10].map(pd.to_datetime)
    stale = (last - bo.sales_order_id.map(odate)).dt.days > 15
    check("unfulfilled backorders are still inside the 14-day window", not stale.any(), f"{len(bo)} open, {int(stale.sum())} stale")
    check("no line with qty_allocated=0 marked ALLOCATED", ((sl.line_status == "ALLOCATED") & (sl.qty_allocated == 0)).sum() == 0)

    # returns
    bad = rt[rt.return_reason.isin(["EXPIRED", "DAMAGED"]) & (rt.disposition == "RESTOCK")]
    check("no RESTOCK for EXPIRED/DAMAGED", bad.empty)
    check("restock returns == ledger restocks", (rt.disposition == "RESTOCK").sum() == (led.txn_type == "RETURN_RESTOCK").sum())
    shp = sl.drop_duplicates(["sales_order_id", "line_no"]).set_index(["sales_order_id", "line_no"]).qty_shipped
    check("qty_returned <= qty_shipped", (rt.qty_returned.values <= shp.reindex(rt.set_index(["sales_order_id", "line_no"]).index).values).all())

    # procurement & batches
    check("batch expiry > production", (pd.to_datetime(bt.expiry_date) > pd.to_datetime(bt.production_date)).all())
    check("receipt expiry after received", (pd.to_datetime(gr.expiry_date) > pd.to_datetime(gr.received_ts.str[:10])).all())
    check("every receipt has batch_number+expiry", gr.batch_number.notna().all() and gr.expiry_date.notna().all())
    check("no null promised dates / lead times", pl.promised_delivery_date.notna().all())
    check("PO CLOSED => all lines CLOSED", (pl.groupby("purchase_order_id").line_status.apply(lambda s: (s == "CLOSED").all())
                                             .reindex(po[po.po_status == "CLOSED"].purchase_order_id)).all())
    rec_po = set(gr.purchase_order_id)
    check("PO with receipts is not OPEN/CANCELLED", po[po.purchase_order_id.isin(rec_po)].po_status.isin(["CLOSED", "PARTIALLY_RECEIVED"]).all())
    bsum = led.groupby("batch_id").qty_change_base.sum().reindex(bt.batch_id).fillna(0)
    check("EXPIRED batches have zero balance (written off)", (bsum.values[(bt.status == "EXPIRED").values] == 0).all())
    check("ACTIVE batches are not past expiry at end", (pd.to_datetime(bt[bt.status == "ACTIVE"].expiry_date) >= pd.to_datetime(led.event_ts.str[:10]).max()).all())
    check("stockout end after start", (st.end_ts.isna() | (st.end_ts >= st.start_ts)).all())
    check("price/cost sanity (positive)", (r("product_prices").list_price_egp > 0).all() and (r("product_costs").cost_per_base_unit_egp > 0).all())

    return _report()


def _report():
    width = max(len(n) for n, _, _ in results)
    for n, ok, det in results:
        print(f"{'PASS' if ok else 'FAIL'}  {n:<{width}}  {det}")
    failed = [n for n, ok, _ in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "data/raw"))
