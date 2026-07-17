#!/usr/bin/env python3
"""Daily PnL backtest Jun 1 – Jul 10 for ORB / top-mover strategies ($10/trade)."""
from __future__ import annotations

import argparse
import csv
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))

from orb30_engine import (
    DAY_MS,
    scan_signals_for_day,
)

FAPI = "https://fapi.binance.com"
FEE_RT = 0.08
ORB_30_BARS = 6
ORB_60_BARS = 12


@dataclass
class Bar5:
    ts: int
    o: float
    h: float
    l: float
    c: float


@dataclass
class TradeRow:
    trade_date: str
    sym: str
    bucket: str
    side: str
    entry: float
    exit: float
    reason: str
    pnl_pct: float
    pnl_usd: float


def fetch_5m_day(sym: str, trade_date: str) -> list[Bar5]:
    from orb30_engine import day_ms, req_json

    start = day_ms(trade_date)
    end = start + DAY_MS - 1
    url = (
        f"{FAPI}/fapi/v1/klines?symbol={sym}&interval=5m"
        f"&startTime={start}&endTime={end}&limit=300"
    )
    return [
        Bar5(int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]))
        for k in req_json(url)
    ]


def pnl_usd(pnl_pct: float, notional: float) -> float:
    return notional * pnl_pct / 100 - notional * FEE_RT / 100


def sim_orb(
    bars: list[Bar5],
    orb_bars: int,
    tp_pct: float,
    sl_pct: float,
    max_trades: int,
) -> list[tuple[str, float, float, str, float]]:
    """Returns list of (side, entry, exit, reason, pnl_pct)."""
    if len(bars) < orb_bars + 2:
        return []
    orb = bars[:orb_bars]
    hi = max(b.h for b in orb)
    lo = min(b.l for b in orb)
    rest = bars[orb_bars:]
    out: list[tuple[str, float, float, str, float]] = []
    pos, entry = None, 0.0

    def flat(px: float, reason: str) -> None:
        nonlocal pos, entry
        if not pos:
            return
        pnl = (px - entry) / entry * 100 if pos == "long" else (entry - px) / entry * 100
        out.append((pos, entry, px, reason, pnl))
        pos = None

    for b in rest:
        if len(out) >= max_trades and not pos:
            break
        if pos:
            tp = entry * (1 + tp_pct / 100) if pos == "long" else entry * (1 - tp_pct / 100)
            sl = entry * (1 - sl_pct / 100) if pos == "long" else entry * (1 + sl_pct / 100)
            if pos == "long":
                if b.l <= sl:
                    flat(sl, "SL")
                elif b.h >= tp:
                    flat(tp, "TP")
            else:
                if b.h >= sl:
                    flat(sl, "SL")
                elif b.l <= tp:
                    flat(tp, "TP")
            continue
        if len(out) >= max_trades:
            continue
        # LIMIT@ORB: only fill if this bar traded through ORB hi/lo.
        if b.l <= hi <= b.h:
            pos, entry = "long", hi
            tp = entry * (1 + tp_pct / 100)
            sl = entry * (1 - sl_pct / 100)
            if b.l <= sl:
                flat(sl, "SL")
            elif b.h >= tp:
                flat(tp, "TP")
        elif b.l <= lo <= b.h:
            pos, entry = "short", lo
            tp = entry * (1 - tp_pct / 100)
            sl = entry * (1 + sl_pct / 100)
            if b.h >= sl:
                flat(sl, "SL")
            elif b.l <= tp:
                flat(tp, "TP")
    if pos:
        flat(bars[-1].c, "EOD")
    return out


def sim_pre1h_follow(
    bars: list[Bar5],
    pre_bars: list[Bar5],
    tp_pct: float,
    sl_pct: float,
) -> list[tuple[str, float, float, str, float]]:
    if len(pre_bars) < 10 or len(bars) < 2:
        return []
    hour = pre_bars[-12:] if len(pre_bars) >= 12 else pre_bars
    o, c = hour[0].o, hour[-1].c
    if o <= 0 or c == o:
        return []
    side = "long" if c > o else "short"
    entry = bars[0].o
    tp = entry * (1 + tp_pct / 100) if side == "long" else entry * (1 - tp_pct / 100)
    sl = entry * (1 - sl_pct / 100) if side == "long" else entry * (1 + sl_pct / 100)
    for b in bars:
        if side == "long":
            if b.l <= sl:
                return [(side, entry, sl, "SL", (sl - entry) / entry * 100)]
            if b.h >= tp:
                return [(side, entry, tp, "TP", (tp - entry) / entry * 100)]
        else:
            if b.h >= sl:
                return [(side, entry, sl, "SL", (entry - sl) / entry * 100)]
            if b.l <= tp:
                return [(side, entry, tp, "TP", (entry - tp) / entry * 100)]
    x = bars[-1].c
    pnl = (x - entry) / entry * 100 if side == "long" else (entry - x) / entry * 100
    return [(side, entry, x, "EOD", pnl)]


STRATEGIES = {
    "ORB30_10tp_10sl_m3": lambda b, pre: sim_orb(b, ORB_30_BARS, 10, 10, 3),
    "ORB1h_5tp_5sl_m3": lambda b, pre: sim_orb(b, ORB_60_BARS, 5, 5, 3),
    "ORB30_5tp_5sl_m3": lambda b, pre: sim_orb(b, ORB_30_BARS, 5, 5, 3),
    "PRE1h_FOLLOW_10tp_5sl": lambda b, pre: sim_pre1h_follow(b, pre, 10, 5),
}


def date_range(start: str, end: str) -> list[str]:
    d0 = datetime.strptime(start, "%Y-%m-%d").date()
    d1 = datetime.strptime(end, "%Y-%m-%d").date()
    out = []
    cur = d0
    while cur <= d1:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def all_signals_for_range(trade_start: str, trade_end: str, lookback: int = 7) -> list:
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from orb30_engine import build_chg_table, fetch_daily_range, list_syms, top5_sets

    trade_days = date_range(trade_start, trade_end)
    first_sig = (datetime.strptime(trade_start, "%Y-%m-%d").date() - timedelta(days=1)).isoformat()
    warmup_start = (
        datetime.strptime(first_sig, "%Y-%m-%d").date() - timedelta(days=lookback + 2)
    ).isoformat()
    all_dates = date_range(warmup_start, trade_end)
    fetch_end = (
        datetime.strptime(trade_end, "%Y-%m-%d").date() + timedelta(days=1)
    ).isoformat()
    syms = list_syms(FAPI)

    print(f"Fetching daily klines {warmup_start} → {fetch_end} for {len(syms)} symbols...")
    sym_bars: dict = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {
            ex.submit(fetch_daily_range, s, warmup_start, fetch_end, FAPI): s for s in syms
        }
        for fut in as_completed(futs):
            sym = futs[fut]
            try:
                sym_bars[sym] = {b.date: b for b in fut.result()}
            except Exception:
                pass

    chg = build_chg_table(sym_bars, all_dates)
    day_top = {d: top5_sets(chg[d]) for d in all_dates if chg.get(d)}

    out = []
    for td in trade_days:
        sig_day = (datetime.strptime(td, "%Y-%m-%d").date() - timedelta(days=1)).isoformat()
        if sig_day not in chg:
            continue
        gainers, losers = day_top.get(sig_day, (set(), set()))
        lb_dates = [
            (datetime.strptime(sig_day, "%Y-%m-%d").date() - timedelta(days=i)).isoformat()
            for i in range(1, lookback + 1)
        ]
        for sym, pct in sorted(chg[sig_day].items(), key=lambda x: x[1], reverse=True):
            if sym in gainers:
                bucket = "TOP5_GAIN"
            elif sym in losers:
                bucket = "TOP5_LOSS"
            else:
                continue
            if any(
                sym in day_top.get(lb, (set(), set()))[0]
                or sym in day_top.get(lb, (set(), set()))[1]
                for lb in lb_dates
                if lb in day_top
            ):
                continue
            out.append(
                type("S", (), {
                    "sym": sym, "signal_date": sig_day, "trade_date": td,
                    "bucket": bucket, "signal_pct": pct,
                })()
            )
    return out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--trade-start", default="2026-06-01")
    p.add_argument("--trade-end", default="2026-07-10")
    p.add_argument("--notional", type=float, default=10.0)
    p.add_argument("--lookback", type=int, default=7)
    args = p.parse_args()

    trade_days = date_range(args.trade_start, args.trade_end)
    print(f"Building signals for trade days {args.trade_start} → {args.trade_end}...")
    signals = all_signals_for_range(args.trade_start, args.trade_end, args.lookback)
    print(f"Total signal-rows: {len(signals)}")

    bar_cache: dict[tuple[str, str], list[Bar5]] = {}
    pre_cache: dict[tuple[str, str], list[Bar5]] = {}

    daily: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    daily_trades: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    all_trades: dict[str, list[TradeRow]] = {k: [] for k in STRATEGIES}

    for i, sig in enumerate(signals):
        k = (sig.sym, sig.trade_date)
        if k not in bar_cache:
            try:
                time.sleep(0.04)
                from orb30_engine import day_ms, req_json

                ds = day_ms(sig.trade_date)
                start = ds - 3600_000
                end = ds + DAY_MS - 1
                url = (
                    f"{FAPI}/fapi/v1/klines?symbol={sig.sym}&interval=5m"
                    f"&startTime={start}&endTime={end}&limit=400"
                )
                rows = req_json(url)
                all_bars = [
                    Bar5(int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]))
                    for r in rows
                ]
                bar_cache[k] = [b for b in all_bars if b.ts >= ds]
                pre_cache[k] = [b for b in all_bars if b.ts < ds]
            except Exception as e:
                print(f"  bar skip {sig.sym} {sig.trade_date}: {e}")
                bar_cache[k] = []
                pre_cache[k] = []

        day_bars = bar_cache[k]
        pre_bars = pre_cache[k]
        if not day_bars:
            continue

        for sname, fn in STRATEGIES.items():
            for side, entry, exit_px, reason, p_pct in fn(day_bars, pre_bars):
                u = pnl_usd(p_pct, args.notional)
                daily[sig.trade_date][sname] += u
                daily_trades[sig.trade_date][sname] += 1
                all_trades[sname].append(
                    TradeRow(
                        sig.trade_date, sig.sym, sig.bucket, side,
                        entry, exit_px, reason, p_pct, u,
                    )
                )

        if (i + 1) % 100 == 0:
            print(f"  simulated {i + 1}/{len(signals)}")

    names = list(STRATEGIES.keys())
    print("\n" + "=" * 100)
    print(f"DAILY PnL @ ${args.notional}/trade | trade days {args.trade_start} → {args.trade_end}")
    print("=" * 100)
    hdr = f"{'Date':<12}" + "".join(f"{n[:18]:>20}" for n in names)
    print(hdr)
    print("-" * len(hdr))

    totals = {n: 0.0 for n in names}
    for d in trade_days:
        row = f"{d:<12}"
        for n in names:
            v = daily[d][n]
            totals[n] += v
            row += f"{v:+20.2f}"
        print(row)

    print("-" * len(hdr))
    tot_row = f"{'TOTAL':<12}" + "".join(f"{totals[n]:+20.2f}" for n in names)
    print(tot_row)

    print("\n--- ORB30 live bot: trades & PnL per day ---")
    for d in trade_days:
        t = daily_trades[d]["ORB30_10tp_10sl_m3"]
        p = daily[d]["ORB30_10tp_10sl_m3"]
        print(f"  {d}: {t:3d} trades  ${p:+8.2f}")

    out = Path(f"data/orb30_daily_pnl_{args.trade_start}_to_{args.trade_end}.csv")
    out.parent.mkdir(exist_ok=True)
    with out.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["trade_date"] + names)
        for d in trade_days:
            w.writerow([d] + [round(daily[d][n], 4) for n in names])
        w.writerow(["TOTAL"] + [round(totals[n], 4) for n in names])
    print(f"\nSaved {out}")

    detail = Path(f"data/orb30_daily_trades_{args.trade_start}_to_{args.trade_end}.csv")
    with detail.open("w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "strategy", "trade_date", "sym", "bucket", "side",
                "entry", "exit", "reason", "pnl_pct", "pnl_usd",
            ],
        )
        w.writeheader()
        for sname, rows in all_trades.items():
            for t in rows:
                w.writerow({
                    "strategy": sname,
                    "trade_date": t.trade_date,
                    "sym": t.sym,
                    "bucket": t.bucket,
                    "side": t.side,
                    "entry": t.entry,
                    "exit": t.exit,
                    "reason": t.reason,
                    "pnl_pct": round(t.pnl_pct, 4),
                    "pnl_usd": round(t.pnl_usd, 4),
                })
    print(f"Saved {detail}")


if __name__ == "__main__":
    main()
