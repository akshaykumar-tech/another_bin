#!/usr/bin/env python3
"""Big prior-day movers (±X% daily) → same / next / +2 day strategies.

Goal filter: >= $10–15 PnL per 100 trades @ $6 notional (realistic fills).

Signal day D: |close-to-close pct| >= threshold (or open-to-close).
Trade days:
  same  = D intraday after first 5m that continues beyond D's open move (fade/cont)
  next  = D+1 open continuation/fade of D's direction
  third = D+2 open

Realistic: entry at open (next/third) or trade-through level; TIME/TP/SL; fee RT.
"""
from __future__ import annotations

import argparse
import csv
import pickle
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))

from orb30_engine import DAY_MS, DayBar, day_ms, fetch_daily_range, list_syms, req_json

ROOT = Path(__file__).resolve().parent.parent
CACHE_DAILY = ROOT / "data" / "cache" / "52w_daily"
CACHE_5M = ROOT / "data" / "cache" / "52w_5m"
FAPI = "https://fapi.binance.com"
FEE_RT = 0.0008
BAR_MS = 5 * 60 * 1000
NOTIONAL = 6.0


@dataclass
class Bar5:
    ts: int
    o: float
    h: float
    l: float
    c: float


@dataclass
class Mover:
    sym: str
    signal_date: str
    move_pct: float  # close-to-close vs prev close
    direction: str  # up | down


def _shift(d: str, n: int) -> str:
    return (datetime.strptime(d, "%Y-%m-%d").date() + timedelta(days=n)).isoformat()


def load_daily(sym: str, start: str, end: str) -> list[DayBar]:
    CACHE_DAILY.mkdir(parents=True, exist_ok=True)
    path = CACHE_DAILY / f"{sym}_{start}_{end}.pkl"
    if path.is_file():
        try:
            return pickle.loads(path.read_bytes())
        except Exception:
            pass
    time.sleep(0.01)
    bars = fetch_daily_range(sym, start, end)
    try:
        path.write_bytes(pickle.dumps(bars, protocol=pickle.HIGHEST_PROTOCOL))
    except Exception:
        pass
    return bars


def fetch_5m(sym: str, d: str) -> list[Bar5]:
    CACHE_5M.mkdir(parents=True, exist_ok=True)
    path = CACHE_5M / f"{sym}_{d}.pkl"
    if path.is_file():
        try:
            return pickle.loads(path.read_bytes())
        except Exception:
            pass
    start = day_ms(d)
    end = start + DAY_MS - 1
    url = f"{FAPI}/fapi/v1/klines?symbol={sym}&interval=5m&startTime={start}&endTime={end}&limit=300"
    time.sleep(0.012)
    rows = req_json(url)
    bars = [Bar5(int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4])) for k in rows]
    try:
        path.write_bytes(pickle.dumps(bars, protocol=pickle.HIGHEST_PROTOCOL))
    except Exception:
        pass
    return bars


def through(b: Bar5, px: float) -> bool:
    return b.l <= px <= b.h


def find_movers(bars: list[DayBar], t0: str, t1: str, min_abs: float) -> list[Mover]:
    out = []
    by = {b.date: b for b in bars}
    dates = [b.date for b in bars]
    idx = {d: i for i, d in enumerate(dates)}
    for d in dates:
        if d < t0 or d > t1:
            continue
        i = idx[d]
        if i < 1:
            continue
        prev = bars[i - 1]
        cur = bars[i]
        if prev.c <= 0:
            continue
        pct = (cur.c - prev.c) / prev.c * 100.0
        if abs(pct) < min_abs:
            continue
        out.append(Mover(bars[0].date and cur.date and "", d, pct, "up" if pct > 0 else "down"))
        out[-1].sym = ""  # filled by caller
    return out


def detect_sym(sym: str, bars: list[DayBar], t0: str, t1: str, min_abs: float) -> list[Mover]:
    out = []
    dates = [b.date for b in bars]
    for i, cur in enumerate(bars):
        d = cur.date
        if d < t0 or d > t1 or i < 1:
            continue
        prev = bars[i - 1]
        if prev.c <= 0:
            continue
        pct = (cur.c - prev.c) / prev.c * 100.0
        if abs(pct) < min_abs:
            continue
        out.append(Mover(sym, d, pct, "up" if pct > 0 else "down"))
    return out


def side_for(mover: Mover, mode: str) -> str:
    # mode: cont | fade
    if mode == "cont":
        return "long" if mover.direction == "up" else "short"
    return "short" if mover.direction == "up" else "long"


def sim_from_open(bars: list[Bar5], side: str, tp: float, sl: float, hold: int):
    """Enter at first bar open (day open). Exit TIME/TP/SL. No exit on entry bar."""
    if not bars or bars[0].o <= 0:
        return None
    entry = bars[0].o
    entry_i = 0
    if side == "long":
        tp_px = entry * (1 + tp / 100) if tp > 0 else None
        sl_px = entry * (1 - sl / 100) if sl > 0 else None
    else:
        tp_px = entry * (1 - tp / 100) if tp > 0 else None
        sl_px = entry * (1 + sl / 100) if sl > 0 else None
    last = min(len(bars) - 1, entry_i + hold)
    for j in range(entry_i + 1, last + 1):
        b = bars[j]
        held = j - entry_i
        hit_sl = through(b, sl_px) if sl_px is not None else False
        hit_tp = through(b, tp_px) if tp_px is not None else False
        if hit_sl:
            return side, entry, sl_px, "SL", held
        if hit_tp:
            return side, entry, tp_px, "TP", held
        if held >= hold:
            return side, entry, b.c, "TIME", held
    b = bars[last]
    return side, entry, b.c, "EOD", last - entry_i


def sim_same_day(bars: list[Bar5], mover: Mover, mode: str, tp: float, sl: float, hold: int):
    """Same-day: wait until price is beyond prior close by min move, then fade/cont at that touch of prior_close±thresh.
    Simpler realistic: after 1h of day (12 bars), enter at that bar's close in mode direction; hold.
    Avoid lookahead of full-day close (signal uses EOD close — same-day is optimistic if we use EOD move).
    So same-day uses OPEN-to-now: at bar i>=12, if |(c-o)/o|*100 >= half threshold proxy... 
    Better: same-day NOT using final close. Use: if day's open-to-high or open-to-low already >= min_abs*0.7 by bar i, enter.
    """
    return None  # handled separately with known-at-time logic


def sim_same_day_intrabar(
    bars: list[Bar5],
    min_abs: float,
    mode: str,
    tp: float,
    sl: float,
    hold: int,
    arm_bars: int = 6,
):
    """Arm after `arm_bars`; when open→price move hits min_abs, enter fade/cont; no EOD close used."""
    if len(bars) < arm_bars + 2:
        return None
    day_o = bars[0].o
    if day_o <= 0:
        return None
    entry_i = -1
    side = ""
    entry = 0.0
    for i in range(arm_bars, len(bars)):
        b = bars[i]
        up = (b.h - day_o) / day_o * 100.0
        dn = (day_o - b.l) / day_o * 100.0
        if up >= min_abs and dn >= min_abs:
            # both — skip ambiguous
            continue
        if up >= min_abs:
            direction = "up"
            # fill at day_o * (1+min_abs/100) trade-through
            level = day_o * (1 + min_abs / 100.0)
            if through(b, level):
                side = "long" if mode == "cont" else "short"
                entry = level
                entry_i = i
                break
        elif dn >= min_abs:
            level = day_o * (1 - min_abs / 100.0)
            if through(b, level):
                side = "long" if mode == "fade" else "short"  # down move: cont=short, fade=long
                if mode == "cont":
                    side = "short"
                else:
                    side = "long"
                entry = level
                entry_i = i
                break
    if entry_i < 0 or not side:
        return None
    if side == "long":
        tp_px = entry * (1 + tp / 100) if tp > 0 else None
        sl_px = entry * (1 - sl / 100) if sl > 0 else None
    else:
        tp_px = entry * (1 - tp / 100) if tp > 0 else None
        sl_px = entry * (1 + sl / 100) if sl > 0 else None
    last = min(len(bars) - 1, entry_i + hold)
    for j in range(entry_i + 1, last + 1):
        b = bars[j]
        held = j - entry_i
        hit_sl = through(b, sl_px) if sl_px is not None else False
        hit_tp = through(b, tp_px) if tp_px is not None else False
        if hit_sl:
            return side, entry, sl_px, "SL", held
        if hit_tp:
            return side, entry, tp_px, "TP", held
        if held >= hold:
            return side, entry, b.c, "TIME", held
    b = bars[last]
    return side, entry, b.c, "EOD", last - entry_i


def pnl_usd(side: str, entry: float, exit_px: float) -> float:
    pct = (exit_px - entry) / entry * 100 if side == "long" else (entry - exit_px) / entry * 100
    return NOTIONAL * pct / 100.0 - NOTIONAL * FEE_RT


def per_100(pnl: float, n: int) -> float:
    return (pnl / n * 100.0) if n else 0.0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-06-01")
    ap.add_argument("--end", default="2026-07-18")
    ap.add_argument("--workers", type=int, default=10)
    args = ap.parse_args()

    thresholds = [5, 8, 10, 12, 15, 20, 25]
    # next/third holds in 5m bars
    holds = [(12, "1h"), (24, "2h"), (36, "3h"), (48, "4h"), (72, "6h"), (96, "8h")]
    tpsl = [(0, 0), (3, 5), (5, 5), (5, 8), (8, 8), (10, 10), (10, 15), (15, 15)]
    modes = ["cont", "fade"]
    lags = ["next", "third"]  # same handled separately

    fetch_start = _shift(args.start, -5)
    fetch_end = _shift(args.end, 5)
    syms = list_syms()
    print(f"Big-mover sweep {args.start}→{args.end} syms={len(syms)}")

    daily = {}
    done = 0
    def one_d(sym):
        try:
            return sym, load_daily(sym, fetch_start, fetch_end)
        except Exception:
            return sym, []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        for fut in as_completed([ex.submit(one_d, s) for s in syms]):
            sym, bars = fut.result()
            if bars:
                daily[sym] = bars
            done += 1
            if done % 100 == 0:
                print(f"  daily {done}")

    # movers per threshold
    movers_by_th: dict[float, list[Mover]] = {t: [] for t in thresholds}
    for sym, bars in daily.items():
        dates = [b.date for b in bars]
        idx = {d: i for i, d in enumerate(dates)}
        for i, cur in enumerate(bars):
            if cur.date < args.start or cur.date > args.end or i < 1:
                continue
            prev = bars[i - 1]
            if prev.c <= 0:
                continue
            pct = (cur.c - prev.c) / prev.c * 100.0
            for th in thresholds:
                if abs(pct) >= th:
                    movers_by_th[th].append(Mover(sym, cur.date, pct, "up" if pct > 0 else "down"))

    for th, ms in movers_by_th.items():
        print(f"  thr>={th}% movers={len(ms)}")

    # Need 5m for signal day, next, third
    need_dates = set()
    for th, ms in movers_by_th.items():
        for m in ms:
            need_dates.add((m.sym, m.signal_date))
            need_dates.add((m.sym, _shift(m.signal_date, 1)))
            need_dates.add((m.sym, _shift(m.signal_date, 2)))
            # same-day also signal date
    need_dates = sorted(need_dates)
    print(f"5m fetch keys={len(need_dates)}")
    bars5 = {}
    done = 0
    def one_5(item):
        try:
            return item, fetch_5m(item[0], item[1])
        except Exception:
            return item, []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        for fut in as_completed([ex.submit(one_5, k) for k in need_dates]):
            k, b = fut.result()
            bars5[k] = b
            done += 1
            if done % 300 == 0:
                print(f"  5m {done}/{len(need_dates)}")

    date_set = set()
    for bars in daily.values():
        for b in bars:
            date_set.add(b.date)

    stats = defaultdict(lambda: {"n": 0, "wins": 0, "pnl": 0.0, "gp": 0.0, "gl": 0.0, "days": defaultdict(float)})

    def add(key, usd, day):
        st = stats[key]
        st["n"] += 1
        st["pnl"] += usd
        st["days"][day] += usd
        if usd > 0:
            st["wins"] += 1
            st["gp"] += usd
        else:
            st["gl"] += abs(usd)

    # NEXT / THIRD
    for th, ms in movers_by_th.items():
        for m in ms:
            for lag, shift in [("next", 1), ("third", 2)]:
                td = _shift(m.signal_date, shift)
                if td not in date_set and (m.sym, td) not in bars5:
                    continue
                bars = bars5.get((m.sym, td)) or []
                if not bars:
                    continue
                for mode in modes:
                    side = side_for(m, mode)
                    for hold, htag in holds:
                        for tp, sl in tpsl:
                            v = f"time_{htag}" if tp == 0 else f"tp{int(tp)}_sl{int(sl)}_{htag}"
                            res = sim_from_open(bars, side, tp, sl, hold)
                            if not res:
                                continue
                            _, entry, xp, _, _ = res
                            usd = pnl_usd(side, entry, xp)
                            key = (f"thr{th}", lag, mode, v)
                            add(key, usd, td)

    # SAME DAY (no EOD lookahead on signal)
    for th in thresholds:
        # all symbols all days in range — arm when intraday move hits th
        for sym, dbars in daily.items():
            for b in dbars:
                if b.date < args.start or b.date > args.end:
                    continue
                bars = bars5.get((sym, b.date)) or []
                if not bars:
                    continue
                for mode in modes:
                    for hold, htag in holds:
                        for tp, sl in tpsl:
                            # only shorter holds for same-day to keep runtime sane — still all
                            if hold > 72:
                                continue
                            v = f"time_{htag}" if tp == 0 else f"tp{int(tp)}_sl{int(sl)}_{htag}"
                            res = sim_same_day_intrabar(bars, float(th), mode, tp, sl, hold)
                            if not res:
                                continue
                            side, entry, xp, _, _ = res
                            usd = pnl_usd(side, entry, xp)
                            key = (f"thr{th}", "same", mode, v)
                            add(key, usd, b.date)

    rows = []
    for key, st in stats.items():
        n = st["n"]
        if n < 30:
            continue
        wr = 100.0 * st["wins"] / n
        pf = (st["gp"] / st["gl"]) if st["gl"] > 1e-9 else 0.0
        green = 100.0 * sum(1 for v in st["days"].values() if v > 0.01) / max(1, len(st["days"]))
        p100 = per_100(st["pnl"], n)
        rows.append({
            "thr": key[0], "lag": key[1], "mode": key[2], "variant": key[3],
            "n": n, "wr": wr, "pnl": st["pnl"], "pf": pf, "green": green,
            "per100": p100, "days": len(st["days"]),
        })

    print("\n" + "=" * 120)
    print("TARGET: per100 >= $10 (stretch $15) @ $6 notional | N>=50 | PF>=1.1")
    print("=" * 120)
    hit = [r for r in rows if r["n"] >= 50 and r["per100"] >= 10 and r["pf"] >= 1.1 and r["pnl"] > 0]
    print(f"{'thr':6} {'lag':5} {'mode':4} {'variant':18} {'N':>5} {'WR':>5} {'PnL$':>8} {'$/100':>7} {'PF':>5} {'g%':>5}")
    for r in sorted(hit, key=lambda x: x["per100"], reverse=True)[:40]:
        print(
            f"{r['thr']:6} {r['lag']:5} {r['mode']:4} {r['variant']:18} "
            f"{r['n']:5d} {r['wr']:5.1f} {r['pnl']:+8.2f} {r['per100']:+7.2f} {r['pf']:5.2f} {r['green']:5.1f}"
        )
    if not hit:
        print("  none hit $10/100 — showing top per100 overall (N>=50, pnl>0)")
        cand = [r for r in rows if r["n"] >= 50 and r["pnl"] > 0]
        for r in sorted(cand, key=lambda x: x["per100"], reverse=True)[:30]:
            print(
                f"{r['thr']:6} {r['lag']:5} {r['mode']:4} {r['variant']:18} "
                f"{r['n']:5d} {r['wr']:5.1f} {r['pnl']:+8.2f} {r['per100']:+7.2f} {r['pf']:5.2f} {r['green']:5.1f}"
            )

    print("\n" + "=" * 120)
    print("TOP by total PnL (N>=80)")
    print("=" * 120)
    for r in sorted([x for x in rows if x["n"] >= 80 and x["pnl"] > 0], key=lambda x: x["pnl"], reverse=True)[:20]:
        print(
            f"{r['thr']:6} {r['lag']:5} {r['mode']:4} {r['variant']:18} "
            f"N={r['n']:4d} WR={r['wr']:.1f}% PnL=${r['pnl']:+.2f} $/100=${r['per100']:+.2f} PF={r['pf']:.2f}"
        )

    out = ROOT / "data" / f"big_mover_strategies_{args.start}_to_{args.end}.csv"
    with out.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [])
        if rows:
            w.writeheader()
            for r in sorted(rows, key=lambda x: x["per100"], reverse=True):
                w.writerow(r)
    print(f"\nWrote {out} ({len(rows)} combos)")


if __name__ == "__main__":
    main()
