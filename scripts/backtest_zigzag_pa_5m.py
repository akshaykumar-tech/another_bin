#!/usr/bin/env python3
"""
Backtest ZigZag PA Strategy V4.1 on Binance futures 5m (last 24h eval).
ZigZag + patterns on 60m (Pine useAltTF=true); entries/exits on 5m close/high/low.
"""
from __future__ import annotations

import argparse
import time
import urllib.request
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from zigzag_pa_lib import Bar, detect_patterns, fib_level, zigzag_pivots

FAPI = "https://fapi.binance.com"
KDIR = Path(__file__).resolve().parents[1] / "data" / "klines" / "1s"
NOTIONAL = 6.0
FEE_RT = 0.0008
EW_RATE = 0.236
TP_RATE = 0.618
SL_RATE = -0.236
BAR_5M = 300_000
BAR_1H = 3_600_000


@dataclass
class Trade:
    symbol: str
    side: str
    pattern: str
    entry_ts: int
    entry_px: float
    exit_ts: int
    exit_px: float
    reason: str
    net_usd: float


def fetch_klines(symbol: str, interval: str, start_ms: int, end_ms: int) -> list[Bar]:
    rows: list[Bar] = []
    cur = start_ms
    while cur < end_ms:
        url = f"{FAPI}/fapi/v1/klines?symbol={symbol}&interval={interval}&startTime={cur}&endTime={end_ms}&limit=1500"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        for attempt in range(5):
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    batch = json.loads(resp.read())
                break
            except Exception:
                time.sleep(min(2 ** attempt, 8) + 0.2)
        else:
            return rows
        if not batch:
            break
        for k in batch:
            rows.append(Bar(int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4])))
        nxt = int(batch[-1][0]) + 1
        if nxt <= cur:
            break
        cur = nxt
        if len(batch) < 1500:
            break
        time.sleep(0.05)
    # dedupe
    seen = set()
    out = []
    for b in rows:
        if b.ts not in seen:
            seen.add(b.ts)
            out.append(b)
    return sorted(out, key=lambda x: x.ts)


def to_1h(bars_5m: list[Bar]) -> list[Bar]:
    buckets: dict[int, list[Bar]] = {}
    for b in bars_5m:
        key = (b.ts // BAR_1H) * BAR_1H
        buckets.setdefault(key, []).append(b)
    out = []
    for ts in sorted(buckets):
        c = buckets[ts]
        out.append(Bar(ts, c[0].o, max(x.h for x in c), min(x.l for x in c), c[-1].c))
    return out


def pnl(side: str, entry: float, exit_px: float) -> float:
    g = (exit_px - entry) / entry * 100 if side == "long" else (entry - exit_px) / entry * 100
    return NOTIONAL * g / 100 - NOTIONAL * FEE_RT


def last_closed_h1_open(ts: int) -> int:
    open_h = (ts // BAR_1H) * BAR_1H
    return open_h - BAR_1H if ts > open_h else open_h - BAR_1H


def backtest_symbol(symbol: str, eval_start_ms: int, end_ms: int) -> list[Trade]:
    warmup_ms = eval_start_ms - 10 * 24 * BAR_1H
    bars_5m = fetch_klines(symbol, "5m", warmup_ms, end_ms)
    if len(bars_5m) < 200:
        return []
    bars_1h = to_1h(bars_5m)
    raw_pivots = zigzag_pivots(bars_1h)
    pivot_points = [(bars_1h[i].ts, px) for i, px in raw_pivots if i < len(bars_1h)]

    trades: list[Trade] = []
    pos: dict | None = None
    prev_bull = prev_bear = False

    for b in bars_5m:
        if b.ts < eval_start_ms:
            continue

        cutoff = last_closed_h1_open(b.ts)
        pv = [px for ts, px in pivot_points if ts <= cutoff]
        if len(pv) < 5:
            if pos:
                fib_tp = fib_level(pos["d"], pos["c"], TP_RATE)
                fib_sl = fib_level(pos["d"], pos["c"], SL_RATE)
                side = pos["side"]
                hit = None
                if side == "long":
                    if b.h >= fib_tp:
                        hit = ("tp", fib_tp)
                    elif b.l <= fib_sl:
                        hit = ("sl", fib_sl)
                else:
                    if b.l <= fib_tp:
                        hit = ("tp", fib_tp)
                    elif b.h >= fib_sl:
                        hit = ("sl", fib_sl)
                if hit:
                    reason, px = hit
                    trades.append(
                        Trade(symbol, side, pos["pattern"], pos["entry_ts"], pos["entry_px"], b.ts, px, reason, pnl(side, pos["entry_px"], px))
                    )
                    pos = None
            continue

        x, a, bb, c, d = pv[-5], pv[-4], pv[-3], pv[-2], pv[-1]
        bull, bear, bull_names, bear_names = detect_patterns(x, a, bb, c, d)
        bull_edge = bull and not prev_bull
        bear_edge = bear and not prev_bear
        prev_bull, prev_bear = bull, bear

        fib_ew = fib_level(d, c, EW_RATE)
        fib_tp = fib_level(d, c, TP_RATE)
        fib_sl = fib_level(d, c, SL_RATE)

        if pos:
            side = pos["side"]
            hit = None
            if side == "long":
                if b.h >= fib_tp:
                    hit = ("tp", fib_tp)
                elif b.l <= fib_sl:
                    hit = ("sl", fib_sl)
            else:
                if b.l <= fib_tp:
                    hit = ("tp", fib_tp)
                elif b.h >= fib_sl:
                    hit = ("sl", fib_sl)
            if hit:
                reason, px = hit
                trades.append(
                    Trade(symbol, side, pos["pattern"], pos["entry_ts"], pos["entry_px"], b.ts, px, reason, pnl(side, pos["entry_px"], px))
                )
                pos = None

        if pos is None and bull_edge and b.c <= fib_ew:
            pos = {"side": "long", "entry_ts": b.ts, "entry_px": b.c, "pattern": bull_names[0], "c": c, "d": d}
        elif pos is None and bear_edge and b.c >= fib_ew:
            pos = {"side": "short", "entry_ts": b.ts, "entry_px": b.c, "pattern": bear_names[0], "c": c, "d": d}

    return trades


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=float, default=24.0)
    ap.add_argument("--notional", type=float, default=6.0)
    ap.add_argument("--symbols", default="")
    ap.add_argument("--max-symbols", type=int, default=20)
    args = ap.parse_args()
    global NOTIONAL
    NOTIONAL = args.notional

    if args.symbols.strip():
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    else:
        symbols = sorted(p.stem for p in KDIR.glob("*.csv"))[: args.max_symbols]
        if not symbols:
            symbols = "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT,ADAUSDT,DOGEUSDT,AVAXUSDT,LINKUSDT,DOTUSDT,LTCUSDT,BCHUSDT,NEARUSDT,FILUSDT,APTUSDT,ARBUSDT,OPUSDT,INJUSDT,ATOMUSDT,UNIUSDT".split(",")

    end_ms = int(time.time() * 1000)
    eval_start_ms = end_ms - int(args.hours * 3600 * 1000)
    eval_start = datetime.fromtimestamp(eval_start_ms / 1000, tz=timezone.utc)

    print(f"ZigZag PA backtest | 5m exec / 60m zigzag | last {args.hours}h | ${NOTIONAL} notional")
    print(f"Eval from {eval_start.isoformat()} UTC | {len(symbols)} symbols\n")
    print(f"{'Symbol':<16} {'Trades':>6} {'Net$':>9} {'WR%':>6} {'L':>3} {'S':>3}  Top patterns")
    print("-" * 72)

    all_trades: list[Trade] = []
    for sym in symbols:
        tr = backtest_symbol(sym, eval_start_ms, end_ms)
        all_trades.extend(tr)
        if not tr:
            print(f"{sym:<16} {0:>6} {'—':>9} {'—':>6}")
            continue
        net = sum(t.net_usd for t in tr)
        wr = 100 * sum(1 for t in tr if t.net_usd > 0) / len(tr)
        lng = sum(1 for t in tr if t.side == "long")
        sht = len(tr) - lng
        from collections import Counter
        top = ", ".join(f"{k}×{v}" for k, v in Counter(t.pattern for t in tr).most_common(2))
        print(f"{sym:<16} {len(tr):>6} {net:>+9.2f} {wr:>5.1f}% {lng:>3} {sht:>3}  {top}")

    print("-" * 72)
    if all_trades:
        net = sum(t.net_usd for t in all_trades)
        wr = 100 * sum(1 for t in all_trades if t.net_usd > 0) / len(all_trades)
        print(f"{'TOTAL':<16} {len(all_trades):>6} {net:>+9.2f} {wr:>5.1f}%")
        print(f"\nNote: Pine uses lookahead_on on 60m zigzag (repaints). Backtest uses closed 1h bars only.")
    else:
        print("No trades — patterns rare on 5m/24h or zigzag needs more warmup.")


if __name__ == "__main__":
    main()
