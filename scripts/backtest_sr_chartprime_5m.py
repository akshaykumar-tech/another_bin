#!/usr/bin/env python3
"""
Backtest ChartPrime SR (High Volume Boxes) on Binance futures 5m — last 24h.
Signals: support/resistance holds + breakouts + flip retests (Pine plotchar/labels).
"""
from __future__ import annotations

import argparse
import json
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from sr_chartprime_lib import Bar, compute_sr_signals, entry_side, signal_name

FAPI = "https://fapi.binance.com"
KDIR = Path(__file__).resolve().parents[1] / "data" / "klines" / "1s"
NOTIONAL = 6.0
FEE_RT = 0.0008
BAR_5M = 300_000
LOOKBACK = 20
VOL_LEN = 2
BOX_WIDTH = 1.0
TP_PCT = 1.5
SL_PCT = 8.0
MAX_BARS = 96


@dataclass
class Trade:
    symbol: str
    side: str
    signal: str
    entry_ts: int
    entry_px: float
    exit_ts: int
    exit_px: float
    reason: str
    net_usd: float


def fetch_klines(symbol: str, start_ms: int, end_ms: int) -> list[Bar]:
    rows: list[Bar] = []
    cur = start_ms
    while cur < end_ms:
        url = (
            f"{FAPI}/fapi/v1/klines?symbol={symbol}&interval=5m"
            f"&startTime={cur}&endTime={end_ms}&limit=1500"
        )
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
            rows.append(Bar(int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])))
        nxt = int(batch[-1][0]) + 1
        if nxt <= cur:
            break
        cur = nxt
        if len(batch) < 1500:
            break
        time.sleep(0.04)
    seen: set[int] = set()
    out: list[Bar] = []
    for b in rows:
        if b.ts not in seen:
            seen.add(b.ts)
            out.append(b)
    return sorted(out, key=lambda x: x.ts)


def list_symbols(max_n: int) -> list[str]:
    syms = sorted(p.stem for p in KDIR.glob("*.csv"))
    if syms:
        return syms[:max_n]
    return (
        "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT,ADAUSDT,DOGEUSDT,AVAXUSDT,LINKUSDT,DOTUSDT,"
        "LTCUSDT,BCHUSDT,NEARUSDT,FILUSDT,APTUSDT,ARBUSDT,OPUSDT,INJUSDT,ATOMUSDT,UNIUSDT,"
        "SUIUSDT,SEIUSDT,TIAUSDT,WLDUSDT,RENDERUSDT,FETUSDT,PEPEUSDT,WIFUSDT,BONKUSDT,FLOKIUSDT,"
        "ENAUSDT,ONDOUSDT,PYTHUSDT,JUPUSDT,STRKUSDT,ALTUSDT,PIXELUSDT,PORTALUSDT,AEVOUSDT,"
        "METISUSDT,AXLUSDT,TAOUSDT,OMUSDT,NOTUSDT,IOUSDT,ZKUSDT,LISTAUSDT,ZROUSDT,BANANAUSDT"
    ).split(",")[:max_n]


def pnl(side: str, entry: float, exit_px: float) -> float:
    g = (exit_px - entry) / entry * 100 if side == "long" else (entry - exit_px) / entry * 100
    return NOTIONAL * g / 100 - NOTIONAL * FEE_RT


def backtest_symbol(symbol: str, eval_start_ms: int, end_ms: int, signal_mode: str, tp_pct: float, sl_pct: float) -> list[Trade]:
    warmup_ms = eval_start_ms - 5 * 24 * BAR_5M
    bars = fetch_klines(symbol, warmup_ms, end_ms)
    if len(bars) < 250:
        return []

    signals = compute_sr_signals(bars, LOOKBACK, VOL_LEN, BOX_WIDTH)
    trades: list[Trade] = []
    pos: dict | None = None

    for i, b in enumerate(bars):
        if b.ts < eval_start_ms:
            continue

        if pos:
            side = pos["side"]
            entry = pos["entry_px"]
            tp = entry * (1 + tp_pct / 100) if side == "long" else entry * (1 - tp_pct / 100)
            sl = entry * (1 - sl_pct / 100) if side == "long" else entry * (1 + sl_pct / 100)
            bars_held = i - pos["entry_i"]
            hit = None
            if side == "long":
                if b.l <= sl:
                    hit = ("sl", sl)
                elif b.h >= tp:
                    hit = ("tp", tp)
            else:
                if b.h >= sl:
                    hit = ("sl", sl)
                elif b.l <= tp:
                    hit = ("tp", tp)
            if hit is None and bars_held >= MAX_BARS:
                hit = ("timeout", b.c)
            if hit:
                reason, px = hit
                trades.append(
                    Trade(
                        symbol,
                        side,
                        pos["signal"],
                        pos["entry_ts"],
                        entry,
                        b.ts,
                        px,
                        reason,
                        pnl(side, entry, px),
                    )
                )
                pos = None

        sig = signals[i]
        side = entry_side(sig, signal_mode)
        if side and pos is None:
            pos = {
                "side": side,
                "entry_ts": b.ts,
                "entry_px": b.c,
                "entry_i": i,
                "signal": signal_name(sig),
            }

    return trades


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=float, default=24.0)
    ap.add_argument("--notional", type=float, default=6.0)
    ap.add_argument("--max-symbols", type=int, default=50)
    ap.add_argument("--symbols", default="")
    ap.add_argument("--signal-mode", choices=["all", "holds", "breaks"], default="all")
    ap.add_argument("--tp-pct", type=float, default=1.5)
    ap.add_argument("--sl-pct", type=float, default=8.0)
    args = ap.parse_args()
    global NOTIONAL
    NOTIONAL = args.notional
    tp_pct = args.tp_pct
    sl_pct = args.sl_pct

    if args.symbols.strip():
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    else:
        symbols = list_symbols(args.max_symbols)

    end_ms = int(time.time() * 1000)
    eval_start_ms = end_ms - int(args.hours * 3600 * 1000)
    eval_start = datetime.fromtimestamp(eval_start_ms / 1000, tz=timezone.utc)

    print(
        f"ChartPrime SR backtest | 5m | last {args.hours}h | ${NOTIONAL} notional | "
        f"TP {tp_pct}% SL {sl_pct}% | signals={args.signal_mode}"
    )
    print(f"lookback={LOOKBACK} vol_len={VOL_LEN} box_width={BOX_WIDTH}")
    print(f"Eval from {eval_start.isoformat()} UTC | {len(symbols)} symbols\n")
    print(f"{'Symbol':<16} {'Trades':>6} {'Net$':>9} {'WR%':>6} {'L':>3} {'S':>3}  Top signals")
    print("-" * 78)

    all_trades: list[Trade] = []
    for sym in symbols:
        tr = backtest_symbol(sym, eval_start_ms, end_ms, args.signal_mode, tp_pct, sl_pct)
        all_trades.extend(tr)
        if not tr:
            print(f"{sym:<16} {0:>6} {'—':>9} {'—':>6}")
            continue
        net = sum(t.net_usd for t in tr)
        wr = 100 * sum(1 for t in tr if t.net_usd > 0) / len(tr)
        lng = sum(1 for t in tr if t.side == "long")
        sht = len(tr) - lng
        from collections import Counter

        top = ", ".join(f"{k}×{v}" for k, v in Counter(t.signal for t in tr).most_common(2))
        print(f"{sym:<16} {len(tr):>6} {net:>+9.2f} {wr:>5.1f}% {lng:>3} {sht:>3}  {top}")

    print("-" * 78)
    if all_trades:
        net = sum(t.net_usd for t in all_trades)
        wr = 100 * sum(1 for t in all_trades if t.net_usd > 0) / len(all_trades)
        lng = sum(1 for t in all_trades if t.side == "long")
        from collections import Counter

        print(
            f"{'TOTAL':<16} {len(all_trades):>6} {net:>+9.2f} {wr:>5.1f}% "
            f"{lng:>3} {len(all_trades)-lng:>3}"
        )
        print("\nSignal breakdown:")
        for k, v in Counter(t.signal for t in all_trades).most_common():
            sub = [t for t in all_trades if t.signal == k]
            print(f"  {k:<14} {v:>4} trades  net ${sum(t.net_usd for t in sub):+.2f}")
        print(f"\nExit reasons: {dict(Counter(t.reason for t in all_trades))}")
    else:
        print("No trades in eval window.")


if __name__ == "__main__":
    main()
