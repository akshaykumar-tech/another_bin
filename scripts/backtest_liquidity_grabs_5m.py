#!/usr/bin/env python3
"""
Backtest Flux Charts Liquidity Grabs on Binance futures 5m — last 24h.

  sellside grab (sweep lows, close back up) → LONG
  buyside grab (sweep highs, close back down) → SHORT
  SL beyond grab wick | TP 1.5% | max hold 96 bars
"""
from __future__ import annotations

import argparse
import json
import random
import time
import urllib.request
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone

from liquidity_grabs_lib import Bar, GrabState, on_bar_confirmed

FAPI = "https://fapi.binance.com"
NOTIONAL = 6.0
FEE_RT = 0.0008
BAR_5M = 300_000
MAX_BARS = 96
PIVOT_LEN = 25
WBR = 0.5
COOLDOWN = 3
SL_PCT = 8.0
TP_PCT = 1.5
SHORTS_ONLY = True  # buyside grabs only — sellside longs drag in 24h samples
MIN_GRAB_SIZE = 1   # 1=all, 2=medium+, 3=large only


@dataclass
class Trade:
    symbol: str
    side: str
    grab_type: str
    grab_size: int
    entry_ts: int
    entry_px: float
    exit_ts: int
    exit_px: float
    sl_px: float
    tp_px: float
    reason: str
    net_usd: float
    hold_bars: int


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
        time.sleep(0.03)
    seen: set[int] = set()
    out: list[Bar] = []
    for b in rows:
        if b.ts not in seen:
            seen.add(b.ts)
            out.append(b)
    return sorted(out, key=lambda x: x.ts)


def list_random_symbols(n: int, seed: int) -> list[str]:
    req = urllib.request.Request(f"{FAPI}/fapi/v1/exchangeInfo", headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        info = json.loads(resp.read())
    syms = [
        s["symbol"]
        for s in info["symbols"]
        if s.get("contractType") == "PERPETUAL"
        and s.get("quoteAsset") == "USDT"
        and s.get("status") == "TRADING"
        and s["symbol"].isascii()
    ]
    rng = random.Random(seed)
    rng.shuffle(syms)
    return sorted(syms[:n])


def pnl(side: str, entry: float, exit_px: float) -> float:
    g = (exit_px - entry) / entry * 100 if side == "long" else (entry - exit_px) / entry * 100
    return NOTIONAL * g / 100 - NOTIONAL * FEE_RT


def backtest_symbol(symbol: str, eval_start_ms: int, end_ms: int) -> list[Trade]:
    warmup_ms = eval_start_ms - 5 * 24 * BAR_5M
    bars = fetch_klines(symbol, warmup_ms, end_ms)
    need = 2 * PIVOT_LEN + 50
    if len(bars) < need:
        return []

    st = GrabState(pivot_len=PIVOT_LEN, wbr=WBR, cooldown=COOLDOWN, tp_pct=TP_PCT)
    trades: list[Trade] = []
    pos: dict | None = None

    for i in range(len(bars)):
        bar = bars[i]
        sig = on_bar_confirmed(st, bars, i)

        if bar.ts >= eval_start_ms and pos:
            side = pos["side"]
            entry = pos["entry_px"]
            tp, sl = pos["tp_px"], pos["sl_px"]
            bars_held = i - pos["entry_i"]
            reason, exit_px = None, None
            if side == "long":
                if bar.l <= sl:
                    reason, exit_px = "sl", sl
                elif bar.h >= tp:
                    reason, exit_px = "tp", tp
            else:
                if bar.h >= sl:
                    reason, exit_px = "sl", sl
                elif bar.l <= tp:
                    reason, exit_px = "tp", tp
            if reason is None and bars_held >= MAX_BARS:
                reason, exit_px = "timeout", bar.c
            if reason:
                trades.append(
                    Trade(
                        symbol, side, pos["grab_type"], pos["grab_size"],
                        pos["entry_ts"], entry, bar.ts, exit_px, sl, tp, reason,
                        pnl(side, entry, exit_px), bars_held,
                    )
                )
                pos = None

        if bar.ts >= eval_start_ms and pos is None and sig is not None:
            if SHORTS_ONLY and sig.side != "short":
                continue
            if sig.grab_size < MIN_GRAB_SIZE:
                continue
            entry = sig.entry_px
            if sig.side == "long":
                sl_px = entry * (1 - SL_PCT / 100)
                tp_px = entry * (1 + TP_PCT / 100)
            else:
                sl_px = entry * (1 + SL_PCT / 100)
                tp_px = entry * (1 - TP_PCT / 100)
            pos = {
                "side": sig.side,
                "grab_type": sig.grab_type,
                "grab_size": sig.grab_size,
                "entry_ts": sig.ts,
                "entry_px": entry,
                "entry_i": i,
                "sl_px": sl_px,
                "tp_px": tp_px,
            }

    return trades


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", type=int, default=50)
    ap.add_argument("--hours", type=float, default=24)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    eval_start_ms = end_ms - int(args.hours * 3600 * 1000)
    syms = list_random_symbols(args.symbols, args.seed)

    print(
        f"Flux Liquidity Grabs | 5m | last {args.hours}h | {len(syms)} symbols (seed={args.seed})"
    )
    print(
        f"  pivot={PIVOT_LEN} WBR={WBR} cooldown={COOLDOWN} | "
        f"buyside grab→SHORT only={SHORTS_ONLY} min_size={MIN_GRAB_SIZE} | "
        f"SL={SL_PCT}% TP={TP_PCT}% | max_hold={MAX_BARS}bars | ${NOTIONAL}\n"
    )

    all_trades: list[Trade] = []
    sym_with = 0
    for j, sym in enumerate(syms):
        tr = backtest_symbol(sym, eval_start_ms, end_ms)
        if tr:
            sym_with += 1
            all_trades.extend(tr)
        if (j + 1) % 10 == 0:
            print(f"  ... {j+1}/{len(syms)}", flush=True)
        time.sleep(0.04)

    if not all_trades:
        print("No trades.")
        return

    net = sum(t.net_usd for t in all_trades)
    wins = sum(1 for t in all_trades if t.net_usd > 0)
    by_reason = defaultdict(list)
    by_side = defaultdict(list)
    by_type = defaultdict(list)
    by_size = defaultdict(list)
    for t in all_trades:
        by_reason[t.reason].append(t)
        by_side[t.side].append(t)
        by_type[t.grab_type].append(t)
        by_size[t.grab_size].append(t)

    print(f"Symbols w/ trades: {sym_with}/{len(syms)}")
    print(f"Total trades: {len(all_trades)}")
    print(f"Net PnL: ${net:+.4f}")
    print(f"Win rate: {wins/len(all_trades)*100:.1f}% ({wins}W / {len(all_trades)-wins}L)")
    print(f"Avg hold: {sum(t.hold_bars for t in all_trades)/len(all_trades):.1f} bars\n")

    print("By exit:")
    for r in ("tp", "sl", "timeout"):
        g = by_reason.get(r, [])
        if g:
            print(f"  {r:7} n={len(g):3} net=${sum(t.net_usd for t in g):+.4f}")

    print("\nBy grab type:")
    for k in ("sellside", "buyside"):
        g = by_type.get(k, [])
        if g:
            print(f"  {k:9} n={len(g):3} net=${sum(t.net_usd for t in g):+.4f}")

    print("\nBy side:")
    for k in ("long", "short"):
        g = by_side.get(k, [])
        if g:
            print(f"  {k:5} n={len(g):3} net=${sum(t.net_usd for t in g):+.4f}")

    print("\nBy grab size (wick strength):")
    for sz in sorted(by_size):
        g = by_size[sz]
        label = {1: "small", 2: "medium", 3: "large"}.get(sz, str(sz))
        print(f"  {label:6} n={len(g):3} net=${sum(t.net_usd for t in g):+.4f}")

    best = max(all_trades, key=lambda t: t.net_usd)
    worst = min(all_trades, key=lambda t: t.net_usd)
    print(f"\nBest:  {best.symbol} {best.grab_type} sz={best.grab_size} ${best.net_usd:+.4f}")
    print(f"Worst: {worst.symbol} {worst.grab_type} sz={worst.grab_size} ${worst.net_usd:+.4f}")


if __name__ == "__main__":
    main()
