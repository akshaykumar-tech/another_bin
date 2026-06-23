#!/usr/bin/env python3
"""
Backtest SMC Supply/Demand (BOS/CHoCH order blocks) — LuxAlgo-style indicator port.

  Bull BOS/CHoCH → demand zone → LONG
  Bear BOS/CHoCH → supply zone → SHORT

  python3 scripts/backtest_smc_sd_5m.py --symbols 50 --hours 24 --seed 42
"""
from __future__ import annotations

import argparse
import json
import random
import time
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone

from smc_sd_lib import SDSignal, SDState, on_bar_confirmed
from sr_chartprime_lib import Bar

FAPI = "https://fapi.binance.com"
NOTIONAL = 6.0
FEE_RT = 0.0008
BAR_5M = 300_000
MAX_BARS = 96
SWING_LEN = 50
SL_MODE = "pct"  # pct | ob
SL_PCT = 8.0
TP_PCT = 1.5
OB_SL_PAD = 0.001  # 0.1% beyond OB edge


@dataclass
class Trade:
    symbol: str
    side: str
    setup: str
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


def sl_tp(sig: SDSignal) -> tuple[float, float]:
    entry = sig.entry_px
    if SL_MODE == "ob":
        if sig.side == "long":
            sl = sig.ob_btm * (1 - OB_SL_PAD)
            tp = entry * (1 + TP_PCT / 100)
        else:
            sl = sig.ob_top * (1 + OB_SL_PAD)
            tp = entry * (1 - TP_PCT / 100)
    else:
        if sig.side == "long":
            sl = entry * (1 - SL_PCT / 100)
            tp = entry * (1 + TP_PCT / 100)
        else:
            sl = entry * (1 + SL_PCT / 100)
            tp = entry * (1 - TP_PCT / 100)
    return sl, tp


def backtest_symbol(
    symbol: str,
    eval_start_ms: int,
    end_ms: int,
    longs_only: bool,
    shorts_only: bool,
    choch_only: bool,
) -> list[Trade]:
    warmup_ms = eval_start_ms - 7 * 24 * BAR_5M
    bars = fetch_klines(symbol, warmup_ms, end_ms)
    need = SWING_LEN + 60
    if len(bars) < need:
        return []

    st = SDState(swing_len=SWING_LEN)
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
                        symbol, side, pos["setup"],
                        pos["entry_ts"], entry, bar.ts, exit_px, sl, tp, reason,
                        pnl(side, entry, exit_px), bars_held,
                    )
                )
                pos = None

        if bar.ts >= eval_start_ms and pos is None and sig is not None:
            if longs_only and sig.side != "long":
                continue
            if shorts_only and sig.side != "short":
                continue
            if choch_only and "choch" not in sig.setup:
                continue
            sl_px, tp_px = sl_tp(sig)
            pos = {
                "side": sig.side,
                "setup": sig.setup,
                "entry_ts": sig.ts,
                "entry_px": sig.entry_px,
                "entry_i": i,
                "sl_px": sl_px,
                "tp_px": tp_px,
            }

    return trades


def run_variant(
    name: str,
    syms: list[str],
    eval_start_ms: int,
    end_ms: int,
    longs_only: bool,
    shorts_only: bool,
    choch_only: bool,
) -> None:
    all_trades: list[Trade] = []
    sym_with = 0
    for j, sym in enumerate(syms):
        tr = backtest_symbol(sym, eval_start_ms, end_ms, longs_only, shorts_only, choch_only)
        if tr:
            sym_with += 1
            all_trades.extend(tr)
        if (j + 1) % 10 == 0:
            print(f"  [{name}] ... {j+1}/{len(syms)}", flush=True)
        time.sleep(0.04)

    print(f"\n=== {name} ===")
    if not all_trades:
        print("No trades.\n")
        return

    net = sum(t.net_usd for t in all_trades)
    wins = sum(1 for t in all_trades if t.net_usd > 0)
    by_reason = defaultdict(list)
    by_setup = defaultdict(list)
    for t in all_trades:
        by_reason[t.reason].append(t)
        by_setup[t.setup].append(t)

    print(f"Symbols w/ trades: {sym_with}/{len(syms)}")
    print(f"Total trades: {len(all_trades)}")
    print(f"Net PnL: ${net:+.4f}")
    print(f"Win rate: {wins/len(all_trades)*100:.1f}%")
    print(f"Avg hold: {sum(t.hold_bars for t in all_trades)/len(all_trades):.1f} bars")
    print("By exit:", end=" ")
    print(", ".join(f"{r}={len(by_reason[r])}" for r in ("tp", "sl", "timeout") if by_reason[r]))
    print("By setup:")
    for k in sorted(by_setup):
        g = by_setup[k]
        print(f"  {k:14} n={len(g):3} net=${sum(t.net_usd for t in g):+.4f}")
    print()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", type=int, default=50)
    ap.add_argument("--hours", type=float, default=24)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--sl-mode", choices=("pct", "ob"), default="pct")
    ap.add_argument(
        "--variant",
        choices=("all", "short", "long", "choch", "full"),
        default="full",
        help="full=all four reports; short=supply SHORT only (recommended)",
    )
    args = ap.parse_args()

    global SL_MODE
    SL_MODE = args.sl_mode

    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    eval_start_ms = end_ms - int(args.hours * 3600 * 1000)
    syms = list_random_symbols(args.symbols, args.seed)

    sl_desc = f"SL={SL_PCT}% OB-pad" if SL_MODE == "ob" else f"SL={SL_PCT}%"
    print(
        f"SMC Supply/Demand (BOS/CHoCH) | 5m | last {args.hours}h | {len(syms)} symbols (seed={args.seed})"
    )
    print(
        f"  swing_len={SWING_LEN} | {sl_desc} TP={TP_PCT}% | max_hold={MAX_BARS}bars | ${NOTIONAL}\n"
    )

    variants = {
        "all": [("ALL (long+short)", False, False, False)],
        "short": [("SHORT only (supply BOS/CHoCH)", False, True, False)],
        "long": [("LONG only (demand BOS/CHoCH)", True, False, False)],
        "choch": [("CHoCH only (reversal)", False, False, True)],
        "full": [
            ("ALL (long+short)", False, False, False),
            ("SHORT only (supply BOS/CHoCH)", False, True, False),
            ("LONG only (demand BOS/CHoCH)", True, False, False),
            ("CHoCH only (reversal)", False, False, True),
        ],
    }
    for name, lo, sh, ch in variants[args.variant]:
        run_variant(name, syms, eval_start_ms, end_ms, lo, sh, ch)


if __name__ == "__main__":
    main()
