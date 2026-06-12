#!/usr/bin/env python3
"""
Ms-based exit policies on user's log trades (aggTrades replay).
Entry: 0.7% first cross OR 2% violent cross + measured delay/slip.
No minute holds — only millisecond rules.
"""
from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Dict, List, Optional, Tuple

# reuse trade list from sibling script
from backtest_07_vs_2pct import (
    ENTRY_CAP_PCT,
    FAST_MS,
    MIN_SEC_NOTIONAL,
    SEC_MS,
    TRADES,
    VIOLENT_PCT,
    adverse_fill,
    fetch_agg,
    find_first_cross,
    find_violent_cross,
    parse_utc,
    price_at_delay,
    slip_bps,
)

MARGIN = 2.0
LEV = 10
DELAY_MS = 35
DEFAULT_SLIP_BPS = 40


def pnl_usdt(side: str, entry: float, exit_px: float) -> Tuple[float, float]:
    if entry <= 0 or exit_px <= 0:
        return 0.0, 0.0
    if side == "BUY":
        pct = (exit_px - entry) / entry * 100.0
    else:
        pct = (entry - exit_px) / entry * 100.0
    return pct, MARGIN * LEV * (pct / 100.0)


def move_pct(side: str, entry: float, px: float) -> float:
    if entry <= 0:
        return 0.0
    if side == "BUY":
        return (px - entry) / entry * 100.0
    return (entry - px) / entry * 100.0


def walk_after(ticks: List[dict], entry_ms: int, max_ms: int) -> List[dict]:
    end = entry_ms + max_ms
    return [x for x in ticks if entry_ms <= x["t"] <= end]


# --- ms exit policies ---


def exit_fixed_ms(ticks: List[dict], side: str, entry_ms: int, entry_px: float, hold_ms: int) -> Tuple[float, str]:
    w = walk_after(ticks, entry_ms, hold_ms)
    if not w:
        return entry_px, f"fix_{hold_ms}ms_empty"
    last = w[-1]
    return last["p"], f"fix_{hold_ms}ms"


def exit_tp_sl_timeout(
    ticks: List[dict],
    side: str,
    entry_ms: int,
    entry_px: float,
    tp_pct: float,
    sl_pct: float,
    max_ms: int,
) -> Tuple[float, str]:
    peak = 0.0
    for x in walk_after(ticks, entry_ms, max_ms):
        m = move_pct(side, entry_px, x["p"])
        if m > peak:
            peak = m
        if m >= tp_pct:
            return x["p"], f"tp_{tp_pct:.2f}%@{x['t']-entry_ms}ms"
        if m <= -sl_pct:
            return x["p"], f"sl_{sl_pct:.2f}%@{x['t']-entry_ms}ms"
    w = walk_after(ticks, entry_ms, max_ms)
    if w:
        return w[-1]["p"], f"tout_{max_ms}ms"
    return entry_px, f"tout_{max_ms}ms_empty"


def exit_reversal_from_peak(
    ticks: List[dict],
    side: str,
    entry_ms: int,
    entry_px: float,
    min_peak_pct: float,
    giveback_pct: float,
    max_ms: int,
) -> Tuple[float, str]:
    peak = 0.0
    for x in walk_after(ticks, entry_ms, max_ms):
        m = move_pct(side, entry_px, x["p"])
        if m > peak:
            peak = m
        if peak >= min_peak_pct and m <= peak - giveback_pct:
            return x["p"], f"rev_pk{min_peak_pct:.2f}_gb{giveback_pct:.2f}@{x['t']-entry_ms}ms"
    w = walk_after(ticks, entry_ms, max_ms)
    if w:
        return w[-1]["p"], f"rev_tout_{max_ms}ms"
    return entry_px, "rev_empty"


def exit_first_fav_then_adv(
    ticks: List[dict],
    side: str,
    entry_ms: int,
    entry_px: float,
    need_fav_pct: float,
    adv_pct: float,
    max_ms: int,
) -> Tuple[float, str]:
    """After fav >= need_fav, exit if adverse >= adv from entry (ms reversal)."""
    got_fav = False
    for x in walk_after(ticks, entry_ms, max_ms):
        m = move_pct(side, entry_px, x["p"])
        if m >= need_fav_pct:
            got_fav = True
        if got_fav and m <= -adv_pct:
            return x["p"], f"fav{need_fav_pct:.2f}_then_adv{adv_pct:.2f}@{x['t']-entry_ms}ms"
        if not got_fav and m <= -adv_pct:
            return x["p"], f"no_fav_sl{adv_pct:.2f}@{x['t']-entry_ms}ms"
    w = walk_after(ticks, entry_ms, max_ms)
    if w:
        return w[-1]["p"], f"ff_tout_{max_ms}ms"
    return entry_px, "ff_empty"


# policy catalog
POLICIES: List[Tuple[str, Callable]] = []

for ms in [50, 100, 150, 200, 300, 500, 750, 1000, 1500, 2000, 3000, 5000]:
    POLICIES.append(
        (f"FIX_{ms}ms", lambda t, s, em, ep, m=ms: exit_fixed_ms(t, s, em, ep, m))
    )

for tp, sl, mx in [
    (0.15, 0.25, 500),
    (0.20, 0.30, 500),
    (0.25, 0.35, 800),
    (0.30, 0.40, 1000),
    (0.35, 0.50, 1500),
    (0.50, 0.60, 2000),
]:
    POLICIES.append(
        (
            f"TP{tp:.2f}_SL{sl:.2f}_MAX{mx}ms",
            lambda t, s, em, ep, tp=tp, sl=sl, mx=mx: exit_tp_sl_timeout(t, s, em, ep, tp, sl, mx),
        )
    )

for pk, gb, mx in [
    (0.10, 0.08, 800),
    (0.15, 0.10, 1000),
    (0.20, 0.12, 1200),
    (0.25, 0.15, 1500),
    (0.35, 0.18, 2000),
]:
    POLICIES.append(
        (
            f"REVpk{pk:.2f}_gb{gb:.2f}_MAX{mx}ms",
            lambda t, s, em, ep, pk=pk, gb=gb, mx=mx: exit_reversal_from_peak(
                t, s, em, ep, pk, gb, mx
            ),
        )
    )

for fav, adv, mx in [
    (0.10, 0.15, 600),
    (0.15, 0.20, 800),
    (0.20, 0.25, 1000),
]:
    POLICIES.append(
        (
            f"FAV{fav:.2f}_ADV{adv:.2f}_MAX{mx}ms",
            lambda t, s, em, ep, fav=fav, adv=adv, mx=mx: exit_first_fav_then_adv(
                t, s, em, ep, fav, adv, mx
            ),
        )
    )


def build_entry(
    ticks: List[dict],
    lt,
    mode: str,
    slip_bps_use: float,
) -> Optional[Tuple[int, float, dict]]:
    sig_ms = int(parse_utc(lt.signal_utc).timestamp() * 1000)
    win_start = sig_ms - 30_000
    if mode == "07":
        cross = find_first_cross(
            ticks, lt.side, ENTRY_CAP_PCT, ENTRY_CAP_PCT + 0.15, win_start, sig_ms + 5000
        )
        if cross is None:
            cross = find_first_cross(
                ticks, lt.side, ENTRY_CAP_PCT, ENTRY_CAP_PCT + 0.15, win_start, sig_ms + 120_000
            )
    else:
        cross = find_violent_cross(ticks, lt.side, win_start, sig_ms + 5000)
    if cross is None:
        return None
    px = price_at_delay(ticks, cross["t_ms"], DELAY_MS)
    entry_px = adverse_fill(lt.side, px, slip_bps_use)
    entry_ms = cross["t_ms"] + DELAY_MS
    return entry_ms, entry_px, cross


def run():
    entry_modes = ["07", "2pct"]
    slip_map = {}
    for lt in TRADES:
        if lt.live_entry and lt.signal_entry:
            slip_map[id(lt)] = max(0.0, -slip_bps(lt.side, lt.signal_entry, lt.live_entry))
        else:
            slip_map[id(lt)] = DEFAULT_SLIP_BPS

    totals: Dict[str, Dict] = {}
    per_trade: List[dict] = []

    for idx, lt in enumerate(TRADES):
        sig_ms = int(parse_utc(lt.signal_utc).timestamp() * 1000)
        pad = 120_000
        ticks = fetch_agg(lt.symbol, sig_ms - 30_000, sig_ms + pad)
        print(f"[{idx+1}/{len(TRADES)}] {lt.symbol} ticks={len(ticks)}", flush=True)
        slip = slip_map[id(lt)]

        row = {"symbol": lt.symbol, "side": lt.side, "log_live_pnl": lt.live_pnl_usdt}
        for mode in entry_modes:
            ent = build_entry(ticks, lt, mode, slip)
            if ent is None:
                row[f"entry_{mode}"] = None
                continue
            entry_ms, entry_px, cross = ent
            row[f"entry_{mode}"] = {
                "sec": round(cross["sec"], 3),
                "ms_before_log": sig_ms - cross["t_ms"],
            }
            pol_pnls = {}
            for pname, fn in POLICIES:
                exit_px, detail = fn(ticks, lt.side, entry_ms, entry_px)
                pct, usdt = pnl_usdt(lt.side, entry_px, exit_px)
                key = f"{mode}|{pname}"
                pol_pnls[key] = {
                    "pnl_usdt": round(usdt, 4),
                    "pnl_pct": round(pct, 3),
                    "detail": detail,
                }
                totals.setdefault(key, {"n": 0, "usdt": 0.0, "wins": 0})
                totals[key]["n"] += 1
                totals[key]["usdt"] += usdt
                if usdt > 0:
                    totals[key]["wins"] += 1
            row[f"policies_{mode}"] = pol_pnls
        per_trade.append(row)
        time.sleep(0.15)

    # rank policies per entry mode
    print("\n=== TOP 15 ms policies (0.7% entry, with slip) ===")
    rank07 = sorted(
        [(k, v) for k, v in totals.items() if k.startswith("07|")],
        key=lambda x: x[1]["usdt"],
        reverse=True,
    )
    for k, v in rank07[:15]:
        print(f"  {k[3:]}: {v['usdt']:+.3f} USDT  wins={v['wins']}/{v['n']}")

    print("\n=== TOP 15 ms policies (2% entry, with slip) ===")
    rank2 = sorted(
        [(k, v) for k, v in totals.items() if k.startswith("2pct|")],
        key=lambda x: x[1]["usdt"],
        reverse=True,
    )
    for k, v in rank2[:15]:
        print(f"  {k[3:]}: {v['usdt']:+.3f} USDT  wins={v['wins']}/{v['n']}")

    print("\n=== WORST 5 (0.7% entry) — overfit warning ===")
    for k, v in rank07[-5:]:
        print(f"  {k[3:]}: {v['usdt']:+.3f} USDT")

    # compare to 60s no_mega from prior run
    print("\n=== vs minute hold (from prior backtest C_early_07 / B_replay_2pct) ===")
    print("  60s no_mega ~ C_early_07: -1.36 USDT | B_replay_2pct: -1.45 USDT (12-14 trades)")

    best07 = rank07[0] if rank07 else None
    best2 = rank2[0] if rank2 else None
    if best07:
        print(f"\n  BEST 0.7% ms policy: {best07[0][3:]} → {best07[1]['usdt']:+.3f} USDT")
    if best2:
        print(f"  BEST 2% ms policy:   {best2[0][3:]} → {best2[1]['usdt']:+.3f} USDT")

    # per-trade best ms policy for 07 entry
    print("\n=== Per trade: best ms exit (0.7% entry) vs log live ===")
    for row in per_trade:
        pols = row.get("policies_07") or {}
        if not pols:
            print(f"  {row['symbol']} {row['side']}: no 0.7% entry")
            continue
        best = max(pols.items(), key=lambda x: x[1]["pnl_usdt"])
        logp = row.get("log_live_pnl")
        logs = f"log={logp:+.2f}" if logp is not None else "sim/no-live"
        print(
            f"  {row['symbol']} {row['side']}: {logs} | best_ms={best[0].split('|',1)[1]} "
            f"{best[1]['pnl_usdt']:+.3f} ({best[1]['detail']})"
        )

    out = "/home/deeporion/Desktop/binance/crypto_announcements_go/scripts/backtest_ms_results.json"
    with open(out, "w") as f:
        json.dump({"totals": totals, "trades": per_trade}, f, indent=2)
    print(f"\nWrote {out}")


if __name__ == "__main__":
    run()
