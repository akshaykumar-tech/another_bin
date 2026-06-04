#!/usr/bin/env python3
"""
Replay user whale logs: 0.7% early entry vs actual ~2% violent entry.
Uses Binance futures aggTrades; applies measured signal→live open delay + slippage.
"""
from __future__ import annotations

import json
import statistics
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Tuple

BASE = "https://fapi.binance.com/fapi/v1/aggTrades"

FAST_MS = 100
SEC_MS = 1000
ENTRY_CAP_PCT = 0.70
VIOLENT_PCT = 2.0
MIN_SEC_NOTIONAL = 10_000  # rough filter like bot
DEFAULT_DELAY_MS = 35  # median from user logs
DEFAULT_SLIP_BPS = 40  # adverse if not measured per trade


@dataclass
class LogTrade:
    symbol: str
    side: str  # BUY / SELL
    signal_utc: str
    signal_entry: float
    live_entry: Optional[float]
    live_pnl_usdt: Optional[float]
    exit_reason: str
    hold_sec: float
    sec_at_signal: float
    fast_at_signal: float
    has_live: bool = True


# All trades from user chat logs (journal + stdout)
TRADES: List[LogTrade] = [
    LogTrade("PTBUSDT", "SELL", "2026-06-04T04:56:04Z", 0.000628, 0.000637, 0.10, "no_mega", 60, -2.04, -2.00),
    LogTrade("GTCUSDT", "SELL", "2026-06-04T05:17:27Z", 0.081289, 0.080790, -0.26, "no_mega", 60, -2.47, -2.47),
    LogTrade("STARUSDT", "BUY", "2026-06-04T05:39:44Z", 0.174117, None, None, "no_mega", 60, 2.67, 2.67, has_live=False),
    LogTrade("QUSDT", "BUY", "2026-06-04T06:04:00Z", 0.019547, 0.019563, -0.09, "no_mega", 60, 2.00, 2.00),
    LogTrade("STARUSDT", "SELL", "2026-06-04T00:34:28.785Z", 0.174213, 0.169560, 0.01, "trail", 2, -2.19, -2.19),
    LogTrade("SHELLUSDT", "SELL", "2026-06-04T01:18:12.456Z", 0.029695, 0.029998, -0.03, "no_mega", 60, -2.01, -2.01),
    LogTrade("CETUSUSDT", "SELL", "2026-06-04T01:21:25.335Z", 0.020490, 0.020760, -0.04, "no_mega", 61, -2.01, -2.01),
    LogTrade("AKEUSDT", "SELL", "2026-06-04T01:30:12.111Z", 0.000294, 0.000292, -0.18, "timeout", 600, -2.07, -2.00),
    LogTrade("BANUSDT", "BUY", "2026-06-04T01:54:08.146Z", 0.074177, 0.073330, -0.01, "no_mega", 63, 2.05, 2.05),
    LogTrade("DUSDT", "SELL", "2026-06-04T01:55:31.301Z", 0.009035, 0.009171, -0.09, "sl", 21, -2.02, -2.01),
    LogTrade("PTBUSDT", "BUY", "2026-06-03T19:22:06.969Z", 0.000753, 0.000745, -0.14, "no_mega", 62, 2.01, 2.01),
    LogTrade("DYMUSDT", "BUY", "2026-06-03T21:20:55.672Z", 0.019530, 0.019530, -0.48, "sl", 36, 2.04, 0.31),
    LogTrade("TRADOORUSDT", "SELL", "2026-06-03T21:55:04.567Z", 0.470065, 0.471600, -0.20, "no_mega", 61, -2.02, -1.96),
    LogTrade("MEWUSDT", "BUY", "2026-06-03T22:25:08.459Z", 0.000457, 0.000451, -0.14, "no_mega", 60, 2.05, 2.05),
]

MARGIN = 2.0  # ~user live margin USDT
LEV = 10


def parse_utc(s: str) -> datetime:
    s = s.replace("Z", "+00:00")
    if "." not in s.split("T")[-1]:
        s = s.replace("+00:00", ".000+00:00")
    return datetime.fromisoformat(s)


def fetch_agg(symbol: str, start_ms: int, end_ms: int) -> List[dict]:
    rows: List[dict] = []
    cur = start_ms
    while cur < end_ms:
        q = urllib.parse.urlencode(
            {"symbol": symbol, "startTime": cur, "endTime": end_ms, "limit": 1000}
        )
        url = f"{BASE}?{q}"
        req = urllib.request.Request(url, headers={"User-Agent": "whale-backtest/1"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            batch = json.loads(resp.read())
        if not batch:
            break
        rows.extend(batch)
        last_t = batch[-1]["T"]
        if last_t <= cur:
            break
        cur = last_t + 1
        time.sleep(0.08)
    # dedupe by agg id
    seen = set()
    out = []
    for r in rows:
        a = r["a"]
        if a in seen:
            continue
        seen.add(a)
        out.append(
            {
                "t": r["T"],
                "p": float(r["p"]),
                "q": float(r["q"]),
                "m": r["m"],
            }
        )
    out.sort(key=lambda x: x["t"])
    return out


def notional(ticks: List[dict]) -> float:
    return sum(x["p"] * x["q"] for x in ticks)


def window_move(ticks: List[dict], ms: int, now_ms: int) -> Tuple[float, float, float]:
    """Return (move_pct, notional, price_at_end) for window ending at now_ms."""
    start = now_ms - ms
    w = [x for x in ticks if start < x["t"] <= now_ms]
    if len(w) < 2:
        return 0.0, 0.0, 0.0
    p0, p1 = w[0]["p"], w[-1]["p"]
    if p0 <= 0:
        return 0.0, 0.0, p1
    return (p1 - p0) / p0 * 100.0, notional(w), p1


def slip_bps(side: str, signal_px: float, live_px: float) -> float:
    if signal_px <= 0 or live_px <= 0:
        return 0.0
    if side == "BUY":
        return (signal_px - live_px) / signal_px * 10000.0
    return (live_px - signal_px) / signal_px * 10000.0


def adverse_fill(side: str, px: float, bps: float) -> float:
    """Apply adverse slippage on top of tick price at signal+delay."""
    if px <= 0:
        return px
    if side == "BUY":
        return px * (1.0 + bps / 10000.0)
    return px * (1.0 - bps / 10000.0)


def pnl_usdt(side: str, entry: float, exit_px: float) -> Tuple[float, float]:
    if entry <= 0 or exit_px <= 0:
        return 0.0, 0.0
    if side == "BUY":
        pct = (exit_px - entry) / entry * 100.0
    else:
        pct = (entry - exit_px) / entry * 100.0
    usdt = MARGIN * LEV * (pct / 100.0)
    return pct, usdt


def find_first_cross(
    ticks: List[dict],
    side: str,
    threshold: float,
    max_cap: float,
    after_ms: int,
    before_ms: int,
) -> Optional[dict]:
    """First tick where |1s move| in [threshold, max_cap] and direction matches side."""
    for i in range(len(ticks)):
        t = ticks[i]["t"]
        if t < after_ms:
            continue
        if t > before_ms:
            break
        sec, n, px = window_move(ticks[: i + 1], SEC_MS, t)
        if n < MIN_SEC_NOTIONAL:
            continue
        ab = abs(sec)
        if ab < threshold or ab > max_cap:
            continue
        if side == "BUY" and sec <= 0:
            continue
        if side == "SELL" and sec >= 0:
            continue
        fast, _, _ = window_move(ticks[: i + 1], FAST_MS, t)
        return {
            "t_ms": t,
            "sec": sec,
            "fast": fast,
            "px": px,
            "notional": n,
        }
    return None


def find_violent_cross(ticks: List[dict], side: str, after_ms: int, before_ms: int) -> Optional[dict]:
    return find_first_cross(ticks, side, VIOLENT_PCT, 20.0, after_ms, before_ms)


def price_at_delay(ticks: List[dict], t_ms: int, delay_ms: int) -> float:
    target = t_ms + delay_ms
    for x in ticks:
        if x["t"] >= target:
            return x["p"]
    return ticks[-1]["p"] if ticks else 0.0


def simulate_exit(
    ticks: List[dict],
    side: str,
    entry_ms: int,
    entry_px: float,
    hold_sec: float,
    reason: str,
) -> Tuple[float, float, str, int]:
    """Walk ticks after entry; return exit_px, pnl%, detail, reversal_ms."""
    end_ms = entry_ms + int(hold_sec * 1000)
    sl_pct = 2.0
    best_fav = 0.0
    reversal_ms = -1
    exit_px = entry_px
    exit_detail = reason

    for x in ticks:
        if x["t"] < entry_ms:
            continue
        if x["t"] > end_ms + 5000:
            break
        px = x["p"]
        if side == "BUY":
            move = (px - entry_px) / entry_px * 100.0
        else:
            move = (entry_px - px) / entry_px * 100.0

        if move > best_fav:
            best_fav = move
        elif best_fav >= 0.15 and move < best_fav - 0.10 and reversal_ms < 0:
            reversal_ms = x["t"] - entry_ms

        if reason == "sl" and move <= -sl_pct:
            exit_px = px
            exit_detail = "sl_hit"
            return exit_px, move, exit_detail, reversal_ms

        if reason == "trail" and best_fav >= 0.5 and move <= best_fav * 0.5:
            exit_px = px
            exit_detail = "trail_sim"
            return exit_px, move, exit_detail, reversal_ms

        if x["t"] >= end_ms:
            exit_px = px
            return exit_px, move, exit_detail, reversal_ms

    exit_px = ticks[-1]["p"] if ticks else entry_px
    if side == "BUY":
        move = (exit_px - entry_px) / entry_px * 100.0
    else:
        move = (entry_px - exit_px) / entry_px * 100.0
    return exit_px, move, exit_detail, reversal_ms


def continuation_stats(ticks: List[dict], t_ms: int, side: str, entry_px: float) -> dict:
    """Max favorable / adverse move in +100ms, +500ms, +1s, +3s after signal tick."""
    horizons = [100, 500, 1000, 3000, 10000]
    out = {}
    for h in horizons:
        fav, adv = 0.0, 0.0
        for x in ticks:
            dt = x["t"] - t_ms
            if dt < 0 or dt > h:
                continue
            if side == "BUY":
                m = (x["p"] - entry_px) / entry_px * 100.0
            else:
                m = (entry_px - x["p"]) / entry_px * 100.0
            fav = max(fav, m)
            adv = min(adv, m)
        out[f"fav_{h}ms"] = fav
        out[f"adv_{h}ms"] = adv
    return out


def run_trade(lt: LogTrade) -> dict:
    sig_t = parse_utc(lt.signal_utc)
    sig_ms = int(sig_t.timestamp() * 1000)
    # window: 30s before signal to hold+30s after
    pad_before = 30_000
    pad_after = int(lt.hold_sec * 1000) + 45_000
    ticks = fetch_agg(lt.symbol, sig_ms - pad_before, sig_ms + pad_after)

    measured_delay = DEFAULT_DELAY_MS
    measured_slip = DEFAULT_SLIP_BPS
    if lt.live_entry and lt.signal_entry:
        measured_slip = max(0.0, -slip_bps(lt.side, lt.signal_entry, lt.live_entry))

    win_start = sig_ms - pad_before
    early = find_first_cross(
        ticks, lt.side, ENTRY_CAP_PCT, ENTRY_CAP_PCT + 0.15, win_start, sig_ms + 5000
    )
    violent = find_violent_cross(ticks, lt.side, win_start, sig_ms + 5000)

    rows = {
        "symbol": lt.symbol,
        "side": lt.side,
        "has_live": lt.has_live,
        "log_live_pnl": lt.live_pnl_usdt,
        "n_ticks": len(ticks),
    }

    if not ticks:
        rows["error"] = "no_aggTrades"
        return rows

    # If no 0.7% before log signal, try first 0.7% in whole window
    if early is None:
        early = find_first_cross(
            ticks, lt.side, ENTRY_CAP_PCT, ENTRY_CAP_PCT + 0.15, win_start, sig_ms + pad_after
        )
        rows["early_in_window_only"] = True

    if early:
        rows["early_ms_before_log_signal"] = sig_ms - early["t_ms"]
        rows["early_sec"] = round(early["sec"], 3)
        rows["early_fast"] = round(early["fast"], 3)
        cont = continuation_stats(ticks, early["t_ms"], lt.side, early["px"])
        rows.update({f"early_{k}": round(v, 4) for k, v in cont.items()})
    else:
        rows["early_signal"] = "NONE"

    if violent:
        rows["violent_ms_before_log"] = sig_ms - violent["t_ms"]
        rows["violent_sec"] = round(violent["sec"], 3)

    scenarios = []
    for name, cross, slip_bps_use, delay_ms in [
        ("A_log_2pct", None, measured_slip, measured_delay),
        ("B_replay_2pct", violent, measured_slip, measured_delay),
        ("C_early_07", early, measured_slip, measured_delay),
        ("D_early_07_zero_slip", early, 0.0, measured_delay),
        ("E_early_07_instant", early, measured_slip, 10),
    ]:
        if name == "A_log_2pct":
            if not lt.has_live or lt.live_entry is None:
                continue
            entry_px = lt.live_entry
            entry_ms = sig_ms + measured_delay
        elif cross is None:
            scenarios.append({"scenario": name, "skip": "no_cross"})
            continue
        else:
            tick_px = price_at_delay(ticks, cross["t_ms"], delay_ms)
            entry_px = adverse_fill(lt.side, tick_px, slip_bps_use)
            entry_ms = cross["t_ms"] + delay_ms

        exit_px, move_pct, detail, rev_ms = simulate_exit(
            ticks, lt.side, entry_ms, entry_px, lt.hold_sec, lt.exit_reason
        )
        pct, usdt = pnl_usdt(lt.side, entry_px, exit_px)
        scenarios.append(
            {
                "scenario": name,
                "entry_px": entry_px,
                "exit_px": exit_px,
                "pnl_pct": round(pct, 3),
                "pnl_usdt": round(usdt, 3),
                "exit": detail,
                "reversal_ms": rev_ms,
            }
        )

    rows["scenarios"] = scenarios
    return rows


def main():
    results = []
    for i, lt in enumerate(TRADES):
        print(f"[{i+1}/{len(TRADES)}] {lt.symbol} {lt.side} @ {lt.signal_utc} ...", flush=True)
        try:
            results.append(run_trade(lt))
        except Exception as e:
            results.append({"symbol": lt.symbol, "error": str(e)})
        time.sleep(0.2)

    # aggregate
    sums = {}
    for r in results:
        for sc in r.get("scenarios", []):
            if sc.get("skip"):
                continue
            k = sc["scenario"]
            sums.setdefault(k, {"n": 0, "usdt": 0.0, "wins": 0})
            sums[k]["n"] += 1
            sums[k]["usdt"] += sc["pnl_usdt"]
            if sc["pnl_usdt"] > 0:
                sums[k]["wins"] += 1

    print("\n=== AGGREGATE (margin=2 lev=10, same hold/exit as log) ===")
    for k, v in sorted(sums.items()):
        print(f"  {k}: trades={v['n']} wins={v['wins']} total_pnl={v['usdt']:+.3f} USDT")

    print("\n=== PER TRADE ===")
    for r in results:
        print(f"\n--- {r.get('symbol')} {r.get('side')} ticks={r.get('n_ticks')} ---")
        if r.get("error"):
            print(f"  ERROR: {r['error']}")
            continue
        if r.get("early_signal") == "NONE":
            print("  0.7% cross: NONE (flash skipped early band)")
        elif "early_ms_before_log_signal" in r:
            print(
                f"  0.7% cross: {r.get('early_sec')}% "
                f"{r['early_ms_before_log_signal']}ms BEFORE log signal"
            )
            print(
                f"    after 0.7%: fav_100ms={r.get('early_fav_100ms')}% "
                f"fav_1s={r.get('early_fav_1000ms')}% adv_100ms={r.get('early_adv_100ms')}%"
            )
        if r.get("log_live_pnl") is not None:
            print(f"  log live pnl: {r['log_live_pnl']:+.2f}")
        for sc in r.get("scenarios", []):
            if sc.get("skip"):
                print(f"  {sc['scenario']}: SKIP ({sc['skip']})")
            else:
                print(
                    f"  {sc['scenario']}: {sc['pnl_usdt']:+.3f} USDT ({sc['pnl_pct']:+.2f}%) "
                    f"rev@{sc['reversal_ms']}ms"
                )

    out_path = "/home/deeporion/Desktop/binance/crypto_announcements_go/scripts/backtest_07_results.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nWrote {out_path}")


if __name__ == "__main__":
    main()
