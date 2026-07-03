#!/usr/bin/env python3
"""Compare IFVG 1h: preclose (-30s) vs first-1min entry on live dry log symbols."""
from __future__ import annotations

import json
import re
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field

FAPI = "https://fapi.binance.com"
FEE_RT = 0.0008
NOTIONAL = 6.0
SL_PCT = 1.0
ATR_LEN = 14
MAX_FVG_AGE = 60
MAX_HIDDEN = 120
Q_GAP_ATR = 0.25
Q_BODY_RATIO = 0.50
Q_RANGE_ATR = 0.60
INV_BUF_ATR = 0.05
SLIP_BPS = 1.0

# Live dry log entries 2026-07-03 04:01 UTC (first 1m of 04:00 hour)
LOG_ENTRIES = """
BREVUSDT SHORT 0.08881000
CFGUSDT SHORT 0.20100000
HYPEUSDT LONG 63.64600000
ARUSDT LONG 1.98700000
JUPUSDT LONG 0.23510000
ADAUSDT LONG 0.15560000
CELOUSDT SHORT 0.06289000
AUSDT LONG 0.06468000
FIGHTUSDT LONG 0.00346600
BARDUSDT SHORT 0.13620000
BLESSUSDT LONG 0.00685100
2ZUSDT SHORT 0.06613000
AINUSDT LONG 0.07786000
AVAAIUSDT LONG 0.00501300
1000RATSUSDT LONG 0.02940000
ERAUSDT LONG 0.08170000
INXUSDT SHORT 0.00828000
BEAMXUSDT SHORT 0.00143400
CELRUSDT SHORT 0.00192400
CTSIUSDT LONG 0.02172000
DIAUSDT LONG 0.10010000
AEVOUSDT SHORT 0.01969000
KAIAUSDT LONG 0.03505000
KNCUSDT LONG 0.10940000
COMPUSDT LONG 15.79000000
ARPAUSDT LONG 0.00795000
"""

HOUR_START_MS = 1_783_051_200_000  # 2026-07-03 04:00:00 UTC
PREV_HOUR_END_MS = HOUR_START_MS
PRECLOSE_MS = HOUR_START_MS - 30_000  # 03:59:30
ENTRY_1M_MS = HOUR_START_MS  # 04:00 1m bar open
TP_HOUR_MS = HOUR_START_MS + 3_600_000  # 05:00 hour close


@dataclass
class Bar:
    ts: int
    o: float
    h: float
    l: float
    c: float


def http_json(url: str) -> object:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def fetch_klines(symbol: str, interval: str, start_ms: int, end_ms: int) -> list[Bar]:
    out: list[Bar] = []
    cur = start_ms
    while cur < end_ms:
        q = urllib.parse.urlencode(
            {
                "symbol": symbol,
                "interval": interval,
                "startTime": str(cur),
                "endTime": str(end_ms),
                "limit": "1500",
            }
        )
        rows = http_json(f"{FAPI}/fapi/v1/klines?{q}")
        if not rows:
            break
        for r in rows:
            ts = int(r[0])
            if ts >= end_ms:
                continue
            out.append(Bar(ts, float(r[1]), float(r[2]), float(r[3]), float(r[4])))
        cur = int(rows[-1][0]) + 1
        if len(rows) < 1500:
            break
        time.sleep(0.05)
    return out


def price_at_ms(symbol: str, ts_ms: int) -> float | None:
    """Last aggTrade price at/before ts_ms (proxy for live price at that second)."""
    start = ts_ms - 5_000
    q = urllib.parse.urlencode(
        {"symbol": symbol, "startTime": str(start), "endTime": str(ts_ms + 1000), "limit": "1000"}
    )
    rows = http_json(f"{FAPI}/fapi/v1/aggTrades?{q}")
    if not rows:
        return None
    best = None
    for r in rows:
        t = int(r["T"])
        if t <= ts_ms:
            best = float(r["p"])
    if best is not None:
        return best
    return float(rows[0]["p"])


def atr_last(bars: list[Bar], n: int) -> float:
    if len(bars) < n + 1:
        return 0.0
    trs = []
    for j in range(len(bars) - n, len(bars)):
        prev = bars[j - 1].c
        b = bars[j]
        trs.append(max(b.h - b.l, abs(b.h - prev), abs(b.l - prev)))
    return sum(trs) / len(trs)


def quality_pass(gap_atr: float, body_ratio: float, range_atr: float) -> bool:
    return gap_atr >= Q_GAP_ATR and body_ratio >= Q_BODY_RATIO and range_atr >= Q_RANGE_ATR


def build_fvg_state(bars_1h: list[Bar], mt: float) -> list[dict]:
    raw: list[dict] = []
    for i in range(3, len(bars_1h)):
        for r in raw:
            r["age"] += 1
        raw[:] = [r for r in raw if r["age"] <= MAX_FVG_AGE]
        b = bars_1h[i]
        a = atr_last(bars_1h[: i + 1], ATR_LEN)
        safe = a if a > 0 else mt
        cr = max(b.h - b.l, mt)
        br = abs(b.c - b.o) / cr
        ra = cr / safe
        if b.l > bars_1h[i - 2].h:
            raw.append(
                {
                    "top": b.l,
                    "bot": bars_1h[i - 2].h,
                    "dir": 1,
                    "age": 0,
                    "gap_atr": (b.l - bars_1h[i - 2].h) / safe,
                    "body_ratio": br,
                    "range_atr": ra,
                }
            )
        if b.h < bars_1h[i - 2].l:
            raw.append(
                {
                    "top": bars_1h[i - 2].l,
                    "bot": b.h,
                    "dir": -1,
                    "age": 0,
                    "gap_atr": (bars_1h[i - 2].l - b.h) / safe,
                    "body_ratio": br,
                    "range_atr": ra,
                }
            )
        if len(raw) > MAX_HIDDEN:
            raw[:] = raw[-MAX_HIDDEN:]
    return raw


def detect(raw: list[dict], bars_1h: list[Bar], close_px: float, mt: float) -> tuple[int, str] | None:
    if len(bars_1h) < ATR_LEN:
        return None
    a = atr_last(bars_1h, ATR_LEN)
    safe = a if a > 0 else mt
    buf = safe * INV_BUF_ATR
    raw_copy = [dict(r) for r in raw]
    for idx in range(len(raw_copy) - 1, -1, -1):
        r = raw_copy[idx]
        bull = r["dir"] == -1 and close_px > r["top"] + buf
        bear = r["dir"] == 1 and close_px < r["bot"] - buf
        if bull or bear:
            if quality_pass(r["gap_atr"], r["body_ratio"], r["range_atr"]):
                return (1 if bull else -1, "LONG" if bull else "SHORT")
            return None
    return None


def slip_entry(px: float, side: int) -> float:
    s = px * (SLIP_BPS / 10000.0)
    return px + s if side == 1 else px - s


def pnl_usd(side: int, entry: float, exit_px: float) -> float:
    move = ((exit_px - entry) / entry * 100) if side == 1 else ((entry - exit_px) / entry * 100)
    return NOTIONAL * move / 100 - NOTIONAL * FEE_RT


def sim_trade(
    side: int,
    entry: float,
    bars_1m: list[Bar],
    entry_after_ms: int,
    sl_px: float,
    tp_px: float,
) -> tuple[str, float, float]:
    for b in bars_1m:
        if b.ts < entry_after_ms:
            continue
        if side == 1:
            if b.l <= sl_px:
                return "sl", sl_px, pnl_usd(side, entry, sl_px)
            if b.h >= tp_px:
                return "tp", tp_px, pnl_usd(side, entry, tp_px)
        else:
            if b.h >= sl_px:
                return "sl", sl_px, pnl_usd(side, entry, sl_px)
            if b.l <= tp_px:
                return "tp", tp_px, pnl_usd(side, entry, tp_px)
    last = bars_1m[-1].c if bars_1m else entry
    return "open", last, pnl_usd(side, entry, last)


def parse_log() -> list[tuple[str, str, float]]:
    out = []
    for line in LOG_ENTRIES.strip().splitlines():
        sym, side, px = line.split()
        out.append((sym, side, float(px)))
    return out


def main() -> None:
    entries = parse_log()
    print(f"Live dry log entries: {len(entries)} @ hour 04:00 UTC (2026-07-03)")
    print(f"Notional=${NOTIONAL} fee_rt={FEE_RT} slip={SLIP_BPS}bps")
    print()

    totals = {
        "pre30_sl1": 0.0,
        "first1m_be": 0.0,
        "first1m_sl1": 0.0,
    }
    n_pre30 = n_first1m = 0

    for sym, log_side, log_entry in entries:
        # 1h history ending BEFORE current hour close (exclude 03-04 bar at preclose)
        h1_start = HOUR_START_MS - 80 * 3_600_000
        h1_all = fetch_klines(sym, "1h", h1_start, HOUR_START_MS + 3_600_000 + 1)
        # FVG state for preclose: only 1h bars with ts < 03:00 (closed before 03:00-04:00 hour)
        h1_pre = [b for b in h1_all if b.ts < HOUR_START_MS - 3_600_000]
        # FVG state for first 1m: include 03:00-04:00 bar (closed at 04:00)
        h1_first = [b for b in h1_all if b.ts < HOUR_START_MS]

        m1 = fetch_klines(sym, "1m", HOUR_START_MS - 3_600_000, TP_HOUR_MS + 60_000)
        mt = 1e-8
        tp_px = next((b.c for b in h1_all if b.ts == HOUR_START_MS), None)
        if tp_px is None:
            print(f"{sym}: missing TP hour bar (04:00-05:00 close)")
            continue

        pre_price = price_at_ms(sym, PRECLOSE_MS)
        first1m_bar = next((b for b in m1 if b.ts == ENTRY_1M_MS), None)
        first1m_close = first1m_bar.c if first1m_bar else None

        raw_pre = build_fvg_state(h1_pre, mt)
        raw_first = build_fvg_state(h1_first, mt)

        sig_pre = detect(raw_pre, h1_pre, pre_price, mt) if pre_price else None
        sig_first = detect(raw_first, h1_first, first1m_close, mt) if first1m_close else None

        print(f"=== {sym} (dry log {log_side} @ {log_entry:.8f}) ===")
        print(f"  pre30 price @03:59:30: {pre_price}")
        print(f"  first1m close @04:00:   {first1m_close}")
        print(f"  hour TP close @05:00:   {tp_px:.8f}")

        if sig_pre:
            side = sig_pre[0]
            ent = slip_entry(pre_price, side)
            sl = ent * (1 - SL_PCT / 100) if side == 1 else ent * (1 + SL_PCT / 100)
            reason, ex, net = sim_trade(side, ent, m1, PRECLOSE_MS, sl, tp_px)
            totals["pre30_sl1"] += net
            n_pre30 += 1
            print(
                f"  PRE30 signal={sig_pre[1]} entry={ent:.8f} SL=1%@{sl:.8f} "
                f"-> {reason} exit={ex:.8f} net=${net:+.4f}"
            )
        else:
            print("  PRE30 signal: NONE")

        if sig_first:
            side = sig_first[0]
            ent = slip_entry(first1m_close, side)
            sl_be = ent
            reason, ex, net_be = sim_trade(side, ent, m1, ENTRY_1M_MS + 60_000, sl_be, tp_px)
            sl1 = ent * (1 - SL_PCT / 100) if side == 1 else ent * (1 + SL_PCT / 100)
            _, _, net_sl1 = sim_trade(side, ent, m1, ENTRY_1M_MS + 60_000, sl1, tp_px)
            totals["first1m_be"] += net_be
            totals["first1m_sl1"] += net_sl1
            n_first1m += 1
            print(
                f"  FIRST1M signal={sig_first[1]} entry={ent:.8f} "
                f"SL=entry -> {reason} exit={ex:.8f} net=${net_be:+.4f}"
            )
            print(
                f"  FIRST1M same entry SL=1%@{sl1:.8f} -> net=${net_sl1:+.4f}"
            )
        else:
            print("  FIRST1M signal: NONE (bot may have used different FVG state/timing)")

        match = sig_pre and sig_first and sig_pre[1] == sig_first[1]
        print(f"  pre30 vs first1m same side: {match}")
        print()

    print("=" * 60)
    print("TOTALS (symbols with signal)")
    print(f"  PRE30 entry + SL 1% + TP hour close:  n={n_pre30}  net=${totals['pre30_sl1']:+.2f}")
    print(f"  FIRST1M entry + SL=entry + TP close:  n={n_first1m}  net=${totals['first1m_be']:+.2f}")
    print(f"  FIRST1M entry + SL 1% + TP close:     n={n_first1m}  net=${totals['first1m_sl1']:+.2f}")
    print()
    print("Note: PRE30 uses FVG state BEFORE 03-04h bar close; FIRST1M includes 03-04h bar.")
    print("PRE30 price from aggTrades @ 03:59:30 UTC; entry sim starts after that timestamp.")


if __name__ == "__main__":
    main()
