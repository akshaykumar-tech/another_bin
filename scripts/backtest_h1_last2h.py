#!/usr/bin/env python3
"""Backtest IFVG 1h: first-1min entry @ 1m close, SL=hour open, TP=hour close."""
from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass

FAPI = "https://fapi.binance.com"
NOTIONAL = 6.0
FEE_RT = 0.0008
SLIP_BPS = 1.0
ATR_LEN = 14
MAX_FVG_AGE = 60
MAX_HIDDEN = 120
Q_GAP_ATR = 0.25
Q_BODY_RATIO = 0.50
Q_RANGE_ATR = 0.60
INV_BUF_ATR = 0.05
N_HOURS = 2
# Live dry log symbols (2026-07-03 04:00 hour) + scan mode
SYMBOLS = [
    "BREVUSDT", "CFGUSDT", "HYPEUSDT", "ARUSDT", "JUPUSDT", "ADAUSDT", "CELOUSDT",
    "AUSDT", "FIGHTUSDT", "BARDUSDT", "BLESSUSDT", "2ZUSDT", "AINUSDT", "AVAAIUSDT",
    "1000RATSUSDT", "ERAUSDT", "INXUSDT", "BEAMXUSDT", "CELRUSDT", "CTSIUSDT",
    "DIAUSDT", "AEVOUSDT", "KAIAUSDT", "KNCUSDT", "COMPUSDT", "ARPAUSDT",
]


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
        time.sleep(0.03)
    return out


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


def detect_inversion(raw: list[dict], bars_1h: list[Bar], close_px: float, mt: float) -> tuple[int, str] | None:
    if len(bars_1h) < ATR_LEN:
        return None
    a = atr_last(bars_1h, ATR_LEN)
    safe = a if a > 0 else mt
    buf = safe * INV_BUF_ATR
    for idx in range(len(raw) - 1, -1, -1):
        r = raw[idx]
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


def last_n_hour_starts(now_ms: int, n: int) -> list[int]:
    cur = (now_ms // 3_600_000) * 3_600_000
    # last n fully closed trading hours (entry hour whose close has passed)
    out = []
    h = cur - 3_600_000
    while len(out) < n and h > 0:
        out.append(h)
        h -= 3_600_000
    return out


def sim_hour(symbol: str, hstart: int, h1_hist: list[Bar], m1: list[Bar]) -> dict | None:
    mt = 1e-8
    first1m = next((b for b in m1 if b.ts == hstart), None)
    hour_bar = next((b for b in h1_hist if b.ts == hstart), None)
    if not first1m or not hour_bar:
        return None

    # FVG state: closed 1h bars strictly before this hour + prev hour bar (closed at hstart)
    prev_ts = hstart - 3_600_000
    bars_for_fvg = [b for b in h1_hist if b.ts <= prev_ts]
    raw = build_fvg_state(bars_for_fvg, mt)

    sig = detect_inversion(raw, bars_for_fvg, first1m.c, mt)
    if not sig:
        return None

    side, side_s = sig[0], sig[1]
    hour_open = first1m.o
    entry = slip_entry(first1m.c, side)
    tp = hour_bar.c  # hour close

    net = pnl_usd(side, entry, tp)
    return {
        "symbol": symbol,
        "hour_utc": time.strftime("%Y-%m-%d %H:00", time.gmtime(hstart / 1000)),
        "side": side_s,
        "hour_open": hour_open,
        "entry_1m_close": first1m.c,
        "entry": entry,
        "tp_close": tp,
        "exit": tp,
        "reason": "hour_close",
        "net": net,
    }


def main() -> None:
    now_ms = int(time.time() * 1000)
    hours = last_n_hour_starts(now_ms, N_HOURS)
    hours.sort()

    print("IFVG 1h backtest — last 2 completed hours")
    print(f"Entry=1m close+{SLIP_BPS}bps | NO SL | exit=hour_close only | notional=${NOTIONAL}")
    print(f"Hours (UTC): {', '.join(time.strftime('%H:%M', time.gmtime(h/1000)) for h in hours)}")
    print(f"Symbols: {len(SYMBOLS)}")
    print()

    all_trades: list[dict] = []
    by_hour: dict[str, float] = {time.strftime("%Y-%m-%d %H:00", time.gmtime(h / 1000)): 0.0 for h in hours}

    for sym in SYMBOLS:
        start = hours[0] - 80 * 3_600_000
        end = hours[-1] + 2 * 3_600_000
        h1 = fetch_klines(sym, "1h", start, end)
        m1 = fetch_klines(sym, "1m", hours[0] - 3_600_000, hours[-1] + 2 * 3_600_000)
        for hstart in hours:
            t = sim_hour(sym, hstart, h1, m1)
            if t:
                all_trades.append(t)
                by_hour[t["hour_utc"]] += t["net"]

    for h in sorted(by_hour):
        ents = [t for t in all_trades if t["hour_utc"] == h]
        tp = sum(1 for t in ents if t["net"] > 0)
        loss = sum(1 for t in ents if t["net"] <= 0)
        print(f"=== {h} UTC ===  entries={len(ents)}  wins={tp}  losses={loss}  net=${by_hour[h]:+.4f}")
        for t in sorted(ents, key=lambda x: x["net"], reverse=True):
            print(
                f"  {t['symbol']:<14} {t['side']:<5} entry={t['entry']:.8f} "
                f"(1m={t['entry_1m_close']:.8f}) hour_close={t['tp_close']:.8f} "
                f"${t['net']:+.4f}"
            )
        print()

    total = sum(t["net"] for t in all_trades)
    print("=" * 60)
    print(f"TOTAL  trades={len(all_trades)}  net=${total:+.4f}")
    print("(Real Binance OHLC — no FVG boundary entry)")


if __name__ == "__main__":
    main()
