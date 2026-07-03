#!/usr/bin/env python3
"""IFVG Pine Sniper 1h backtest for a single UTC day (trading1.log parity)."""
from __future__ import annotations

import json
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone

FAPI = "https://fapi.binance.com"
DAY = "2026-07-01"
DAY_START_MS = int(datetime(2026, 7, 1, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
DAY_END_MS = DAY_START_MS + 86_400_000
SIM_END_MS = DAY_END_MS + 3 * 86_400_000  # allow exits up to 3 days after
WARMUP_START_MS = DAY_START_MS - 80 * 3_600_000

NOTIONAL = float(sys.argv[1]) if len(sys.argv) > 1 else 100.0
MAX_SYMBOLS = int(sys.argv[2]) if len(sys.argv) > 2 else 300
FEE_RT = 0.0008
SLIP_BPS = 1.0
ATR_LEN = 14
SL_MULT = 1.5
TP_RR = 3.0
MAX_FVG_AGE = 60
MAX_HIDDEN = 120
Q_GAP = 0.25
Q_BODY = 0.50
Q_RANGE = 0.60
BREAK_BUF = 0.05


@dataclass
class Bar:
    ts: int
    o: float
    h: float
    l: float
    c: float


@dataclass
class Trade:
    sym: str
    side: int
    entry_ts: int
    entry: float
    sl: float
    tp: float
    exit_ts: int = 0
    exit_px: float = 0.0
    reason: str = "open"


def http_json(url: str) -> object:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def fetch_symbols(n: int) -> list[str]:
    info = http_json(f"{FAPI}/fapi/v1/exchangeInfo")
    out = []
    for s in info["symbols"]:
        sym = s["symbol"]
        if (
            s.get("contractType") == "PERPETUAL"
            and s.get("quoteAsset") == "USDT"
            and s.get("status") == "TRADING"
            and sym.isascii()
        ):
            out.append(sym)
    out.sort()
    return out[:n]


def fetch_klines(symbol: str, start_ms: int, end_ms: int) -> list[Bar]:
    out: list[Bar] = []
    cur = start_ms
    while cur < end_ms:
        q = urllib.parse.urlencode(
            {
                "symbol": symbol,
                "interval": "1h",
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
        time.sleep(0.02)
    return out


def atr_wilder_at(bars: list[Bar], end_idx: int, n: int) -> float | None:
    if end_idx < n:
        return None
    trs: list[float] = []
    for j in range(1, end_idx + 1):
        prev = bars[j - 1].c
        b = bars[j]
        trs.append(max(b.h - b.l, abs(b.h - prev), abs(b.l - prev)))
    rma = sum(trs[:n]) / n
    for tr in trs[n:]:
        rma = (rma * (n - 1) + tr) / n
    return rma


def slip(px: float, side: int) -> float:
    s = px * (SLIP_BPS / 10000.0)
    return px + s if side == 1 else px - s


def pnl(side: int, entry: float, exit_px: float) -> float:
    move = ((exit_px - entry) / entry * 100) if side == 1 else ((entry - exit_px) / entry * 100)
    return NOTIONAL * move / 100 - NOTIONAL * FEE_RT


def sim_symbol(sym: str, bars: list[Bar]) -> list[Trade]:
    raw: list[dict] = []
    active: Trade | None = None
    trades: list[Trade] = []
    mt = 1e-8

    for i, b in enumerate(bars):
        if i < 3:
            continue

        if active is not None and b.ts > active.entry_ts:
            if active.side == 1:
                hit_sl = b.l <= active.sl
                hit_tp = b.h >= active.tp
            else:
                hit_sl = b.h >= active.sl
                hit_tp = b.l <= active.tp
            if hit_sl:
                active.exit_ts = b.ts
                active.exit_px = active.sl
                active.reason = "sl"
                trades.append(active)
                active = None
            elif hit_tp:
                active.exit_ts = b.ts
                active.exit_px = active.tp
                active.reason = "tp"
                trades.append(active)
                active = None

        if b.ts >= SIM_END_MS:
            break

        for r in raw:
            r["age"] += 1
        raw[:] = [r for r in raw if r["age"] <= MAX_FVG_AGE]

        a = atr_wilder_at(bars, i, ATR_LEN)
        safe = a if (a and a > 0) else mt
        cr = max(b.h - b.l, mt)
        br = abs(b.c - b.o) / cr
        ra = cr / safe

        if b.l > bars[i - 2].h:
            raw.append(
                {
                    "top": b.l,
                    "bot": bars[i - 2].h,
                    "dir": 1,
                    "age": 0,
                    "gap": (b.l - bars[i - 2].h) / safe,
                    "br": br,
                    "ra": ra,
                }
            )
        if b.h < bars[i - 2].l:
            raw.append(
                {
                    "top": bars[i - 2].l,
                    "bot": b.h,
                    "dir": -1,
                    "age": 0,
                    "gap": (bars[i - 2].l - b.h) / safe,
                    "br": br,
                    "ra": ra,
                }
            )
        if len(raw) > MAX_HIDDEN:
            raw[:] = raw[-MAX_HIDDEN:]

        buf = safe * BREAK_BUF
        sig = None
        for idx in range(len(raw) - 1, -1, -1):
            r = raw[idx]
            bull = r["dir"] == -1 and b.c > r["top"] + buf
            bear = r["dir"] == 1 and b.c < r["bot"] - buf
            if bull or bear:
                if r["gap"] >= Q_GAP and r["br"] >= Q_BODY and r["ra"] >= Q_RANGE:
                    sig = (1 if bull else -1, r["top"], r["bot"], safe)
                raw.pop(idx)
                break

        if sig is None or not (DAY_START_MS <= b.ts < DAY_END_MS):
            continue
        if active is not None:
            continue

        direction, top, bot, safe_atr = sig
        base = top if direction == 1 else bot
        entry = slip(base, direction)
        risk = safe_atr * SL_MULT
        sl = entry - risk if direction == 1 else entry + risk
        tp = entry + risk * TP_RR if direction == 1 else entry - risk * TP_RR
        active = Trade(sym, direction, b.ts, entry, sl, tp)

    if active is not None:
        last = next((x for x in reversed(bars) if x.ts < SIM_END_MS), bars[-1])
        active.exit_ts = last.ts
        active.exit_px = last.c
        active.reason = "eod_open"
        trades.append(active)

    return trades


def main() -> None:
    syms = fetch_symbols(MAX_SYMBOLS)
    print(f"IFVG Pine Sniper 1h backtest — {DAY} UTC")
    print(f"symbols={len(syms)} notional=${NOTIONAL} entry=IFVG line SL={SL_MULT}xATR TP={TP_RR}R")
    print(f"filter=Balanced | tradeActive=1/symbol")
    print()

    all_trades: list[Trade] = []
    for n, sym in enumerate(syms, 1):
        bars = fetch_klines(sym, WARMUP_START_MS, SIM_END_MS)
        if len(bars) < ATR_LEN + 4:
            continue
        t = sim_symbol(sym, bars)
        all_trades.extend(t)
        if n % 50 == 0:
            print(f"  ... {n}/{len(syms)} symbols", flush=True)

    entries = [t for t in all_trades if DAY_START_MS <= t.entry_ts < DAY_END_MS]
    closed = [t for t in entries if t.reason != "eod_open"]
    open_eod = [t for t in entries if t.reason == "eod_open"]

    total = sum(pnl(t.side, t.entry, t.exit_px) for t in entries)
    wins = sum(1 for t in closed if pnl(t.side, t.entry, t.exit_px) > 0)
    losses = sum(1 for t in closed if pnl(t.side, t.entry, t.exit_px) <= 0)

    print(f"=== SUMMARY {DAY} UTC ===")
    print(f"entries={len(entries)}  closed={len(closed)}  still_open@sim_end={len(open_eod)}")
    print(f"wins={wins}  losses={losses}  wr={wins/(wins+losses)*100:.1f}%" if wins + losses else "wins=0 losses=0")
    print(f"net_pnl=${total:+.2f}")
    print()

    by_reason: dict[str, float] = {}
    for t in entries:
        by_reason[t.reason] = by_reason.get(t.reason, 0.0) + pnl(t.side, t.entry, t.exit_px)
    print("By exit reason:")
    for r, v in sorted(by_reason.items(), key=lambda x: -abs(x[1])):
        print(f"  {r:<10} ${v:+.2f}")

    print()
    print("Trades (sorted by PnL):")
    print(f"{'Time UTC':<18} {'Symbol':<14} {'Side':<5} {'Entry':>12} {'Exit':>12} {'Reason':<8} {'PnL$':>8}")
    print("-" * 85)
    for t in sorted(entries, key=lambda x: pnl(x.side, x.entry, x.exit_px), reverse=True):
        ts = datetime.fromtimestamp(t.entry_ts / 1000, timezone.utc).strftime("%m-%d %H:%M")
        side = "LONG" if t.side == 1 else "SHORT"
        net = pnl(t.side, t.entry, t.exit_px)
        print(
            f"{ts:<18} {t.sym:<14} {side:<5} {t.entry:>12.8f} {t.exit_px:>12.8f} "
            f"{t.reason:<8} {net:>+8.2f}"
        )


if __name__ == "__main__":
    main()
