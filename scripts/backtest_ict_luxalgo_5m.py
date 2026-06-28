#!/usr/bin/env python3
"""LuxAlgo-style ICT signals on 5m: +OB/-OB retest + liquidity grabs (Jun 26 backtest)."""

from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from liquidity_grabs_lib import Bar, GrabState, on_bar_confirmed  # noqa: E402

FAPI = "https://fapi.binance.com"
SPOT = "https://data-api.binance.vision"
NOTIONAL = 6.0
FEE_RT = 0.0008
TP_PCT = 1.5
SL_PCT = 8.0
MAX_HOLD = 96
OB_LENGTH = 10
OB_RETEST_BARS = 20
SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "SUIUSDT",
]
DAY_START = "2026-06-26T00:00:00+00:00"
DAY_END = "2026-06-27T00:00:00+00:00"


@dataclass
class OBZone:
    top: float
    btm: float
    formed_i: int
    formed_ts: int
    kind: str  # ob_plus | ob_minus
    broken: bool = False
    traded: bool = False


@dataclass
class SigStats:
    ent: int = 0
    tp: int = 0
    sl: int = 0
    to: int = 0
    real: float = 0.0
    wins: int = 0


@dataclass
class OpenPos:
    sym: str
    sig: str
    side: str
    entry: float
    entry_i: int
    sl: float
    tp: float
    bars: int = 0


def _get(url: str):
    req = urllib.request.Request(url, headers={"User-Agent": "ict-backtest"})
    for attempt in range(6):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code in (418, 429, 400):
                time.sleep(0.5 * (2**attempt))
                continue
            raise
        except Exception:
            time.sleep(0.5 * (2**attempt))
    return None


def fetch_klines(sym: str, start_ms: int, end_ms: int) -> list[Bar]:
    rows: list[Bar] = []
    cur = start_ms
    while cur < end_ms:
        ok = False
        for base, path in [(FAPI, "/fapi/v1/klines"), (SPOT, "/api/v3/klines")]:
            url = f"{base}{path}?symbol={sym}&interval=5m&startTime={cur}&endTime={end_ms}&limit=1500"
            batch = _get(url)
            if batch and isinstance(batch, list):
                ok = True
                break
        if not ok or not batch:
            break
        for k in batch:
            rows.append(Bar(int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])))
        nxt = int(batch[-1][0]) + 1
        if nxt <= cur:
            break
        cur = nxt
        if len(batch) < 1500:
            break
        time.sleep(0.05)
    return rows


def pnl(side: str, entry: float, exit_px: float) -> float:
    g = (exit_px - entry) / entry * 100 if side == "long" else (entry - exit_px) / entry * 100
    return NOTIONAL * g / 100 - NOTIONAL * FEE_RT


def sl_tp(side: str, entry: float) -> tuple[float, float]:
    if side == "long":
        return entry * (1 - SL_PCT / 100), entry * (1 + TP_PCT / 100)
    return entry * (1 + SL_PCT / 100), entry * (1 - TP_PCT / 100)


def check_exit(side: str, hi: float, lo: float, sl: float, tp: float) -> tuple[str, float] | None:
    if side == "long":
        if lo <= sl:
            return "sl", sl
        if hi >= tp:
            return "tp", tp
    else:
        if hi >= sl:
            return "sl", sl
        if lo <= tp:
            return "tp", tp
    return None


def open_trade(stats: dict[str, SigStats], sig: str) -> None:
    stats[sig].ent += 1


def close_trade(st: SigStats, reason: str, net: float) -> None:
    if reason == "tp":
        st.tp += 1
    elif reason == "sl":
        st.sl += 1
    else:
        st.to += 1
    st.real += net
    if net > 0:
        st.wins += 1


def swing_state_update(bars: list[Bar], i: int, length: int, st: dict) -> None:
    if i < length:
        return
    upper = max(b.h for b in bars[i - length + 1 : i + 1])
    lower = min(b.l for b in bars[i - length + 1 : i + 1])
    hi_len = bars[i - length].h
    lo_len = bars[i - length].l
    prev_os = st["os"]
    if hi_len > upper:
        st["os"] = 0
    elif lo_len < lower:
        st["os"] = 1
    if st["os"] == 0 and prev_os != 0:
        st["top_y"] = hi_len
        st["top_x"] = i - length
        st["top_crossed"] = False
    if st["os"] == 1 and prev_os != 1:
        st["btm_y"] = lo_len
        st["btm_x"] = i - length
        st["btm_crossed"] = False


def maybe_form_ob(bars: list[Bar], i: int, st: dict, obs: list[OBZone]) -> None:
    b = bars[i]
    # +OB: close breaks swing top
    if st.get("top_y") and not st.get("top_crossed") and b.c > st["top_y"]:
        st["top_crossed"] = True
        tx = st["top_x"]
        minima = min(bars[tx].l, bars[tx].h)
        maxima = max(bars[tx].l, bars[tx].h)
        for j in range(tx, i):
            mn = min(bars[j].l, bars[j].h if False else bars[j].l)
            mn = min(bars[j].l, bars[j].c, bars[j].o)
            mx = max(bars[j].h, bars[j].c, bars[j].o)
            if mn < minima:
                minima = mn
                maxima = mx
            elif mn == minima:
                maxima = max(maxima, mx)
        obs.append(OBZone(maxima, minima, i, b.ts, "ob_plus"))

    if st.get("btm_y") and not st.get("btm_crossed") and b.c < st["btm_y"]:
        st["btm_crossed"] = True
        tx = st["btm_x"]
        minima = min(bars[tx].l, bars[tx].c, bars[tx].o)
        maxima = max(bars[tx].h, bars[tx].c, bars[tx].o)
        for j in range(tx, i):
            mn = min(bars[j].l, bars[j].c, bars[j].o)
            mx = max(bars[j].h, bars[j].c, bars[j].o)
            if mx > maxima:
                maxima = mx
                minima = mn
            elif mx == maxima:
                minima = min(minima, mn)
        obs.append(OBZone(maxima, minima, i, b.ts, "ob_minus"))


def run_symbol(sym: str, day_start_ms: int, day_end_ms: int) -> dict[str, SigStats]:
    warm_ms = day_start_ms - 3 * 24 * 3600 * 1000
    bars = fetch_klines(sym, warm_ms, day_end_ms + MAX_HOLD * 5 * 60 * 1000)
    if len(bars) < 200:
        return {}

    stats: dict[str, SigStats] = defaultdict(SigStats)
    swing_st = {"os": 0, "top_y": None, "top_x": 0, "top_crossed": False, "btm_y": None, "btm_x": 0, "btm_crossed": False}
    obs: list[OBZone] = []
    grab_st = GrabState(tp_pct=TP_PCT)
    open_pos: OpenPos | None = None

    start_i = next((i for i, b in enumerate(bars) if b.ts >= day_start_ms), len(bars))
    end_i = next((i for i, b in enumerate(bars) if b.ts >= day_end_ms), len(bars))

    for i in range(start_i, min(end_i, len(bars))):
        b = bars[i]

        # manage open
        if open_pos and i > open_pos.entry_i:
            hit = check_exit(open_pos.side, b.h, b.l, open_pos.sl, open_pos.tp)
            open_pos.bars += 1
            if hit:
                reason, px = hit
                close_trade(stats[open_pos.sig], reason, pnl(open_pos.side, open_pos.entry, px))
                open_pos = None
            elif open_pos.bars >= MAX_HOLD:
                close_trade(stats[open_pos.sig], "timeout", pnl(open_pos.side, open_pos.entry, b.c))
                open_pos = None

        if open_pos:
            continue

        swing_state_update(bars, i, OB_LENGTH, swing_st)
        maybe_form_ob(bars, i, swing_st, obs)
        for ob in obs:
            if ob.broken or ob.traded:
                continue
            if ob.kind == "ob_plus" and min(b.c, b.o) < ob.btm:
                ob.broken = True
            if ob.kind == "ob_minus" and max(b.c, b.o) > ob.top:
                ob.broken = True

        # OB retest entries (ICT: fade into OB zone)
        for ob in obs:
            if ob.traded or ob.broken or i <= ob.formed_i:
                continue
            if i - ob.formed_i > OB_RETEST_BARS:
                continue
            touch = b.l <= ob.top and b.h >= ob.btm
            if not touch:
                continue
            if ob.kind == "ob_plus":
                side = "long"
                sig = "ob_plus"
            else:
                side = "short"
                sig = "ob_minus"
            sl, tp = sl_tp(side, b.c)
            open_pos = OpenPos(sym, sig, side, b.c, i, sl, tp)
            open_trade(stats, sig)
            ob.traded = True
            break

        if open_pos:
            continue

        # liquidity grabs (existing lib — closest to liq sweep signals)
        if i < 60:
            continue
        g = on_bar_confirmed(grab_st, bars, i)
        if not g or bars[i].ts < day_start_ms:
            continue
        sig = g.grab_type  # buyside | sellside
        side = g.side
        sl, tp = sl_tp(side, g.entry_px)
        # use grab SL if tighter
        if side == "short":
            sl = max(sl, g.sl_px)
        else:
            sl = min(sl, g.sl_px)
        open_pos = OpenPos(sym, sig, side, g.entry_px, i, sl, tp)
        open_trade(stats, sig)

    # close any still open at end of scan window
    if open_pos:
        last_i = min(len(bars) - 1, end_i + MAX_HOLD)
        for j in range(max(open_pos.entry_i + 1, end_i), last_i + 1):
            bb = bars[j]
            hit = check_exit(open_pos.side, bb.h, bb.l, open_pos.sl, open_pos.tp)
            open_pos.bars += 1
            if hit:
                reason, px = hit
                close_trade(stats[open_pos.sig], reason, pnl(open_pos.side, open_pos.entry, px))
                open_pos = None
                break
            if open_pos.bars >= MAX_HOLD:
                close_trade(stats[open_pos.sig], "timeout", pnl(open_pos.side, open_pos.entry, bb.c))
                open_pos = None
                break
        if open_pos:
            px = bars[min(last_i, len(bars) - 1)].c
            close_trade(stats[open_pos.sig], "timeout", pnl(open_pos.side, open_pos.entry, px))

    return stats


def merge(a: dict[str, SigStats], b: dict[str, SigStats]) -> dict[str, SigStats]:
    out: dict[str, SigStats] = defaultdict(SigStats)
    for src in (a, b):
        for k, s in src.items():
            t = out[k]
            t.ent += s.ent
            t.tp += s.tp
            t.sl += s.sl
            t.to += s.to
            t.real += s.real
            t.wins += s.wins
    return out


def main() -> None:
    day_start_ms = int(datetime.fromisoformat(DAY_START).timestamp() * 1000)
    day_end_ms = int(datetime.fromisoformat(DAY_END).timestamp() * 1000)

    total: dict[str, SigStats] = defaultdict(SigStats)
    per_sym: dict[str, dict[str, SigStats]] = {}

    lines = [
        "ICT LuxAlgo-style 5m backtest — 26 Jun 2026 UTC",
        f"symbols={len(SYMBOLS)} TP={TP_PCT}% SL={SL_PCT}% hold={MAX_HOLD}bars notional=${NOTIONAL}",
        "signals: ob_plus (+OB retest LONG), ob_minus (-OB retest SHORT), buyside (grab SHORT), sellside (grab LONG)",
        "note: trading1.log is Pine Script source, not signal export — logic ported from LuxAlgo ICT + liquidity_grabs_lib",
        "",
    ]

    for sym in SYMBOLS:
        print(f"running {sym}...", flush=True)
        st = run_symbol(sym, day_start_ms, day_end_ms)
        per_sym[sym] = st
        total = merge(total, st)
        time.sleep(0.1)

    lines.append("=== per symbol ===")
    for sym in SYMBOLS:
        lines.append(f"\n--- {sym} ---")
        for sig in sorted(per_sym[sym]):
            s = per_sym[sym][sig]
            if s.ent == 0:
                continue
            ex = s.tp + s.sl + s.to
            wr = s.wins / ex * 100 if ex else 0
            lines.append(f"  {sig}: ent={s.ent} tp={s.tp} sl={s.sl} to={s.to} wr={wr:.0f}% real=${s.real:+.2f}")

    lines.append("\n=== combined (10 symbols) ===")
    for sig in sorted(total):
        s = total[sig]
        if s.ent == 0:
            continue
        ex = s.tp + s.sl + s.to
        wr = s.wins / ex * 100 if ex else 0
        lines.append(f"  {sig}: ent={s.ent} tp={s.tp} sl={s.sl} to={s.to} wr={wr:.0f}% real=${s.real:+.2f}")

    comb_real = sum(s.real for s in total.values())
    lines.append(f"\n# total real=${comb_real:+.2f}")

    text = "\n".join(lines) + "\n"
    out = ROOT / "data/aws/sr_chartprime/ict_26jun_10sym_backtest.txt"
    out.write_text(text)
    print(text)


if __name__ == "__main__":
    main()
