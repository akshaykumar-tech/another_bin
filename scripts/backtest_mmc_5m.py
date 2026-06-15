#!/usr/bin/env python3
"""
Candle King MMC (Mirror Market Concept) backtest on 5m candles.

Public MMC flow codified (aligned with ICT/SMC + Candle King teaching):
  1. Dealing range — tight consolidation (accumulation)
  2. Judas sweep — liquidity grab beyond range extreme, wick rejection
  3. MSS / displacement — break of structure in reversal direction
  4. Entry — FVG retest OR displacement close (not on the sweep itself)
  5. SL beyond sweep wick | TP at opposing range extreme or fixed %

Exit: SL or TP only (no fixed hold). SAHARAUSDT excluded by default.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
KDIR_1S = ROOT / "data" / "klines" / "1s"
NOTIONAL = 6.0
FEE_RT = 0.0008
REARM_MS = 300_000
BAR_MS = 300_000
MAX_BARS = 288


@dataclass
class Bar:
    ts: int
    o: float
    h: float
    l: float
    c: float
    v: float

    def body_pct(self) -> float:
        return abs(self.c - self.o) / self.o * 100 if self.o else 0.0

    def upper_wick_pct(self) -> float:
        top = max(self.o, self.c)
        return (self.h - top) / self.o * 100 if self.o else 0.0

    def lower_wick_pct(self) -> float:
        bot = min(self.o, self.c)
        return (bot - self.l) / self.o * 100 if self.o else 0.0


@dataclass
class Setup:
    entry_i: int
    side: int
    entry_px: float
    sl_pct: float
    tp_pct: float
    tag: str


@dataclass
class Trade:
    symbol: str
    side: int
    entry: float
    exit_px: float
    reason: str
    net_usd: float
    tag: str


def load_1s(sym: str) -> list[Bar]:
    p = KDIR_1S / f"{sym}.csv"
    if not p.is_file():
        return []
    out = []
    with p.open() as f:
        for r in csv.DictReader(f):
            out.append(
                Bar(
                    ts=int(r["timestamp_ms"]),
                    o=float(r["open"]),
                    h=float(r["high"]),
                    l=float(r["low"]),
                    c=float(r["close"]),
                    v=float(r["volume"]),
                )
            )
    return out


def to_5m(bars_1s: list[Bar]) -> list[Bar]:
    if not bars_1s:
        return []
    buckets: dict[int, list[Bar]] = {}
    for b in bars_1s:
        key = (b.ts // BAR_MS) * BAR_MS
        buckets.setdefault(key, []).append(b)
    out = []
    for ts in sorted(buckets):
        chunk = buckets[ts]
        out.append(
            Bar(
                ts=ts,
                o=chunk[0].o,
                h=max(x.h for x in chunk),
                l=min(x.l for x in chunk),
                c=chunk[-1].c,
                v=sum(x.v for x in chunk),
            )
        )
    return out


def pnl_usd(side: int, entry: float, exit_px: float, notional: float) -> float:
    if entry <= 0:
        return 0.0
    g = (exit_px - entry) / entry * 100 if side > 0 else (entry - exit_px) / entry * 100
    return notional * g / 100 - notional * FEE_RT


def sim_exit(bars: list[Bar], entry_i: int, side: int, entry_px: float, sl_pct: float, tp_pct: float) -> tuple[float, str]:
    if side > 0:
        sl_px = entry_px * (1 - sl_pct / 100)
        tp_px = entry_px * (1 + tp_pct / 100)
    else:
        sl_px = entry_px * (1 + sl_pct / 100)
        tp_px = entry_px * (1 - tp_pct / 100)

    for j in range(entry_i + 1, min(len(bars), entry_i + 1 + MAX_BARS)):
        b = bars[j]
        if side > 0:
            hit_sl = b.l <= sl_px
            hit_tp = b.h >= tp_px
            if hit_sl and hit_tp:
                return sl_px, "sl"
            if hit_sl:
                return sl_px, "sl"
            if hit_tp:
                return tp_px, "tp"
        else:
            hit_sl = b.h >= sl_px
            hit_tp = b.l <= tp_px
            if hit_sl and hit_tp:
                return sl_px, "sl"
            if hit_sl:
                return sl_px, "sl"
            if hit_tp:
                return tp_px, "tp"

    last = min(len(bars) - 1, entry_i + MAX_BARS)
    return bars[last].c, "timeout"


def dealing_range(bars: list[Bar], i: int, n: int) -> tuple[float, float, float]:
    seg = bars[i - n : i]
    lo = min(b.l for b in seg)
    hi = max(b.h for b in seg)
    mid = (lo + hi) / 2
    width_pct = (hi - lo) / mid * 100 if mid else 99.0
    return lo, hi, width_pct


def swing_high(bars: list[Bar], i: int, n: int) -> float:
    return max(b.h for b in bars[max(0, i - n) : i])


def swing_low(bars: list[Bar], i: int, n: int) -> float:
    return min(b.l for b in bars[max(0, i - n) : i])


def bullish_fvg(bars: list[Bar], i: int) -> tuple[float, float] | None:
    """Gap: bar[i-2].h < bar[i].l → zone [bar[i-2].h, bar[i].l]."""
    if i < 2:
        return None
    top = bars[i - 2].h
    bot = bars[i].l
    if bot > top:
        return top, bot
    return None


def bearish_fvg(bars: list[Bar], i: int) -> tuple[float, float] | None:
    if i < 2:
        return None
    bot = bars[i - 2].l
    top = bars[i].h
    if top < bot:
        return top, bot
    return None


def bar_amp_pct(b: Bar) -> float:
    if b.o <= 0:
        return 0.0
    return max((b.h - b.o) / b.o, (b.o - b.l) / b.o) * 100


def sl_tp_from_range(
    side: int,
    entry: float,
    sweep_extreme: float,
    range_lo: float,
    range_hi: float,
    sl_buffer_pct: float,
    tp_mode: str,
    tp_fixed: float,
    sl_cap: float | None = None,
) -> tuple[float, float]:
    if side > 0:
        sl_pct = max((entry - sweep_extreme) / entry * 100 + sl_buffer_pct, 0.15)
        if tp_mode == "range":
            tp_pct = max((range_hi - entry) / entry * 100, 0.3)
        else:
            tp_pct = tp_fixed
    else:
        sl_pct = max((sweep_extreme - entry) / entry * 100 + sl_buffer_pct, 0.15)
        if tp_mode == "range":
            tp_pct = max((entry - range_lo) / entry * 100, 0.3)
        else:
            tp_pct = tp_fixed
    if sl_cap is not None:
        sl_pct = min(sl_pct, sl_cap)
        if tp_mode == "fixed":
            tp_pct = max(tp_fixed, sl_pct * 1.5)
    return sl_pct, tp_pct


def mmc_judas_mss(
    bars: list[Bar],
    dr_bars: int = 12,
    dr_max_pct: float = 2.5,
    wick_min: float = 0.12,
    mss_bars: int = 4,
    disp_min: float = 0.25,
    entry_mode: str = "displacement",
    fvg_wait: int = 6,
    sl_buffer: float = 0.05,
    tp_mode: str = "range",
    tp_fixed: float = 3.0,
    sl_cap: float | None = None,
    burst_min: float = 0.0,
) -> list[Setup]:
    """
    Full MMC: dealing range → Judas sweep → MSS displacement → entry.
    entry_mode: displacement | fvg
    """
    out: list[Setup] = []
    start = dr_bars + 3
    for i in range(start, len(bars) - mss_bars - fvg_wait - 2):
        lo, hi, width = dealing_range(bars, i, dr_bars)
        if width > dr_max_pct or width < 0.15:
            continue
        b = bars[i]
        if burst_min and bar_amp_pct(b) < burst_min:
            continue

        # Bullish Judas: sweep below range, wick rejection, close back inside
        if b.l < lo and b.c > lo and b.lower_wick_pct() >= wick_min:
            sweep_low = b.l
            mss_i = None
            for j in range(i + 1, min(i + 1 + mss_bars, len(bars))):
                sh = swing_high(bars, j, 6)
                if bars[j].c > sh and bars[j].body_pct() >= disp_min:
                    mss_i = j
                    break
            if mss_i is None:
                continue

            if entry_mode == "displacement":
                entry_i = mss_i + 1
                if entry_i >= len(bars):
                    continue
                entry_px = bars[entry_i].o
                sl_pct, tp_pct = sl_tp_from_range(1, entry_px, sweep_low, lo, hi, sl_buffer, tp_mode, tp_fixed, sl_cap)
                out.append(Setup(entry_i, 1, entry_px, sl_pct, tp_pct, "judas_mss"))
            else:
                fvg = bullish_fvg(bars, mss_i)
                if not fvg:
                    continue
                z_lo, z_hi = fvg
                for k in range(mss_i + 1, min(mss_i + 1 + fvg_wait, len(bars))):
                    if bars[k].l <= z_hi and bars[k].h >= z_lo:
                        entry_i = k
                        entry_px = max(z_lo, min(bars[k].o, z_hi))
                        sl_pct, tp_pct = sl_tp_from_range(1, entry_px, sweep_low, lo, hi, sl_buffer, tp_mode, tp_fixed, sl_cap)
                        out.append(Setup(entry_i, 1, entry_px, sl_pct, tp_pct, "judas_fvg"))
                        break

        # Bearish Judas
        elif b.h > hi and b.c < hi and b.upper_wick_pct() >= wick_min:
            sweep_high = b.h
            mss_i = None
            for j in range(i + 1, min(i + 1 + mss_bars, len(bars))):
                slv = swing_low(bars, j, 6)
                if bars[j].c < slv and bars[j].body_pct() >= disp_min:
                    mss_i = j
                    break
            if mss_i is None:
                continue

            if entry_mode == "displacement":
                entry_i = mss_i + 1
                if entry_i >= len(bars):
                    continue
                entry_px = bars[entry_i].o
                sl_pct, tp_pct = sl_tp_from_range(-1, entry_px, sweep_high, lo, hi, sl_buffer, tp_mode, tp_fixed, sl_cap)
                out.append(Setup(entry_i, -1, entry_px, sl_pct, tp_pct, "judas_mss"))
            else:
                fvg = bearish_fvg(bars, mss_i)
                if not fvg:
                    continue
                z_top, z_bot = fvg
                for k in range(mss_i + 1, min(mss_i + 1 + fvg_wait, len(bars))):
                    if bars[k].h >= z_bot and bars[k].l <= z_top:
                        entry_i = k
                        entry_px = min(z_top, max(bars[k].o, z_bot))
                        sl_pct, tp_pct = sl_tp_from_range(-1, entry_px, sweep_high, lo, hi, sl_buffer, tp_mode, tp_fixed, sl_cap)
                        out.append(Setup(entry_i, -1, entry_px, sl_pct, tp_pct, "judas_fvg"))
                        break
    return out


def mmc_wick_line(
    bars: list[Bar],
    dr_bars: int = 12,
    dr_max_pct: float = 2.5,
    wick_min: float = 0.18,
    sl_buffer: float = 0.05,
    tp_mode: str = "range",
    tp_fixed: float = 2.0,
    sl_cap: float | None = None,
    burst_min: float = 0.0,
) -> list[Setup]:
    """Candle King Wick Line: sweep + close back inside dealing range, no MSS required."""
    out: list[Setup] = []
    for i in range(dr_bars + 1, len(bars) - 2):
        lo, hi, width = dealing_range(bars, i, dr_bars)
        if width > dr_max_pct:
            continue
        b = bars[i]
        if burst_min and bar_amp_pct(b) < burst_min:
            continue
        if b.l < lo and b.c > lo and b.lower_wick_pct() >= wick_min:
            entry_i = i + 1
            entry_px = bars[entry_i].o
            sl_pct, tp_pct = sl_tp_from_range(1, entry_px, b.l, lo, hi, sl_buffer, tp_mode, tp_fixed, sl_cap)
            out.append(Setup(entry_i, 1, entry_px, sl_pct, tp_pct, "wick_line"))
        elif b.h > hi and b.c < hi and b.upper_wick_pct() >= wick_min:
            entry_i = i + 1
            entry_px = bars[entry_i].o
            sl_pct, tp_pct = sl_tp_from_range(-1, entry_px, b.h, lo, hi, sl_buffer, tp_mode, tp_fixed, sl_cap)
            out.append(Setup(entry_i, -1, entry_px, sl_pct, tp_pct, "wick_line"))
    return out


def mmc_mirror_structure(bars: list[Bar], look: int = 20, sweep_pct: float = 0.08, sl_buffer: float = 0.05, tp_fixed: float = 2.5) -> list[Setup]:
    """Mirror / failed HH-LL: new high fails + bearish close, or new low fails + bullish close."""
    out: list[Setup] = []
    for i in range(look + 2, len(bars) - 2):
        prev_hi = max(b.h for b in bars[i - look : i - 1])
        prev_lo = min(b.l for b in bars[i - look : i - 1])
        b = bars[i]
        # Failed breakout high → short
        if b.h > prev_hi * (1 + sweep_pct / 100) and b.c < prev_hi and b.upper_wick_pct() >= 0.15:
            entry_i = i + 1
            entry_px = bars[entry_i].o
            sl_pct = max((b.h - entry_px) / entry_px * 100 + sl_buffer, 0.2)
            out.append(Setup(entry_i, -1, entry_px, sl_pct, tp_fixed, "mirror"))
        elif b.l < prev_lo * (1 - sweep_pct / 100) and b.c > prev_lo and b.lower_wick_pct() >= 0.15:
            entry_i = i + 1
            entry_px = bars[entry_i].o
            sl_pct = max((entry_px - b.l) / entry_px * 100 + sl_buffer, 0.2)
            out.append(Setup(entry_i, 1, entry_px, sl_pct, tp_fixed, "mirror"))
    return out


def mmc_order_block_retest(bars: list[Bar], dr_bars: int = 12, dr_max_pct: float = 2.0, disp_min: float = 0.35, retest_bars: int = 8, sl_buffer: float = 0.05, tp_fixed: float = 3.0) -> list[Setup]:
    """After Judas sweep, displacement leaves OB (last opposite candle before move) → retest entry."""
    out: list[Setup] = []
    for i in range(dr_bars + 3, len(bars) - retest_bars - 2):
        lo, hi, width = dealing_range(bars, i, dr_bars)
        if width > dr_max_pct:
            continue
        b = bars[i]
        if b.l < lo and b.c > lo:
            for j in range(i + 1, min(i + 5, len(bars))):
                if bars[j].body_pct() >= disp_min and bars[j].c > bars[j].o and bars[j].c > hi:
                    ob_lo, ob_hi = bars[j - 1].l, bars[j - 1].h
                    for k in range(j + 1, min(j + 1 + retest_bars, len(bars))):
                        if bars[k].l <= ob_hi and bars[k].h >= ob_lo:
                            entry_i = k
                            entry_px = bars[entry_i].o
                            sl_pct = max((entry_px - b.l) / entry_px * 100 + sl_buffer, 0.2)
                            out.append(Setup(entry_i, 1, entry_px, sl_pct, tp_fixed, "ob_retest"))
                            break
                    break
        elif b.h > hi and b.c < hi:
            for j in range(i + 1, min(i + 5, len(bars))):
                if bars[j].body_pct() >= disp_min and bars[j].c < bars[j].o and bars[j].c < lo:
                    ob_lo, ob_hi = bars[j - 1].l, bars[j - 1].h
                    for k in range(j + 1, min(j + 1 + retest_bars, len(bars))):
                        if bars[k].h >= ob_lo and bars[k].l <= ob_hi:
                            entry_i = k
                            entry_px = bars[entry_i].o
                            sl_pct = max((b.h - entry_px) / entry_px * 100 + sl_buffer, 0.2)
                            out.append(Setup(entry_i, -1, entry_px, sl_pct, tp_fixed, "ob_retest"))
                            break
                    break
    return out


def mmc_burst_fade(
    bars: list[Bar],
    dr_bars: int = 12,
    dr_max_pct: float = 3.0,
    burst_min: float = 3.0,
    wick_min: float = 0.10,
    sl_pct: float = 0.5,
    tp_pct: float = 8.0,
) -> list[Setup]:
    """
    Hybrid: MMC trap on volatility burst (thin-alt Judas).
    Prior dealing range → burst sweeps extreme with wick → fade reversal.
    """
    out: list[Setup] = []
    for i in range(dr_bars + 1, len(bars) - 2):
        lo, hi, width = dealing_range(bars, i, dr_bars)
        if width > dr_max_pct:
            continue
        b = bars[i]
        amp = bar_amp_pct(b)
        if amp < burst_min:
            continue
        # Bearish trap: spike above range, upper wick, fade short
        if b.h > hi and b.c < hi and b.upper_wick_pct() >= wick_min:
            entry_i = i + 1
            out.append(Setup(entry_i, -1, bars[entry_i].o, sl_pct, tp_pct, "burst_fade"))
        # Bullish trap: sweep below range, lower wick, fade long
        elif b.l < lo and b.c > lo and b.lower_wick_pct() >= wick_min:
            entry_i = i + 1
            out.append(Setup(entry_i, 1, bars[entry_i].o, sl_pct, tp_pct, "burst_fade"))
    return out


def burst_dir(b: Bar) -> int:
    if b.o <= 0:
        return 0
    up = (b.h - b.o) / b.o
    dn = (b.o - b.l) / b.o
    return 1 if up >= dn else -1


def mmc_burst_fade_loose(bars: list[Bar], burst_min: float, sl_pct: float, tp_pct: float) -> list[Setup]:
    """Burst spike fade — Judas reversal on thin-alt volatility (matches 5m fade grid)."""
    out: list[Setup] = []
    for i, b in enumerate(bars):
        if bar_amp_pct(b) < burst_min:
            continue
        side = burst_dir(b)
        if side == 0:
            continue
        entry_i = i + 1
        if entry_i >= len(bars):
            continue
        out.append(Setup(entry_i, -side, bars[entry_i].o, sl_pct, tp_pct, "burst_fade_loose"))
    return out


def run_setups(sym: str, bars: list[Bar], setups: list[Setup], notional: float) -> list[Trade]:
    trades: list[Trade] = []
    last_ts = 0
    for s in setups:
        if last_ts and bars[s.entry_i].ts - last_ts < REARM_MS:
            continue
        exit_px, reason = sim_exit(bars, s.entry_i, s.side, s.entry_px, s.sl_pct, s.tp_pct)
        trades.append(
            Trade(sym, s.side, s.entry_px, exit_px, reason, pnl_usd(s.side, s.entry_px, exit_px, notional), s.tag)
        )
        last_ts = bars[s.entry_i].ts
    return trades


def summarize(trades: list[Trade]) -> dict:
    n = len(trades)
    if not n:
        return {"n": 0, "wr": 0, "net": 0, "tp": 0, "sl": 0, "to": 0}
    wins = sum(1 for t in trades if t.net_usd > 0)
    return {
        "n": n,
        "wr": 100 * wins / n,
        "net": sum(t.net_usd for t in trades),
        "tp": sum(1 for t in trades if t.reason == "tp"),
        "sl": sum(1 for t in trades if t.reason == "sl"),
        "to": sum(1 for t in trades if t.reason == "timeout"),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--exclude", default="SAHARAUSDT")
    ap.add_argument("--notional", type=float, default=NOTIONAL)
    args = ap.parse_args()
    skip = {s.strip().upper() for s in args.exclude.split(",") if s.strip()}
    notional = args.notional

    sym_bars: dict[str, list[Bar]] = {}
    for p in sorted(KDIR_1S.glob("*.csv")):
        if p.stem.upper() in skip:
            continue
        b5 = to_5m(load_1s(p.stem))
        if len(b5) >= 30:
            sym_bars[p.stem] = b5

    print(f"MMC 5m backtest | {len(sym_bars)} symbols | excluded={skip}")
    print(f"  notional=${notional} | fee={FEE_RT*100:.2f}% RT | exit SL/TP only\n")
    print("Candle King MMC concepts tested:")
    print("  judas_mss   — dealing range + Judas sweep + MSS displacement entry")
    print("  judas_fvg   — same + FVG retest entry (ICT style)")
    print("  wick_line   — Wick Line trap at range edge (simpler reversal)")
    print("  mirror      — failed HH/LL liquidity sweep (Mirror structure)")
    print("  ob_retest   — order block retest after displacement\n")

    configs: list[tuple[str, str, callable]] = []

    for dr in (8, 12, 16):
        for dr_max in (1.5, 2.0, 2.5, 3.0):
            for tp_mode, tp_fix in (("range", 0), ("fixed", 2.0), ("fixed", 3.0), ("fixed", 5.0)):
                label = f"judas_mss dr={dr} w={dr_max}% tp={tp_mode}"
                configs.append(
                    (
                        label,
                        "judas_mss",
                        lambda b, dr=dr, dr_max=dr_max, tp_mode=tp_mode, tp_fix=tp_fix: mmc_judas_mss(
                            b, dr_bars=dr, dr_max_pct=dr_max, entry_mode="displacement", tp_mode=tp_mode, tp_fixed=tp_fix
                        ),
                    )
                )
                label = f"judas_fvg dr={dr} w={dr_max}% tp={tp_mode}"
                configs.append(
                    (
                        label,
                        "judas_fvg",
                        lambda b, dr=dr, dr_max=dr_max, tp_mode=tp_mode, tp_fix=tp_fix: mmc_judas_mss(
                            b, dr_bars=dr, dr_max_pct=dr_max, entry_mode="fvg", tp_mode=tp_mode, tp_fixed=tp_fix
                        ),
                    )
                )

    for dr in (8, 12, 16):
        for dr_max in (2.0, 2.5, 3.0):
            for tp_mode, tp_fix in (("range", 0), ("fixed", 2.0), ("fixed", 3.0)):
                configs.append(
                    (
                        f"wick_line dr={dr} w={dr_max}% tp={tp_mode}",
                        "wick_line",
                        lambda b, dr=dr, dr_max=dr_max, tp_mode=tp_mode, tp_fix=tp_fix: mmc_wick_line(
                            b, dr_bars=dr, dr_max_pct=dr_max, tp_mode=tp_mode, tp_fixed=tp_fix
                        ),
                    )
                )

    for look in (15, 20, 30):
        for tp in (2.0, 3.0, 5.0):
            configs.append(
                (
                    f"mirror look={look} tp={tp}%",
                    "mirror",
                    lambda b, look=look, tp=tp: mmc_mirror_structure(b, look=look, tp_fixed=tp),
                )
            )

    for dr in (10, 12):
        for tp in (2.5, 3.0, 5.0):
            configs.append(
                (
                    f"ob_retest dr={dr} tp={tp}%",
                    "ob_retest",
                    lambda b, dr=dr, tp=tp: mmc_order_block_retest(b, dr_bars=dr, tp_fixed=tp),
                )
            )

    # Burst-gated MMC (align with live 3-5% spike context)
    for burst in (2.5, 3.0, 4.0, 5.0):
        for sl_cap in (0.5, 1.0, 1.5, 2.0):
            for tp in (2.0, 3.0, 5.0, 8.0):
                configs.append(
                    (
                        f"judas_mss burst>={burst}% sl_cap={sl_cap} tp={tp}%",
                        "judas_burst",
                        lambda b, burst=burst, sl_cap=sl_cap, tp=tp: mmc_judas_mss(
                            b,
                            dr_bars=12,
                            dr_max_pct=3.0,
                            entry_mode="displacement",
                            tp_mode="fixed",
                            tp_fixed=tp,
                            sl_cap=sl_cap,
                            burst_min=burst,
                        ),
                    )
                )
                configs.append(
                    (
                        f"wick_line burst>={burst}% sl_cap={sl_cap} tp={tp}%",
                        "wick_burst",
                        lambda b, burst=burst, sl_cap=sl_cap, tp=tp: mmc_wick_line(
                            b,
                            dr_bars=12,
                            dr_max_pct=3.0,
                            tp_mode="fixed",
                            tp_fixed=tp,
                            sl_cap=sl_cap,
                            burst_min=burst,
                        ),
                    )
                )

    # Hybrid: MMC burst fade (Candle King trap on spike)
    for burst in (2.5, 3.0, 4.0, 5.0):
        for sl, tp in ((0.5, 8.0), (0.5, 5.0), (1.0, 5.0), (1.5, 5.0), (3.0, 5.0)):
            configs.append(
                (
                    f"burst_fade MMC dr=12 burst>={burst}% SL={sl} TP={tp}",
                    "burst_fade",
                    lambda b, burst=burst, sl=sl, tp=tp: mmc_burst_fade(b, burst_min=burst, sl_pct=sl, tp_pct=tp),
                )
            )
            configs.append(
                (
                    f"burst_fade_loose burst>={burst}% SL={sl} TP={tp}",
                    "burst_fade_loose",
                    lambda b, burst=burst, sl=sl, tp=tp: mmc_burst_fade_loose(b, burst_min=burst, sl_pct=sl, tp_pct=tp),
                )
            )

    rows: list[tuple[float, str, str, dict]] = []
    for label, tag, fn in configs:
        all_t: list[Trade] = []
        for sym, bars in sym_bars.items():
            all_t.extend(run_setups(sym, bars, fn(bars), notional))
        s = summarize(all_t)
        if s["n"] < 5:
            continue
        rows.append((s["net"], label, tag, s))

    rows.sort(key=lambda x: -x[0])
    print(f"{'strategy':<42} {'n':>4} {'WR%':>5} {'net$':>8} {'TP':>3} {'SL':>3} {'TO':>3}")
    print("-" * 72)
    for net, label, _, s in rows[:20]:
        print(f"{label:<42} {s['n']:4} {s['wr']:5.1f} {s['net']:+8.2f} {s['tp']:3} {s['sl']:3} {s['to']:3}")

    viable = [r for r in rows if r[0] > 0 and 20 <= r[3]["n"] <= 55]
    print(f"\n=== Positive & 20<=n<=55 (realistic sample): {len(viable)} ===")
    for net, label, tag, s in viable[:12]:
        print(f"  [{tag}] {label}: n={s['n']} WR={s['wr']:.0f}% net=${net:+.2f} (tp={s['tp']} sl={s['sl']})")

    if rows:
        net, label, tag, s = rows[0]
        print(f"\n>>> TOP (any n): [{tag}] {label}")
        print(f"    n={s['n']} WR={s['wr']:.1f}% net=${s['net']:+.2f}")

    # Per-concept best
    print("\n=== Best per MMC concept ===")
    for concept in ("judas_mss", "judas_fvg", "wick_line", "mirror", "ob_retest", "judas_burst", "wick_burst", "burst_fade", "burst_fade_loose"):
        sub = [r for r in rows if r[2] == concept and r[3]["n"] >= 10]
        if sub:
            net, label, _, s = sub[0]
            print(f"  {concept}: {label} → n={s['n']} WR={s['wr']:.0f}% net=${net:+.2f}")


if __name__ == "__main__":
    main()
