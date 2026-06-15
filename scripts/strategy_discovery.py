#!/usr/bin/env python3
"""
Fresh strategy discovery on 1s klines — no legacy rules.
Scans many new hypotheses; ranks by net PnL (fees only, no slip).
"""

from __future__ import annotations

import csv
import statistics as st
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

ROOT = Path(__file__).resolve().parents[1]
KDIR = ROOT / "data" / "klines" / "1s"
NOTIONAL = 6.0
FEE_RT = 0.0008
REARM_MS = 300_000


@dataclass
class Bar:
    ts: int
    o: float
    h: float
    l: float
    c: float
    v: float

    def amp_pct(self) -> float:
        if self.o <= 0:
            return 0.0
        return max((self.h - self.o) / self.o, (self.o - self.l) / self.o) * 100

    def dir(self) -> int:
        if self.o <= 0:
            return 0
        up = (self.h - self.o) / self.o
        dn = (self.o - self.l) / self.o
        if up >= dn and up > 0:
            return 1
        if dn > up and dn > 0:
            return -1
        return 0

    def body_pct(self) -> float:
        if self.o <= 0:
            return 0.0
        return abs(self.c - self.o) / self.o * 100

    def upper_wick_pct(self) -> float:
        if self.o <= 0:
            return 0.0
        top = max(self.o, self.c)
        return (self.h - top) / self.o * 100

    def lower_wick_pct(self) -> float:
        if self.o <= 0:
            return 0.0
        bot = min(self.o, self.c)
        return (bot - self.l) / self.o * 100


@dataclass
class Result:
    name: str
    desc: str
    trades: int = 0
    wins: int = 0
    net_usd: float = 0.0
    hold: int = 0

    @property
    def wr(self) -> float:
        return 100 * self.wins / self.trades if self.trades else 0.0

    @property
    def avg_usd(self) -> float:
        return self.net_usd / self.trades if self.trades else 0.0


def load(sym: str) -> list[Bar]:
    p = KDIR / f"{sym}.csv"
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


def pnl(side: int, entry: float, exit_px: float) -> float:
    if entry <= 0:
        return 0.0
    gross = (exit_px - entry) / entry * 100 if side > 0 else (entry - exit_px) / entry * 100
    return NOTIONAL * gross / 100 - NOTIONAL * FEE_RT


def backtest(bars: list[Bar], sigs: list[tuple[int, int, int]], rearm: int = REARM_MS) -> tuple[int, int, float]:
    """sigs: (bar_i, side, hold_sec). Entry T+1 open, exit after hold."""
    trades = wins = 0
    net = 0.0
    last_ts = 0
    busy_until = -1
    for i, side, hold in sigs:
        if i <= busy_until:
            continue
        if bars[i].ts - last_ts < rearm:
            continue
        ei, xi = i + 1, i + hold
        if ei >= len(bars) or xi >= len(bars):
            continue
        u = pnl(side, bars[ei].o, bars[xi].c)
        trades += 1
        net += u
        if u > 0:
            wins += 1
        last_ts = bars[i].ts
        busy_until = xi
    return trades, wins, net


def med_vol(bars: list[Bar], i: int, n: int = 60) -> float:
    v = [bars[j].v for j in range(max(0, i - n), i) if bars[j].v > 0]
    return st.median(v) if v else 0.0


def range_pct(bars: list[Bar], i: int, n: int) -> float:
    seg = bars[max(0, i - n) : i]
    if not seg:
        return 99.0
    hi, lo = max(b.h for b in seg), min(b.l for b in seg)
    mid = (hi + lo) / 2
    return (hi - lo) / mid * 100 if mid else 99.0


# ─── NEW hypothesis builders ───────────────────────────────────────────

def h_impulse_mom(bars: list[Bar], amp_min: float, hold: int, entry_delay: int = 1) -> list[tuple[int, int, int]]:
    """Big 1s impulse → trade WITH direction after delay."""
    out = []
    for i, b in enumerate(bars):
        if b.amp_pct() < amp_min or b.v < 100:
            continue
        d = b.dir()
        if d == 0:
            continue
        out.append((i, d, hold + entry_delay - 1))
    return out


def h_impulse_fade(bars: list[Bar], amp_min: float, hold: int) -> list[tuple[int, int, int]]:
    out = []
    for i, b in enumerate(bars):
        if b.amp_pct() < amp_min or b.v < 100:
            continue
        d = b.dir()
        if d == 0:
            continue
        out.append((i, -d, hold))
    return out


def h_vol_climax_fade(bars: list[Bar], vol_mult: float, amp_min: float, hold: int) -> list[tuple[int, int, int]]:
    """Volume spike + wide bar → fade."""
    out = []
    for i, b in enumerate(bars):
        mv = med_vol(bars, i)
        if mv <= 0 or b.v < mv * vol_mult or b.amp_pct() < amp_min:
            continue
        d = b.dir()
        if d:
            out.append((i, -d, hold))
    return out


def h_squeeze_break(bars: list[Bar], quiet_sec: int, max_rng: float, break_amp: float, hold: int) -> list[tuple[int, int, int]]:
    """Tight range then expansion → with break direction."""
    out = []
    for i, b in enumerate(bars):
        if range_pct(bars, i, quiet_sec) > max_rng:
            continue
        if b.amp_pct() < break_amp or b.v < 100:
            continue
        d = b.dir()
        if d:
            out.append((i, d, hold))
    return out


def h_pullback_entry(bars: list[Bar], impulse_amp: float, pb_max: float, hold: int) -> list[tuple[int, int, int]]:
    """Impulse bar, 2-8s shallow pullback, resume → continuation."""
    out = []
    n = len(bars)
    for i in range(n - hold - 10):
        b = bars[i]
        if b.amp_pct() < impulse_amp or b.v < 100:
            continue
        d = b.dir()
        if d == 0:
            continue
        imp_hi, imp_lo = b.h, b.l
        ok = False
        for j in range(i + 2, min(i + 9, n - hold)):
            seg = bars[i + 1 : j + 1]
            if d > 0:
                pb = (imp_hi - min(x.l for x in seg)) / b.o * 100
                resume = bars[j].c > imp_hi * 0.999
            else:
                pb = (max(x.h for x in seg) - imp_lo) / b.o * 100
                resume = bars[j].c < imp_lo * 1.001
            if 0.05 <= pb <= pb_max and resume:
                out.append((j, d, hold))
                ok = True
                break
        _ = ok
    return out


def h_exhaustion_wick(bars: list[Bar], wick_min: float, amp_min: float, hold: int) -> list[tuple[int, int, int]]:
    """Long rejection wick → fade."""
    out = []
    for i, b in enumerate(bars):
        if b.amp_pct() < amp_min:
            continue
        uw, lw = b.upper_wick_pct(), b.lower_wick_pct()
        body = b.body_pct()
        if uw > wick_min and uw > body * 1.5 and b.dir() > 0:
            out.append((i, -1, hold))
        elif lw > wick_min and lw > body * 1.5 and b.dir() < 0:
            out.append((i, 1, hold))
    return out


def h_double_impulse(bars: list[Bar], amp_min: float, gap: int, hold: int) -> list[tuple[int, int, int]]:
    """Two same-dir impulses within gap seconds → 2nd entry."""
    out = []
    last_i, last_d = -999, 0
    for i, b in enumerate(bars):
        if b.amp_pct() < amp_min or b.v < 100:
            continue
        d = b.dir()
        if d == 0:
            continue
        if last_d == d and 2 <= i - last_i <= gap:
            out.append((i, d, hold))
        last_i, last_d = i, d
    return out


def h_delayed_mom(bars: list[Bar], amp_min: float, delay: int, hold: int) -> list[tuple[int, int, int]]:
    """Wait N seconds after impulse before entry (anti-chase)."""
    out = []
    n = len(bars)
    for i, b in enumerate(bars):
        if b.amp_pct() < amp_min or b.v < 100:
            continue
        d = b.dir()
        if d and i + delay + hold < n:
            out.append((i + delay - 1, d, hold))
    return out


def h_failed_breakout(bars: list[Bar], lookback: int, hold: int) -> list[tuple[int, int, int]]:
    """Sweep 120s extreme then close back → fade the fake break."""
    out = []
    for i in range(lookback + 5, len(bars) - hold - 2):
        seg = bars[i - lookback : i]
        hi, lo = max(b.h for b in seg), min(b.l for b in seg)
        b = bars[i]
        if b.l < lo * 0.997 and b.c > lo and b.c > b.o:
            out.append((i, 1, hold))
        elif b.h > hi * 1.003 and b.c < hi and b.c < b.o:
            out.append((i, -1, hold))
    return out


def h_body_mom(bars: list[Bar], body_min: float, hold: int) -> list[tuple[int, int, int]]:
    """Strong body (not wick) candle → direction."""
    out = []
    for i, b in enumerate(bars):
        if b.body_pct() < body_min or b.v < 100:
            continue
        d = 1 if b.c > b.o else -1 if b.c < b.o else 0
        if d:
            out.append((i, d, hold))
    return out


def h_cascade(bars: list[Bar], n_bars: int, sum_amp: float, hold: int) -> list[tuple[int, int, int]]:
    """N consecutive same-dir moves summing to X% → momentum."""
    out = []
    for i in range(n_bars, len(bars) - hold):
        seg = bars[i - n_bars : i]
        s = sum(b.amp_pct() for b in seg)
        if s < sum_amp:
            continue
        dirs = [b.dir() for b in seg if b.dir() != 0]
        if len(dirs) == n_bars and len(set(dirs)) == 1:
            out.append((i - 1, dirs[0], hold))
    return out


HYPOTHESES: list[tuple[str, str, Callable[[list[Bar]], list[tuple[int, int, int]]], int]] = [
    ("imp2_m45", "2%+ impulse → mom 45s", lambda b: h_impulse_mom(b, 2.0, 45), 45),
    ("imp2_m90", "2%+ impulse → mom 90s", lambda b: h_impulse_mom(b, 2.0, 90), 90),
    ("imp3_m60", "3%+ impulse → mom 60s", lambda b: h_impulse_mom(b, 3.0, 60), 60),
    ("imp3_m120", "3%+ impulse → mom 120s", lambda b: h_impulse_mom(b, 3.0, 120), 120),
    ("imp4_m90", "4%+ impulse → mom 90s", lambda b: h_impulse_mom(b, 4.0, 90), 90),
    ("imp5_m120", "5%+ impulse → mom 120s", lambda b: h_impulse_mom(b, 5.0, 120), 120),
    ("imp3_f60", "3%+ impulse → fade 60s", lambda b: h_impulse_fade(b, 3.0, 60), 60),
    ("imp4_f90", "4%+ impulse → fade 90s", lambda b: h_impulse_fade(b, 4.0, 90), 90),
    ("vol4_f90", "4x vol + 2% bar → fade 90s", lambda b: h_vol_climax_fade(b, 4.0, 2.0, 90), 90),
    ("vol6_f60", "6x vol + 2.5% → fade 60s", lambda b: h_vol_climax_fade(b, 6.0, 2.5, 60), 60),
    ("sqz_brk_90", "120s tight + 1.8% break → mom 90s", lambda b: h_squeeze_break(b, 120, 0.8, 1.8, 90), 90),
    ("sqz_brk_120", "180s tight + 2% break → mom 120s", lambda b: h_squeeze_break(b, 180, 0.6, 2.0, 120), 120),
    ("pb_cont_90", "impulse + pullback → cont 90s", lambda b: h_pullback_entry(b, 2.5, 0.8, 90), 90),
    ("pb_cont_120", "impulse + pullback → cont 120s", lambda b: h_pullback_entry(b, 3.0, 1.0, 120), 120),
    ("wick_f60", "rejection wick → fade 60s", lambda b: h_exhaustion_wick(b, 1.0, 2.0, 60), 60),
    ("wick_f90", "rejection wick → fade 90s", lambda b: h_exhaustion_wick(b, 1.2, 2.5, 90), 90),
    ("dbl_imp_60", "double 2% impulse → mom 60s", lambda b: h_double_impulse(b, 2.0, 15, 60), 60),
    ("dbl_imp_90", "double 2.5% impulse → mom 90s", lambda b: h_double_impulse(b, 2.5, 20, 90), 90),
    ("delay5_m90", "3% impulse, enter T+5, hold 90s", lambda b: h_delayed_mom(b, 3.0, 5, 90), 90),
    ("delay10_m120", "3% impulse, enter T+10, hold 120s", lambda b: h_delayed_mom(b, 3.0, 10, 120), 120),
    ("fail_brk_90", "failed sweep → reversal 90s", lambda b: h_failed_breakout(b, 120, 90), 90),
    ("fail_brk_120", "failed sweep → reversal 120s", lambda b: h_failed_breakout(b, 180, 120), 120),
    ("body1_m60", "1%+ body candle → mom 60s", lambda b: h_body_mom(b, 1.0, 60), 60),
    ("body15_m90", "1.5%+ body → mom 90s", lambda b: h_body_mom(b, 1.5, 90), 90),
    ("cascade3_m60", "3x consecutive → mom 60s", lambda b: h_cascade(b, 3, 4.0, 60), 60),
    ("cascade4_m90", "4x consecutive → mom 90s", lambda b: h_cascade(b, 4, 5.0, 90), 90),
]


def main() -> None:
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--exclude", default="", help="comma symbols to skip e.g. SAHARAUSDT")
    args = ap.parse_args()
    skip = {s.strip().upper() for s in args.exclude.split(",") if s.strip()}

    syms = sorted(p.stem for p in KDIR.glob("*.csv") if p.stem.upper() not in skip)
    excl = f" | excluded={skip}" if skip else ""
    print(f"Strategy discovery | {len(syms)} symbols{excl} | fee={FEE_RT*100:.2f}% RT | no slip")
    print(f"{'id':<16} {'trades':>6} {'WR%':>5} {'net$':>8} {'$/tr':>7}  description")
    print("-" * 78)

    results: list[Result] = []
    sym_bars: dict[str, list[Bar]] = {}
    for s in syms:
        b = load(s)
        if len(b) >= 500:
            sym_bars[s] = b

    for name, desc, builder, hold in HYPOTHESES:
        r = Result(name, desc, hold=hold)
        for bars in sym_bars.values():
            sigs = builder(bars)
            t, w, n = backtest(bars, sigs)
            r.trades += t
            r.wins += w
            r.net_usd += n
        results.append(r)

    results.sort(key=lambda x: -x.net_usd)
    for r in results:
        print(f"{r.name:<16} {r.trades:6} {r.wr:5.1f} {r.net_usd:+8.2f} {r.avg_usd:+7.4f}  {r.desc}")

    good = [r for r in results if r.trades >= 15 and r.net_usd > 0]
    print(f"\n=== VIABLE (n>=15, net>0): {len(good)} ===")
    for r in good[:10]:
        print(f"  {r.name}: ${r.net_usd:+.2f} | {r.trades} trades | WR {r.wr:.0f}% | {r.desc}")

    if good:
        best = good[0]
        print(f"\n>>> BEST: {best.name} → ${best.net_usd:+.2f} ({best.trades} trades, WR {best.wr:.0f}%)")
        print(f"    {best.desc}")
    else:
        top = results[0]
        print(f"\n>>> TOP (may be low sample): {top.name} → ${top.net_usd:+.2f} ({top.trades} trades)")


if __name__ == "__main__":
    main()
