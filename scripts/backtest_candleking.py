#!/usr/bin/env python3
"""
Candle King (@candleking19) inspired backtests on 1s klines.

Public concepts codified (MMC / Wick Line / S&D / reversal focus):
  - Wick trap: sweep range extreme + close back inside → fade the sweep
  - CCC reversal: impulse + opposite rejection candle
  - Supply/demand zone bounce at range edges
  - Mirror structure: failed HH/LL + liquidity sweep → reversal
  - ERL→IRL: external range sweep then opposite entry (ICT/MMC aligned)

Fees 0.08%% RT, no slippage. Default excludes SAHARAUSDT.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass, field
from itertools import combinations
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parents[1]
KDIR = ROOT / "data" / "klines" / "1s"
NOTIONAL = 6.0
FEE_RT = 0.0008
REARM_MS = 300_000
DEFAULT_EXCLUDE = {"SAHARAUSDT"}


@dataclass
class Bar:
    ts: int
    o: float
    h: float
    l: float
    c: float
    v: float

    @property
    def bull(self) -> bool:
        return self.c >= self.o

    @property
    def bear(self) -> bool:
        return self.c < self.o

    def body_pct(self) -> float:
        return abs(self.c - self.o) / self.o * 100 if self.o else 0.0

    def upper_wick_pct(self) -> float:
        top = max(self.o, self.c)
        return (self.h - top) / self.o * 100 if self.o else 0.0

    def lower_wick_pct(self) -> float:
        bot = min(self.o, self.c)
        return (bot - self.l) / self.o * 100 if self.o else 0.0


@dataclass
class Result:
    name: str
    desc: str
    trades: int = 0
    wins: int = 0
    net_usd: float = 0.0

    @property
    def wr(self) -> float:
        return 100 * self.wins / self.trades if self.trades else 0.0

    @property
    def avg(self) -> float:
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
    g = (exit_px - entry) / entry * 100 if side > 0 else (entry - exit_px) / entry * 100
    return NOTIONAL * g / 100 - NOTIONAL * FEE_RT


def backtest(bars: list[Bar], sigs: list[tuple[int, int, int]]) -> tuple[int, int, float]:
    t = w = 0
    net = 0.0
    last_ts = 0
    busy = -1
    for i, side, hold in sigs:
        if i < 0 or i >= len(bars) or i <= busy:
            continue
        if bars[i].ts - last_ts < REARM_MS:
            continue
        ei, xi = i + 1, i + hold
        if ei >= len(bars) or xi >= len(bars):
            continue
        u = pnl(side, bars[ei].o, bars[xi].c)
        t += 1
        net += u
        if u > 0:
            w += 1
        last_ts = bars[i].ts
        busy = xi
    return t, w, net


def range_bounds(bars: list[Bar], i: int, n: int) -> tuple[float, float]:
    seg = bars[max(0, i - n) : i]
    return min(b.l for b in seg), max(b.h for b in seg)


# ─── Candle King style signals ───────────────────────────────────────────

def ck_wick_trap(bars: list[Bar], rng_sec: int, hold: int, wick_min: float = 0.15) -> list[tuple[int, int, int]]:
    """Wick Line / trap: sweep range extreme, close back inside → reversal."""
    out = []
    for i in range(rng_sec + 5, len(bars) - hold - 2):
        lo, hi = range_bounds(bars, i, rng_sec)
        b = bars[i]
        if b.l < lo * 0.998 and b.c > lo and b.lower_wick_pct() >= wick_min:
            out.append((i, 1, hold))
        elif b.h > hi * 1.002 and b.c < hi and b.upper_wick_pct() >= wick_min:
            out.append((i, -1, hold))
    return out


def ck_ccc_reversal(bars: list[Bar], hold: int) -> list[tuple[int, int, int]]:
    """CCC: big impulse candle then rejection candle → trade reversal."""
    out = []
    for i in range(3, len(bars) - hold - 1):
        a, b = bars[i - 1], bars[i]
        if a.body_pct() < 0.5:
            continue
        if a.c > a.o and b.c < b.o and b.c < a.o:
            out.append((i, -1, hold))
        elif a.c < a.o and b.c > b.o and b.c > a.o:
            out.append((i, 1, hold))
    return out


def ck_supply_demand(bars: list[Bar], zone_sec: int, hold: int) -> list[tuple[int, int, int]]:
    """S&D: touch demand (bottom 15%% of range) / supply (top 15%%) + rejection."""
    out = []
    for i in range(zone_sec + 2, len(bars) - hold - 1):
        seg = bars[i - zone_sec : i]
        lo, hi = min(b.l for b in seg), max(b.h for b in seg)
        span = hi - lo
        if span <= 0:
            continue
        demand = lo + span * 0.15
        supply = hi - span * 0.15
        b = bars[i]
        if b.l <= demand and b.c > demand and b.bull:
            out.append((i, 1, hold))
        elif b.h >= supply and b.c < supply and b.bear:
            out.append((i, -1, hold))
    return out


def ck_mirror_structure(bars: list[Bar], hold: int) -> list[tuple[int, int, int]]:
    """Mirror / failed structure: HH fails + sweep low → long; LL fails + sweep high → short."""
    lb = 300
    out = []
    for i in range(lb + 10, len(bars) - hold - 1):
        seg = bars[i - lb : i]
        mid = len(seg) // 2
        h1 = max(b.h for b in seg[:mid])
        h2 = max(b.h for b in seg[mid:])
        l1 = min(b.l for b in seg[:mid])
        l2 = min(b.l for b in seg[mid:])
        b = bars[i]
        if h2 < h1 * 0.998 and b.l < l2 * 0.999 and b.c > l2 and b.bull:
            out.append((i, 1, hold))
        elif l2 > l1 * 1.002 and b.h > h2 * 1.001 and b.c < h2 and b.bear:
            out.append((i, -1, hold))
    return out


def ck_erl_sweep(bars: list[Bar], lookback: int, hold: int) -> list[tuple[int, int, int]]:
    """ERL sweep → opposite (MMC / ICT liquidity raid reversal)."""
    out = []
    for i in range(lookback + 5, len(bars) - hold - 1):
        seg = bars[i - lookback : i]
        hi, lo = max(b.h for b in seg), min(b.l for b in seg)
        b = bars[i]
        if b.h > hi * 1.001 and b.c < hi * 0.9995 and b.bear:
            out.append((i, -1, hold))
        elif b.l < lo * 0.999 and b.c > lo * 1.0005 and b.bull:
            out.append((i, 1, hold))
    return out


def ck_trend_bend(bars: list[Bar], hold: int) -> list[tuple[int, int, int]]:
    """Trend bending: 3 pushes up, 3rd fails → short; 3 pushes down → long."""
    out = []
    for i in range(180, len(bars) - hold - 1):
        highs = [bars[j].h for j in range(i - 60, i, 20)]
        lows = [bars[j].l for j in range(i - 60, i, 20)]
        if len(highs) < 3:
            continue
        b = bars[i]
        if highs[0] < highs[1] < highs[2] and b.c < highs[2] * 0.997 and b.bear:
            out.append((i, -1, hold))
        elif lows[0] > lows[1] > lows[2] and b.c > lows[2] * 1.003 and b.bull:
            out.append((i, 1, hold))
    return out


def ck_dealing_range_judas(bars: list[Bar], acc_sec: int, hold: int) -> list[tuple[int, int, int]]:
    """MMC dealing range: tight accumulation → Judas sweep → reversal."""
    out = []
    for i in range(acc_sec + 30, len(bars) - hold - 1):
        acc = bars[i - acc_sec : i - 10]
        hi, lo = max(b.h for b in acc), min(b.l for b in acc)
        mid = (hi + lo) / 2
        if mid <= 0 or (hi - lo) / mid * 100 > 1.0:
            continue
        b = bars[i]
        swept_lo = any(x.l < lo * 0.997 for x in bars[i - 8 : i])
        swept_hi = any(x.h > hi * 1.003 for x in bars[i - 8 : i])
        if swept_lo and b.c > hi and b.bull:
            out.append((i, 1, hold))
        elif swept_hi and b.c < lo and b.bear:
            out.append((i, -1, hold))
    return out


STRATEGIES: list[tuple[str, str, Callable[[list[Bar]], list[tuple[int, int, int]]]]] = [
    ("ck_wick_trap_90", "Wick trap 180s range hold 90s", lambda b: ck_wick_trap(b, 180, 90)),
    ("ck_wick_trap_120", "Wick trap 300s range hold 120s", lambda b: ck_wick_trap(b, 300, 120)),
    ("ck_wick_trap_60", "Wick trap 120s range hold 60s", lambda b: ck_wick_trap(b, 120, 60)),
    ("ck_ccc_rev_90", "CCC impulse+rejection hold 90s", lambda b: ck_ccc_reversal(b, 90)),
    ("ck_ccc_rev_120", "CCC impulse+rejection hold 120s", lambda b: ck_ccc_reversal(b, 120)),
    ("ck_sd_90", "Supply/demand zone hold 90s", lambda b: ck_supply_demand(b, 600, 90)),
    ("ck_sd_120", "Supply/demand zone hold 120s", lambda b: ck_supply_demand(b, 600, 120)),
    ("ck_mirror_90", "Mirror failed HH/LL hold 90s", lambda b: ck_mirror_structure(b, 90)),
    ("ck_mirror_120", "Mirror failed HH/LL hold 120s", lambda b: ck_mirror_structure(b, 120)),
    ("ck_erl_90", "ERL liquidity sweep hold 90s", lambda b: ck_erl_sweep(b, 180, 90)),
    ("ck_erl_120", "ERL liquidity sweep hold 120s", lambda b: ck_erl_sweep(b, 300, 120)),
    ("ck_trend_bend_90", "Trend bend 3-push fail hold 90s", lambda b: ck_trend_bend(b, 90)),
    ("ck_judas_90", "Dealing range Judas hold 90s", lambda b: ck_dealing_range_judas(b, 180, 90)),
    ("ck_judas_120", "Dealing range Judas hold 120s", lambda b: ck_dealing_range_judas(b, 240, 120)),
]


def expand_window(sig: list[tuple[int, int, int]], w: int) -> dict[int, tuple[int, int]]:
  d: dict[int, tuple[int, int]] = {}
  for i, side, hold in sig:
    for dt in range(-w, w + 1):
      j = i + dt
      if j >= 0:
        d[j] = (side, hold)
  return d


def combo_sigs(maps: list[dict[int, tuple[int, int]]]) -> list[tuple[int, int, int]]:
    keys = set(maps[0].keys()) if maps else set()
    for m in maps[1:]:
        keys &= set(m.keys())
    raw = []
    for i in sorted(keys):
        sides = {m[i][0] for m in maps}
        holds = [m[i][1] for m in maps]
        if len(sides) == 1:
            raw.append((i, sides.pop(), max(holds)))
    out: list[tuple[int, int, int]] = []
    last = -9999
    for i, side, hold in raw:
        if i - last < 60:
            continue
        out.append((i, side, hold))
        last = i
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--exclude", default="SAHARAUSDT")
    args = ap.parse_args()
    skip = {s.strip().upper() for s in args.exclude.split(",") if s.strip()}

    sym_bars: dict[str, list[Bar]] = {}
    for p in sorted(KDIR.glob("*.csv")):
        if p.stem.upper() in skip:
            continue
        b = load(p.stem)
        if len(b) >= 800:
            sym_bars[p.stem] = b

    print("Candle King (@candleking19) inspired backtest")
    print(f"  symbols={len(sym_bars)} | excluded={skip} | fee={FEE_RT*100:.2f}% RT | no slip")
    print(f"{'strategy':<22} {'trades':>6} {'WR%':>5} {'net$':>8} {'$/tr':>7}  concept")
    print("-" * 85)

    results: list[Result] = []
    per_sym_sigs: dict[str, dict[str, list[tuple[int, int, int]]]] = {}

    for sym, bars in sym_bars.items():
        per_sym_sigs[sym] = {}
        for name, _, fn in STRATEGIES:
            per_sym_sigs[sym][name] = fn(bars)

    for name, desc, _ in STRATEGIES:
        r = Result(name, desc)
        for sym, bars in sym_bars.items():
            t, w, n = backtest(bars, per_sym_sigs[sym][name])
            r.trades += t
            r.wins += w
            r.net_usd += n
        results.append(r)
        print(f"{name:<22} {r.trades:6} {r.wr:5.1f} {r.net_usd:+8.2f} {r.avg:+7.4f}  {desc}")

    ranked = sorted(results, key=lambda x: -x.net_usd)
    top = [r.name for r in ranked if r.trades >= 10][:6]

    print("\n=== CK COMBOS (agree ±30s) ===")
    print(f"{'combo':<40} {'trades':>6} {'WR%':>5} {'net$':>8}")
    combos: list[Result] = []
    for k in (2, 3):
        for combo in combinations(top, k):
            cname = "+".join(combo)
            r = Result(cname, "combo")
            for sym, bars in sym_bars.items():
                maps = [expand_window(per_sym_sigs[sym][n], 30) for n in combo]
                sigs = [(i, s, h) for i, s, h in combo_sigs(maps) if i < len(bars)]
                t, w, n = backtest(bars, sigs)
                r.trades += t
                r.wins += w
                r.net_usd += n
            combos.append(r)
    combos.sort(key=lambda x: -x.net_usd)
    for r in combos[:12]:
        if r.trades:
            print(f"{r.name:<40} {r.trades:6} {r.wr:5.1f} {r.net_usd:+8.2f}")

    print("\n=== SUMMARY (no SAHARA) ===")
    for r in ranked:
        tag = " ✓" if r.net_usd > 0 and r.trades >= 10 else ""
        print(f"  {r.name:<22} n={r.trades:4} WR={r.wr:5.1f}% net=${r.net_usd:+.2f}{tag}")
    viable = [r for r in ranked if r.trades >= 10 and r.net_usd > 0]
    if viable:
        b = viable[0]
        print(f"\n>>> Best CK: {b.name} → ${b.net_usd:+.2f} ({b.trades} trades, WR {b.wr:.0f}%)")
        print(f"    {b.desc}")
    else:
        print("\n>>> No CK strategy profitable with n>=10 without SAHARA")


if __name__ == "__main__":
    main()
