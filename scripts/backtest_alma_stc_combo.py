#!/usr/bin/env python3
"""
Backtest Alma SD SuperTrend + Schaff Trend Cycle combinations on 15m.

Tests all entry combos with SL/TP exit (3%/8% default) and flip exit.
SAHARA excluded.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path

from alma_st_lib import OHLC, compute_supertrend, signal_flip, signal_series
from stc_lib import LOWER, UPPER, WARMUP_BARS as STC_WARMUP, compute_stc, stc_signals

ROOT = Path(__file__).resolve().parents[1]
KDIR_1S = ROOT / "data" / "klines" / "1s"
BAR_MS = 900_000
NOTIONAL = 6.0
FEE_RT = 0.0008
MAX_BARS = 96
ALMA_WARMUP = 73


@dataclass
class Bar:
    ts: int
    o: float
    h: float
    l: float
    c: float
    v: float


@dataclass
class Setup:
    entry_i: int
    side: str
    tag: str


def load_1s(sym: str) -> list[Bar]:
    p = KDIR_1S / f"{sym}.csv"
    if not p.is_file():
        return []
    out = []
    with p.open() as f:
        for r in csv.DictReader(f):
            out.append(Bar(int(r["timestamp_ms"]), float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"]), float(r["volume"])))
    return out


def to_15m(bars_1s: list[Bar]) -> list[Bar]:
    buckets: dict[int, list[Bar]] = {}
    for b in bars_1s:
        key = (b.ts // BAR_MS) * BAR_MS
        buckets.setdefault(key, []).append(b)
    out = []
    for ts in sorted(buckets):
        c = buckets[ts]
        out.append(Bar(ts, c[0].o, max(x.h for x in c), min(x.l for x in c), c[-1].c, sum(x.v for x in c)))
    return out


def pnl(side: str, entry: float, exit_px: float, notional: float) -> float:
    if entry <= 0:
        return 0.0
    g = (exit_px - entry) / entry * 100 if side == "long" else (entry - exit_px) / entry * 100
    return notional * g / 100 - notional * FEE_RT


def sim_sltp(bars: list[Bar], entry_i: int, side: str, entry_px: float, sl_pct: float, tp_pct: float) -> tuple[float, str]:
    if side == "long":
        sl_px = entry_px * (1 - sl_pct / 100)
        tp_px = entry_px * (1 + tp_pct / 100)
    else:
        sl_px = entry_px * (1 + sl_pct / 100)
        tp_px = entry_px * (1 - tp_pct / 100)
    for j in range(entry_i + 1, min(len(bars), entry_i + 1 + MAX_BARS)):
        b = bars[j]
        if side == "long":
            hit_sl, hit_tp = b.l <= sl_px, b.h >= tp_px
        else:
            hit_sl, hit_tp = b.h >= sl_px, b.l <= tp_px
        if hit_sl and hit_tp:
            return sl_px, "sl"
        if hit_sl:
            return sl_px, "sl"
        if hit_tp:
            return tp_px, "tp"
    last = min(len(bars) - 1, entry_i + MAX_BARS)
    return bars[last].c, "timeout"


def sim_flip(bars: list[Bar], entry_i: int, side: str, entry_px: float, exit_signals: list[str | None]) -> tuple[float, str, int]:
    opp = "short" if side == "long" else "long"
    for j in range(entry_i + 1, len(bars)):
        if exit_signals[j] == opp:
            return bars[j].o, "flip", j
    return bars[-1].c, "eod", len(bars) - 1


def alma_flip_at(sigs: list[int], i: int) -> str | None:
    if i < 1:
        return None
    return signal_flip(sigs[i - 1], sigs[i])


def build_context(bars: list[Bar]) -> dict | None:
    if len(bars) < max(ALMA_WARMUP, STC_WARMUP) + 5:
        return None
    ohlc = [OHLC(b.ts, b.o, b.h, b.l, b.c) for b in bars]
    closes = [b.c for b in bars]
    direction = compute_supertrend(ohlc)
    alma_sig = signal_series(direction)
    stc = compute_stc(closes)
    n = len(bars)

    alma_long = [alma_flip_at(alma_sig, i) == "long" for i in range(n)]
    alma_short = [alma_flip_at(alma_sig, i) == "short" for i in range(n)]
    stc_buy = [False] * n
    stc_sell = [False] * n
    stc_upper_x = [False] * n
    stc_upper_xu = [False] * n
    stc_lower_x = [False] * n
    stc_lower_xu = [False] * n
    stc_rising = [False] * n
    stc_val = [float("nan")] * n

    for i in range(n):
        if not (stc[i] == stc[i]):  # nan
            continue
        stc_val[i] = stc[i]
        sg = stc_signals(stc, i)
        stc_buy[i] = sg["buy"]
        stc_sell[i] = sg["sell"]
        stc_upper_x[i] = sg["upper_cross"]
        stc_upper_xu[i] = sg["upper_crossunder"]
        stc_lower_x[i] = sg["lower_cross"]
        stc_lower_xu[i] = sg["lower_crossunder"]
        stc_rising[i] = sg["rising"]

    alma_bull = [d < 0 for d in direction]
    alma_bear = [d > 0 for d in direction]

    return {
        "alma_long": alma_long,
        "alma_short": alma_short,
        "alma_bull": alma_bull,
        "alma_bear": alma_bear,
        "stc_buy": stc_buy,
        "stc_sell": stc_sell,
        "stc_upper_x": stc_upper_x,
        "stc_upper_xu": stc_upper_xu,
        "stc_lower_x": stc_lower_x,
        "stc_lower_xu": stc_lower_xu,
        "stc_rising": stc_rising,
        "stc_val": stc_val,
    }


def within(bools: list[bool], i: int, look: int) -> bool:
    lo = max(0, i - look)
    return any(bools[lo : i + 1])


COMBOS: dict[str, callable] = {}


def _reg(name: str):
    def deco(fn):
        COMBOS[name] = fn
        return fn
    return deco


@_reg("alma_only")
def c_alma_only(ctx, i):
    if ctx["alma_long"][i]:
        return "long"
    if ctx["alma_short"][i]:
        return "short"
    return None


@_reg("stc_alert")
def c_stc_alert(ctx, i):
    if ctx["stc_buy"][i]:
        return "long"
    if ctx["stc_sell"][i]:
        return "short"
    return None


@_reg("stc_shapes")
def c_stc_shapes(ctx, i):
    if ctx["stc_lower_x"][i]:
        return "long"
    if ctx["stc_upper_xu"][i]:
        return "short"
    return None


@_reg("and_same_bar")
def c_and_same(ctx, i):
    if ctx["alma_long"][i] and ctx["stc_buy"][i]:
        return "long"
    if ctx["alma_short"][i] and ctx["stc_sell"][i]:
        return "short"
    return None


@_reg("or_either")
def c_or(ctx, i):
    long_hit = ctx["alma_long"][i] or ctx["stc_buy"][i]
    short_hit = ctx["alma_short"][i] or ctx["stc_sell"][i]
    if long_hit and not short_hit:
        return "long"
    if short_hit and not long_hit:
        return "short"
    return None


@_reg("alma+stc_trend_filter")
def c_alma_stc_trend(ctx, i):
    if ctx["alma_long"][i] and ctx["stc_rising"][i]:
        return "long"
    if ctx["alma_short"][i] and not ctx["stc_rising"][i]:
        return "short"
    return None


@_reg("stc_entry+alma_trend")
def c_stc_alma_trend(ctx, i):
    if ctx["stc_buy"][i] and ctx["alma_bull"][i]:
        return "long"
    if ctx["stc_sell"][i] and ctx["alma_bear"][i]:
        return "short"
    return None


@_reg("alma+stc_not_extreme")
def c_alma_not_extreme(ctx, i):
    v = ctx["stc_val"][i]
    if v != v:
        return None
    if ctx["alma_long"][i] and v < UPPER:
        return "long"
    if ctx["alma_short"][i] and v > LOWER:
        return "short"
    return None


@_reg("alma+stc_oversold_overbought")
def c_alma_zone(ctx, i):
    v = ctx["stc_val"][i]
    if v != v:
        return None
    if ctx["alma_long"][i] and v <= 50:
        return "long"
    if ctx["alma_short"][i] and v >= 50:
        return "short"
    return None


@_reg("confirm_2bar")
def c_confirm_2(ctx, i):
    if ctx["alma_long"][i] and within(ctx["stc_buy"], i, 2):
        return "long"
    if ctx["alma_short"][i] and within(ctx["stc_sell"], i, 2):
        return "short"
    if within(ctx["alma_long"], i, 2) and ctx["stc_buy"][i]:
        return "long"
    if within(ctx["alma_short"], i, 2) and ctx["stc_sell"][i]:
        return "short"
    return None


@_reg("stc_first_alma_3bar")
def c_stc_first(ctx, i):
    if ctx["stc_buy"][i] and within(ctx["alma_long"], i, 3):
        return "long"
    if ctx["stc_sell"][i] and within(ctx["alma_short"], i, 3):
        return "short"
    return None


@_reg("alma_first_stc_3bar")
def c_alma_first(ctx, i):
    if ctx["alma_long"][i] and within(ctx["stc_buy"], i, 3):
        return "long"
    if ctx["alma_short"][i] and within(ctx["stc_sell"], i, 3):
        return "short"
    return None


@_reg("stc_upper_break+alma_bull")
def c_upper_alma(ctx, i):
    if ctx["stc_upper_x"][i] and ctx["alma_bull"][i]:
        return "long"
    if ctx["stc_lower_xu"][i] and ctx["alma_bear"][i]:
        return "short"
    return None


@_reg("stc_sell_shape+alma_short")
def c_shape_alma(ctx, i):
    if ctx["stc_lower_x"][i] and ctx["alma_bull"][i]:
        return "long"
    if ctx["stc_upper_xu"][i] and ctx["alma_bear"][i]:
        return "short"
    return None


@_reg("dual_flip_consensus")
def c_dual_flip(ctx, i):
    al = ctx["alma_long"][i] or (ctx["alma_bull"][i] and ctx["stc_buy"][i])
    sh = ctx["alma_short"][i] or (ctx["alma_bear"][i] and ctx["stc_sell"][i])
    if al and not sh:
        return "long"
    if sh and not al:
        return "short"
    return None


def gen_setups(bars: list[Bar], combo: str) -> list[Setup]:
    ctx = build_context(bars)
    if not ctx:
        return []
    fn = COMBOS[combo]
    n = len(bars)
    out: list[Setup] = []
    for i in range(1, n - 2):
        side = fn(ctx, i)
        if side:
            out.append(Setup(i + 1, side, combo))
    return out


def gen_flip_exits(bars: list[Bar], combo: str) -> list[str | None]:
    ctx = build_context(bars)
    if not ctx:
        return [None] * len(bars)
    fn = COMBOS[combo]
    return [fn(ctx, i) for i in range(len(bars))]


def backtest_symbol(bars: list[Bar], combo: str, sl_pct: float, tp_pct: float, mode: str, notional: float) -> list[tuple[float, str]]:
    trades: list[tuple[float, str]] = []
    setups = gen_setups(bars, combo)
    exit_sigs = gen_flip_exits(bars, combo) if mode == "flip" else None
    busy = -1
    for s in setups:
        if s.entry_i <= busy:
            continue
        entry_px = bars[s.entry_i].o
        if mode == "flip":
            assert exit_sigs is not None
            exit_px, reason, exit_i = sim_flip(bars, s.entry_i, s.side, entry_px, exit_sigs)
            busy = exit_i
        else:
            exit_px, reason = sim_sltp(bars, s.entry_i, s.side, entry_px, sl_pct, tp_pct)
            busy = s.entry_i + MAX_BARS
        trades.append((pnl(s.side, entry_px, exit_px, notional), reason))
    return trades


def summarize(trades: list[tuple[float, str]]) -> dict:
    n = len(trades)
    if not n:
        return {"n": 0, "wr": 0, "net": 0, "tp": 0, "sl": 0}
    wins = sum(1 for x, _ in trades if x > 0)
    return {
        "n": n,
        "wr": 100 * wins / n,
        "net": sum(x for x, _ in trades),
        "tp": sum(1 for _, r in trades if r == "tp"),
        "sl": sum(1 for _, r in trades if r == "sl"),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--exclude", default="SAHARAUSDT")
    ap.add_argument("--notional", type=float, default=NOTIONAL)
    ap.add_argument("--sl", type=float, default=3.0)
    ap.add_argument("--tp", type=float, default=8.0)
    args = ap.parse_args()
    skip = {s.strip().upper() for s in args.exclude.split(",") if s.strip()}

    sym_bars: dict[str, list[Bar]] = {}
    for p in sorted(KDIR_1S.glob("*.csv")):
        if p.stem.upper() in skip:
            continue
        b = to_15m(load_1s(p.stem))
        if len(b) >= max(ALMA_WARMUP, STC_WARMUP) + 10:
            sym_bars[p.stem] = b

    print(f"Alma + STC combo backtest | 15m | {len(sym_bars)} symbols | excluded={skip}")
    print(f"  notional=${args.notional} | SL={args.sl}% TP={args.tp}% | flip mode also tested\n")

    rows_sltp = []
    rows_flip = []
    for combo in COMBOS:
        all_t: list[tuple[float, str]] = []
        for bars in sym_bars.values():
            all_t.extend(backtest_symbol(bars, combo, args.sl, args.tp, "sltp", args.notional))
        s = summarize(all_t)
        if s["n"] >= 3:
            rows_sltp.append((s["net"], combo, s))

        all_f: list[tuple[float, str]] = []
        for bars in sym_bars.values():
            all_f.extend(backtest_symbol(bars, combo, args.sl, args.tp, "flip", args.notional))
        sf = summarize(all_f)
        if sf["n"] >= 3:
            rows_flip.append((sf["net"], combo, sf))

    rows_sltp.sort(key=lambda x: -x[0])
    rows_flip.sort(key=lambda x: -x[0])

    print("=== EXIT: SL/TP ===")
    print(f"{'combo':<32} {'n':>5} {'WR%':>6} {'net$':>9} {'TP':>4} {'SL':>4}")
    print("-" * 62)
    for net, combo, s in rows_sltp:
        print(f"{combo:<32} {s['n']:5} {s['wr']:6.1f} {s['net']:+9.2f} {s['tp']:4} {s['sl']:4}")

    print("\n=== EXIT: Signal flip (no SL/TP) ===")
    print(f"{'combo':<32} {'n':>5} {'WR%':>6} {'net$':>9}")
    print("-" * 55)
    for net, combo, s in rows_flip:
        print(f"{combo:<32} {s['n']:5} {s['wr']:6.1f} {s['net']:+9.2f}")

    if rows_sltp:
        net, combo, s = rows_sltp[0]
        print(f"\n>>> BEST SL/TP: {combo} → n={s['n']} WR={s['wr']:.1f}% net=${net:+.2f} (${args.notional*10:+.2f} @ $60)")

    base = next((r for r in rows_sltp if r[1] == "alma_only"), None)
    best = rows_sltp[0] if rows_sltp else None
    if base and best:
        print(f"\n=== Alma-only baseline: n={base[2]['n']} WR={base[2]['wr']:.0f}% net=${base[0]:+.2f}")
        if best[1] != "alma_only":
            imp = best[0] - base[0]
            print(f"    Best combo ({best[1]}): net=${best[0]:+.2f} ({imp:+.2f} vs alma_only)")

    viable = [r for r in rows_sltp if r[0] > 0 and 10 <= r[2]["n"] <= 200]
    print(f"\n=== Positive SL/TP combos (10<=n<=200): {len(viable)} ===")
    for net, combo, s in viable[:8]:
        print(f"  {combo}: n={s['n']} WR={s['wr']:.0f}% net=${net:+.2f}")


if __name__ == "__main__":
    main()
