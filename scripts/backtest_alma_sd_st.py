#!/usr/bin/env python3
"""
Backtest: Alma SD SuperTrend (Oquant Pine indicator).

Pine logic:
  - ALMA(close, 35, 0.85, 4)
  - Bands: alma ± factor * stdev(close, 33), factor=1.8
  - SuperTrend state machine on bands
  - signal=1 when dir<0 (bull), signal=-1 when dir>0 (bear)
  - Long on crossover(signal,0), Short on crossunder(signal,0)

Exit modes:
  flip  — hold until opposite signal (classic ST)
  sltp  — fixed SL/TP per leg
"""

from __future__ import annotations

import argparse
import csv
import math
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
KDIR_1S = ROOT / "data" / "klines" / "1s"
NOTIONAL = 6.0
FEE_ONE_WAY = 0.0004  # 0.04% per side
DEFAULT_EXCLUDE = {"SAHARAUSDT"}

# Pine defaults
FACTOR = 1.8
SD_LEN = 33
ALMA_LEN = 35
ALMA_SIGMA = 4.0
ALMA_OFFSET = 0.85


@dataclass
class Bar:
    ts: int
    o: float
    h: float
    l: float
    c: float
    v: float


@dataclass
class Trade:
    symbol: str
    side: int
    entry: float
    exit_px: float
    reason: str
    net_usd: float
    bars_held: int


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


def aggregate(bars_1s: list[Bar], bar_ms: int) -> list[Bar]:
    if not bars_1s:
        return []
    buckets: dict[int, list[Bar]] = {}
    for b in bars_1s:
        key = (b.ts // bar_ms) * bar_ms
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


def alma_weights(length: int, offset: float, sigma: float) -> list[float]:
    m = offset * (length - 1)
    s = length / sigma
    w = [math.exp(-((i - m) ** 2) / (2 * s * s)) for i in range(length)]
    norm = sum(w)
    return [x / norm for x in w]


def compute_alma(closes: list[float], length: int, offset: float, sigma: float) -> list[float | None]:
    w = alma_weights(length, offset, sigma)
    out: list[float | None] = [None] * len(closes)
    for t in range(length - 1, len(closes)):
        out[t] = sum(closes[t - i] * w[i] for i in range(length))
    return out


def compute_stdev(closes: list[float], length: int) -> list[float | None]:
    out: list[float | None] = [None] * len(closes)
    for t in range(length - 1, len(closes)):
        window = closes[t - length + 1 : t + 1]
        mean = sum(window) / length
        if length < 2:
            out[t] = 0.0
        else:
            var = sum((x - mean) ** 2 for x in window) / (length - 1)
            out[t] = math.sqrt(var)
    return out


def compute_supertrend(
    bars: list[Bar],
    factor: float = FACTOR,
    sd_len: int = SD_LEN,
    alma_len: int = ALMA_LEN,
    alma_sigma: float = ALMA_SIGMA,
    alma_offset: float = ALMA_OFFSET,
) -> list[int]:
    """Returns direction per bar: -1=bull (long), 1=bear (short), 0=warmup."""
    closes = [b.c for b in bars]
    alma = compute_alma(closes, alma_len, alma_offset, alma_sigma)
    sd = compute_stdev(closes, sd_len)

    n = len(bars)
    upper = [0.0] * n
    lower = [0.0] * n
    st_line = [0.0] * n
    direction = [0] * n

    for t in range(n):
        if alma[t] is None or sd[t] is None:
            direction[t] = 0
            continue

        ub = alma[t] + factor * sd[t]
        lb = alma[t] - factor * sd[t]

        if t > 0 and direction[t - 1] != 0:
            prev_ub = upper[t - 1]
            prev_lb = lower[t - 1]
            if not (ub < prev_ub or bars[t - 1].c > prev_ub):
                ub = prev_ub
            if not (lb > prev_lb or bars[t - 1].c < prev_lb):
                lb = prev_lb
        upper[t] = ub
        lower[t] = lb

        if t == 0 or sd[t - 1] is None or direction[t - 1] == 0:
            direction[t] = 1
        else:
            prev_st = st_line[t - 1]
            prev_ub = upper[t - 1]
            if prev_st == prev_ub:
                direction[t] = -1 if bars[t].c > ub else 1
            else:
                direction[t] = 1 if bars[t].c < lb else -1

        st_line[t] = lower[t] if direction[t] == -1 else upper[t]

    return direction


def signal_series(direction: list[int]) -> list[int]:
    """Pine: signal=1 if long, -1 if short."""
    sig = [0] * len(direction)
    cur = 0
    for t, d in enumerate(direction):
        if d == 0:
            sig[t] = 0
            continue
        if d < 0:
            cur = 1
        else:
            cur = -1
        sig[t] = cur
    return sig


def pnl_leg(side: int, entry: float, exit_px: float, notional: float) -> float:
    if entry <= 0:
        return 0.0
    g = (exit_px - entry) / entry * 100 if side > 0 else (entry - exit_px) / entry * 100
    return notional * g / 100 - notional * FEE_ONE_WAY * 2


def backtest_flip(sym: str, bars: list[Bar], direction: list[int], notional: float) -> list[Trade]:
    sig = signal_series(direction)
    trades: list[Trade] = []
    pos = 0
    entry_i = -1
    entry_px = 0.0

    for t in range(1, len(bars)):
        prev, cur = sig[t - 1], sig[t]
        if cur == 0:
            continue

        # crossover(signal, 0) → long
        long_entry = prev <= 0 and cur > 0
        # crossunder(signal, 0) → short
        short_entry = prev >= 0 and cur < 0

        if not (long_entry or short_entry):
            continue

        new_side = 1 if long_entry else -1
        fill_i = min(t + 1, len(bars) - 1)
        fill_px = bars[fill_i].o

        if pos != 0:
            trades.append(
                Trade(
                    sym,
                    pos,
                    entry_px,
                    fill_px,
                    "flip",
                    pnl_leg(pos, entry_px, fill_px, notional),
                    fill_i - entry_i,
                )
            )

        pos = new_side
        entry_i = fill_i
        entry_px = fill_px

    if pos != 0 and entry_i >= 0:
        last = bars[-1]
        trades.append(
            Trade(sym, pos, entry_px, last.c, "eod", pnl_leg(pos, entry_px, last.c, notional), len(bars) - 1 - entry_i)
        )
    return trades


def backtest_sltp(
    sym: str,
    bars: list[Bar],
    direction: list[int],
    notional: float,
    sl_pct: float,
    tp_pct: float,
    max_bars: int = 288,
) -> list[Trade]:
    sig = signal_series(direction)
    trades: list[Trade] = []
    pos = 0
    entry_i = -1
    entry_px = 0.0

    t = 1
    while t < len(bars):
        if pos == 0:
            prev, cur = sig[t - 1], sig[t]
            if cur == 0:
                t += 1
                continue
            long_entry = prev <= 0 and cur > 0
            short_entry = prev >= 0 and cur < 0
            if long_entry or short_entry:
                pos = 1 if long_entry else -1
                entry_i = min(t + 1, len(bars) - 1)
                entry_px = bars[entry_i].o
                t = entry_i + 1
                continue
            t += 1
            continue

        # in position — check SL/TP
        sl_px = entry_px * (1 - sl_pct / 100) if pos > 0 else entry_px * (1 + sl_pct / 100)
        tp_px = entry_px * (1 + tp_pct / 100) if pos > 0 else entry_px * (1 - tp_pct / 100)
        reason = ""
        exit_px = 0.0
        exit_i = entry_i

        for j in range(entry_i + 1, min(len(bars), entry_i + 1 + max_bars)):
            b = bars[j]
            if pos > 0:
                hit_sl = b.l <= sl_px
                hit_tp = b.h >= tp_px
            else:
                hit_sl = b.h >= sl_px
                hit_tp = b.l <= tp_px
            if hit_sl and hit_tp:
                reason, exit_px, exit_i = "sl", sl_px, j
                break
            if hit_sl:
                reason, exit_px, exit_i = "sl", sl_px, j
                break
            if hit_tp:
                reason, exit_px, exit_i = "tp", tp_px, j
                break
        else:
            exit_i = min(len(bars) - 1, entry_i + max_bars)
            reason, exit_px = "timeout", bars[exit_i].c

        trades.append(Trade(sym, pos, entry_px, exit_px, reason, pnl_leg(pos, entry_px, exit_px, notional), exit_i - entry_i))
        pos = 0
        entry_i = -1
        t = exit_i + 1

    return trades


def summarize(trades: list[Trade]) -> dict:
    n = len(trades)
    if not n:
        return {"n": 0, "wr": 0, "net": 0, "avg": 0}
    wins = sum(1 for t in trades if t.net_usd > 0)
    return {
        "n": n,
        "wr": 100 * wins / n,
        "net": sum(t.net_usd for t in trades),
        "avg": sum(t.net_usd for t in trades) / n,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--exclude", default="SAHARAUSDT")
    ap.add_argument("--notional", type=float, default=NOTIONAL)
    ap.add_argument("--tf", default="5m", choices=["1m", "5m", "15m", "1h"])
    args = ap.parse_args()
    skip = {s.strip().upper() for s in args.exclude.split(",") if s.strip()}
    notional = args.notional
    tf_ms = {"1m": 60_000, "5m": 300_000, "15m": 900_000, "1h": 3_600_000}[args.tf]

    sym_bars: dict[str, list[Bar]] = {}
    for p in sorted(KDIR_1S.glob("*.csv")):
        if p.stem.upper() in skip:
            continue
        bars = aggregate(load_1s(p.stem), tf_ms)
        if len(bars) >= ALMA_LEN + SD_LEN + 10:
            sym_bars[p.stem] = bars

    print(f"Alma SD SuperTrend backtest | TF={args.tf} | {len(sym_bars)} symbols | excluded={skip}")
    print(f"  ALMA({ALMA_LEN},{ALMA_OFFSET},{ALMA_SIGMA}) | SD={SD_LEN} | factor={FACTOR}")
    print(f"  notional=${notional} | fee=0.08% RT\n")

    # ── Flip mode (hold until opposite signal) ──
    all_flip: list[Trade] = []
    per_sym_flip: list[tuple[str, dict]] = []
    for sym, bars in sym_bars.items():
        d = compute_supertrend(bars)
        tr = backtest_flip(sym, bars, d, notional)
        all_flip.extend(tr)
        s = summarize(tr)
        if s["n"]:
            per_sym_flip.append((s["net"], sym, s))

    s_flip = summarize(all_flip)
    print("=== MODE: Flip on signal (classic SuperTrend) ===")
    print(f"  Total trades: {s_flip['n']}")
    print(f"  Win rate:     {s_flip['wr']:.1f}%")
    print(f"  Net PnL:      ${s_flip['net']:+.2f}")
    print(f"  Avg/trade:    ${s_flip['avg']:+.4f}")

    per_sym_flip.sort(key=lambda x: -x[0])
    print("\n  Top 5 symbols:")
    for net, sym, s in per_sym_flip[:5]:
        print(f"    {sym}: n={s['n']} WR={s['wr']:.0f}% net=${net:+.2f}")
    print("  Bottom 5 symbols:")
    for net, sym, s in per_sym_flip[-5:]:
        print(f"    {sym}: n={s['n']} WR={s['wr']:.0f}% net=${net:+.2f}")

    pos = sum(1 for t in all_flip if t.net_usd > 0)
    neg = s_flip["n"] - pos
    print(f"\n  Winners: {pos} | Losers: {neg}")

    # ── SL/TP grid on signal entries ──
    print("\n=== MODE: Signal entry + fixed SL/TP ===")
    print(f"{'SL%':>5} {'TP%':>5} {'n':>5} {'WR%':>6} {'net$':>9} {'avg$':>8}")
    print("-" * 42)
    sltp_rows = []
    for sl in (0.5, 1.0, 1.5, 2.0, 3.0):
        for tp in (1.0, 2.0, 3.0, 5.0, 8.0):
            if tp <= sl:
                continue
            all_t: list[Trade] = []
            for sym, bars in sym_bars.items():
                d = compute_supertrend(bars)
                all_t.extend(backtest_sltp(sym, bars, d, notional, sl, tp))
            s = summarize(all_t)
            if s["n"] >= 5:
                sltp_rows.append((s["net"], sl, tp, s))

    sltp_rows.sort(key=lambda x: -x[0])
    for net, sl, tp, s in sltp_rows[:15]:
        print(f"{sl:5.1f} {tp:5.1f} {s['n']:5} {s['wr']:6.1f} {s['net']:+9.2f} {s['avg']:+8.4f}")

    if sltp_rows:
        net, sl, tp, s = sltp_rows[0]
        print(f"\n>>> Best SL/TP: SL={sl}% TP={tp}% → n={s['n']} WR={s['wr']:.1f}% net=${net:+.2f}")

    # ── Multi-timeframe flip summary ──
    print("\n=== Flip mode across timeframes ===")
    for tf_name, ms in [("1m", 60_000), ("5m", 300_000), ("15m", 900_000), ("1h", 3_600_000)]:
        tot: list[Trade] = []
        for p in sorted(KDIR_1S.glob("*.csv")):
            if p.stem.upper() in skip:
                continue
            bars = aggregate(load_1s(p.stem), ms)
            if len(bars) < ALMA_LEN + SD_LEN + 10:
                continue
            d = compute_supertrend(bars)
            tot.extend(backtest_flip(p.stem, bars, d, notional))
        s = summarize(tot)
        print(f"  {tf_name}: n={s['n']} WR={s['wr']:.1f}% net=${s['net']:+.2f}")


if __name__ == "__main__":
    main()
