#!/usr/bin/env python3
"""
5m candle backtest: burst entry, exit via SL or TP only (no fixed hold).
Aggregates local 1s klines → 5m. Skips SAHARAUSDT by default.
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
BAR_MS = 300_000  # 5m
MAX_BARS = 288  # 24h cap if neither SL nor TP


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
        return 1 if up >= dn else -1


@dataclass
class Trade:
    symbol: str
    side: int
    entry: float
    exit_px: float
    reason: str  # tp | sl | timeout
    net_usd: float


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


def pnl_usd(side: int, entry: float, exit_px: float, notional: float = NOTIONAL) -> float:
    if entry <= 0:
        return 0.0
    g = (exit_px - entry) / entry * 100 if side > 0 else (entry - exit_px) / entry * 100
    return notional * g / 100 - notional * FEE_RT


def sim_exit(
    bars: list[Bar], entry_i: int, side: int, entry_px: float, sl_pct: float, tp_pct: float
) -> tuple[float, str]:
    """Walk forward 5m bars; exit at SL/TP intrabar (SL priority if both)."""
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


def collect_events(bars: list[Bar], amp_min: float, min_vol: float = 0) -> list[int]:
    """Bar indices with burst + rearm."""
    idxs = []
    last_ts = 0
    for i, b in enumerate(bars):
        if b.amp_pct() < amp_min:
            continue
        if min_vol and b.v < min_vol:
            continue
        if last_ts and b.ts - last_ts < REARM_MS:
            continue
        idxs.append(i)
        last_ts = b.ts
    return idxs


def backtest_symbol(
    sym: str,
    bars: list[Bar],
    amp_min: float,
    sl_pct: float,
    tp_pct: float,
    fade: bool,
    notional: float,
) -> list[Trade]:
    trades: list[Trade] = []
    for i in collect_events(bars, amp_min):
        if i + 1 >= len(bars):
            continue
        b = bars[i]
        side = b.dir()
        if side == 0:
            continue
        if fade:
            side = -side
        entry_i = i + 1
        entry_px = bars[entry_i].o
        exit_px, reason = sim_exit(bars, entry_i, side, entry_px, sl_pct, tp_pct)
        trades.append(
            Trade(sym, side, entry_px, exit_px, reason, pnl_usd(side, entry_px, exit_px, notional))
        )
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
    notional = args.notional
    skip = {s.strip().upper() for s in args.exclude.split(",") if s.strip()}

    sym_bars: dict[str, list[Bar]] = {}
    for p in sorted(KDIR_1S.glob("*.csv")):
        if p.stem.upper() in skip:
            continue
        b5 = to_5m(load_1s(p.stem))
        if len(b5) >= 20:
            sym_bars[p.stem] = b5

    total_5m = sum(len(v) for v in sym_bars.values())
    print(f"5m SL/TP backtest | {len(sym_bars)} symbols | excluded={skip}")
    print(f"  5m bars total={total_5m} | fee={FEE_RT*100:.2f}% RT | notional=${notional}")
    print(f"  exit: SL or TP only (timeout after {MAX_BARS}×5m if neither)\n")

    # count events at thresholds
    for amp in (2.0, 2.5, 3.0, 4.0, 5.0):
        ev = sum(len(collect_events(b, amp)) for b in sym_bars.values())
        print(f"  {amp}% 5m events (rearm 300s): {ev}")

    sls = [0.5, 1.0, 1.5, 2.0, 3.0]
    tps = [1.0, 1.5, 2.0, 3.0, 5.0, 8.0]
    amps = [2.0, 2.5, 3.0, 4.0]

    print(f"\n{'mode':<6} {'amp':>4} {'SL%':>5} {'TP%':>5} {'n':>4} {'WR%':>5} {'net$':>8} {'TP':>3} {'SL':>3} {'TO':>3}")
    print("-" * 62)

    rows = []
    for fade in (False, True):
        mode = "fade" if fade else "mom"
        for amp in amps:
            for sl in sls:
                for tp in tps:
                    if tp <= sl:
                        continue
                    all_t: list[Trade] = []
                    for sym, bars in sym_bars.items():
                        all_t.extend(backtest_symbol(sym, bars, amp, sl, tp, fade, notional))
                    s = summarize(all_t)
                    if s["n"] < 5:
                        continue
                    rows.append((s["net"], mode, amp, sl, tp, s))

    rows.sort(key=lambda x: -x[0])
    for net, mode, amp, sl, tp, s in rows[:25]:
        print(
            f"{mode:<6} {amp:4.1f} {sl:5.1f} {tp:5.1f} {s['n']:4} {s['wr']:5.1f} "
            f"{s['net']:+8.2f} {s['tp']:3} {s['sl']:3} {s['to']:3}"
        )

    if rows:
        net, mode, amp, sl, tp, s = rows[0]
        print(f"\n>>> BEST: {mode} amp={amp}% SL={sl}% TP={tp}%")
        print(f"    n={s['n']} WR={s['wr']:.1f}% net=${s['net']:+.2f} (tp={s['tp']} sl={s['sl']} timeout={s['to']})")

    viable = [r for r in rows if r[0] > 0 and r[5]["n"] <= 55]
    print(f"\n=== Positive & n<=55: {len(viable)} configs ===")
    for net, mode, amp, sl, tp, s in viable[:10]:
        print(f"  {mode} {amp}% SL={sl} TP={tp}: n={s['n']} WR={s['wr']:.0f}% net=${net:+.2f}")


if __name__ == "__main__":
    main()
