#!/usr/bin/env python3
"""
Big-mover strategy engine (dry/backtest).

Research strategy #1 (from our sweep):
  - Entry trigger: first non-ambiguous touch of OPEN + 25% (direction=up only)
  - Mode: CONT  => UP touch => LONG
  - Entry fill: 5m LIMIT trade-through at exact level OPEN*(1+thr)
  - Exit: TP 3% (exact level touch) else EOD close
  - top1/day selection:
      move_abs  => pick symbol with maximum (bar_high - open)/open at trigger bar
      first_fill => pick symbol with earliest trigger bar

Note: live execution cannot perfectly replicate "move_abs top1/day"
because you only know the max after seeing the whole day. This engine
supports both selection modes so dry/backtest can match your chosen definition.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal

import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))

# Reuse the cached 5m loader and candle type.
from backtest_big_mover_days import Bar5, fetch_5m, through  # noqa: E402
from orb30_engine import list_syms  # noqa: E402


Top1Mode = Literal["move_abs", "first_fill"]


@dataclass(frozen=True)
class Trigger:
    sym: str
    trade_date: str
    entry_i: int
    entry_px: float
    move_abs_pct: float


@dataclass(frozen=True)
class Trade:
    trade_date: str
    sym: str
    entry: float
    exit: float
    entry_bar_i: int
    move_abs_pct: float
    reason: str  # TP or EOD
    pnl: float


def _day_iter(start: str, end: str) -> list[str]:
    d0 = datetime.strptime(start, "%Y-%m-%d").date()
    d1 = datetime.strptime(end, "%Y-%m-%d").date()
    out: list[str] = []
    d = d0
    while d <= d1:
        out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def _entry_levels(day_open: float, thr_pct: float) -> tuple[float, float]:
    """Up level and down level for the same thr% distance."""
    up = day_open * (1.0 + thr_pct / 100.0)
    dn = day_open * (1.0 - thr_pct / 100.0)
    return up, dn


def find_up_trigger_first_touch(bars: list[Bar5], trade_date: str, thr_pct: float) -> Trigger | None:
    """
    Return a LONG trigger only if the *first non-ambiguous* threshold touch is UP.

    Ambiguous bars (touching both OPEN+thr and OPEN-thr in same 5m candle) are skipped.
    If the first threshold touch is DOWN, returns None (because direction=up only).
    """
    if not bars or bars[0].o <= 0:
        return None
    day_open = bars[0].o
    up_lvl, dn_lvl = _entry_levels(day_open, thr_pct)

    for i in range(1, len(bars)):
        b = bars[i]
        up_move = (b.h - day_open) / day_open * 100.0
        dn_move = (day_open - b.l) / day_open * 100.0

        if up_move >= thr_pct and dn_move >= thr_pct:
            # ambiguous: both sides touched => ignore this bar and keep searching
            continue

        if up_move >= thr_pct:
            # UP first-touch
            if through(b, up_lvl):
                move_abs = up_move  # positive
                return Trigger(
                    sym="",
                    trade_date=trade_date,
                    entry_i=i,
                    entry_px=up_lvl,
                    move_abs_pct=move_abs,
                )
            return None

        if dn_move >= thr_pct:
            # DOWN first-touch (direction filter rejects)
            if through(b, dn_lvl):
                return None

    return None


def exit_tp_or_eod(bars: list[Bar5], entry_i: int, entry_px: float, tp_pct: float) -> tuple[float, str]:
    tp_px = entry_px * (1.0 + tp_pct / 100.0)
    for j in range(entry_i + 1, len(bars)):
        if through(bars[j], tp_px):
            return tp_px, "TP"
    return bars[-1].c, "EOD"


def pnl_usd_long(entry_px: float, exit_px: float, notional: float, fee_rt: float) -> float:
    if entry_px <= 0 or exit_px <= 0:
        return 0.0
    g_pct = (exit_px - entry_px) / entry_px * 100.0
    return notional * g_pct / 100.0 - notional * fee_rt


def select_top1(triggers: list[Trigger], mode: Top1Mode) -> Trigger | None:
    if not triggers:
        return None
    if mode == "move_abs":
        # max bar-high deviation at trigger, then earliest fill
        return max(triggers, key=lambda t: (t.move_abs_pct, -t.entry_i))
    # first_fill
    return min(triggers, key=lambda t: (t.entry_i, -t.move_abs_pct))


def backtest_range(
    start: str,
    end: str,
    *,
    thr_pct: float = 25.0,
    tp_pct: float = 3.0,
    notional: float = 6.0,
    fee_rt: float = 0.0008,
    top1_mode: Top1Mode = "move_abs",
    syms: list[str] | None = None,
    universe_limit: int = 0,
) -> list[Trade]:
    """
    Dry/backtest loop.

    Note: selection happens per day across all symbols that have a valid UP-first trigger.
    """
    if syms is None:
        syms = list_syms()
    if universe_limit and universe_limit > 0:
        syms = syms[:universe_limit]

    days = _day_iter(start, end)
    out: list[Trade] = []

    for d in days:
        candidates: list[Trigger] = []
        for sym in syms:
            bars = fetch_5m(sym, d)
            if not bars or len(bars) < 50:
                continue
            t = find_up_trigger_first_touch(bars, d, thr_pct)
            if not t:
                continue
            t = Trigger(
                sym=sym,
                trade_date=d,
                entry_i=t.entry_i,
                entry_px=t.entry_px,
                move_abs_pct=t.move_abs_pct,
            )
            candidates.append(t)

        top = select_top1(candidates, top1_mode)
        if not top:
            continue

        bars_top = fetch_5m(top.sym, d)
        if not bars_top or len(bars_top) < 50:
            continue
        exit_px, reason = exit_tp_or_eod(bars_top, top.entry_i, top.entry_px, tp_pct)
        pnl = pnl_usd_long(top.entry_px, exit_px, notional, fee_rt)
        out.append(
            Trade(
                trade_date=d,
                sym=top.sym,
                entry=top.entry_px,
                exit=exit_px,
                entry_bar_i=top.entry_i,
                move_abs_pct=top.move_abs_pct,
                reason=reason,
                pnl=pnl,
            )
        )

    return out


def main() -> None:
    import argparse
    import csv

    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-06-01")
    ap.add_argument("--end", default="2026-07-18")
    ap.add_argument("--top1-mode", default="move_abs", choices=["move_abs", "first_fill"])
    ap.add_argument("--universe-limit", type=int, default=0)
    ap.add_argument("--thr", type=float, default=25.0)
    ap.add_argument("--tp", type=float, default=3.0)
    ap.add_argument("--notional", type=float, default=6.0)
    ap.add_argument("--fee-rt", type=float, default=0.0008)
    args = ap.parse_args()

    trades = backtest_range(
        args.start,
        args.end,
        thr_pct=args.thr,
        tp_pct=args.tp,
        notional=args.notional,
        fee_rt=args.fee_rt,
        top1_mode=args.top1_mode,  # type: ignore[arg-type]
        universe_limit=args.universe_limit,
    )
    if not trades:
        print("No trades")
        return

    n = len(trades)
    tot = sum(t.pnl for t in trades)
    wr = 100.0 * sum(1 for t in trades if t.pnl > 0) / n
    print(f"N={n} total=${tot:+.2f} WR={wr:.1f}%")

    out = Path("data") / f"bm_thr25_cont_tp3_dirup_top1_{args.top1_mode}_{args.start}_to_{args.end}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "trade_date",
                "sym",
                "entry",
                "exit",
                "entry_bar_i",
                "move_abs_pct",
                "reason",
                "pnl",
            ],
        )
        w.writeheader()
        for t in trades:
            w.writerow(
                {
                    "trade_date": t.trade_date,
                    "sym": t.sym,
                    "entry": t.entry,
                    "exit": t.exit,
                    "entry_bar_i": t.entry_bar_i,
                    "move_abs_pct": t.move_abs_pct,
                    "reason": t.reason,
                    "pnl": round(t.pnl, 6),
                }
            )
    print("wrote", out)


if __name__ == "__main__":
    main()

