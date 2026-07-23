#!/usr/bin/env python3
"""WinBE hedge engine — prev-day |c2c|≥thr → D+1 LONG+SHORT @ open.

Per variant TP X%: first leg to TP closes; other arms BE at entry (next bar).
Else both EOD. Research dry variants: TP3 / TP5 / TP8.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

from orb30_engine import DayBar, pnl_usd

DEFAULT_THR = 20.0
DEFAULT_NOTIONAL = 6.0
DEFAULT_FEE_RT = 0.0008
DEFAULT_TPS = (3.0, 5.0, 8.0)


def _shift(d: str, n: int) -> str:
    return (datetime.strptime(d, "%Y-%m-%d").date() + timedelta(days=n)).isoformat()


@dataclass
class Signal:
    sym: str
    signal_date: str
    trade_date: str
    move_pct: float


@dataclass
class HedgePos:
    """One symbol book for one TP variant (both legs)."""

    variant: str  # TP3 | TP5 | TP8
    tp_pct: float
    sym: str
    signal_date: str
    trade_date: str
    move_pct: float
    entry: float
    long_open: bool = True
    short_open: bool = True
    long_sl: float | None = None  # BE level when armed
    short_sl: float | None = None
    long_arm_bar: int = -1  # active from arm_bar+1
    short_arm_bar: int = -1
    long_exit: float = 0.0
    short_exit: float = 0.0
    long_reason: str = ""
    short_reason: str = ""
    closed: bool = False
    pnl: float = 0.0

    def key(self) -> str:
        return f"{self.variant}:{self.sym}"


def scan_signals_for_trade_day(
    trade_date: str,
    daily_by_sym: dict[str, list[DayBar]],
    *,
    thr: float = DEFAULT_THR,
) -> list[Signal]:
    """Signal = prior UTC day |c2c| ≥ thr."""
    sig = _shift(trade_date, -1)
    out: list[Signal] = []
    for sym, bars in daily_by_sym.items():
        by = {b.date: i for i, b in enumerate(bars)}
        if sig not in by or by[sig] < 1:
            continue
        i = by[sig]
        prev, cur = bars[i - 1], bars[i]
        if prev.c <= 0:
            continue
        m = (cur.c / prev.c - 1.0) * 100.0
        if abs(m) >= thr:
            out.append(Signal(sym=sym, signal_date=sig, trade_date=trade_date, move_pct=m))
    out.sort(key=lambda s: (-abs(s.move_pct), s.sym))
    return out


def _leg_pnl(side: str, entry: float, exit_px: float, notional: float, fee_rt: float) -> float:
    return pnl_usd(side, entry, exit_px, notional, fee_rt)


def apply_bar(
    pos: HedgePos,
    bar_i: int,
    high: float,
    low: float,
    *,
    notional: float = DEFAULT_NOTIONAL,
    fee_rt: float = DEFAULT_FEE_RT,
) -> bool:
    """Update pos on one 5m bar. Returns True if fully closed this bar."""
    if pos.closed:
        return True
    entry = pos.entry
    tp_l = entry * (1.0 + pos.tp_pct / 100.0)
    tp_s = entry * (1.0 - pos.tp_pct / 100.0)

    # BE only after arm bar. Direction depends which side won TP:
    # short TP'd first → long underwater → BE when price rises back (high >= entry)
    # long TP'd first → short underwater → BE when price falls back (low <= entry)
    if pos.long_open and pos.long_sl is not None and bar_i > pos.long_arm_bar:
        if high >= pos.long_sl:
            pos.long_exit = pos.long_sl
            pos.long_reason = "BE"
            pos.long_open = False
    if pos.short_open and pos.short_sl is not None and bar_i > pos.short_arm_bar:
        if low <= pos.short_sl:
            pos.short_exit = pos.short_sl
            pos.short_reason = "BE"
            pos.short_open = False

    if pos.long_open and high >= tp_l:
        pos.long_exit = tp_l
        pos.long_reason = "TP"
        pos.long_open = False
        if pos.short_open:
            pos.short_sl = entry
            pos.short_arm_bar = bar_i
    if pos.short_open and low <= tp_s:
        pos.short_exit = tp_s
        pos.short_reason = "TP"
        pos.short_open = False
        if pos.long_open:
            pos.long_sl = entry
            pos.long_arm_bar = bar_i

    if not pos.long_open and not pos.short_open:
        _finalize(pos, notional, fee_rt)
        return True
    return False


def close_eod(
    pos: HedgePos,
    exit_px: float,
    *,
    notional: float = DEFAULT_NOTIONAL,
    fee_rt: float = DEFAULT_FEE_RT,
) -> None:
    if pos.closed:
        return
    if pos.long_open:
        pos.long_exit = exit_px
        pos.long_reason = "EOD"
        pos.long_open = False
    if pos.short_open:
        pos.short_exit = exit_px
        pos.short_reason = "EOD"
        pos.short_open = False
    _finalize(pos, notional, fee_rt)


def _finalize(pos: HedgePos, notional: float, fee_rt: float) -> None:
    pnl = 0.0
    if pos.long_reason:
        pnl += _leg_pnl("long", pos.entry, pos.long_exit, notional, fee_rt)
    if pos.short_reason:
        pnl += _leg_pnl("short", pos.entry, pos.short_exit, notional, fee_rt)
    pos.pnl = pnl
    pos.closed = True


def variant_name(tp_pct: float) -> str:
    return f"TP{int(tp_pct) if float(tp_pct).is_integer() else tp_pct}"


def make_books_for_signal(
    sig: Signal,
    entry: float,
    tps: tuple[float, ...] = DEFAULT_TPS,
) -> list[HedgePos]:
    out: list[HedgePos] = []
    for tp in tps:
        out.append(
            HedgePos(
                variant=variant_name(tp),
                tp_pct=float(tp),
                sym=sig.sym,
                signal_date=sig.signal_date,
                trade_date=sig.trade_date,
                move_pct=sig.move_pct,
                entry=entry,
            )
        )
    return out
