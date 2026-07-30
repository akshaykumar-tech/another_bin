#!/usr/bin/env python3
"""RSI FADE UP70 reclaim engine.

Logic (Wilder RSI):
  1) RSI crosses ≥ thr (default 70) → arm (no trade)
  2) RSI falls < thr → wait reclaim
  3) RSI crosses ≥ thr again → SHORT fade next open
  4) Hold HOLD_DAYS trading bars, exit at that day's close

Research (May–Jul 2026, $6, fee 0.08% RT):
  RSI7 / thr70 / FADE / H5  → ~+$102, 3G (Jul healthier)
  RSI7 / thr70 / FADE / H8  → ~+$186, 3G (Jul thinner)
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from orb30_engine import DayBar, pnl_usd

DEFAULT_RSI_PERIOD = 7
DEFAULT_RSI_THR = 70.0
DEFAULT_HOLD_DAYS = 5  # July-friendlier than H8
DEFAULT_MAX_ARM_DAYS = 15
DEFAULT_NOTIONAL = 6.0
DEFAULT_FEE_RT = 0.0008
STRATEGY_NAME = "RSI_FADE_UP70_RECLAIM"


@dataclass
class Signal:
    sym: str
    signal_date: str  # reclaim close day
    entry_date: str
    exit_date: str
    side: str  # short
    rsi: float
    thr: float
    entry: float = 0.0  # filled at entry open


@dataclass
class Trade:
    sym: str
    signal_date: str
    entry_date: str
    exit_date: str
    side: str
    entry: float
    exit: float
    rsi: float
    thr: float
    pnl_usd: float
    reason: str = "HOLD"


def _shift(d: str, n: int) -> str:
    return (datetime.strptime(d, "%Y-%m-%d").date() + timedelta(days=n)).isoformat()


def wilder_rsi(closes: list[float], period: int = DEFAULT_RSI_PERIOD) -> list[float | None]:
    n = len(closes)
    out: list[float | None] = [None] * n
    if n < period + 1:
        return out
    gains: list[float] = []
    losses: list[float] = []
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_g = sum(gains) / period
    avg_l = sum(losses) / period
    out[period] = 100.0 if avg_l == 0 else 100.0 - 100.0 / (1.0 + avg_g / avg_l)
    for i in range(period + 1, n):
        d = closes[i] - closes[i - 1]
        avg_g = (avg_g * (period - 1) + max(d, 0.0)) / period
        avg_l = (avg_l * (period - 1) + max(-d, 0.0)) / period
        out[i] = 100.0 if avg_l == 0 else 100.0 - 100.0 / (1.0 + avg_g / avg_l)
    return out


def reclaim_signal_indices(
    rsi: list[float | None],
    *,
    thr: float = DEFAULT_RSI_THR,
    max_arm: int = DEFAULT_MAX_ARM_DAYS,
) -> list[int]:
    """Return bar indices where UP reclaim cross happens (entry = i+1)."""
    out: list[int] = []
    st = 0  # 0 idle, 1 seen, 2 left waiting reclaim
    leave_i: int | None = None
    for i in range(1, len(rsi)):
        r, prev = rsi[i], rsi[i - 1]
        if r is None or prev is None:
            continue
        if st == 2 and leave_i is not None and i - leave_i > max_arm:
            st = 0
            leave_i = None
        crossed = r >= thr and prev < thr
        left = r < thr
        if st == 0:
            if crossed:
                st = 1
        elif st == 1:
            if left:
                st = 2
                leave_i = i
        elif st == 2:
            if crossed:
                out.append(i)
                st = 1  # back in zone; need leave again for next reclaim
                leave_i = None
    return out


def signals_for_sym(
    sym: str,
    bars: list[DayBar],
    *,
    entry_date: str | None = None,
    trade_start: str | None = None,
    trade_end: str | None = None,
    rsi_period: int = DEFAULT_RSI_PERIOD,
    thr: float = DEFAULT_RSI_THR,
    hold_days: int = DEFAULT_HOLD_DAYS,
    max_arm: int = DEFAULT_MAX_ARM_DAYS,
) -> list[Signal]:
    """Build reclaim signals. If entry_date set, only that trade day."""
    if len(bars) < rsi_period + 3:
        return []
    rsi = wilder_rsi([b.c for b in bars], rsi_period)
    hits = reclaim_signal_indices(rsi, thr=thr, max_arm=max_arm)
    out: list[Signal] = []
    for i in hits:
        if i + 1 >= len(bars):
            continue
        sig_day = bars[i].date
        ent_day = bars[i + 1].date
        if entry_date is not None and ent_day != entry_date:
            continue
        if trade_start is not None and ent_day < trade_start:
            continue
        if trade_end is not None and ent_day > trade_end:
            continue
        ex_i = i + hold_days  # entry at i+1, hold_days bars → exit index i+hold_days
        # entry bar = i+1; exit = entry + (hold_days-1) = i+hold_days
        if ex_i >= len(bars):
            exit_date = _shift(ent_day, hold_days - 1)
        else:
            exit_date = bars[ex_i].date
        rv = rsi[i]
        out.append(
            Signal(
                sym=sym,
                signal_date=sig_day,
                entry_date=ent_day,
                exit_date=exit_date,
                side="short",
                rsi=float(rv) if rv is not None else thr,
                thr=thr,
                entry=bars[i + 1].o,
            )
        )
    return out


def scan_for_entry_day(
    sym_bars: dict[str, list[DayBar]],
    entry_date: str,
    *,
    rsi_period: int = DEFAULT_RSI_PERIOD,
    thr: float = DEFAULT_RSI_THR,
    hold_days: int = DEFAULT_HOLD_DAYS,
    max_arm: int = DEFAULT_MAX_ARM_DAYS,
) -> list[Signal]:
    """All symbols with reclaim → entry on entry_date."""
    out: list[Signal] = []
    for sym, bars in sym_bars.items():
        out.extend(
            signals_for_sym(
                sym,
                bars,
                entry_date=entry_date,
                rsi_period=rsi_period,
                thr=thr,
                hold_days=hold_days,
                max_arm=max_arm,
            )
        )
    out.sort(key=lambda s: (-s.rsi, s.sym))
    return out


def backtest_range(
    sym_bars: dict[str, list[DayBar]],
    start: str,
    end: str,
    *,
    rsi_period: int = DEFAULT_RSI_PERIOD,
    thr: float = DEFAULT_RSI_THR,
    hold_days: int = DEFAULT_HOLD_DAYS,
    max_arm: int = DEFAULT_MAX_ARM_DAYS,
    notional: float = DEFAULT_NOTIONAL,
    fee_rt: float = DEFAULT_FEE_RT,
    one_per_sym: bool = True,
) -> list[Trade]:
    """Overlap: skip new entry while prior trade still open (by exit_date)."""
    trades: list[Trade] = []
    busy_until: dict[str, str] = {}
    # collect all signals in range
    all_sigs: list[Signal] = []
    for sym, bars in sym_bars.items():
        all_sigs.extend(
            signals_for_sym(
                sym,
                bars,
                trade_start=start,
                trade_end=end,
                rsi_period=rsi_period,
                thr=thr,
                hold_days=hold_days,
                max_arm=max_arm,
            )
        )
    all_sigs.sort(key=lambda s: (s.entry_date, -s.rsi, s.sym))
    by_sym = {sym: bars for sym, bars in sym_bars.items()}
    for s in all_sigs:
        if one_per_sym and s.sym in busy_until and s.entry_date < busy_until[s.sym]:
            continue
        bars = by_sym.get(s.sym) or []
        idx = {b.date: i for i, b in enumerate(bars)}
        ei = idx.get(s.entry_date)
        if ei is None or bars[ei].o <= 0:
            continue
        entry = bars[ei].o
        ex_i = ei + (hold_days - 1)
        if ex_i >= len(bars):
            continue
        exit_px = bars[ex_i].c
        exit_date = bars[ex_i].date
        pnl = pnl_usd(s.side, entry, exit_px, notional, fee_rt)
        trades.append(
            Trade(
                sym=s.sym,
                signal_date=s.signal_date,
                entry_date=s.entry_date,
                exit_date=exit_date,
                side=s.side,
                entry=entry,
                exit=exit_px,
                rsi=s.rsi,
                thr=s.thr,
                pnl_usd=pnl,
            )
        )
        if one_per_sym:
            busy_until[s.sym] = exit_date
    return trades
