#!/usr/bin/env python3
"""Pump-fade engine: c22_s2 + top_uw → SHORT D+1 @ open+30m → hold 10d.

Research label: c22_s2_top_uw | delay30m | hold10
  (~n229 / +$135 May–Jul18 2026 @ $6, fee 0.08% RT)

Signal (peak day D, daily bars):
  - ≥2 consecutive up closes ending on D
  - cum return from close-before-streak to D close ≥ CUM_PCT (22)
  - close in top of range: close_loc ≥ CLOSE_LOC_MIN (0.75)
  - upper wick ≥ UW_PCT (2% of open)

Entry: SHORT @ D+1, 30m after UTC open (6×5m bar open)
Exit:  close of entry day + (HOLD_DAYS-1)  → 10 trading days total
Overlap: one open trade per symbol (skip new signals until exit date).
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable

from backtest_big_mover_days import fetch_5m
from orb30_engine import DayBar, pnl_usd

DEFAULT_CUM_PCT = 22.0
DEFAULT_STREAK_MIN = 2
DEFAULT_CLOSE_LOC_MIN = 0.75
DEFAULT_UW_PCT = 2.0
DEFAULT_HOLD_DAYS = 10
DEFAULT_ENTRY_DELAY_BARS = 6  # 6 × 5m = 30m after UTC open
DEFAULT_NOTIONAL = 6.0
DEFAULT_FEE_RT = 0.0008


@dataclass
class Signal:
    sym: str
    signal_date: str  # peak day D
    entry_date: str  # D+1
    exit_date: str  # entry + hold-1 bars
    streak: int
    cum_pct: float
    close_loc: float
    uw_pct: float
    start_px: float
    peak_px: float
    entry: float
    exit_px: float


@dataclass
class Trade:
    sym: str
    signal_date: str
    entry_date: str
    exit_date: str
    side: str
    entry: float
    exit: float
    streak: int
    cum_pct: float
    close_loc: float
    uw_pct: float
    pnl_usd: float
    reason: str = "HOLD"


def _shift(d: str, n: int) -> str:
    return (datetime.strptime(d, "%Y-%m-%d").date() + timedelta(days=n)).isoformat()


def close_loc(b: DayBar) -> float:
    rng = b.h - b.l
    if rng <= 0:
        return 0.5
    return (b.c - b.l) / rng


def upper_wick_pct(b: DayBar) -> float:
    if b.o <= 0:
        return 0.0
    return (b.h - max(b.o, b.c)) / b.o * 100.0


def detect_peak_signal(
    bars: list[DayBar],
    i: int,
    *,
    cum_pct: float = DEFAULT_CUM_PCT,
    streak_min: int = DEFAULT_STREAK_MIN,
    close_loc_min: float = DEFAULT_CLOSE_LOC_MIN,
    uw_pct: float = DEFAULT_UW_PCT,
) -> tuple[int, float, float, float, float] | None:
    """If bars[i] is a valid peak, return (streak, cum, close_loc, uw, start_px)."""
    if i < 1 or i + 1 >= len(bars):
        return None
    j = i
    while j >= 1 and bars[j].c > bars[j - 1].c:
        j -= 1
    streak = i - j
    if streak < streak_min:
        return None
    start_px = bars[j].c
    if start_px <= 0:
        return None
    peak_px = bars[i].c
    cum = (peak_px / start_px - 1.0) * 100.0
    if cum < cum_pct:
        return None
    cl = close_loc(bars[i])
    if cl < close_loc_min:
        return None
    uw = upper_wick_pct(bars[i])
    if uw < uw_pct:
        return None
    return streak, cum, cl, uw, start_px


def signals_for_sym(
    sym: str,
    bars: list[DayBar],
    trade_start: str,
    trade_end: str,
    *,
    cum_pct: float = DEFAULT_CUM_PCT,
    streak_min: int = DEFAULT_STREAK_MIN,
    close_loc_min: float = DEFAULT_CLOSE_LOC_MIN,
    uw_pct: float = DEFAULT_UW_PCT,
    hold_days: int = DEFAULT_HOLD_DAYS,
) -> list[Signal]:
    """All raw signals (no overlap filter) with entry/exit prices filled."""
    if len(bars) < streak_min + 2:
        return []
    by_date = {b.date: idx for idx, b in enumerate(bars)}
    out: list[Signal] = []
    for i, b in enumerate(bars):
        if b.date < trade_start or b.date > trade_end:
            continue
        hit = detect_peak_signal(
            bars,
            i,
            cum_pct=cum_pct,
            streak_min=streak_min,
            close_loc_min=close_loc_min,
            uw_pct=uw_pct,
        )
        if hit is None:
            continue
        streak, cum, cl, uw, start_px = hit
        entry_i = i + 1
        if entry_i >= len(bars):
            continue
        entry_bar = bars[entry_i]
        if entry_bar.date < trade_start:
            continue
        # Match research: if < hold_days bars remain, exit on last available bar
        exit_i = min(entry_i + (hold_days - 1), len(bars) - 1)
        if exit_i < entry_i:
            continue
        exit_bar = bars[exit_i]
        out.append(
            Signal(
                sym=sym,
                signal_date=b.date,
                entry_date=entry_bar.date,
                exit_date=exit_bar.date,
                streak=streak,
                cum_pct=cum,
                close_loc=cl,
                uw_pct=uw,
                start_px=start_px,
                peak_px=b.c,
                entry=entry_bar.o,
                exit_px=exit_bar.c,
            )
        )
    _ = by_date
    return out


def apply_overlap(signals: Iterable[Signal]) -> list[Signal]:
    """Per-symbol: skip new signal if peak_date < previous exit_date."""
    ordered = sorted(signals, key=lambda s: (s.signal_date, s.sym))
    busy: dict[str, str] = {}
    kept: list[Signal] = []
    for s in ordered:
        if s.sym in busy and s.signal_date < busy[s.sym]:
            continue
        kept.append(s)
        busy[s.sym] = s.exit_date
    return kept


def signal_to_trade(
    s: Signal,
    *,
    notional: float = DEFAULT_NOTIONAL,
    fee_rt: float = DEFAULT_FEE_RT,
) -> Trade:
    usd = pnl_usd("short", s.entry, s.exit_px, notional, fee_rt)
    return Trade(
        sym=s.sym,
        signal_date=s.signal_date,
        entry_date=s.entry_date,
        exit_date=s.exit_date,
        side="short",
        entry=s.entry,
        exit=s.exit_px,
        streak=s.streak,
        cum_pct=s.cum_pct,
        close_loc=s.close_loc,
        uw_pct=s.uw_pct,
        pnl_usd=usd,
        reason="HOLD",
    )


def apply_entry_delay(
    signals: list[Signal],
    *,
    delay_bars: int = DEFAULT_ENTRY_DELAY_BARS,
) -> list[Signal]:
    """Rewrite entry px to D+1 5m bar[delay_bars].o (skip if bars missing)."""
    delay = max(0, int(delay_bars))
    if delay <= 0 or not signals:
        return signals
    need = {(s.sym, s.entry_date) for s in signals}
    bars5: dict[tuple[str, str], list] = {}

    def one(k: tuple[str, str]):
        try:
            return k, fetch_5m(*k)
        except Exception:
            return k, []

    with ThreadPoolExecutor(max_workers=16) as ex:
        for fut in as_completed([ex.submit(one, k) for k in need]):
            k, b = fut.result()
            bars5[k] = b

    out: list[Signal] = []
    for s in signals:
        b5 = bars5.get((s.sym, s.entry_date)) or []
        if delay >= len(b5) or b5[delay].o <= 0:
            continue
        out.append(
            Signal(
                sym=s.sym,
                signal_date=s.signal_date,
                entry_date=s.entry_date,
                exit_date=s.exit_date,
                streak=s.streak,
                cum_pct=s.cum_pct,
                close_loc=s.close_loc,
                uw_pct=s.uw_pct,
                start_px=s.start_px,
                peak_px=s.peak_px,
                entry=b5[delay].o,
                exit_px=s.exit_px,
            )
        )
    return out


def backtest_range(
    daily: dict[str, list[DayBar]],
    start: str,
    end: str,
    *,
    cum_pct: float = DEFAULT_CUM_PCT,
    streak_min: int = DEFAULT_STREAK_MIN,
    close_loc_min: float = DEFAULT_CLOSE_LOC_MIN,
    uw_pct: float = DEFAULT_UW_PCT,
    hold_days: int = DEFAULT_HOLD_DAYS,
    entry_delay_bars: int = DEFAULT_ENTRY_DELAY_BARS,
    notional: float = DEFAULT_NOTIONAL,
    fee_rt: float = DEFAULT_FEE_RT,
) -> list[Trade]:
    raw: list[Signal] = []
    for sym, bars in daily.items():
        raw.extend(
            signals_for_sym(
                sym,
                bars,
                start,
                end,
                cum_pct=cum_pct,
                streak_min=streak_min,
                close_loc_min=close_loc_min,
                uw_pct=uw_pct,
                hold_days=hold_days,
            )
        )
    kept = apply_overlap(raw)
    kept = apply_entry_delay(kept, delay_bars=entry_delay_bars)
    return [signal_to_trade(s, notional=notional, fee_rt=fee_rt) for s in kept]


def scan_yesterday_for_entries(
    daily: dict[str, list[DayBar]],
    trade_date: str,
    *,
    cum_pct: float = DEFAULT_CUM_PCT,
    streak_min: int = DEFAULT_STREAK_MIN,
    close_loc_min: float = DEFAULT_CLOSE_LOC_MIN,
    uw_pct: float = DEFAULT_UW_PCT,
    hold_days: int = DEFAULT_HOLD_DAYS,
) -> list[Signal]:
    """Signals whose entry_date == trade_date (yesterday was peak)."""
    yest = _shift(trade_date, -1)
    # Allow weekend gaps: find peak on any bar whose next bar date == trade_date
    out: list[Signal] = []
    for sym, bars in daily.items():
        idx = {b.date: i for i, b in enumerate(bars)}
        if trade_date not in idx:
            continue
        ei = idx[trade_date]
        if ei < 1:
            continue
        # peak is previous bar in series (not calendar yest — crypto daily is continuous)
        pi = ei - 1
        peak = bars[pi]
        hit = detect_peak_signal(
            bars,
            pi,
            cum_pct=cum_pct,
            streak_min=streak_min,
            close_loc_min=close_loc_min,
            uw_pct=uw_pct,
        )
        if hit is None:
            continue
        streak, cum, cl, uw, start_px = hit
        exit_i = ei + (hold_days - 1)
        # exit_date may be unknown yet — estimate calendar + hold_days-1; refine later
        if exit_i < len(bars):
            exit_date = bars[exit_i].date
            exit_px = bars[exit_i].c
        else:
            exit_date = _shift(trade_date, hold_days - 1)
            exit_px = 0.0
        out.append(
            Signal(
                sym=sym,
                signal_date=peak.date,
                entry_date=trade_date,
                exit_date=exit_date,
                streak=streak,
                cum_pct=cum,
                close_loc=cl,
                uw_pct=uw,
                start_px=start_px,
                peak_px=peak.c,
                entry=bars[ei].o,
                exit_px=exit_px,
            )
        )
    _ = yest
    return out
