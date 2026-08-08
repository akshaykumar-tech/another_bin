#!/usr/bin/env python3
"""L15 · start30 · step10 pyramid SHORT engine (until-recovery / optional H45).

Canonical research rules:
  Initiate: first day L15 return crosses ≥ START_PCT (30)
  Entry:    next UTC day OPEN (one short leg per ladder rung already crossed)
  Pyramid:  +$NOTIONAL short each +STEP_PCT from anchor (L15-ago close)
  Exit:     daily close ≤ anchor; else optional max_hold calendar days
  Overlap:  one open book per symbol

No practical leg limit when max_legs ≥ 99.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Iterable

from orb30_engine import DayBar, pnl_usd

DEFAULT_LOOKBACK = 15
DEFAULT_START_PCT = 30.0
DEFAULT_STEP_PCT = 10.0
DEFAULT_MAX_LEGS = 99
DEFAULT_MAX_HOLD = 0  # 0 = until recovery (no max hold)
DEFAULT_MIN_LIFE = 90
DEFAULT_NOTIONAL = 6.0
DEFAULT_FEE_RT = 0.0008


@dataclass
class Signal:
    """Yesterday first-touch → enter today."""
    sym: str
    signal_date: str
    entry_date: str
    anchor: float
    signal_ret: float
    n_init_legs: int
    ladder: list[float]


@dataclass
class Leg:
    entry_px: float
    entry_date: str
    level_idx: int  # 0-based rung index


@dataclass
class Book:
    """Open short pyramid on one symbol."""
    sym: str
    signal_date: str
    entry_date: str
    anchor: float
    ladder: list[float]
    next_li: int
    legs: list[Leg] = field(default_factory=list)
    max_hold: int = 0

    @property
    def n_legs(self) -> int:
        return len(self.legs)

    def avg_entry(self) -> float:
        if not self.legs:
            return 0.0
        return sum(lg.entry_px for lg in self.legs) / len(self.legs)


@dataclass
class Trade:
    sym: str
    signal_date: str
    entry_date: str
    exit_date: str
    legs: int
    pnl: float
    reason: str
    hold_days: int


def _shift(d: str, n: int) -> str:
    return (datetime.strptime(d, "%Y-%m-%d").date() + timedelta(days=n)).isoformat()


def life_days(bars: list[DayBar], idx: int) -> int:
    if not bars or idx < 0 or idx >= len(bars):
        return 0
    d0 = datetime.strptime(bars[0].date, "%Y-%m-%d").date()
    d1 = datetime.strptime(bars[idx].date, "%Y-%m-%d").date()
    return (d1 - d0).days


def build_ladder(start: float, step: float, max_legs: int) -> list[float]:
    lv = [float(start)]
    while len(lv) < max_legs:
        lv.append(round(lv[-1] + step, 10))
    return lv


def first_touch_signal(
    bars: list[DayBar],
    i: int,
    *,
    lookback: int = DEFAULT_LOOKBACK,
    start_pct: float = DEFAULT_START_PCT,
    step_pct: float = DEFAULT_STEP_PCT,
    max_legs: int = DEFAULT_MAX_LEGS,
    min_life: int = DEFAULT_MIN_LIFE,
) -> Signal | None:
    """If bars[i] is first touch L{lookback} ret ≥ start_pct, return Signal for i+1 entry."""
    if i < lookback or i + 1 >= len(bars):
        return None
    c0, c1 = bars[i - lookback].c, bars[i].c
    if c0 <= 0 or c1 <= 0:
        return None
    ret = 100.0 * (c1 / c0 - 1.0)
    prev = None
    if i - 1 >= lookback and bars[i - 1 - lookback].c > 0 and bars[i - 1].c > 0:
        prev = 100.0 * (bars[i - 1].c / bars[i - 1 - lookback].c - 1.0)
    if not (ret >= start_pct and (prev is None or prev < start_pct)):
        return None
    fill_i = i + 1
    if bars[fill_i].o <= 0:
        return None
    if min_life > 0 and life_days(bars, fill_i) < min_life:
        return None
    lad = build_ladder(start_pct, step_pct, max_legs)
    n_init = 0
    for lv in lad:
        if ret >= lv:
            n_init += 1
        else:
            break
    n_init = min(n_init, max_legs)
    if n_init <= 0:
        return None
    return Signal(
        sym="",
        signal_date=bars[i].date,
        entry_date=bars[fill_i].date,
        anchor=bars[i - lookback].c,
        signal_ret=ret,
        n_init_legs=n_init,
        ladder=lad,
    )


def scan_yesterday_for_entries(
    daily: dict[str, list[DayBar]],
    trade_date: str,
    *,
    lookback: int = DEFAULT_LOOKBACK,
    start_pct: float = DEFAULT_START_PCT,
    step_pct: float = DEFAULT_STEP_PCT,
    max_legs: int = DEFAULT_MAX_LEGS,
    min_life: int = DEFAULT_MIN_LIFE,
) -> list[Signal]:
    """Signals whose entry_date == trade_date (signal was yesterday)."""
    out: list[Signal] = []
    for sym, bars in daily.items():
        by = {b.date: i for i, b in enumerate(bars)}
        # signal day = trade_date - 1 calendar (approx); search bar before trade_date
        if trade_date not in by:
            continue
        fill_i = by[trade_date]
        sig_i = fill_i - 1
        if sig_i < lookback:
            continue
        sig = first_touch_signal(
            bars,
            sig_i,
            lookback=lookback,
            start_pct=start_pct,
            step_pct=step_pct,
            max_legs=max_legs,
            min_life=min_life,
        )
        if sig is None:
            continue
        if sig.entry_date != trade_date:
            continue
        sig.sym = sym
        out.append(sig)
    out.sort(key=lambda s: (-s.signal_ret, s.sym))
    return out


def open_book_from_signal(sig: Signal, entry_px: float, max_hold: int = 0) -> Book:
    legs = [
        Leg(entry_px=entry_px, entry_date=sig.entry_date, level_idx=i)
        for i in range(sig.n_init_legs)
    ]
    return Book(
        sym=sig.sym,
        signal_date=sig.signal_date,
        entry_date=sig.entry_date,
        anchor=sig.anchor,
        ladder=list(sig.ladder),
        next_li=sig.n_init_legs,
        legs=legs,
        max_hold=max_hold,
    )


def rungs_crossed(ret_from_anchor: float, ladder: list[float], next_li: int, max_legs: int) -> int:
    add = 0
    while next_li + add < len(ladder) and next_li + add < max_legs:
        if ret_from_anchor >= ladder[next_li + add]:
            add += 1
        else:
            break
    return add


def should_exit_anchor(close_px: float, anchor: float) -> bool:
    return close_px > 0 and close_px <= anchor


def hold_days(entry_date: str, asof: str) -> int:
    a = datetime.strptime(entry_date, "%Y-%m-%d").date()
    b = datetime.strptime(asof, "%Y-%m-%d").date()
    return (b - a).days


def should_exit_max_hold(entry_date: str, asof: str, max_hold: int) -> bool:
    if max_hold <= 0:
        return False
    return hold_days(entry_date, asof) >= max_hold - 1


def book_pnl(book: Book, exit_px: float, notional: float, fee_rt: float) -> float:
    tot = 0.0
    for lg in book.legs:
        if lg.entry_px > 0 and exit_px > 0:
            tot += pnl_usd("short", lg.entry_px, exit_px, notional, fee_rt)
    return tot


def simulate_symbol(
    sym: str,
    bars: list[DayBar],
    win_s: str,
    win_e: str,
    *,
    lookback: int = DEFAULT_LOOKBACK,
    start_pct: float = DEFAULT_START_PCT,
    step_pct: float = DEFAULT_STEP_PCT,
    max_legs: int = DEFAULT_MAX_LEGS,
    max_hold: int = DEFAULT_MAX_HOLD,
    min_life: int = DEFAULT_MIN_LIFE,
    notional: float = DEFAULT_NOTIONAL,
    fee_rt: float = DEFAULT_FEE_RT,
) -> list[Trade]:
    """Full path-dependent backtest for one symbol."""
    trades: list[Trade] = []
    i = lookback
    while i < len(bars) - 1:
        sig = first_touch_signal(
            bars,
            i,
            lookback=lookback,
            start_pct=start_pct,
            step_pct=step_pct,
            max_legs=max_legs,
            min_life=min_life,
        )
        if sig is None:
            i += 1
            continue
        sig.sym = sym
        fill_i = i + 1
        if bars[fill_i].date < win_s or bars[fill_i].date > win_e:
            i += 1
            continue
        book = open_book_from_signal(sig, bars[fill_i].o, max_hold=max_hold)

        if max_hold > 0:
            last = min(len(bars) - 1, fill_i + max_hold - 1)
        else:
            last = len(bars) - 1
        while last > fill_i and bars[last].date > win_e:
            last -= 1

        exit_i = last
        reason = "window_end" if max_hold <= 0 else "maxhold"
        j = fill_i
        while j <= last:
            px = bars[j].c
            if px <= 0:
                j += 1
                continue
            ret = 100.0 * (px / book.anchor - 1.0)
            add = rungs_crossed(ret, book.ladder, book.next_li, max_legs)
            if add > 0:
                add_i = j + 1
                if add_i <= last and bars[add_i].o > 0:
                    for k in range(add):
                        if book.n_legs >= max_legs:
                            break
                        book.legs.append(
                            Leg(
                                entry_px=bars[add_i].o,
                                entry_date=bars[add_i].date,
                                level_idx=book.next_li + k,
                            )
                        )
                book.next_li += add
            if should_exit_anchor(px, book.anchor):
                exit_i = j
                reason = "anchor"
                break
            if max_hold > 0 and (j - fill_i + 1) >= max_hold:
                exit_i = j
                reason = "maxhold"
                break
            j += 1

        exit_px = bars[exit_i].c
        if exit_px <= 0:
            i = exit_i + 1
            continue
        pnl = book_pnl(book, exit_px, notional, fee_rt)
        trades.append(
            Trade(
                sym=sym,
                signal_date=sig.signal_date,
                entry_date=book.entry_date,
                exit_date=bars[exit_i].date,
                legs=book.n_legs,
                pnl=pnl,
                reason=reason,
                hold_days=hold_days(book.entry_date, bars[exit_i].date),
            )
        )
        i = exit_i + 1
    return trades


def backtest_universe(
    daily: dict[str, list[DayBar]],
    win_s: str,
    win_e: str,
    **kwargs,
) -> list[Trade]:
    out: list[Trade] = []
    for sym, bars in daily.items():
        if not bars:
            continue
        out.extend(simulate_symbol(sym, bars, win_s, win_e, **kwargs))
    out.sort(key=lambda t: (t.entry_date, t.sym))
    return out
