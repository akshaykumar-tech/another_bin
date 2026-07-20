#!/usr/bin/env python3
"""PB9 combo engine — A-priority CONT after big-move pullback.

Final strategy (matches research backtest Jun1–Jul18 2026):
  Leg A (priority): signal day |c2c| > thr_a (default 50%)
    → trade D+1 CONT after open ±pb% pullback
    → entry ONLY in first 4h (48 × 5m bars from 00:00 UTC)
  Leg B (fill): signal day 2-day cum |move| > thr_b (default 50%)
    → trade D+1 CONT after open ±pb% pullback (full day)
    → only if (symbol, trade_day) has NO A fill

CONT: signal UP → LONG on −pb% from open; DOWN → SHORT on +pb% from open.
Fill: trade-through only. Price = exact pullback level (LIMIT).
Exit: EOD (last 5m close of trade day). One entry per (symbol, trade_day).
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from backtest_big_mover_days import fetch_5m, load_daily
from orb30_engine import (
    BAR_MS,
    DayBar,
    bars_5m_day,
    day_ms,
    fetch_daily_range,
    list_syms,
    pnl_usd,
    through,
)

FAPI_DEFAULT = "https://fapi.binance.com"
FIRST4H_BARS = 48  # 4h * 12 bars/hour @ 5m
DEFAULT_THR = 50.0
DEFAULT_PB = 9.0


@dataclass
class WatchItem:
    sym: str
    trade_date: str
    signal_date: str
    leg: str  # A_1D | B_CUM2D
    direction: str  # up | down
    side: str  # long | short
    move_pct: float
    open_px: float
    level: float
    pb_pct: float
    first4h_only: bool


@dataclass
class ComboTrade:
    sym: str
    signal_date: str
    trade_date: str
    leg: str
    direction: str
    side: str
    move_pct: float
    entry: float
    exit: float
    entry_bar_i: int
    pnl: float


@dataclass
class SymDaySetup:
    """Both legs can qualify same day; live prefers A until first4h ends."""

    a: WatchItem | None = None
    b: WatchItem | None = None


def _shift(d: str, n: int) -> str:
    return (datetime.strptime(d, "%Y-%m-%d").date() + timedelta(days=n)).isoformat()


def cont_side(direction: str) -> str:
    return "long" if direction == "up" else "short"


def pullback_level(open_px: float, direction: str, pb_pct: float) -> float:
    if open_px <= 0:
        return 0.0
    if direction == "up":
        return open_px * (1.0 - pb_pct / 100.0)
    return open_px * (1.0 + pb_pct / 100.0)


def find_pullback_fill(
    bars: list,
    direction: str,
    pb_pct: float,
    *,
    first4h_only: bool = False,
) -> tuple[str, int, float] | None:
    """Return (side, bar_index, fill_price) on first trade-through of pb level."""
    if not bars or bars[0].o <= 0:
        return None
    open_px = bars[0].o
    level = pullback_level(open_px, direction, pb_pct)
    side = cont_side(direction)
    hi = FIRST4H_BARS if first4h_only else len(bars)
    for i, b in enumerate(bars):
        if i >= hi:
            break
        if direction == "up" and (through(b, level) or b.l <= level):
            return side, i, level
        if direction == "down" and (through(b, level) or b.h >= level):
            return side, i, level
    return None


def c2c_move(prev: DayBar, cur: DayBar) -> float | None:
    if prev.c <= 0:
        return None
    return (cur.c - prev.c) / prev.c * 100.0


def _make_watch(
    sym: str,
    trade_date: str,
    signal_date: str,
    leg: str,
    direction: str,
    move_pct: float,
    pb_pct: float,
    first4h_only: bool,
) -> WatchItem:
    return WatchItem(
        sym=sym,
        trade_date=trade_date,
        signal_date=signal_date,
        leg=leg,
        direction=direction,
        side=cont_side(direction),
        move_pct=move_pct,
        open_px=0.0,
        level=0.0,
        pb_pct=pb_pct,
        first4h_only=first4h_only,
    )


def build_setups_for_trade_day(
    trade_date: str,
    daily_by_sym: dict[str, list[DayBar]],
    *,
    thr_a: float = DEFAULT_THR,
    thr_b: float = DEFAULT_THR,
    pb_pct: float = DEFAULT_PB,
) -> dict[str, SymDaySetup]:
    """Build A/B setups for live trade_date (signal = prior UTC day)."""
    sig_date = _shift(trade_date, -1)
    sig_date_2 = _shift(trade_date, -2)
    out: dict[str, SymDaySetup] = {}

    for sym, bars in daily_by_sym.items():
        by = {b.date: (i, b) for i, b in enumerate(bars)}
        if sig_date not in by:
            continue
        i1, d1 = by[sig_date]
        setup = SymDaySetup()

        if i1 >= 1:
            m = c2c_move(bars[i1 - 1], d1)
            if m is not None and abs(m) > thr_a:
                setup.a = _make_watch(
                    sym,
                    trade_date,
                    sig_date,
                    "A_1D",
                    "up" if m > 0 else "down",
                    m,
                    pb_pct,
                    True,
                )

        if sig_date_2 in by and i1 >= 2:
            d0 = by[sig_date_2][1]
            if d0.c > 0:
                m2 = (d1.c - d0.c) / d0.c * 100.0
                if abs(m2) > thr_b:
                    setup.b = _make_watch(
                        sym,
                        trade_date,
                        sig_date,
                        "B_CUM2D",
                        "up" if m2 > 0 else "down",
                        m2,
                        pb_pct,
                        False,
                    )

        if setup.a or setup.b:
            out[sym] = setup
    return out


def build_watch_for_trade_day(
    trade_date: str,
    daily_by_sym: dict[str, list[DayBar]],
    *,
    thr_a: float = DEFAULT_THR,
    thr_b: float = DEFAULT_THR,
    pb_pct: float = DEFAULT_PB,
    after_first4h: bool = False,
) -> list[WatchItem]:
    """Active watches for live.

    Before first4h end: A if present else B.
    After first4h: B only when A did not fill (caller filters filled); drop A.
    """
    setups = build_setups_for_trade_day(
        trade_date, daily_by_sym, thr_a=thr_a, thr_b=thr_b, pb_pct=pb_pct
    )
    out: list[WatchItem] = []
    for setup in setups.values():
        if after_first4h:
            if setup.b:
                out.append(setup.b)
        else:
            if setup.a:
                out.append(setup.a)
            elif setup.b:
                out.append(setup.b)
    out.sort(key=lambda w: (-abs(w.move_pct), w.sym))
    return out


def attach_open_levels(watch: list[WatchItem], bars: list) -> list[WatchItem]:
    if not bars or bars[0].o <= 0:
        return []
    o = bars[0].o
    fixed: list[WatchItem] = []
    for w in watch:
        lvl = pullback_level(o, w.direction, w.pb_pct)
        fixed.append(
            WatchItem(
                sym=w.sym,
                trade_date=w.trade_date,
                signal_date=w.signal_date,
                leg=w.leg,
                direction=w.direction,
                side=w.side,
                move_pct=w.move_pct,
                open_px=o,
                level=lvl,
                pb_pct=w.pb_pct,
                first4h_only=w.first4h_only,
            )
        )
    return fixed


def bar_index_in_day(bar_ts_ms: int, trade_date: str) -> int:
    start = day_ms(trade_date)
    return int((bar_ts_ms - start) // BAR_MS)


def within_entry_window(bar_i: int, first4h_only: bool) -> bool:
    if first4h_only:
        return 0 <= bar_i < FIRST4H_BARS
    return bar_i >= 0


def try_fill_on_bar(w: WatchItem, b, trade_date: str) -> bool:
    """True if this 5m bar trade-throughs the pullback level inside the leg window."""
    bi = bar_index_in_day(b.ts, trade_date)
    if not within_entry_window(bi, w.first4h_only):
        return False
    if w.direction == "up" and (through(b, w.level) or b.l <= w.level):
        return True
    if w.direction == "down" and (through(b, w.level) or b.h >= w.level):
        return True
    return False


def backtest_range(
    start: str,
    end: str,
    *,
    thr_a: float = DEFAULT_THR,
    thr_b: float = DEFAULT_THR,
    pb_pct: float = DEFAULT_PB,
    notional: float = 6.0,
    fee_rt: float = 0.0008,
    fapi: str = FAPI_DEFAULT,
) -> list[ComboTrade]:
    """Historical replay matching research: signal days in [start,end], trade D+1.

    Combine: keep all A fills; add B fills only when (sym, trade_day) has no A fill.
    """
    pad_end = _shift(end, 3)
    syms = list_syms(fapi)
    daily: dict[str, list[DayBar]] = {}

    def one_d(sym: str):
        try:
            return sym, load_daily(sym, "2025-05-01", pad_end)
        except Exception:
            return sym, []

    with ThreadPoolExecutor(max_workers=12) as ex:
        for fut in as_completed([ex.submit(one_d, s) for s in syms]):
            sym, bars = fut.result()
            if bars:
                daily[sym] = bars

    sig_1d: list[tuple[str, str, float, str]] = []
    sig_2d: list[tuple[str, str, float, str]] = []
    for sym, bars in daily.items():
        for i, cur in enumerate(bars):
            if cur.date < start or cur.date > end:
                continue
            if i >= 1:
                prev = bars[i - 1]
                if prev.c > 0:
                    m = (cur.c - prev.c) / prev.c * 100.0
                    if abs(m) > thr_a:
                        sig_1d.append((sym, cur.date, m, "up" if m > 0 else "down"))
            if i >= 2:
                p2 = bars[i - 2]
                if p2.c > 0:
                    m2 = (cur.c - p2.c) / p2.c * 100.0
                    if abs(m2) > thr_b:
                        sig_2d.append((sym, cur.date, m2, "up" if m2 > 0 else "down"))

    need = {(sym, _shift(d, 1)) for sym, d, _, _ in sig_1d + sig_2d}
    bars5: dict[tuple[str, str], list] = {}

    def one_5(item: tuple[str, str]):
        try:
            return item, fetch_5m(*item)
        except Exception:
            return item, []

    with ThreadPoolExecutor(max_workers=12) as ex:
        for fut in as_completed([ex.submit(one_5, k) for k in need]):
            k, b = fut.result()
            bars5[k] = b

    def run_leg(
        signals: list[tuple[str, str, float, str]],
        *,
        first4h_only: bool,
        leg: str,
    ) -> list[ComboTrade]:
        trades: list[ComboTrade] = []
        for sym, d, m, direction in signals:
            td = _shift(d, 1)
            bars = bars5.get((sym, td)) or []
            if len(bars) < 3:
                continue
            hit = find_pullback_fill(
                bars, direction, pb_pct, first4h_only=first4h_only
            )
            if not hit:
                continue
            side, ei, entry = hit
            exit_px = bars[-1].c
            trades.append(
                ComboTrade(
                    sym=sym,
                    signal_date=d,
                    trade_date=td,
                    leg=leg,
                    direction=direction,
                    side=side,
                    move_pct=m,
                    entry=entry,
                    exit=exit_px,
                    entry_bar_i=ei,
                    pnl=pnl_usd(side, entry, exit_px, notional, fee_rt),
                )
            )
        return trades

    leg_a = run_leg(sig_1d, first4h_only=True, leg="A_1D")
    leg_b = run_leg(sig_2d, first4h_only=False, leg="B_CUM2D")

    chosen: dict[tuple[str, str], ComboTrade] = {}
    for t in leg_a:
        chosen[(t.sym, t.trade_date)] = t
    for t in leg_b:
        key = (t.sym, t.trade_date)
        if key not in chosen:
            chosen[key] = t

    out = list(chosen.values())
    out.sort(key=lambda t: (t.trade_date, t.sym))
    return out


def first4h_end_ms(trade_date: str) -> int:
    return day_ms(trade_date) + FIRST4H_BARS * BAR_MS


def load_daily_live(
    trade_date: str,
    lookback_days: int = 10,
    fapi: str = FAPI_DEFAULT,
) -> dict[str, list[DayBar]]:
    """Fetch recent dailies for live watch build (uncached path OK)."""
    start = _shift(trade_date, -lookback_days)
    out: dict[str, list[DayBar]] = {}
    for sym in list_syms(fapi):
        try:
            bars = fetch_daily_range(sym, start, trade_date, fapi)
        except Exception:
            continue
        if bars:
            out[sym] = bars
    return out
