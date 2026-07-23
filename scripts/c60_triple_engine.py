#!/usr/bin/env python3
"""C60 triple engine — cum60 pb9 + >40% pb9 + down15 delayed (EARLY combine).

Research mix (May–Jul18 2026, $6, earliest fill wins 1/(sym,day)):
  L1: cum2d |move| > 60% → D+1 CONT @ open ±9% pullback (full day)
  L2: 1d |c2c| > 40% → D+1 CONT @ open ±9% pullback (full day)
  L3: 1d c2c < −15% and |move| < 40% → D+1 SHORT @ open+15m (3×5m bars)

CONT: UP→LONG @ −pb%; DOWN→SHORT @ +pb%.
Fill: trade-through (pb) or delayed open (down15). Exit EOD.
Target: skipD40 + delay15m DOWN ≈ n643 / +$131 / WR61% / G57/79.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from backtest_big_mover_days import fetch_5m, load_daily
from orb30_engine import (
    BAR_MS,
    DayBar,
    day_ms,
    fetch_daily_range,
    list_syms,
    pnl_usd,
    through,
)

FAPI_DEFAULT = "https://fapi.binance.com"
DEFAULT_CUM_THR = 60.0
DEFAULT_L40_THR = 40.0
DEFAULT_DOWN_THR = 15.0
DEFAULT_DOWN_SKIP_ABS = 40.0  # skip DOWN15 if |c2c| >= this (0 = off)
DEFAULT_DOWN_DELAY_BARS = 3  # 3 × 5m = 15m after UTC open
DEFAULT_PB = 9.0


@dataclass
class WatchItem:
    sym: str
    trade_date: str
    signal_date: str
    leg: str  # CUM60 | L40_PB9 | DOWN15_OPEN
    direction: str  # up | down
    side: str  # long | short
    move_pct: float
    open_px: float
    level: float
    pb_pct: float
    open_entry: bool  # True = market-style entry (down15), possibly delayed
    entry_delay_bars: int = 0  # 5m bars after open before DOWN fill


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
    candidates: list[WatchItem] = field(default_factory=list)


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


def c2c_move(prev: DayBar, cur: DayBar) -> float | None:
    if prev.c <= 0:
        return None
    return (cur.c - prev.c) / prev.c * 100.0


def _make(
    sym: str,
    trade_date: str,
    signal_date: str,
    leg: str,
    direction: str,
    move_pct: float,
    pb_pct: float,
    open_entry: bool,
    entry_delay_bars: int = 0,
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
        open_entry=open_entry,
        entry_delay_bars=entry_delay_bars,
    )


def build_setups_for_trade_day(
    trade_date: str,
    daily_by_sym: dict[str, list[DayBar]],
    *,
    cum_thr: float = DEFAULT_CUM_THR,
    l40_thr: float = DEFAULT_L40_THR,
    down_thr: float = DEFAULT_DOWN_THR,
    down_skip_abs: float = DEFAULT_DOWN_SKIP_ABS,
    down_delay_bars: int = DEFAULT_DOWN_DELAY_BARS,
    pb_pct: float = DEFAULT_PB,
) -> dict[str, SymDaySetup]:
    """Signal = prior UTC day. All three legs can qualify same symbol."""
    sig = _shift(trade_date, -1)
    # 2-day cum: close[signal-2] → close[signal]  (matches bars[i-2]→bars[i])
    sig_cum_base = _shift(trade_date, -3)
    out: dict[str, SymDaySetup] = {}

    for sym, bars in daily_by_sym.items():
        by = {b.date: (i, b) for i, b in enumerate(bars)}
        if sig not in by:
            continue
        i1, d1 = by[sig]
        cands: list[WatchItem] = []

        # L1 cum2d > cum_thr
        if sig_cum_base in by and i1 >= 2:
            d0 = by[sig_cum_base][1]
            if d0.c > 0:
                m2 = (d1.c - d0.c) / d0.c * 100.0
                if abs(m2) > cum_thr:
                    direction = "up" if m2 > 0 else "down"
                    cands.append(
                        _make(
                            sym, trade_date, sig, "CUM60", direction, m2, pb_pct, False
                        )
                    )

        # L2 1d |c2c| > l40_thr
        if i1 >= 1:
            m = c2c_move(bars[i1 - 1], d1)
            if m is not None and abs(m) > l40_thr:
                direction = "up" if m > 0 else "down"
                cands.append(
                    _make(sym, trade_date, sig, "L40_PB9", direction, m, pb_pct, False)
                )

        # L3 down_only: c2c < -down_thr, skip |m| >= down_skip_abs → short @ open+delay
        if i1 >= 1:
            m = c2c_move(bars[i1 - 1], d1)
            if m is not None and m < -down_thr:
                if down_skip_abs <= 0 or abs(m) < down_skip_abs:
                    cands.append(
                        _make(
                            sym,
                            trade_date,
                            sig,
                            "DOWN15_OPEN",
                            "down",
                            m,
                            0.0,
                            True,
                            entry_delay_bars=max(0, int(down_delay_bars)),
                        )
                    )

        if cands:
            out[sym] = SymDaySetup(candidates=cands)
    return out


def attach_open_levels(watch: list[WatchItem], bars: list) -> list[WatchItem]:
    if not bars or bars[0].o <= 0:
        return []
    o = bars[0].o
    fixed: list[WatchItem] = []
    for w in watch:
        if w.open_entry:
            # Provisional level = day open; delayed fills rewrite to delay-bar open.
            lvl = o
        else:
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
                open_entry=w.open_entry,
                entry_delay_bars=w.entry_delay_bars,
            )
        )
    return fixed


def bar_index_in_day(bar_ts_ms: int, trade_date: str) -> int:
    return int((bar_ts_ms - day_ms(trade_date)) // BAR_MS)


def try_fill_on_bar(w: WatchItem, b, trade_date: str) -> bool:
    bi = bar_index_in_day(b.ts, trade_date)
    if bi < 0:
        return False
    if w.open_entry:
        # Delayed open: eligible from entry_delay_bars onward (catch-up OK).
        return bi >= max(0, int(w.entry_delay_bars))
    if w.direction == "up" and (through(b, w.level) or b.l <= w.level):
        return True
    if w.direction == "down" and (through(b, w.level) or b.h >= w.level):
        return True
    return False


def find_fill(w: WatchItem, bars: list) -> tuple[int, float] | None:
    """Return (bar_i, entry_px) for this watch on the day.

    DOWN open-entry still *wins EARLY combine* as if at bar 0 (beats pullbacks),
    but the executable fill is at entry_delay_bars open — matching research
    skipD40+delay15 (post-select delay on the DOWN leg).
    """
    if not bars or bars[0].o <= 0:
        return None
    if w.open_entry:
        delay = max(0, int(w.entry_delay_bars))
        if delay >= len(bars):
            return None
        # Selection clock = 0 so DOWN still beats CONT pb; exec px = delay open.
        return 0, bars[delay].o
    for i, b in enumerate(bars):
        if w.direction == "up" and (through(b, w.level) or b.l <= w.level):
            return i, w.level
        if w.direction == "down" and (through(b, w.level) or b.h >= w.level):
            return i, w.level
    return None


def resolve_entry(w: WatchItem, bars: list, ei: int, entry: float) -> tuple[int, float] | None:
    """Map pick_early hit to executable (bar_i, px), applying DOWN delay."""
    if not w.open_entry:
        return ei, entry
    delay = max(0, int(w.entry_delay_bars))
    if delay >= len(bars):
        return None
    px = bars[delay].o
    if px <= 0:
        return None
    return delay, px


def pick_early(cands: list[WatchItem], bars: list) -> tuple[WatchItem, int, float] | None:
    """Among filled candidates, earliest ei wins; tie → larger |move|."""
    if not bars:
        return None
    attached = attach_open_levels(cands, bars)
    hits: list[tuple[WatchItem, int, float]] = []
    for w in attached:
        hit = find_fill(w, bars)
        if hit:
            hits.append((w, hit[0], hit[1]))
    if not hits:
        return None
    hits.sort(key=lambda x: (x[1], -abs(x[0].move_pct)))
    w, _sel_ei, _sel_px = hits[0]
    resolved = resolve_entry(w, bars, hits[0][1], hits[0][2])
    if not resolved:
        return None
    ei, entry = resolved
    # Rewrite watch level to executable entry for live/dry fills.
    w = WatchItem(
        sym=w.sym,
        trade_date=w.trade_date,
        signal_date=w.signal_date,
        leg=w.leg,
        direction=w.direction,
        side=w.side,
        move_pct=w.move_pct,
        open_px=w.open_px,
        level=entry,
        pb_pct=w.pb_pct,
        open_entry=w.open_entry,
        entry_delay_bars=w.entry_delay_bars,
    )
    return w, ei, entry


def prefer_live_watch(cands: list[WatchItem], bars: list) -> WatchItem | None:
    """Active watch for live: open_entry preferred; else first pb candidate after attach."""
    if not cands or not bars:
        return None
    attached = attach_open_levels(cands, bars)
    opens = [w for w in attached if w.open_entry]
    if opens:
        # highest |move| down15 if multiple (shouldn't happen)
        opens.sort(key=lambda w: abs(w.move_pct), reverse=True)
        w = opens[0]
        delay = max(0, int(w.entry_delay_bars))
        # At early scan, delay bar may not exist yet — keep day-open level;
        # executable px is resolved at fill time via delayed_entry_px.
        if delay < len(bars) and bars[delay].o > 0:
            entry = bars[delay].o
        else:
            entry = w.level
        return WatchItem(
            sym=w.sym,
            trade_date=w.trade_date,
            signal_date=w.signal_date,
            leg=w.leg,
            direction=w.direction,
            side=w.side,
            move_pct=w.move_pct,
            open_px=w.open_px,
            level=entry,
            pb_pct=w.pb_pct,
            open_entry=w.open_entry,
            entry_delay_bars=w.entry_delay_bars,
        )
    # Prefer single pb watch — if multiple same side/level, pick highest |move|
    attached.sort(key=lambda w: abs(w.move_pct), reverse=True)
    return attached[0] if attached else None


def delayed_entry_px(w: WatchItem, bars: list) -> float | None:
    """Executable entry for open_entry (delay-bar open) or pb level."""
    if not bars:
        return None
    if not w.open_entry:
        return w.level if w.level > 0 else None
    delay = max(0, int(w.entry_delay_bars))
    if delay >= len(bars):
        return None
    px = bars[delay].o
    return px if px > 0 else None


def backtest_range(
    start: str,
    end: str,
    *,
    cum_thr: float = DEFAULT_CUM_THR,
    l40_thr: float = DEFAULT_L40_THR,
    down_thr: float = DEFAULT_DOWN_THR,
    down_skip_abs: float = DEFAULT_DOWN_SKIP_ABS,
    down_delay_bars: int = DEFAULT_DOWN_DELAY_BARS,
    pb_pct: float = DEFAULT_PB,
    notional: float = 6.0,
    fee_rt: float = 0.0008,
    fapi: str = FAPI_DEFAULT,
) -> list[ComboTrade]:
    """Signal days in [start,end], trade D+1, EARLY combine."""
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

    # Collect signals on signal dates
    need: set[tuple[str, str]] = set()
    # iterate trade dates via signal+1
    d0 = datetime.strptime(start, "%Y-%m-%d").date()
    d1 = datetime.strptime(end, "%Y-%m-%d").date()
    sig_dates: list[str] = []
    cur = d0
    while cur <= d1:
        sig_dates.append(cur.isoformat())
        cur += timedelta(days=1)

    setup_kw = dict(
        cum_thr=cum_thr,
        l40_thr=l40_thr,
        down_thr=down_thr,
        down_skip_abs=down_skip_abs,
        down_delay_bars=down_delay_bars,
        pb_pct=pb_pct,
    )

    for sig in sig_dates:
        td = _shift(sig, 1)
        setups = build_setups_for_trade_day(td, daily, **setup_kw)
        for sym in setups:
            need.add((sym, td))

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

    min_bars = max(3, int(down_delay_bars) + 1)
    trades: list[ComboTrade] = []
    for sig in sig_dates:
        td = _shift(sig, 1)
        setups = build_setups_for_trade_day(td, daily, **setup_kw)
        for sym, setup in setups.items():
            bars = bars5.get((sym, td)) or []
            if len(bars) < min_bars:
                continue
            picked = pick_early(setup.candidates, bars)
            if not picked:
                continue
            w, ei, entry = picked
            exit_px = bars[-1].c
            trades.append(
                ComboTrade(
                    sym=w.sym,
                    signal_date=w.signal_date,
                    trade_date=td,
                    leg=w.leg,
                    direction=w.direction,
                    side=w.side,
                    move_pct=w.move_pct,
                    entry=entry,
                    exit=exit_px,
                    entry_bar_i=ei,
                    pnl=pnl_usd(w.side, entry, exit_px, notional, fee_rt),
                )
            )
    trades.sort(key=lambda t: (t.trade_date, t.sym))
    return trades
