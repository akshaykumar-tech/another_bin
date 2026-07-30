#!/usr/bin/env python3
"""Quiet3 ladder TP30 engine (replaces WinBE hedge).

Filter (trade day D):
  - Prev day |c2c| ≥ PREV_THR (default 30%)
  - Quiet3: each of the 3 days before prev has |c2c| ≤ QUIET_MAX (default 10%)

Trade:
  - same-side continuation (prev up → long, prev down → short)
  - ladder levels from day open: 8 / 10 / 12 / 15 %
  - next-bar fill after level touch
  - TP 30% from entry, no hard SL, EOD flatten
  - max MAX_PER_LEVEL concurrent opens per level; re-arm after price leaves level
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

from orb30_engine import DayBar, pnl_usd

DEFAULT_PREV_THR = 30.0
DEFAULT_QUIET_MAX = 10.0
DEFAULT_LEVELS = (8.0, 10.0, 12.0, 15.0)
DEFAULT_TP_PCT = 30.0
DEFAULT_MAX_PER_LEVEL = 3
DEFAULT_NOTIONAL = 6.0
DEFAULT_FEE_RT = 0.0008
STRATEGY_NAME = "LADDER_TP30"


def _shift(d: str, n: int) -> str:
    return (datetime.strptime(d, "%Y-%m-%d").date() + timedelta(days=n)).isoformat()


@dataclass
class Signal:
    sym: str
    signal_date: str  # prev day (big move)
    trade_date: str
    move_pct: float  # prev c2c %
    side: str  # long | short
    quiet_d1: float = 0.0
    quiet_d2: float = 0.0
    quiet_d3: float = 0.0


@dataclass
class LadderLeg:
    """One filled ladder rung."""

    id: str  # unique leg id
    sym: str
    side: str
    level: float
    signal_date: str
    trade_date: str
    move_pct: float
    day_open: float
    entry: float
    tp_px: float
    qty: float = 0.0  # live qty; dry uses notional PnL
    open: bool = True
    exit_px: float = 0.0
    reason: str = ""
    pnl: float = 0.0
    fill_bar: int = -1

    def key(self) -> str:
        return self.id


@dataclass
class SymbolDayState:
    """Per-symbol arming / pending / open-legs for one UTC day."""

    sym: str
    side: str
    signal_date: str
    trade_date: str
    move_pct: float
    day_open: float
    levels: tuple[float, ...] = DEFAULT_LEVELS
    max_per: int = DEFAULT_MAX_PER_LEVEL
    tp_pct: float = DEFAULT_TP_PCT
    armed: dict[float, bool] = field(default_factory=dict)
    pending: dict[float, int | None] = field(default_factory=dict)  # level -> touch bar i
    open_legs: list[LadderLeg] = field(default_factory=list)
    leg_seq: int = 0

    def __post_init__(self) -> None:
        if not self.armed:
            self.armed = {lv: True for lv in self.levels}
        if not self.pending:
            self.pending = {lv: None for lv in self.levels}

    def level_px(self, lv: float) -> float:
        if self.side == "long":
            return self.day_open * (1.0 + lv / 100.0)
        return self.day_open * (1.0 - lv / 100.0)

    def open_count(self, lv: float) -> int:
        return sum(1 for x in self.open_legs if x.open and x.level == lv)

    def next_leg_id(self, lv: float) -> str:
        self.leg_seq += 1
        return f"{self.sym}:{self.trade_date}:L{lv:g}:{self.leg_seq}"


def scan_signals_for_trade_day(
    trade_date: str,
    daily_by_sym: dict[str, list[DayBar]],
    *,
    prev_thr: float = DEFAULT_PREV_THR,
    quiet_max: float = DEFAULT_QUIET_MAX,
) -> list[Signal]:
    """quiet3 + |prev c2c| ≥ prev_thr → same-side signal for trade_date."""
    out: list[Signal] = []
    for sym, bars in daily_by_sym.items():
        by = {b.date: i for i, b in enumerate(bars)}
        if trade_date not in by:
            continue
        i = by[trade_date]
        # need indices i-1 (prev), i-2,i-3,i-4,i-5 for quiet3 on days before prev
        if i < 5:
            continue
        # closes for quiet check: days before prev = bars[i-2], i-3, i-4 vs prior
        # quiet d1 = (c[i-2]-c[i-3])/c[i-3]  (day before prev)
        # quiet d2 = (c[i-3]-c[i-4])/c[i-4]
        # quiet d3 = (c[i-4]-c[i-5])/c[i-5]
        cs = [bars[j].c for j in range(i - 5, i)]  # [i-5 .. i-1]
        if min(cs) <= 0:
            continue
        prev_c, prev2_c = bars[i - 1].c, bars[i - 2].c
        move = (prev_c / prev2_c - 1.0) * 100.0
        if abs(move) < prev_thr:
            continue
        d1 = (bars[i - 2].c / bars[i - 3].c - 1.0) * 100.0
        d2 = (bars[i - 3].c / bars[i - 4].c - 1.0) * 100.0
        d3 = (bars[i - 4].c / bars[i - 5].c - 1.0) * 100.0
        if abs(d1) > quiet_max or abs(d2) > quiet_max or abs(d3) > quiet_max:
            continue
        side = "long" if move > 0 else "short"
        out.append(
            Signal(
                sym=sym,
                signal_date=bars[i - 1].date,
                trade_date=trade_date,
                move_pct=move,
                side=side,
                quiet_d1=d1,
                quiet_d2=d2,
                quiet_d3=d3,
            )
        )
    out.sort(key=lambda s: (-abs(s.move_pct), s.sym))
    return out


def make_day_state(
    sig: Signal,
    day_open: float,
    *,
    levels: tuple[float, ...] = DEFAULT_LEVELS,
    max_per: int = DEFAULT_MAX_PER_LEVEL,
    tp_pct: float = DEFAULT_TP_PCT,
) -> SymbolDayState:
    return SymbolDayState(
        sym=sig.sym,
        side=sig.side,
        signal_date=sig.signal_date,
        trade_date=sig.trade_date,
        move_pct=sig.move_pct,
        day_open=day_open,
        levels=tuple(float(x) for x in levels),
        max_per=int(max_per),
        tp_pct=float(tp_pct),
    )


def _tp_px(side: str, entry: float, tp_pct: float) -> float:
    if side == "long":
        return entry * (1.0 + tp_pct / 100.0)
    return entry * (1.0 - tp_pct / 100.0)


def apply_bar_dry(
    st: SymbolDayState,
    bar_i: int,
    o: float,
    h: float,
    l: float,
    *,
    notional: float = DEFAULT_NOTIONAL,
    fee_rt: float = DEFAULT_FEE_RT,
) -> list[LadderLeg]:
    """Process one 5m bar (dry / research fills). Returns newly closed legs."""
    closed: list[LadderLeg] = []

    # 1) manage open legs — TP (no SL)
    for leg in st.open_legs:
        if not leg.open:
            continue
        hit_tp = (leg.side == "long" and h >= leg.tp_px) or (
            leg.side == "short" and l <= leg.tp_px
        )
        if hit_tp:
            leg.exit_px = leg.tp_px
            leg.reason = "TP"
            leg.open = False
            leg.pnl = pnl_usd(leg.side, leg.entry, leg.exit_px, notional, fee_rt)
            closed.append(leg)

    # 2) fill pendings on next bar
    for lv in st.levels:
        pi = st.pending.get(lv)
        if pi is None:
            continue
        if pi + 1 != bar_i:
            if pi + 1 < bar_i:
                st.pending[lv] = None
            continue
        st.pending[lv] = None
        if st.open_count(lv) >= st.max_per:
            continue
        if o <= 0:
            continue
        # next-bar open fill (no adverse slip in live bot layer; research used slip in BT)
        entry = o
        # same-bar TP check
        tp = _tp_px(st.side, entry, st.tp_pct)
        hit_tp = (st.side == "long" and h >= tp) or (st.side == "short" and l <= tp)
        leg = LadderLeg(
            id=st.next_leg_id(lv),
            sym=st.sym,
            side=st.side,
            level=lv,
            signal_date=st.signal_date,
            trade_date=st.trade_date,
            move_pct=st.move_pct,
            day_open=st.day_open,
            entry=entry,
            tp_px=tp,
            fill_bar=bar_i,
        )
        if hit_tp:
            leg.exit_px = tp
            leg.reason = "TP"
            leg.open = False
            leg.pnl = pnl_usd(leg.side, leg.entry, leg.exit_px, notional, fee_rt)
            st.open_legs.append(leg)
            closed.append(leg)
        else:
            st.open_legs.append(leg)

    # 3) arm / trigger touches
    for lv in st.levels:
        px = st.level_px(lv)
        cnt = st.open_count(lv)
        if st.pending.get(lv) is not None:
            continue
        if st.armed.get(lv, True) and cnt < st.max_per:
            hit = (h >= px) if st.side == "long" else (l <= px)
            if hit:
                st.pending[lv] = bar_i
                st.armed[lv] = False
        # re-arm after leave if under max
        if not st.armed.get(lv, True):
            left = (l < px) if st.side == "long" else (h > px)
            if left and cnt < st.max_per and st.pending.get(lv) is None:
                st.armed[lv] = True

    return closed


def close_eod_legs(
    st: SymbolDayState,
    exit_px: float,
    *,
    notional: float = DEFAULT_NOTIONAL,
    fee_rt: float = DEFAULT_FEE_RT,
    reason: str = "EOD",
) -> list[LadderLeg]:
    closed: list[LadderLeg] = []
    for leg in st.open_legs:
        if not leg.open:
            continue
        leg.exit_px = exit_px
        leg.reason = reason
        leg.open = False
        leg.pnl = pnl_usd(leg.side, leg.entry, leg.exit_px, notional, fee_rt)
        closed.append(leg)
    return closed


def active_open_legs(st: SymbolDayState) -> list[LadderLeg]:
    return [x for x in st.open_legs if x.open]
