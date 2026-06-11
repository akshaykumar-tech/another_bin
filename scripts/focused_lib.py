"""Shared helpers: 6% event detection and sig_open_break adaptive direction."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Literal, Optional

Direction = Literal["high", "low"]


@dataclass
class Bar:
    sec: int
    o: float
    h: float
    l: float
    c: float
    vol: float

    @property
    def notional(self) -> float:
        return self.vol * self.c

    def amp_pct(self) -> float:
        if self.o <= 0:
            return 0.0
        up = (self.h - self.o) / self.o * 100
        dn = (self.o - self.l) / self.o * 100
        return max(up, dn)

    def direction(self) -> str:
        if self.o <= 0:
            return "high"
        up = (self.h - self.o) / self.o
        dn = (self.o - self.l) / self.o
        return "high" if up >= dn else "low"


@dataclass
class SymbolState:
    history: Deque[Bar] = field(default_factory=lambda: deque(maxlen=120))
    last_signal_ms: int = 0

    def push(self, bar: Bar) -> None:
        self.history.append(bar)

    def vol10(self) -> float:
        bars = list(self.history)
        if not bars:
            return 0.0
        return sum(b.vol for b in bars[-10:])

    def max_ret60_pct(self) -> float:
        bars = list(self.history)
        if len(bars) < 2:
            return 0.0
        window = bars[-60:]
        base = window[0].o
        if base <= 0:
            return 0.0
        mx = 0.0
        for b in window:
            mx = max(mx, (b.h - base) / base * 100, (base - b.l) / base * 100)
        return mx


def gate_passes(gate: dict, bar: Bar, st: SymbolState) -> bool:
    if not gate or gate.get("type") == "disabled":
        return False
    gtype = gate.get("type", "pct_burst")
    if gtype == "pct_burst":
        amp_min = float(gate.get("amp_min_pct", 2.0))
        notional_min = float(gate.get("notional_min_usdt", 5000))
        return bar.amp_pct() >= amp_min and bar.notional >= notional_min
    if gtype == "vol_ramp":
        vol10_min = float(gate.get("vol10_min", 1_000_000))
        ret60_min = float(gate.get("ret60_min_pct", 1.0))
        return st.vol10() >= vol10_min and st.max_ret60_pct() >= ret60_min
    return False


def in_cooldown(st: SymbolState, sec_ms: int, rearm_sec: int) -> bool:
    return sec_ms - st.last_signal_ms < rearm_sec * 1000


def pnl_t1_bars(bars: list, sig_idx: int, direction: str, hold_sec: int = 60) -> Optional[tuple[float, float]]:
    entry_i = sig_idx + 1
    exit_i = sig_idx + hold_sec
    if entry_i >= len(bars) or exit_i >= len(bars):
        return None
    entry = bars[entry_i].o
    exit_p = bars[exit_i].c
    if entry <= 0:
        return None
    hi = max(b.h for b in bars[entry_i : exit_i + 1])
    lo = min(b.l for b in bars[entry_i : exit_i + 1])
    if direction == "high":
        pnl = (exit_p - entry) / entry * 100
        max_fav = (hi - entry) / entry * 100
    else:
        pnl = (entry - exit_p) / entry * 100
        max_fav = (entry - lo) / entry * 100
    return pnl, max_fav


def inv_direction(d: Direction) -> Direction:
    return "low" if d == "high" else "high"


def side_label(d: Direction) -> str:
    return "LONG" if d == "high" else "SHORT"


def is_six_pct_event(bar: Bar, thresh_pct: float = 6.0, min_vol: float = 100.0) -> bool:
    if bar.vol < min_vol or bar.o <= 0:
        return False
    thresh = thresh_pct / 100.0
    return (bar.h - bar.o) / bar.o >= thresh or (bar.o - bar.l) / bar.o >= thresh


def burst_direction(bar: Bar) -> Direction:
    return bar.direction()


def pnl_pct(entry: float, price: float, direction: Direction) -> float:
    if entry <= 0:
        return 0.0
    if direction == "high":
        return (price - entry) / entry * 100
    return (entry - price) / entry * 100


def bar_fav_pct(entry: float, bar: Bar, direction: Direction) -> float:
    if entry <= 0:
        return 0.0
    if direction == "high":
        return (bar.h - entry) / entry * 100
    return (entry - bar.l) / entry * 100


def bar_adv_pct(entry: float, bar: Bar, direction: Direction) -> float:
    if entry <= 0:
        return 0.0
    if direction == "high":
        return (entry - bar.l) / entry * 100
    return (bar.h - entry) / entry * 100


def sig_open_flip(burst_dir: Direction, sig_open: float, close: float) -> Optional[Direction]:
    """Return flipped trade direction if close crosses signal open, else None."""
    if sig_open <= 0:
        return None
    if burst_dir == "high" and close < sig_open:
        return "low"
    if burst_dir == "low" and close > sig_open:
        return "high"
    return None


@dataclass
class SigOpenBreakTrade:
    symbol: str
    signal_ts_ms: int
    burst_dir: Direction
    sig_open: float
    sig_amp_pct: float
    entry_ts_ms: int
    exit_ts_ms: int
    hold_sec: int
    sig_range_pct: float = 0.0
    status: str = "pending_entry"  # pending_entry | active | closed
    trade_dir: Direction = "high"
    entry_price: float = 0.0
    flipped: bool = False
    flip_ts_ms: int = 0
    flip_sec: int = 0
    max_fav_pct: float = 0.0
    max_adv_pct: float = 0.0
    late_entry_price: float = 0.0
    late_exit_price: float = 0.0

    def __post_init__(self) -> None:
        self.trade_dir = self.burst_dir

    def on_tick(self, t_ms: int, price: float, late_ms: int) -> None:
        if self.status in ("pending_entry", "active") and t_ms >= self.entry_ts_ms + late_ms:
            if self.late_entry_price <= 0:
                self.late_entry_price = price
        if self.status == "active" and t_ms >= self.exit_ts_ms + late_ms:
            self.late_exit_price = price

    def on_entry_bar(self, open_price: float) -> None:
        self.entry_price = open_price
        self.status = "active"

    def on_bar(self, bar: Bar) -> None:
        if self.status != "active" or self.entry_price <= 0:
            return
        self.max_fav_pct = max(self.max_fav_pct, bar_fav_pct(self.entry_price, bar, self.trade_dir))
        self.max_adv_pct = max(self.max_adv_pct, bar_adv_pct(self.entry_price, bar, self.trade_dir))
        if not self.flipped:
            new_dir = sig_open_flip(self.burst_dir, self.sig_open, bar.c)
            if new_dir is not None:
                self.trade_dir = new_dir
                self.flipped = True
                self.flip_ts_ms = bar.sec
                self.flip_sec = (bar.sec - self.entry_ts_ms) // 1000

    def close_pnl_pct(self, exit_price: float) -> float:
        return pnl_pct(self.entry_price, exit_price, self.trade_dir)

    def late_pnl_pct(self, exit_price: float) -> tuple[float, float, float]:
        entry = self.late_entry_price if self.late_entry_price > 0 else self.entry_price
        exit_p = self.late_exit_price if self.late_exit_price > 0 else exit_price
        return pnl_pct(entry, exit_p, self.trade_dir), entry, exit_p


def dry_pnl_usdt(margin_usdt: float, leverage: float, pnl_pct: float) -> float:
    return margin_usdt * leverage * (pnl_pct / 100.0)


def net_pnl_usdt(notional_usdt: float, pnl_pct: float, fee_per_side: float = 0.0004) -> float:
    gross = notional_usdt * (pnl_pct / 100.0)
    fees = notional_usdt * fee_per_side * 2
    return gross - fees
