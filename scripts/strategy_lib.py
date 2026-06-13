"""Shared logic for 4-strategy dry paper (3% burst events)."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Literal, Optional, Sequence

from focused_lib import Bar, bar_adv_pct, bar_fav_pct, pnl_pct, side_label

Direction = Literal["high", "low"]

LOCKS = [0.5, 1, 2, 3, 4, 5, 6, 8, 10, 15, 20]


def trail_stop_price(entry_price: float, trade_dir: Direction, lock_pct: float) -> float:
    if trade_dir == "high":
        return entry_price * (1 + lock_pct / 100)
    return entry_price * (1 - lock_pct / 100)


def amp_burst(bar: Bar, amp_min: float = 3.0, min_vol: float = 100.0) -> tuple[Optional[Direction], float]:
    if bar.vol < min_vol or bar.o <= 0:
        return None, 0.0
    up = (bar.h - bar.o) / bar.o * 100
    dn = (bar.o - bar.l) / bar.o * 100
    amp = max(up, dn)
    if amp < amp_min:
        return None, amp
    return ("high" if up >= dn else "low"), amp


def ll_cont_at(bars: Sequence[Bar], sig_i: int) -> bool:
    if sig_i + 30 >= len(bars):
        return False
    seg = bars[sig_i + 1 : sig_i + 31]
    hh = sum(1 for j in range(1, len(seg)) if seg[j].h > seg[j - 1].h)
    ll = sum(1 for j in range(1, len(seg)) if seg[j].l < seg[j - 1].l)
    return ll > hh * 1.5


def post_struct(bars: Sequence[Bar], sig_i: int, burst: Direction, entry: float) -> str:
    if sig_i + 30 >= len(bars):
        return "UNKNOWN"
    seg = bars[sig_i + 1 : sig_i + 31]
    hi = max(b.h for b in seg)
    lo = min(b.l for b in seg)
    rng = (hi - lo) / entry * 100 if entry > 0 else 0
    hh = sum(1 for j in range(1, len(seg)) if seg[j].h > seg[j - 1].h)
    ll = sum(1 for j in range(1, len(seg)) if seg[j].l < seg[j - 1].l)
    if rng < 2:
        return "TIGHT_FLAG"
    if burst == "high" and hh > ll * 1.5:
        return "HH_CONT"
    if burst == "low" and ll > hh * 1.5:
        return "LL_CONT"
    return "CHOP"


def net_pnl_pct(gross_pct: float, fee_rt: float = 0.0008) -> float:
    return gross_pct - fee_rt * 100


def net_pnl_usdt(gross_pct: float, notional: float, fee_rt: float = 0.0008) -> float:
    return notional * (gross_pct / 100.0) - notional * fee_rt


@dataclass
class PendingConfirm:
    symbol: str
    signal_ms: int
    burst: Direction
    sig_open: float
    amp_pct: float
    confirm_ms: int
    entry_ms: int
    strategy: str


@dataclass
class DryTrade:
    symbol: str
    strategy: str
    signal_ms: int
    burst: Direction
    trade_dir: Direction
    sig_open: float
    amp_pct: float
    entry_ms: int
    exit_ms: int
    hold_sec: int
    entry_price: float = 0.0
    status: str = "pending_entry"  # pending_entry | active | closed
    lock_pct: float = 0.0
    best_mfe_pct: float = 0.0
    max_adv_pct: float = 0.0
    exit_price: float = 0.0
    exit_reason: str = ""

    def on_entry(self, open_price: float) -> None:
        self.entry_price = open_price
        self.status = "active"

    def on_bar(self, bar: Bar) -> Optional[float]:
        """Return stop exit price if trail lock hit, else None."""
        if self.status != "active" or self.entry_price <= 0:
            return None
        self.best_mfe_pct = max(self.best_mfe_pct, bar_fav_pct(self.entry_price, bar, self.trade_dir))
        self.max_adv_pct = max(self.max_adv_pct, bar_adv_pct(self.entry_price, bar, self.trade_dir))
        if self.strategy != "trail_lock_300":
            return None
        for lv in LOCKS:
            if self.best_mfe_pct >= lv:
                self.lock_pct = max(self.lock_pct, lv)
        if self.lock_pct <= 0:
            return None
        ep = self.entry_price
        stop = trail_stop_price(ep, self.trade_dir, self.lock_pct)
        if self.trade_dir == "high" and bar.l <= stop:
            return stop
        if self.trade_dir == "low" and bar.h >= stop:
            return stop
        return None

    def close_pnl_pct(self, exit_price: float) -> float:
        return pnl_pct(self.entry_price, exit_price, self.trade_dir)


@dataclass
class StrategyState:
    name: str
    hold_sec: int
    entry_offset: int
    burst_filter: Optional[Direction] = None
    confirm_kind: Optional[str] = None  # ll_cont | hh_cont
    fade: bool = False
    use_trail: bool = False
    history: dict[str, Deque[Bar]] = field(default_factory=dict)
    active: dict[str, DryTrade] = field(default_factory=dict)
    pending: dict[str, PendingConfirm] = field(default_factory=dict)
    last_event_ms: dict[str, int] = field(default_factory=dict)
    stats: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.stats = {
            "signals": 0,
            "confirm_pass": 0,
            "confirm_fail": 0,
            "entries": 0,
            "exits": 0,
            "cooldown_skips": 0,
            "overlap_skips": 0,
            "net_usd": 0.0,
            "wins": 0,
        }

    def hist(self, symbol: str) -> Deque[Bar]:
        if symbol not in self.history:
            self.history[symbol] = deque(maxlen=180)
        return self.history[symbol]

    def in_cooldown(self, symbol: str, sec_ms: int, rearm_sec: int) -> bool:
        prev = self.last_event_ms.get(symbol, 0)
        return sec_ms - prev < rearm_sec * 1000

    def busy(self, symbol: str) -> bool:
        return symbol in self.active or symbol in self.pending
