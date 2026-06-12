"""15m structure strategies: Price Range double-supply short + AMD+FVG short."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Optional, Sequence

from focused_lib import Bar, pnl_pct

# --- Option A: Price Range + double supply (15m LOOSE SHORT) ---
PRICE_RANGE_PARAMS = {
    "range_bars": 16,
    "min_range_pct": 0.25,
    "max_range_pct": 6.5,
    "zone_frac": 0.30,
    "min_bounce_pct": 0.15,
    "touch_lookback": 24,
    "touch_gap": 1,
    "wick_frac": 0.20,
    "min_impulse_pct": 0.20,
    "impulse_look": 5,
    "stop_buf_pct": 0.18,
    "max_retests": 3,
    "hold_bars": 16,
}

# --- Option B: AMD + FVG (15m LOOSE SHORT) ---
AMD_FVG_PARAMS = {
    "range_bars": 8,
    "min_range_pct": 0.03,
    "max_range_pct": 4.5,
    "sweep_min_pct": 0.03,
    "fvg_lookahead": 8,
    "min_fvg_pct": 0.0,
    "max_entry_wait": 16,
    "entry_mode": "close",
    "stop_buf_pct": 0.20,
    "hold_bars": 15,
}


def net_pnl_pct(gross_pct: float, fee_rt: float = 0.0008) -> float:
    return gross_pct - fee_rt * 100


def net_pnl_usdt(gross_pct: float, notional: float, fee_rt: float = 0.0008) -> float:
    return notional * (gross_pct / 100.0) - notional * fee_rt


def impulse_dn(bars: Sequence[Bar], i: int, look: int) -> float:
    if i < look:
        return 0.0
    top = max(b.h for b in bars[i - look : i])
    return (top - bars[i].c) / top * 100 if top > 0 else 0.0


def double_supply_touch(bars: Sequence[Bar], i: int, rh: float, zone_bot: float, p: dict) -> bool:
    touches = []
    for j in range(max(0, i - p["touch_lookback"]), i):
        b = bars[j]
        if b.h >= zone_bot and b.c <= rh:
            touches.append(j)
    if len(touches) < 2:
        return False
    t1, t2 = touches[-2], touches[-1]
    if t2 - t1 < p["touch_gap"]:
        return False
    mid_low = min(b.l for b in bars[t1 : t2 + 1])
    bounce = (bars[t1].h - mid_low) / bars[t1].h * 100 if bars[t1].h > 0 else 0.0
    if bounce < p["min_bounce_pct"]:
        return False
    b2 = bars[t2]
    rng = b2.h - b2.l
    if rng <= 0:
        return False
    upper_wick = (b2.h - max(b2.o, b2.c)) / rng
    return b2.c <= b2.o or upper_wick >= p["wick_frac"]


def bear_fvg(bars: Sequence[Bar], i: int) -> Optional[tuple[float, float]]:
    if i < 2:
        return None
    a, c = bars[i - 2], bars[i]
    if c.h < a.l:
        return (c.h, a.l)
    return None


def bull_fvg(bars: Sequence[Bar], i: int) -> Optional[tuple[float, float]]:
    if i < 2:
        return None
    a, c = bars[i - 2], bars[i]
    if c.l > a.h:
        return (a.h, c.l)
    return None


@dataclass
class StructureTrade:
    symbol: str
    strategy: str
    signal_ms: int
    entry_ms: int
    exit_ms: int
    side: str
    entry_price: float = 0.0
    stop: float = 0.0
    target: float = 0.0
    status: str = "pending_entry"  # pending_entry | active
    exit_price: float = 0.0
    exit_reason: str = ""
    hold_bars: int = 0
    meta: str = ""

    def gross_pnl_pct(self) -> float:
        return pnl_pct(self.entry_price, self.exit_price, "low" if self.side == "SHORT" else "high")


@dataclass
class AmdPending:
    symbol: str
    signal_ms: int
    m_bar_ms: int
    rh: float
    rl: float
    stop: float
    bars_left: int
    fvg_bot: float = 0.0
    fvg_top: float = 0.0
    fvg_ms: int = 0
    phase: str = "seek_fvg"  # seek_fvg | seek_entry
    entry_wait_left: int = 0


@dataclass
class EngineState:
    name: str
    log_name: str
    params: dict
    kind: str  # price_range | amd_fvg
    history: dict[str, Deque[Bar]] = field(default_factory=dict)
    active: dict[str, StructureTrade] = field(default_factory=dict)
    amd_pending: dict[str, AmdPending] = field(default_factory=dict)
    last_signal_ms: dict[str, int] = field(default_factory=dict)
    supply_retests: dict[str, int] = field(default_factory=dict)
    stats: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.stats = {
            "signals": 0,
            "entries": 0,
            "exits": 0,
            "wins": 0,
            "net_usd": 0.0,
            "cooldown_skips": 0,
            "overlap_skips": 0,
        }

    def hist(self, symbol: str) -> Deque[Bar]:
        if symbol not in self.history:
            self.history[symbol] = deque(maxlen=120)
        return self.history[symbol]

    def busy(self, symbol: str) -> bool:
        return symbol in self.active or symbol in self.amd_pending

    def in_cooldown(self, symbol: str, sec_ms: int, rearm_sec: int) -> bool:
        prev = self.last_signal_ms.get(symbol, 0)
        return sec_ms - prev < rearm_sec * 1000


def planned_exit_ms(entry_ms: int, hold_bars: int, bar_ms: int) -> int:
    return entry_ms + (hold_bars - 1) * bar_ms


def check_price_range_short(
    symbol: str, bars: list[Bar], i: int, st: EngineState
) -> Optional[StructureTrade]:
    p = st.params
    if i < p["range_bars"] + 6:
        return None
    window = bars[i - p["range_bars"] : i]
    rh = max(b.h for b in window)
    rl = min(b.l for b in window)
    mid = (rh + rl) / 2
    if mid <= 0:
        return None
    rng_pct = (rh - rl) / mid * 100
    if rng_pct < p["min_range_pct"] or rng_pct > p["max_range_pct"]:
        return None
    zone_l = rh - (rh - rl) * p["zone_frac"]
    b = bars[i]
    if b.h < zone_l or b.c > rh:
        return None
    retests = st.supply_retests.get(symbol, 0)
    if retests >= p["max_retests"]:
        return None
    if not double_supply_touch(bars, i, rh, zone_l, p):
        return None
    if impulse_dn(bars, i, p["impulse_look"]) < p["min_impulse_pct"]:
        return None
    entry = b.c
    stop = rh * (1 + p["stop_buf_pct"] / 100)
    if entry >= stop:
        return None
    st.supply_retests[symbol] = retests + 1
    hold = p["hold_bars"]
    return StructureTrade(
        symbol=symbol,
        strategy=st.name,
        signal_ms=b.sec,
        entry_ms=b.sec,
        exit_ms=planned_exit_ms(b.sec, hold, 900_000),
        side="SHORT",
        entry_price=entry,
        stop=stop,
        target=rl,
        status="active",
        hold_bars=hold,
        meta=f"rng={rng_pct:.2f}% rh={rh:.8f} rl={rl:.8f}",
    )


def check_amd_manipulation_short(bars: list[Bar], i: int, p: dict) -> Optional[tuple[float, float, float]]:
    rb = p["range_bars"]
    if i < rb:
        return None
    window = bars[i - rb : i]
    rh = max(b.h for b in window)
    rl = min(b.l for b in window)
    mid = (rh + rl) / 2
    if mid <= 0:
        return None
    rng_pct = (rh - rl) / mid * 100
    if rng_pct < p["min_range_pct"] or rng_pct > p["max_range_pct"]:
        return None
    b = bars[i]
    sweep_high = rh * (1 + p["sweep_min_pct"] / 100)
    if b.h <= sweep_high or b.c >= rh:
        return None
    stop = rh * (1 + p["stop_buf_pct"] / 100)
    return rh, rl, stop


def entry_from_fvg(bars: list[Bar], k: int, bot: float, top: float, mode: str) -> float:
    bk = bars[k]
    if mode == "gap_edge":
        return bot
    if mode == "mid_gap":
        return (bot + top) / 2
    return bk.c
