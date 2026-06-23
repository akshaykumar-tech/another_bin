"""Strong Demands & Supplies + Liquidity — SMC structure / OB signals (Pine port)."""
from __future__ import annotations

from dataclasses import dataclass

from sr_chartprime_lib import Bar, atr, highest, lowest


@dataclass
class SDSignal:
    ts: int
    side: str  # long | short
    setup: str  # demand_bos | supply_bos | demand_choch | supply_choch
    entry_px: float
    ob_top: float
    ob_btm: float
    level: float


@dataclass
class SDState:
    swing_len: int = 50
    trend: int = 0
    top_y: float = 0.0
    top_x: int = 0
    btm_y: float = 0.0
    btm_x: int = 0
    top_cross: bool = True
    btm_cross: bool = True
    os: int = 0


def _update_swings(st: SDState, bars: list[Bar], i: int) -> tuple[float | None, float | None]:
    """Pine swings_calc(len) — stateful os flip."""
    length = st.swing_len
    if i < length:
        return None, None
    highs = [b.h for b in bars]
    lows = [b.l for b in bars]
    upper = highest(highs, length, i)
    lower = lowest(lows, length, i)
    center = i - length
    os_prev = st.os
    if bars[center].h > upper:
        st.os = 0
    elif bars[center].l < lower:
        st.os = 1
    top = bars[center].h if st.os == 0 and os_prev != 0 else None
    btm = bars[center].l if st.os == 1 and os_prev != 1 else None
    return top, btm


def _ob_zone(bars: list[Bar], loc_i: int, use_max: bool, i: int) -> tuple[float, float]:
    """Pine ob_coord — order block from candles since structure pivot."""
    ob_threshold = atr(bars, min(200, i), i)
    if ob_threshold <= 0:
        ob_threshold = bars[i].h - bars[i].l
    mx, mn = 0.0, float("inf")
    span = i - loc_i
    for j in range(1, max(span, 1)):
        bi = i - j
        if bi <= loc_i or bi < 0:
            break
        b = bars[bi]
        rng = b.h - b.l
        if rng < ob_threshold * 2:
            if use_max:
                if b.h > mx:
                    mx, mn = b.h, b.l
            else:
                if b.l < mn:
                    mn, mx = b.l, b.h
    if mx <= 0:
        mx = bars[i].h
    if mn == float("inf"):
        mn = bars[i].l
    return mx, mn


def on_bar_confirmed(st: SDState, bars: list[Bar], i: int) -> SDSignal | None:
    """BOS/CHoCH on swing structure → demand/supply zone entry."""
    if i < st.swing_len + 2:
        return None
    b = bars[i]
    prev = bars[i - 1]

    ph, pl = _update_swings(st, bars, i)
    if ph is not None:
        st.top_cross = True
        st.top_y = ph
        st.top_x = i - st.swing_len
    if pl is not None:
        st.btm_cross = True
        st.btm_y = pl
        st.btm_x = i - st.swing_len

    if st.top_y > 0 and prev.c <= st.top_y < b.c and st.top_cross:
        choch = st.trend < 0
        ob_top, ob_btm = _ob_zone(bars, st.top_x, False, i)
        setup = "demand_choch" if choch else "demand_bos"
        st.top_cross = False
        st.trend = 1
        return SDSignal(b.ts, "long", setup, b.c, ob_top, ob_btm, st.top_y)

    if st.btm_y > 0 and prev.c >= st.btm_y > b.c and st.btm_cross:
        choch = st.trend > 0
        ob_top, ob_btm = _ob_zone(bars, st.btm_x, True, i)
        setup = "supply_choch" if choch else "supply_bos"
        st.btm_cross = False
        st.trend = -1
        return SDSignal(b.ts, "short", setup, b.c, ob_top, ob_btm, st.btm_y)

    return None
