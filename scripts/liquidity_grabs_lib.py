"""Flux Charts Liquidity Grabs detector (Pine v6 port)."""
from __future__ import annotations

from dataclasses import dataclass, field
import math


@dataclass
class Bar:
    ts: int
    o: float
    h: float
    l: float
    c: float
    v: float = 0.0


@dataclass
class GrabSignal:
    bar_i: int
    ts: int
    side: str  # long | short
    grab_type: str  # sellside | buyside
    grab_size: int  # 1 small, 2 med, 3 large
    entry_px: float
    liq_level: float
    sl_px: float
    tp_px: float


@dataclass
class GrabState:
    pivot_len: int = 25
    liq_zone_count: int = 5
    wbr: float = 0.5
    cooldown: int = 3
    tp_pct: float = 1.5

    buyside_liqs: list[float] = field(default_factory=list)
    sellside_liqs: list[float] = field(default_factory=list)
    last_buyside_grab: int = -999
    last_sellside_grab: int = -999


def _invalidate_top(bar: Bar, is_close: bool) -> float:
    return max(bar.c, bar.o) if is_close else bar.h


def _invalidate_bottom(bar: Bar, is_close: bool) -> float:
    return min(bar.c, bar.o) if is_close else bar.l


def _pivot_high(bars: list[Bar], i: int, left: int, right: int) -> float | None:
    if i < left + right:
        return None
    center = i - right
    h = bars[center].h
    for j in range(center - left, center + right + 1):
        if bars[j].h > h:
            return None
    return h


def _pivot_low(bars: list[Bar], i: int, left: int, right: int) -> float | None:
    if i < left + right:
        return None
    center = i - right
    lo = bars[center].l
    for j in range(center - left, center + right + 1):
        if bars[j].l < lo:
            return None
    return lo


def on_bar_confirmed(st: GrabState, bars: list[Bar], i: int) -> GrabSignal | None:
    """Process confirmed bar i; return entry signal if liquidity grab detected."""
    bar = bars[i]
    pl = st.pivot_len

    ph = _pivot_high(bars, i, pl, pl)
    if ph is not None:
        st.buyside_liqs.append(ph)
        if len(st.buyside_liqs) > st.liq_zone_count:
            st.buyside_liqs.pop(0)

    plow = _pivot_low(bars, i, pl, pl)
    if plow is not None:
        st.sellside_liqs.append(plow)
        if len(st.sellside_liqs) > st.liq_zone_count:
            st.sellside_liqs.pop(0)

    grab_found = False
    grab_buyside = True
    grab_size = 1
    liq_level = 0.0
    to_remove: list[float] = []

    for cur_liq in list(st.buyside_liqs):
        if _invalidate_top(bar, False) > cur_liq and _invalidate_top(bar, True) < cur_liq:
            body = abs(bar.c - bar.o) or 1e-12
            wick = bar.h - max(bar.c, bar.o)
            cur_wbr = wick / body
            grab_found = True
            grab_buyside = True
            grab_size = int(math.floor(min(cur_wbr / st.wbr, 3)))
            liq_level = cur_liq
            to_remove.append(cur_liq)
            break
        if _invalidate_top(bar, True) > cur_liq:
            to_remove.append(cur_liq)
            break

    if not grab_found:
        for cur_liq in list(st.sellside_liqs):
            if _invalidate_bottom(bar, False) < cur_liq and _invalidate_bottom(bar, True) > cur_liq:
                body = abs(bar.c - bar.o) or 1e-12
                wick = min(bar.c, bar.o) - bar.l
                cur_wbr = wick / body
                grab_found = True
                grab_buyside = False
                grab_size = int(math.floor(min(cur_wbr / st.wbr, 3)))
                liq_level = cur_liq
                to_remove.append(cur_liq)
                break
            if _invalidate_bottom(bar, True) < cur_liq:
                to_remove.append(cur_liq)
                break

    for liq in to_remove:
        if liq in st.buyside_liqs:
            st.buyside_liqs.remove(liq)
        elif liq in st.sellside_liqs:
            st.sellside_liqs.remove(liq)

    if not grab_found or grab_size <= 0:
        return None

    if grab_buyside:
        if i - st.last_buyside_grab <= st.cooldown:
            return None
        st.last_buyside_grab = i
        entry = bar.c
        sl_px = bar.h * 1.0005
        tp_px = entry * (1 - st.tp_pct / 100)
        return GrabSignal(i, bar.ts, "short", "buyside", grab_size, entry, liq_level, sl_px, tp_px)

    if i - st.last_sellside_grab <= st.cooldown:
        return None
    st.last_sellside_grab = i
    entry = bar.c
    sl_px = bar.l * 0.9995
    tp_px = entry * (1 + st.tp_pct / 100)
    return GrabSignal(i, bar.ts, "long", "sellside", grab_size, entry, liq_level, sl_px, tp_px)
