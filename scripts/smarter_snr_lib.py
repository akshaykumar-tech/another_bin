"""Smarter SnR (hanabil) — swing S/R + trendline break signals on 5m bars."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Bar:
    ts: int
    o: float
    h: float
    l: float
    c: float
    v: float = 0.0


@dataclass
class SNRConfig:
    swing_period: int = 20
    tl_period: int = 20
    use_shadow: bool = False
    signals: str = "tl_break"  # tl_break | snr_cross | all


@dataclass
class Signal:
    ts: int
    bar_i: int
    sig_type: str
    side: str
    entry: float


def _pivot_high(bars: list[Bar], i: int, left: int, right: int) -> float | None:
    p = i - right
    if p < left or p < 0:
        return None
    h = bars[p].h
    for j in range(p - left, p + right + 1):
        if j != p and bars[j].h >= h:
            return None
    return h


def _pivot_low(bars: list[Bar], i: int, left: int, right: int) -> float | None:
    p = i - right
    if p < left or p < 0:
        return None
    lo = bars[p].l
    for j in range(p - left, p + right + 1):
        if j != p and bars[j].l <= lo:
            return None
    return lo


def _src_h(b: Bar, cfg: SNRConfig) -> float:
    return b.h if cfg.use_shadow else b.c


def _src_l(b: Bar, cfg: SNRConfig) -> float:
    return b.l if cfg.use_shadow else b.c


def scan_signals(bars: list[Bar], cfg: SNRConfig) -> list[Signal]:
    n = len(bars)
    p, tp = cfg.swing_period, cfg.tl_period
    if n < tp * 4 + 10:
        return []

    pl_levels: list[float] = []
    ph_levels: list[float] = []
    ph_tl: list[tuple[int, float]] = []
    pl_tl: list[tuple[int, float]] = []
    upper: list[float | None] = [None] * n
    lower: list[float | None] = [None] * n

    for i in range(n):
        if _pivot_high(bars, i, p, p) is not None:
            ph_levels.insert(0, bars[i - p].h)
            ph_levels = ph_levels[:3]
        if _pivot_low(bars, i, p, p) is not None:
            pl_levels.insert(0, bars[i - p].l)
            pl_levels = pl_levels[:3]

        if _pivot_high(bars, i, tp, tp) is not None:
            ph_tl.insert(0, (i - tp, _src_h(bars[i - tp], cfg)))
            ph_tl = ph_tl[:5]
        if _pivot_low(bars, i, tp, tp) is not None:
            pl_tl.insert(0, (i - tp, _src_l(bars[i - tp], cfg)))
            pl_tl = pl_tl[:5]

        if len(ph_tl) >= 2 and ph_tl[0][1] < ph_tl[1][1]:
            x2, y2 = ph_tl[0]
            x1, y1 = ph_tl[1]
            dt = bars[x2].ts - bars[x1].ts
            if dt > 0:
                upper[i] = y2 + (bars[i].ts - bars[x2].ts) * ((y2 - y1) / dt)

        if len(pl_tl) >= 2 and pl_tl[0][1] > pl_tl[1][1]:
            x2, y2 = pl_tl[0]
            x1, y1 = pl_tl[1]
            dt = bars[x2].ts - bars[x1].ts
            if dt > 0:
                lower[i] = y2 + (bars[i].ts - bars[x2].ts) * ((y2 - y1) / dt)

    out: list[Signal] = []
    for i in range(1, n):
        sh0, sh1 = _src_h(bars[i - 1], cfg), _src_h(bars[i], cfg)
        sl0, sl1 = _src_l(bars[i - 1], cfg), _src_l(bars[i], cfg)
        c0, c1 = bars[i - 1].c, bars[i].c

        if cfg.signals in ("tl_break", "all"):
            u0, u1 = upper[i - 1], upper[i]
            lo0, lo1 = lower[i - 1], lower[i]
            if u0 is not None and u1 is not None and sh0 < u0 and sh1 > u1:
                out.append(Signal(bars[i].ts, i, "break_upper", "long", c1))
            if lo0 is not None and lo1 is not None and sl0 > lo0 and sl1 < lo1:
                out.append(Signal(bars[i].ts, i, "break_lower", "short", c1))

        if cfg.signals in ("snr_cross", "all"):
            for j, lv in enumerate(pl_levels[:3]):
                if c0 <= lv < c1:
                    out.append(Signal(bars[i].ts, i, f"s_co_{j+1}", "long", c1))
                if c0 >= lv > c1:
                    out.append(Signal(bars[i].ts, i, f"s_cu_{j+1}", "short", c1))
            for j, lv in enumerate(ph_levels[:3]):
                if c0 <= lv < c1:
                    out.append(Signal(bars[i].ts, i, f"r_co_{j+1}", "long", c1))
                if c0 >= lv > c1:
                    out.append(Signal(bars[i].ts, i, f"r_cu_{j+1}", "short", c1))

    return out
