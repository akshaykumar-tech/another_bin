"""ZigZag + harmonic pattern ratios (Pine ZigZag PA V4.1)."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Bar:
    ts: int
    o: float
    h: float
    l: float
    c: float


def zigzag_pivots(bars: list[Bar]) -> list[tuple[int, float]]:
    if len(bars) < 2:
        return []
    direction = 0
    out: list[tuple[int, float]] = []
    for i in range(1, len(bars)):
        is_up = bars[i].c >= bars[i].o
        is_down = bars[i].c <= bars[i].o
        prev_up = bars[i - 1].c >= bars[i - 1].o
        prev_down = bars[i - 1].c <= bars[i - 1].o
        prev_dir = direction
        if prev_up and is_down:
            direction = -1
        elif prev_down and is_up:
            direction = 1
        if prev_up and is_down and prev_dir != -1:
            out.append((i, max(bars[i - 1].h, bars[i].h)))
        elif prev_down and is_up and prev_dir != 1:
            out.append((i, min(bars[i - 1].l, bars[i].l)))
    return out


def _ratios(x: float, a: float, b: float, c: float, d: float) -> tuple[float, float, float, float]:
    def safe(n: float, den: float) -> float:
        return abs(n / den) if den else 0.0

    return (
        safe(b - a, x - a),
        safe(a - d, x - a),
        safe(b - c, a - b),
        safe(c - d, b - c),
    )


def _ok(v: float, lo: float, hi: float) -> bool:
    return lo <= v <= hi


def is_bat(mode: int, xab: float, abc: float, bcd: float, xad: float, d: float, c: float) -> bool:
    cond = _ok(xab, 0.382, 0.5) and _ok(abc, 0.382, 0.886) and _ok(bcd, 1.618, 2.618) and xad <= 0.618
    return cond and (d < c if mode == 1 else d > c)


def is_anti_bat(mode: int, xab, abc, bcd, xad, d, c) -> bool:
    cond = _ok(xab, 0.5, 0.886) and _ok(abc, 1.0, 2.618) and _ok(bcd, 1.618, 2.618) and _ok(xad, 0.886, 1.0)
    return cond and (d < c if mode == 1 else d > c)


def is_alt_bat(mode, xab, abc, bcd, xad, d, c) -> bool:
    cond = xab <= 0.382 and _ok(abc, 0.382, 0.886) and _ok(bcd, 2.0, 3.618) and xad <= 1.13
    return cond and (d < c if mode == 1 else d > c)


def is_butterfly(mode, xab, abc, bcd, xad, d, c) -> bool:
    cond = xab <= 0.786 and _ok(abc, 0.382, 0.886) and _ok(bcd, 1.618, 2.618) and _ok(xad, 1.27, 1.618)
    return cond and (d < c if mode == 1 else d > c)


def is_anti_butterfly(mode, xab, abc, bcd, xad, d, c) -> bool:
    cond = _ok(xab, 0.236, 0.886) and _ok(abc, 1.13, 2.618) and _ok(bcd, 1.0, 1.382) and _ok(xad, 0.5, 0.886)
    return cond and (d < c if mode == 1 else d > c)


def is_abcd(mode, xab, abc, bcd, xad, d, c) -> bool:
    cond = _ok(abc, 0.382, 0.886) and _ok(bcd, 1.13, 2.618)
    return cond and (d < c if mode == 1 else d > c)


def is_gartley(mode, xab, abc, bcd, xad, d, c) -> bool:
    cond = _ok(xab, 0.5, 0.618) and _ok(abc, 0.382, 0.886) and _ok(bcd, 1.13, 2.618) and _ok(xad, 0.75, 0.875)
    return cond and (d < c if mode == 1 else d > c)


def is_anti_gartley(mode, xab, abc, bcd, xad, d, c) -> bool:
    cond = _ok(xab, 0.5, 0.886) and _ok(abc, 1.0, 2.618) and _ok(bcd, 1.5, 5.0) and _ok(xad, 1.0, 5.0)
    return cond and (d < c if mode == 1 else d > c)


def is_crab(mode, xab, abc, bcd, xad, d, c) -> bool:
    cond = _ok(xab, 0.5, 0.875) and _ok(abc, 0.382, 0.886) and _ok(bcd, 2.0, 5.0) and _ok(xad, 1.382, 5.0)
    return cond and (d < c if mode == 1 else d > c)


def is_anti_crab(mode, xab, abc, bcd, xad, d, c) -> bool:
    cond = _ok(xab, 0.25, 0.5) and _ok(abc, 1.13, 2.618) and _ok(bcd, 1.618, 2.618) and _ok(xad, 0.5, 0.75)
    return cond and (d < c if mode == 1 else d > c)


def is_shark(mode, xab, abc, bcd, xad, d, c) -> bool:
    cond = _ok(xab, 0.5, 0.875) and _ok(abc, 1.13, 1.618) and _ok(bcd, 1.27, 2.24) and _ok(xad, 0.886, 1.13)
    return cond and (d < c if mode == 1 else d > c)


def is_anti_shark(mode, xab, abc, bcd, xad, d, c) -> bool:
    cond = _ok(xab, 0.382, 0.875) and _ok(abc, 0.5, 1.0) and _ok(bcd, 1.25, 2.618) and _ok(xad, 0.5, 1.25)
    return cond and (d < c if mode == 1 else d > c)


def is_5o(mode, xab, abc, bcd, xad, d, c) -> bool:
    cond = _ok(xab, 1.13, 1.618) and _ok(abc, 1.618, 2.24) and _ok(bcd, 0.5, 0.625) and _ok(xad, 0.0, 0.236)
    return cond and (d < c if mode == 1 else d > c)


def is_wolf(mode, xab, abc, bcd, xad, d, c) -> bool:
    cond = _ok(xab, 1.27, 1.618) and abc >= 0 and abc <= 5 and _ok(bcd, 1.27, 1.618) and xad >= 0 and xad <= 5
    return cond and (d < c if mode == 1 else d > c)


def is_hns(mode, xab, abc, bcd, xad, d, c) -> bool:
    cond = _ok(xab, 2.0, 10) and _ok(abc, 0.9, 1.1) and _ok(bcd, 0.236, 0.88) and _ok(xad, 0.9, 1.1)
    return cond and (d < c if mode == 1 else d > c)


def is_con_tria(mode, xab, abc, bcd, xad, d, c) -> bool:
    cond = _ok(xab, 0.382, 0.618) and _ok(abc, 0.382, 0.618) and _ok(bcd, 0.382, 0.618) and _ok(xad, 0.236, 0.764)
    return cond and (d < c if mode == 1 else d > c)


def is_exp_tria(mode, xab, abc, bcd, xad, d, c) -> bool:
    cond = _ok(xab, 1.236, 1.618) and _ok(abc, 1.0, 1.618) and _ok(bcd, 1.236, 2.0) and _ok(xad, 2.0, 2.236)
    return cond and (d < c if mode == 1 else d > c)


PATTERN_FNS = [
    ("Bat", is_bat),
    ("AntiBat", is_anti_bat),
    ("AltBat", is_alt_bat),
    ("Butterfly", is_butterfly),
    ("AntiButterfly", is_anti_butterfly),
    ("ABCD", is_abcd),
    ("Gartley", is_gartley),
    ("AntiGartley", is_anti_gartley),
    ("Crab", is_crab),
    ("AntiCrab", is_anti_crab),
    ("Shark", is_shark),
    ("AntiShark", is_anti_shark),
    ("5O", is_5o),
    ("Wolf", is_wolf),
    ("HnS", is_hns),
    ("ConTria", is_con_tria),
    ("ExpTria", is_exp_tria),
]


def fib_level(d: float, c: float, rate: float) -> float:
    r = abs(d - c)
    return d - r * rate if d > c else d + r * rate


def detect_patterns(x: float, a: float, b: float, c: float, d: float) -> tuple[bool, bool, list[str], list[str]]:
    xab, xad, abc, bcd = _ratios(x, a, b, c, d)
    bull_names, bear_names = [], []
    for name, fn in PATTERN_FNS:
        if fn(1, xab, abc, bcd, xad, d, c):
            bull_names.append(name)
        if fn(-1, xab, abc, bcd, xad, d, c):
            bear_names.append(name)
    return bool(bull_names), bool(bear_names), bull_names, bear_names
