"""Alma SD SuperTrend — shared indicator math (Pine / Oquant defaults)."""
from __future__ import annotations

import math
from dataclasses import dataclass

FACTOR = 1.8
SD_LEN = 33
ALMA_LEN = 35
ALMA_SIGMA = 4.0
ALMA_OFFSET = 0.85
WARMUP_BARS = ALMA_LEN + SD_LEN + 5


@dataclass
class OHLC:
    ts: int
    o: float
    h: float
    l: float
    c: float


def alma_weights(length: int, offset: float, sigma: float) -> list[float]:
    m = offset * (length - 1)
    s = length / sigma
    w = [math.exp(-((i - m) ** 2) / (2 * s * s)) for i in range(length)]
    norm = sum(w)
    return [x / norm for x in w]


def compute_alma(closes: list[float], length: int, offset: float, sigma: float) -> list[float | None]:
    w = alma_weights(length, offset, sigma)
    out: list[float | None] = [None] * len(closes)
    for t in range(length - 1, len(closes)):
        out[t] = sum(closes[t - i] * w[i] for i in range(length))
    return out


def compute_stdev(closes: list[float], length: int) -> list[float | None]:
    out: list[float | None] = [None] * len(closes)
    for t in range(length - 1, len(closes)):
        window = closes[t - length + 1 : t + 1]
        mean = sum(window) / length
        var = sum((x - mean) ** 2 for x in window) / (length - 1) if length > 1 else 0.0
        out[t] = math.sqrt(var)
    return out


def compute_supertrend(
    bars: list[OHLC],
    factor: float = FACTOR,
    sd_len: int = SD_LEN,
    alma_len: int = ALMA_LEN,
    alma_sigma: float = ALMA_SIGMA,
    alma_offset: float = ALMA_OFFSET,
) -> list[int]:
    """direction: -1=bull, 1=bear, 0=warmup."""
    closes = [b.c for b in bars]
    alma = compute_alma(closes, alma_len, alma_offset, alma_sigma)
    sd = compute_stdev(closes, sd_len)
    n = len(bars)
    upper = [0.0] * n
    lower = [0.0] * n
    st_line = [0.0] * n
    direction = [0] * n

    for t in range(n):
        if alma[t] is None or sd[t] is None:
            direction[t] = 0
            continue

        ub = alma[t] + factor * sd[t]
        lb = alma[t] - factor * sd[t]

        if t > 0 and direction[t - 1] != 0:
            prev_ub = upper[t - 1]
            prev_lb = lower[t - 1]
            if not (ub < prev_ub or bars[t - 1].c > prev_ub):
                ub = prev_ub
            if not (lb > prev_lb or bars[t - 1].c < prev_lb):
                lb = prev_lb
        upper[t] = ub
        lower[t] = lb

        if t == 0 or sd[t - 1] is None or direction[t - 1] == 0:
            direction[t] = 1
        else:
            prev_st = st_line[t - 1]
            prev_ub = upper[t - 1]
            if prev_st == prev_ub:
                direction[t] = -1 if bars[t].c > ub else 1
            else:
                direction[t] = 1 if bars[t].c < lb else -1

        st_line[t] = lower[t] if direction[t] == -1 else upper[t]

    return direction


def signal_series(direction: list[int]) -> list[int]:
    sig = [0] * len(direction)
    cur = 0
    for t, d in enumerate(direction):
        if d == 0:
            sig[t] = 0
            continue
        cur = 1 if d < 0 else -1
        sig[t] = cur
    return sig


def signal_flip(prev_sig: int, cur_sig: int) -> str | None:
    """Return 'long', 'short', or None."""
    if cur_sig == 0:
        return None
    if prev_sig <= 0 and cur_sig > 0:
        return "long"
    if prev_sig >= 0 and cur_sig < 0:
        return "short"
    return None


def sl_tp_prices(entry: float, side: str, sl_pct: float, tp_pct: float) -> tuple[float, float]:
    if side == "long":
        return entry * (1 - sl_pct / 100), entry * (1 + tp_pct / 100)
    return entry * (1 + sl_pct / 100), entry * (1 - tp_pct / 100)


def check_exit(side: str, bar_h: float, bar_l: float, sl_px: float, tp_px: float) -> tuple[str, float] | None:
    if side == "long":
        hit_sl = bar_l <= sl_px
        hit_tp = bar_h >= tp_px
        if hit_sl and hit_tp:
            return "sl", sl_px
        if hit_sl:
            return "sl", sl_px
        if hit_tp:
            return "tp", tp_px
    else:
        hit_sl = bar_h >= sl_px
        hit_tp = bar_l <= tp_px
        if hit_sl and hit_tp:
            return "sl", sl_px
        if hit_sl:
            return "sl", sl_px
        if hit_tp:
            return "tp", tp_px
    return None
