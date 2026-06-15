"""Schaff Trend Cycle (everget / TradingView Pine v4)."""
from __future__ import annotations

import math

FAST = 23
SLOW = 50
CYCLE = 10
D1 = 3
D2 = 3
UPPER = 75.0
LOWER = 25.0
WARMUP_BARS = SLOW + CYCLE * 2 + D1 + D2 + 10


def ema_series(values: list[float], length: int) -> list[float]:
    out: list[float] = [float("nan")] * len(values)
    if not values or length < 1:
        return out
    alpha = 2.0 / (length + 1)
    started = False
    prev = 0.0
    for t, v in enumerate(values):
        if math.isnan(v):
            continue
        if not started:
            prev = v
            out[t] = v
            started = True
        else:
            prev = alpha * v + (1 - alpha) * prev
            out[t] = prev
    return out


def pine_stoch(source: list[float], high: list[float], low: list[float], length: int) -> list[float]:
    n = len(source)
    out: list[float] = [float("nan")] * n
    for t in range(length - 1, n):
        hh = max(high[t - length + 1 : t + 1])
        ll = min(low[t - length + 1 : t + 1])
        if hh == ll:
            out[t] = float("nan")
        else:
            out[t] = 100.0 * (source[t] - ll) / (hh - ll)
    return out


def fixnan(values: list[float]) -> list[float]:
    out = []
    last = 0.0
    for v in values:
        if math.isnan(v):
            out.append(last)
        else:
            out.append(v)
            last = v
    return out


def compute_stc(
    closes: list[float],
    fast: int = FAST,
    slow: int = SLOW,
    cycle: int = CYCLE,
    d1: int = D1,
    d2: int = D2,
) -> list[float]:
    ema_fast = ema_series(closes, fast)
    ema_slow = ema_series(closes, slow)
    macd = [
        (f - s) if not (math.isnan(f) or math.isnan(s)) else float("nan")
        for f, s in zip(ema_fast, ema_slow)
    ]
    k = fixnan(pine_stoch(macd, macd, macd, cycle))
    d = ema_series(k, d1)
    kd = fixnan(pine_stoch(d, d, d, cycle))
    stc_raw = ema_series(kd, d2)
    return [max(0.0, min(100.0, v)) if not math.isnan(v) else float("nan") for v in stc_raw]


def crossover(series: list[float], level: float, i: int) -> bool:
    if i < 1 or math.isnan(series[i]) or math.isnan(series[i - 1]):
        return False
    return series[i - 1] <= level and series[i] > level


def crossunder(series: list[float], level: float, i: int) -> bool:
    if i < 1 or math.isnan(series[i]) or math.isnan(series[i - 1]):
        return False
    return series[i - 1] >= level and series[i] < level


def stc_signals(stc: list[float], i: int) -> dict[str, bool]:
    return {
        "buy": crossover(stc, LOWER, i),
        "sell": crossunder(stc, UPPER, i),
        "upper_cross": crossover(stc, UPPER, i),
        "upper_crossunder": crossunder(stc, UPPER, i),
        "lower_cross": crossover(stc, LOWER, i),
        "lower_crossunder": crossunder(stc, LOWER, i),
        "rising": i >= 1 and not math.isnan(stc[i]) and not math.isnan(stc[i - 1]) and stc[i] > stc[i - 1],
        "falling": i >= 1 and not math.isnan(stc[i]) and not math.isnan(stc[i - 1]) and stc[i] < stc[i - 1],
    }
