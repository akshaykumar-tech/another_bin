"""Trend + Pullback 4H signal engine (200 EMA, 21 EMA pullback, RSI)."""
from __future__ import annotations

from dataclasses import dataclass

EMA_FAST, EMA_SLOW = 21, 200
RSI_LEN, ATR_LEN = 14, 14
SL_ATR_MULT = 1.5
MAX_HOLD_BARS = 30


@dataclass
class Bar:
    ts: int
    o: float
    h: float
    l: float
    c: float


@dataclass
class Signal:
    sym: str
    signal_side: str
    entry: float
    sl: float
    tp: float
    risk: float
    bar_ts: int
    pull_low: float = 0.0
    pull_high: float = 0.0
    atr_val: float = 0.0


def ema_series(closes: list[float], span: int) -> list[float]:
    if not closes:
        return []
    k = 2 / (span + 1)
    e = closes[0]
    out = [e]
    for c in closes[1:]:
        e = c * k + e * (1 - k)
        out.append(e)
    return out


def rsi(closes: list[float], i: int, n: int = RSI_LEN) -> float:
    if i < n:
        return 50.0
    gains, losses = [], []
    for j in range(i - n + 1, i + 1):
        d = closes[j] - closes[j - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    ag, al = sum(gains) / n, sum(losses) / n
    if al == 0:
        return 100.0
    return 100 - 100 / (1 + ag / al)


def atr(bars: list[Bar], i: int) -> float:
    if i < ATR_LEN:
        return 0.0
    trs = []
    for j in range(i - ATR_LEN + 1, i + 1):
        pc = bars[j - 1].c if j else bars[j].o
        trs.append(max(bars[j].h - bars[j].l, abs(bars[j].h - pc), abs(bars[j].l - pc)))
    return sum(trs) / ATR_LEN


def levels(side: str, entry: float, pull_low: float, pull_high: float, a: float) -> tuple[float, float, float] | None:
    if side == "long":
        sl = pull_low - SL_ATR_MULT * a
        risk = entry - sl
        if risk <= 0:
            return None
        return sl, entry + 2 * risk, risk
    sl = pull_high + SL_ATR_MULT * a
    risk = sl - entry
    if risk <= 0:
        return None
    return sl, entry - 2 * risk, risk


def mirror_side(signal_side: str) -> str:
    return "short" if signal_side == "long" else "long"


def signal_on_closed_bar(sym: str, bars: list[Bar], i: int) -> Signal | None:
    if i < max(EMA_SLOW, RSI_LEN, ATR_LEN) + 2 or i >= len(bars) - 1:
        return None
    closes = [b.c for b in bars]
    e21 = ema_series(closes, EMA_FAST)
    e200 = ema_series(closes, EMA_SLOW)
    px, lo, hi = bars[i].c, bars[i].l, bars[i].h
    a = atr(bars, i)
    r = rsi(closes, i)
    if a <= 0:
        return None
    touched = lo <= e21[i] * 1.003 and hi >= e21[i] * 0.997
    side = None
    if px > e200[i] and touched and px > e21[i] and r > 50:
        side = "long"
    elif px < e200[i] and touched and px < e21[i] and r < 50:
        side = "short"
    if not side:
        return None
    entry = bars[i + 1].o if i + 1 < len(bars) else bars[i].c
    lv = levels(side, entry, lo, hi, a)
    if not lv:
        return None
    sl, tp, risk = lv
    return Signal(sym, side, entry, sl, tp, risk, bars[i].ts, lo, hi, a)


def dry_check_exit(side: str, sl: float, tp: float, hi: float, lo: float) -> tuple[float, str] | None:
    if side == "long":
        if lo <= sl:
            return sl, "sl"
        if hi >= tp:
            return tp, "tp2r"
    else:
        if hi >= sl:
            return sl, "sl"
        if lo <= tp:
            return tp, "tp2r"
    return None


def pnl_usd(side: str, entry: float, exit_px: float, notional: float, fee_rt: float, slip_bps: float = 1.0) -> float:
    slip = slip_bps / 10000.0
    if side == "long":
        e, x = entry * (1 + slip), exit_px * (1 - slip)
        g = (x - e) / e
    else:
        e, x = entry * (1 - slip), exit_px * (1 + slip)
        g = (e - x) / e
    return notional * g - notional * fee_rt * 2
