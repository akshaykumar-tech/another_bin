"""Strong Pullback Signals engine — Pine parity (trading1.log)."""
from __future__ import annotations

from dataclasses import dataclass, field

H4_MS = 4 * 3_600_000

# Pine defaults
FAST_LEN, SLOW_LEN, PULL_LEN, SLOPE_LOOK = 34, 144, 21, 5
BREAK_LOOK, MIN_WAIT, MAX_HUNT = 20, 2, 40
MIN_BREAK_BODY = 0.20
USE_COOLDOWN, COOLDOWN_BARS = True, 10
ATR_LEN, ENTRY_DEPTH = 14, 0.40
USE_HTF, HTF_EMA_LEN = True, 50
SL_BUF, MAX_RISK_ATR, MIN_RISK_ATR = 0.30, 2.5, 0.5
TP1_R, TP2_R, TP3_R = 1.0, 2.0, 3.0


@dataclass
class Bar:
    ts: int
    o: float
    h: float
    l: float
    c: float
    v: float = 0.0


@dataclass
class ActiveTrade:
    side: int
    entry_ts: int
    entry: float
    sl: float
    tp1: float
    tp2: float
    tp3: float
    risk: float
    best_stage: int = 0


@dataclass
class SymbolState:
    bars: list[Bar] = field(default_factory=list)
    bars_4h: list[Bar] = field(default_factory=list)
    armed: bool = False
    adir: int = 0
    arm_i: int = -1
    swing_ext: float = 0.0
    last_sig_i: int = -1
    trade: ActiveTrade | None = None


def ema_series(vals: list[float], length: int) -> list[float]:
    if not vals:
        return []
    k = 2.0 / (length + 1)
    out = [vals[0]]
    for v in vals[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def atr_series(bars: list[Bar], length: int) -> list[float]:
    out = [0.0] * len(bars)
    if len(bars) < 2:
        return out
    trs: list[float] = []
    for i in range(1, len(bars)):
        prev = bars[i - 1].c
        b = bars[i]
        trs.append(max(b.h - b.l, abs(b.h - prev), abs(b.l - prev)))
    if len(trs) < length:
        return out
    rma = sum(trs[:length]) / length
    out[length] = rma
    for j in range(length + 1, len(bars)):
        tr = trs[j - 1]
        rma = (rma * (length - 1) + tr) / length
        out[j] = rma
    return out


def htf_ema_at(h4: list[Bar], ts: int) -> float | None:
    closed = [b for b in h4 if b.ts + H4_MS <= ts]
    if len(closed) < HTF_EMA_LEN:
        return None
    return ema_series([b.c for b in closed], HTF_EMA_LEN)[-1]


def min_start_i() -> int:
    return max(SLOW_LEN, BREAK_LOOK, ATR_LEN) + SLOPE_LOOK + 2


@dataclass
class EntrySignal:
    side: int
    entry: float
    sl: float
    tp1: float
    tp2: float
    tp3: float
    risk: float
    ts: int


@dataclass
class ExitSignal:
    exit_px: float
    reason: str
    res_r: float
    ts: int


def check_exit(trade: ActiveTrade, hi: float, lo: float, ts: int) -> ExitSignal | None:
    side = trade.side
    tp1_hit = hi >= trade.tp1 if side == 1 else lo <= trade.tp1
    tp2_hit = hi >= trade.tp2 if side == 1 else lo <= trade.tp2
    tp3_hit = hi >= trade.tp3 if side == 1 else lo <= trade.tp3
    sl_hit = lo <= trade.sl if side == 1 else hi >= trade.sl
    stage = 3 if tp3_hit else 2 if tp2_hit else 1 if tp1_hit else 0
    same_bar_sl = sl_hit and stage > 0
    best = trade.best_stage
    if stage > best and not same_bar_sl:
        best = stage
        trade.best_stage = best
    final_tp3 = not same_bar_sl and best == 3 and tp3_hit
    final_sl = sl_hit and (best == 0 or same_bar_sl)
    final_tp = sl_hit and not same_bar_sl and best > 0 and best < 3
    if final_tp3:
        return ExitSignal(trade.tp3, "tp3", TP3_R, ts)
    if final_tp:
        px = trade.tp2 if best == 2 else trade.tp1
        r = TP2_R if best == 2 else TP1_R
        return ExitSignal(px, f"tp{best}", r, ts)
    if final_sl:
        return ExitSignal(trade.sl, "sl", -1.0, ts)
    return None


def on_closed_bar(st: SymbolState, bar: Bar, *, allow_entry: bool) -> EntrySignal | None:
    """Append closed bar and return entry signal if any."""
    if st.bars and st.bars[-1].ts == bar.ts:
        return None
    st.bars.append(bar)
    i = len(st.bars) - 1
    if i < min_start_i():
        return None

    closes = [b.c for b in st.bars]
    fast = ema_series(closes, FAST_LEN)
    slow = ema_series(closes, SLOW_LEN)
    pull = ema_series(closes, PULL_LEN)
    atrs = atr_series(st.bars, ATR_LEN)

    b = st.bars[i]
    fa, sl_e, pl, a = fast[i], slow[i], pull[i], atrs[i]
    if a <= 0:
        return None

    bull_trend = fa > sl_e and b.c > sl_e and fa > fast[i - SLOPE_LOOK]
    bear_trend = fa < sl_e and b.c < sl_e and fa < fast[i - SLOPE_LOOK]
    hi_before = max(st.bars[j].h for j in range(i - BREAK_LOOK, i))
    lo_before = min(st.bars[j].l for j in range(i - BREAK_LOOK, i))
    body = abs(b.c - b.o)
    bull_break = bull_trend and b.c > hi_before and b.c > b.o and body >= a * MIN_BREAK_BODY
    bear_break = bear_trend and b.c < lo_before and b.c < b.o and body >= a * MIN_BREAK_BODY
    cooldown_ok = not USE_COOLDOWN or st.last_sig_i < 0 or (i - st.last_sig_i >= COOLDOWN_BARS)

    if (bull_break or bear_break) and not st.armed and cooldown_ok:
        st.armed = True
        st.adir = 1 if bull_break else -1
        st.arm_i = i
        st.swing_ext = b.l if bull_break else b.h

    if st.armed:
        st.swing_ext = min(st.swing_ext, b.l) if st.adir == 1 else max(st.swing_ext, b.h)

    age = i - st.arm_i if st.armed else 0
    expire = st.armed and age > MAX_HUNT
    flip = st.armed and ((st.adir == 1 and not bull_trend) or (st.adir == -1 and not bear_trend))
    limit_px = pl - ENTRY_DEPTH * a * st.adir if st.armed and st.adir else 0.0
    fill_now = st.armed and age >= MIN_WAIT and (b.l <= limit_px if st.adir == 1 else b.h >= limit_px)
    bar_ms = st.bars[i].ts - st.bars[i - 1].ts if i > 0 else 3_600_000
    htf = htf_ema_at(st.bars_4h, b.ts + bar_ms)
    htf_ok = not USE_HTF or htf is None or (st.adir == 1 and b.c > htf) or (st.adir == -1 and b.c < htf)
    trend_ok = (st.adir == 1 and b.c > sl_e) or (st.adir == -1 and b.c < sl_e)
    gates = htf_ok and trend_ok
    can_open = st.trade is None and st.armed
    take_sig = fill_now and can_open and gates
    sig_dir = st.adir if take_sig else 0

    if st.armed and (fill_now or expire or flip):
        st.armed = False
        st.adir = 0

    if not take_sig or not allow_entry:
        return None

    is_long = sig_dir == 1
    side = 1 if is_long else -1
    fill_px = min(b.o, limit_px) if is_long else max(b.o, limit_px)
    raw_stop = st.swing_ext - a * SL_BUF if is_long else st.swing_ext + a * SL_BUF
    risk0 = abs(fill_px - raw_stop)
    risk = min(max(risk0, a * MIN_RISK_ATR), a * MAX_RISK_ATR)
    stopv = fill_px - risk if is_long else fill_px + risk
    t1 = fill_px + risk * TP1_R if is_long else fill_px - risk * TP1_R
    t2 = fill_px + risk * TP2_R if is_long else fill_px - risk * TP2_R
    t3 = fill_px + risk * TP3_R if is_long else fill_px - risk * TP3_R

    st.last_sig_i = i
    st.trade = ActiveTrade(side, b.ts, fill_px, stopv, t1, t2, t3, risk)
    return EntrySignal(side, fill_px, stopv, t1, t2, t3, risk, b.ts)
