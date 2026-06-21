"""ChartPrime Support/Resistance (High Volume Boxes) — Pine v5 logic."""
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


def delta_volume(bars: list[Bar], i: int, is_buy: bool) -> tuple[float, bool]:
    """Pine upAndDownVolume: +vol on buy bar, -vol on sell bar; doji keeps prior side."""
    b = bars[i]
    if b.c > b.o:
        is_buy = True
    elif b.c < b.o:
        is_buy = False
    vol = b.v if is_buy else -b.v
    return vol, is_buy


def atr(bars: list[Bar], period: int, i: int) -> float:
    if i < 1:
        return 0.0
    start = max(1, i - period + 1)
    trs: list[float] = []
    for j in range(start, i + 1):
        hi, lo = bars[j].h, bars[j].l
        prev_c = bars[j - 1].c
        trs.append(max(hi - lo, abs(hi - prev_c), abs(lo - prev_c)))
    return sum(trs) / len(trs) if trs else 0.0


def highest(vals: list[float], length: int, i: int) -> float:
    start = max(0, i - length + 1)
    chunk = vals[start : i + 1]
    return max(chunk) if chunk else 0.0


def lowest(vals: list[float], length: int, i: int) -> float:
    start = max(0, i - length + 1)
    chunk = vals[start : i + 1]
    return min(chunk) if chunk else 0.0


def pivothigh(highs: list[float], lb: int, i: int) -> float | None:
    p = i - lb
    if p < lb or i < 2 * lb:
        return None
    h = highs[p]
    for j in range(p - lb, p + lb + 1):
        if j != p and highs[j] > h:
            return None
    return h


def pivotlow(lows: list[float], lb: int, i: int) -> float | None:
    p = i - lb
    if p < lb or i < 2 * lb:
        return None
    lo = lows[p]
    for j in range(p - lb, p + lb + 1):
        if j != p and lows[j] < lo:
            return None
    return lo


def crossover(cur: float, prev_cur: float, level: float, prev_level: float) -> bool:
    return prev_cur <= prev_level and cur > level


def crossunder(cur: float, prev_cur: float, level: float, prev_level: float) -> bool:
    return prev_cur >= prev_level and cur < level


@dataclass
class SRState:
    support: float | None = None
    support_1: float | None = None
    resistance: float | None = None
    resistance_1: float | None = None
    res_is_sup: bool = False
    sup_is_res: bool = False


@dataclass
class SRSignals:
    sup_holds: bool = False
    res_holds: bool = False
    breakout_res: bool = False
    breakout_sup: bool = False
    res_as_sup_holds: bool = False
    sup_as_res_holds: bool = False
    break_res_fresh: bool = False
    break_sup_fresh: bool = False


def compute_sr_signals(
    bars: list[Bar],
    lookback: int = 20,
    vol_len: int = 2,
    box_width_mult: float = 1.0,
) -> list[SRSignals]:
    n = len(bars)
    vols: list[float] = []
    is_buy = True
    for i in range(n):
        v, is_buy = delta_volume(bars, i, is_buy)
        vols.append(v)

    st = SRState()
    out: list[SRSignals] = []
    highs = [b.h for b in bars]
    lows = [b.l for b in bars]

    for i in range(n):
        sig = SRSignals()
        if i > 0:
            prev_h, prev_l = bars[i - 1].h, bars[i - 1].l
            hi = bars[i].h
            lo = bars[i].l
            vol = vols[i]
            vol_scaled = vol / 2.5
            vol_hi = highest([v / 2.5 for v in vols], vol_len, i)
            vol_lo = lowest([v / 2.5 for v in vols], vol_len, i)
            width = atr(bars, 200, i) * box_width_mult

            ph = pivothigh(highs, lookback, i)
            pl = pivotlow(lows, lookback, i)

            if pl is not None and vol > vol_hi:
                st.support = pl
                st.support_1 = pl - width

            if ph is not None and vol < vol_lo:
                st.resistance = ph
                st.resistance_1 = ph + width

            prev_res_is_sup = st.res_is_sup
            prev_sup_is_res = st.sup_is_res

            if st.resistance_1 is not None:
                sig.breakout_res = crossover(lo, prev_l, st.resistance_1, st.resistance_1)
            if st.resistance is not None:
                sig.res_holds = crossunder(hi, prev_h, st.resistance, st.resistance)

            if st.support is not None:
                sig.sup_holds = crossover(lo, prev_l, st.support, st.support)
            if st.support_1 is not None:
                sig.breakout_sup = crossunder(hi, prev_h, st.support_1, st.support_1)

            if sig.breakout_res:
                st.res_is_sup = True
            if sig.res_holds:
                st.res_is_sup = False
            if sig.breakout_sup:
                st.sup_is_res = True
            if sig.sup_holds:
                st.sup_is_res = False

            sig.res_as_sup_holds = sig.breakout_res and prev_res_is_sup
            sig.sup_as_res_holds = sig.breakout_sup and prev_sup_is_res
            sig.break_res_fresh = sig.breakout_res and not prev_res_is_sup
            sig.break_sup_fresh = sig.breakout_sup and not prev_sup_is_res

        out.append(sig)
    return out


class SRTracker:
    """Incremental bar-by-bar SR state (live paper)."""

    def __init__(self, lookback: int = 20, vol_len: int = 2, box_width_mult: float = 1.0) -> None:
        self.lookback = lookback
        self.vol_len = vol_len
        self.box_width_mult = box_width_mult
        self.bars: list[Bar] = []
        self.vols: list[float] = []
        self.is_buy = True
        self.st = SRState()

    def load_history(self, bars: list[Bar]) -> None:
        self.bars.clear()
        self.vols.clear()
        self.is_buy = True
        self.st = SRState()
        for b in bars:
            self._append(b)

    def on_bar(self, bar: Bar) -> SRSignals:
        if self.bars and self.bars[-1].ts == bar.ts:
            return SRSignals()
        self._append(bar)
        return self._signal_at(len(self.bars) - 1)

    def _append(self, bar: Bar) -> None:
        self.bars.append(bar)
        v, self.is_buy = delta_volume(self.bars, len(self.bars) - 1, self.is_buy)
        self.vols.append(v)

    def _signal_at(self, i: int) -> SRSignals:
        sig = SRSignals()
        if i < 1:
            return sig

        prev_h, prev_l = self.bars[i - 1].h, self.bars[i - 1].l
        hi, lo = self.bars[i].h, self.bars[i].l
        vol = self.vols[i]
        vol_hi = highest([v / 2.5 for v in self.vols], self.vol_len, i)
        vol_lo = lowest([v / 2.5 for v in self.vols], self.vol_len, i)
        width = atr(self.bars, 200, i) * self.box_width_mult
        highs = [b.h for b in self.bars]
        lows = [b.l for b in self.bars]

        ph = pivothigh(highs, self.lookback, i)
        pl = pivotlow(lows, self.lookback, i)

        if pl is not None and vol > vol_hi:
            self.st.support = pl
            self.st.support_1 = pl - width

        if ph is not None and vol < vol_lo:
            self.st.resistance = ph
            self.st.resistance_1 = ph + width

        prev_res_is_sup = self.st.res_is_sup
        prev_sup_is_res = self.st.sup_is_res

        if self.st.resistance_1 is not None:
            sig.breakout_res = crossover(lo, prev_l, self.st.resistance_1, self.st.resistance_1)
        if self.st.resistance is not None:
            sig.res_holds = crossunder(hi, prev_h, self.st.resistance, self.st.resistance)
        if self.st.support is not None:
            sig.sup_holds = crossover(lo, prev_l, self.st.support, self.st.support)
        if self.st.support_1 is not None:
            sig.breakout_sup = crossunder(hi, prev_h, self.st.support_1, self.st.support_1)

        if sig.breakout_res:
            self.st.res_is_sup = True
        if sig.res_holds:
            self.st.res_is_sup = False
        if sig.breakout_sup:
            self.st.sup_is_res = True
        if sig.sup_holds:
            self.st.sup_is_res = False

        sig.res_as_sup_holds = sig.breakout_res and prev_res_is_sup
        sig.sup_as_res_holds = sig.breakout_sup and prev_sup_is_res
        sig.break_res_fresh = sig.breakout_res and not prev_res_is_sup
        sig.break_sup_fresh = sig.breakout_sup and not prev_sup_is_res
        return sig


def entry_side(sig: SRSignals, mode: str = "all") -> str | None:
    """Return 'long' / 'short' for signal bar, or None."""
    if mode == "holds":
        if sig.sup_holds or sig.res_as_sup_holds:
            return "long"
        if sig.res_holds or sig.sup_as_res_holds:
            return "short"
        return None
    if mode == "breaks":
        if sig.break_res_fresh:
            return "long"
        if sig.break_sup_fresh:
            return "short"
        return None
    # all plotchar + break labels
    if sig.sup_holds or sig.break_res_fresh or sig.res_as_sup_holds:
        return "long"
    if sig.res_holds or sig.break_sup_fresh or sig.sup_as_res_holds:
        return "short"
    return None


def signal_name(sig: SRSignals) -> str:
    if sig.sup_holds:
        return "sup_holds"
    if sig.res_holds:
        return "res_holds"
    if sig.res_as_sup_holds:
        return "res_as_sup"
    if sig.sup_as_res_holds:
        return "sup_as_res"
    if sig.break_res_fresh:
        return "break_res"
    if sig.break_sup_fresh:
        return "break_sup"
    return "unknown"
