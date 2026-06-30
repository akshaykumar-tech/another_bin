"""God-mode S/R fakeout + breakout + retest — incremental 5m logic."""
from __future__ import annotations

from dataclasses import dataclass

from sr_chartprime_lib import Bar, atr, pivotlow, pivothigh


@dataclass
class GodSignal:
    side: str
    setup: str
    entry_px: float


@dataclass
class PendingRetest:
    side: str
    level: float
    bars: int = 0


@dataclass
class GodModeState:
    support: float | None = None
    resistance: float | None = None
    pending: PendingRetest | None = None


class GodModeTracker:
    def __init__(
        self,
        lookback: int = 15,
        vol_ma: int = 20,
        vol_spike: float = 1.4,
        wick_ratio: float = 0.55,
        skip_breakdown: bool = True,
    ) -> None:
        self.lookback = lookback
        self.vol_ma = vol_ma
        self.vol_spike = vol_spike
        self.wick_ratio = wick_ratio
        self.skip_breakdown = skip_breakdown
        self.bars: list[Bar] = []
        self.st = GodModeState()

    def load_history(self, bars: list[Bar]) -> None:
        self.bars.clear()
        self.st = GodModeState()
        for b in bars:
            self._step(b, emit=False)

    def on_bar(self, bar: Bar) -> GodSignal | None:
        if self.bars and self.bars[-1].ts == bar.ts:
            return None
        return self._step(bar, emit=True)

    def _vol_ma(self, i: int) -> float:
        vols = [b.v for b in self.bars[max(0, i - self.vol_ma + 1) : i + 1]]
        return sum(vols) / len(vols) if vols else self.bars[i].v

    @staticmethod
    def _wick_up(b: Bar) -> float:
        rng = b.h - b.l
        return (b.h - max(b.o, b.c)) / rng if rng > 0 else 0.0

    @staticmethod
    def _wick_dn(b: Bar) -> float:
        rng = b.h - b.l
        return (min(b.o, b.c) - b.l) / rng if rng > 0 else 0.0

    def _update_levels(self, i: int, b: Bar, vma: float) -> float:
        highs = [x.h for x in self.bars]
        lows = [x.l for x in self.bars]
        pl = pivotlow(lows, self.lookback, i)
        ph = pivothigh(highs, self.lookback, i)
        zone_w = atr(self.bars, 14, i) * 0.8
        if pl is not None and b.v > vma * self.vol_spike:
            self.st.support = pl
        if ph is not None and b.v > vma * self.vol_spike:
            self.st.resistance = ph
        return zone_w

    def _step(self, bar: Bar, emit: bool) -> GodSignal | None:
        self.bars.append(bar)
        i = len(self.bars) - 1
        if i < 1:
            return None

        b = self.bars[i]
        prev = self.bars[i - 1]
        vma = self._vol_ma(i)
        zone_w = self._update_levels(i, b, vma)
        support, resistance = self.st.support, self.st.resistance
        sig: GodSignal | None = None

        if resistance and b.h > resistance and b.c < resistance and self._wick_up(b) >= self.wick_ratio and b.v > vma * self.vol_spike:
            sig = GodSignal("short", "fakeout_res", b.c)
        elif support and b.l < support and b.c > support and self._wick_dn(b) >= self.wick_ratio and b.v > vma * self.vol_spike:
            sig = GodSignal("long", "fakeout_sup", b.c)
        elif resistance and prev.c <= resistance and b.c > resistance + zone_w * 0.15 and b.c > b.o and b.v > vma * self.vol_spike:
            self.st.pending = PendingRetest("long", resistance)
            sig = GodSignal("long", "breakout_res", b.c)
        elif (
            not self.skip_breakdown
            and support
            and prev.c >= support
            and b.c < support - zone_w * 0.15
            and b.c < b.o
            and b.v > vma * self.vol_spike
        ):
            self.st.pending = PendingRetest("short", support)
            sig = GodSignal("short", "breakdown_sup", b.c)
        elif self.st.pending:
            self.st.pending.bars += 1
            p = self.st.pending
            if p.bars <= 6:
                lvl = p.level
                if p.side == "long" and b.l <= lvl + zone_w * 0.2 and b.c > lvl and self._wick_dn(b) > 0.4:
                    sig = GodSignal("long", "retest_res", b.c)
                    self.st.pending = None
                elif p.side == "short" and b.h >= lvl - zone_w * 0.2 and b.c < lvl and self._wick_up(b) > 0.4:
                    sig = GodSignal("short", "retest_sup", b.c)
                    self.st.pending = None
            if self.st.pending and self.st.pending.bars > 6:
                self.st.pending = None

        return sig if emit else None
