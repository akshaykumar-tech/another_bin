"""HYB entry watcher — once-touch adverse latch + timed open fallback."""
from __future__ import annotations

import asyncio
import json
import time
import urllib.request
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Callable

from accel5x_engine import adverse_touched, adverse_trigger_px, entry_px


@dataclass
class PendingHyb:
    sym: str
    side: str
    signal_side: str
    ref: float
    entry_day: str
    mult: float
    base_pct: float
    prev_pct: float
    btc_prev_green: bool | None
    day_start_ms: int
    watch_high: float = 0.0
    watch_low: float = 0.0
    adverse_latched: bool = False


def fetch_mark(fapi: str, sym: str) -> float:
    url = f"{fapi}/fapi/v1/ticker/price?symbol={sym}"
    req = urllib.request.Request(url, headers={"User-Agent": "accel5x"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return float(json.loads(r.read())["price"])


def fetch_today_ohlc(fapi: str, sym: str) -> tuple[float, float, float, float]:
    """Current UTC daily candle (running H/L since day open)."""
    url = f"{fapi}/fapi/v1/klines?symbol={sym}&interval=1d&limit=1"
    req = urllib.request.Request(url, headers={"User-Agent": "accel5x"})
    with urllib.request.urlopen(req, timeout=30) as r:
        row = json.loads(r.read())[0]
    return float(row[1]), float(row[2]), float(row[3]), float(row[4])


class HybEntryWatcher:
    def __init__(
        self,
        fapi: str,
        *,
        adverse_pct: float,
        poll_sec: float,
        open_after_sec: float,
        slip_bps: float,
        log: Callable[[str], None],
        on_fill: Callable[[PendingHyb, float, str], None],
        state_file: Path,
    ) -> None:
        self.fapi = fapi.rstrip("/")
        self.adverse_pct = adverse_pct
        self.poll_sec = max(5.0, poll_sec)
        self.open_after_sec = max(0.0, open_after_sec)
        self.slip_bps = slip_bps
        self.log = log
        self.on_fill = on_fill
        self.state_file = state_file
        self.pending: dict[str, PendingHyb] = {}
        self._task: asyncio.Task | None = None
        self._stop = False

    def save(self) -> None:
        self.state_file.write_text(
            json.dumps([asdict(p) for p in self.pending.values()], indent=2)
        )

    def _coerce_pending(self, row: dict) -> PendingHyb:
        row.setdefault("watch_high", row.get("ref", 0.0))
        row.setdefault("watch_low", row.get("ref", 0.0))
        row.setdefault("adverse_latched", False)
        return PendingHyb(**row)

    def load(self) -> None:
        if not self.state_file.is_file():
            return
        try:
            rows = json.loads(self.state_file.read_text())
            self.pending = {r["sym"]: self._coerce_pending(r) for r in rows}
        except Exception as e:
            self.log(f"[HYB] state load failed: {e}")

    def clear(self) -> None:
        self.pending.clear()
        if self.state_file.is_file():
            self.state_file.unlink(missing_ok=True)

    def _seed_extremes(self, p: PendingHyb) -> None:
        try:
            _, day_high, day_low, _ = fetch_today_ohlc(self.fapi, p.sym)
            mark = fetch_mark(self.fapi, p.sym)
        except Exception:
            day_high = day_low = mark = p.ref
        hi = max(p.ref, day_high, mark)
        lo = min(p.ref, day_low, mark)
        p.watch_high = hi
        p.watch_low = lo
        if adverse_touched(hi, lo, p.ref, p.side, self.adverse_pct):
            p.adverse_latched = True

    def add(self, p: PendingHyb) -> None:
        self._seed_extremes(p)
        self.pending[p.sym] = p
        self.save()

    def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop = False
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._stop = True
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def _loop(self) -> None:
        self.log(
            f"[HYB] watcher adverse={self.adverse_pct}% poll={self.poll_sec}s "
            f"open_after={self.open_after_sec}s pending={len(self.pending)} "
            f"(once-touch latch + daily H/L)"
        )
        while not self._stop and self.pending:
            now_ms = int(time.time() * 1000)
            for sym in list(self.pending.keys()):
                p = self.pending[sym]
                elapsed_s = (now_ms - p.day_start_ms) / 1000.0
                loop = asyncio.get_running_loop()
                try:
                    mark, ohlc = await asyncio.gather(
                        loop.run_in_executor(None, fetch_mark, self.fapi, sym),
                        loop.run_in_executor(None, fetch_today_ohlc, self.fapi, sym),
                    )
                except Exception as e:
                    self.log(f"[HYB] {sym} quote fail: {e}")
                    continue

                _, day_high, day_low, _ = ohlc
                p.watch_high = max(p.watch_high, p.ref, day_high, mark)
                lows = [x for x in (p.watch_low, p.ref, day_low, mark) if x > 0]
                p.watch_low = min(lows) if lows else p.ref

                if adverse_touched(
                    p.watch_high, p.watch_low, p.ref, p.side, self.adverse_pct
                ):
                    p.adverse_latched = True

                if p.adverse_latched:
                    raw = adverse_trigger_px(p.ref, p.side, self.adverse_pct)
                    ent = entry_px(raw, p.side, self.slip_bps)
                    self._fill(p, ent, "HYB_ADV", raw=raw, mark=mark)
                    continue

                if elapsed_s >= self.open_after_sec:
                    raw = mark if mark > 0 else p.ref
                    ent = entry_px(raw, p.side, self.slip_bps)
                    self._fill(p, ent, "HYB_OPEN", raw=raw, mark=mark)

            if self.pending:
                self.save()
                await asyncio.sleep(self.poll_sec)

    def _fill(self, p: PendingHyb, ent: float, tag: str, *, raw: float, mark: float) -> None:
        self.pending.pop(p.sym, None)
        self.save()
        self.log(
            f"[HYB_FILL] {p.sym} {p.side.upper()} {tag} ref={p.ref:.6f} "
            f"raw={raw:.6f} mark={mark:.6f} fill={ent:.6f} "
            f"hi={p.watch_high:.6f} lo={p.watch_low:.6f}"
        )
        self.on_fill(p, ent, tag)
