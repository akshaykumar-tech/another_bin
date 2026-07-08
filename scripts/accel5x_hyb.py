"""HYB entry watcher — poll until adverse % hit or open fallback."""
from __future__ import annotations

import asyncio
import json
import time
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable

from accel5x_engine import adverse_hit, adverse_trigger_px, entry_px


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


def fetch_mark(fapi: str, sym: str) -> float:
    url = f"{fapi}/fapi/v1/ticker/price?symbol={sym}"
    req = urllib.request.Request(url, headers={"User-Agent": "accel5x"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return float(json.loads(r.read())["price"])


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

    def load(self) -> None:
        if not self.state_file.is_file():
            return
        try:
            rows = json.loads(self.state_file.read_text())
            self.pending = {r["sym"]: PendingHyb(**r) for r in rows}
        except Exception as e:
            self.log(f"[HYB] state load failed: {e}")

    def clear(self) -> None:
        self.pending.clear()
        if self.state_file.is_file():
            self.state_file.unlink(missing_ok=True)

    def add(self, p: PendingHyb) -> None:
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
            f"open_after={self.open_after_sec}s pending={len(self.pending)}"
        )
        while not self._stop and self.pending:
            now_ms = int(time.time() * 1000)
            for sym in list(self.pending.keys()):
                p = self.pending[sym]
                elapsed_s = (now_ms - p.day_start_ms) / 1000.0
                try:
                    mark = await asyncio.get_running_loop().run_in_executor(
                        None, fetch_mark, self.fapi, sym
                    )
                except Exception as e:
                    self.log(f"[HYB] {sym} mark fail: {e}")
                    continue

                if adverse_hit(mark, p.ref, p.side, self.adverse_pct):
                    raw = adverse_trigger_px(p.ref, p.side, self.adverse_pct)
                    ent = entry_px(raw, p.side, self.slip_bps)
                    self._fill(p, ent, "HYB_ADV", raw=raw, mark=mark)
                    continue

                if elapsed_s >= self.open_after_sec:
                    raw = p.ref
                    ent = entry_px(raw, p.side, self.slip_bps)
                    self._fill(p, ent, "HYB_OPEN", raw=raw, mark=mark)

            if self.pending:
                await asyncio.sleep(self.poll_sec)

    def _fill(self, p: PendingHyb, ent: float, tag: str, *, raw: float, mark: float) -> None:
        self.pending.pop(p.sym, None)
        self.save()
        self.log(
            f"[HYB_FILL] {p.sym} {p.side.upper()} {tag} ref={p.ref:.6f} "
            f"raw={raw:.6f} mark={mark:.6f} fill={ent:.6f}"
        )
        self.on_fill(p, ent, tag)
