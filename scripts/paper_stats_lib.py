"""Per-setup dry-paper stats for periodic snapshot logs."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field


@dataclass
class TypeStats:
    signals: int = 0
    skipped: int = 0
    entries: int = 0
    tp: int = 0
    sl: int = 0
    timeout: int = 0
    realized_net: float = 0.0

    def record_exit(self, reason: str, net: float) -> None:
        if reason == "tp":
            self.tp += 1
        elif reason == "sl":
            self.sl += 1
        elif reason == "timeout":
            self.timeout += 1
        self.realized_net += net


class TypeStatsBook:
    def __init__(self) -> None:
        self.by: dict[str, TypeStats] = defaultdict(TypeStats)

    def get(self, key: str) -> TypeStats:
        return self.by[key]

    def format_lines(
        self,
        open_counts: dict[str, int],
        unrealized_by: dict[str, float],
    ) -> list[str]:
        keys = sorted(set(self.by) | set(open_counts) | set(unrealized_by))
        lines: list[str] = []
        for key in keys:
            t = self.by[key]
            open_n = open_counts.get(key, 0)
            unrl = unrealized_by.get(key, 0.0)
            if t.signals == 0 and t.skipped == 0 and t.entries == 0 and open_n == 0:
                if abs(t.realized_net) < 1e-9 and abs(unrl) < 1e-9:
                    continue
            lines.append(
                f"  {key}: sig={t.signals} skip={t.skipped} ent={t.entries} "
                f"tp={t.tp} sl={t.sl} to={t.timeout} open={open_n} "
                f"real=${t.realized_net:+.2f} unrl=${unrl:+.2f}"
            )
        return lines


def unrealized_usd(side: str, entry: float, mark: float, notional: float) -> float:
    if entry <= 0 or mark <= 0:
        return 0.0
    move = (mark - entry) / entry * 100 if side == "long" else (entry - mark) / entry * 100
    return notional * move / 100
