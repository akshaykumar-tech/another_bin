"""BTC 5m correlation buckets for dry-paper stats (dependent vs independent symbols)."""
from __future__ import annotations

import json
import math
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from urllib.parse import quote

from paper_stats_lib import unrealized_usd


@dataclass
class BtcBucketStats:
    ent: int = 0
    tp: int = 0
    sl: int = 0
    to: int = 0
    real: float = 0.0

    def record_exit(self, reason: str, net: float) -> None:
        if reason == "tp":
            self.tp += 1
        elif reason == "sl":
            self.sl += 1
        elif reason == "timeout":
            self.to += 1
        self.real += net


class BtcBucketBook:
    """Track realized stats for btc_dependent (corr≥linked_min) vs btc_independent (corr<indep_max)."""

    def __init__(self, linked_min: float = 0.60, indep_max: float = 0.40) -> None:
        self.linked_min = linked_min
        self.indep_max = indep_max
        self.corr: dict[str, float | None] = {}
        self.dependent = BtcBucketStats()
        self.independent = BtcBucketStats()

    def bucket(self, sym: str) -> str | None:
        c = self.corr.get(sym)
        if c is None:
            return None
        if c >= self.linked_min:
            return "btc_dependent"
        if c < self.indep_max:
            return "btc_independent"
        return None

    def is_btc_independent(self, sym: str) -> bool:
        return self.bucket(sym) == "btc_independent"

    def corr_str(self, sym: str) -> str:
        c = self.corr.get(sym)
        return f"{c:.2f}" if c is not None else "n/a"

    def _stats(self, bucket: str) -> BtcBucketStats:
        return self.dependent if bucket == "btc_dependent" else self.independent

    def record_entry(self, sym: str) -> None:
        b = self.bucket(sym)
        if b:
            self._stats(b).ent += 1

    def record_exit(self, sym: str, reason: str, net: float) -> None:
        b = self.bucket(sym)
        if b:
            self._stats(b).record_exit(reason, net)

    def format_lines(
        self,
        open_dep: int,
        open_indep: int,
        unrl_dep: float,
        unrl_indep: float,
    ) -> list[str]:
        lines: list[str] = []
        for key, label, opn, unrl in (
            ("btc_dependent", "btc_dependent", open_dep, unrl_dep),
            ("btc_independent", "btc_independent", open_indep, unrl_indep),
        ):
            s = self._stats(key)
            lines.append(
                f"  {label}: ent={s.ent} tp={s.tp} sl={s.sl} to={s.to} open={opn} "
                f"real=${s.real:+.2f} unrl=${unrl:+.2f}"
            )
        return lines


def _fetch_closes(fapi: str, symbol: str, interval: str, limit: int) -> dict[int, float]:
    url = f"{fapi}/fapi/v1/klines?symbol={quote(symbol)}&interval={interval}&limit={limit}"
    with urllib.request.urlopen(url, timeout=20) as r:
        rows = json.loads(r.read())
    return {int(row[0]): float(row[4]) for row in rows}


def _corr_from_closes(btc: dict[int, float], sym: dict[int, float]) -> float | None:
    common = sorted(set(btc) & set(sym))
    if len(common) < 100:
        return None
    bpx = [btc[t] for t in common]
    px = [sym[t] for t in common]
    brets = [math.log(bpx[i] / bpx[i - 1]) for i in range(1, len(bpx))]
    rets = [math.log(px[i] / px[i - 1]) for i in range(1, len(px))]
    n = len(rets)
    mb, ms = sum(brets) / n, sum(rets) / n
    cov = sum((brets[i] - mb) * (rets[i] - ms) for i in range(n)) / n
    vb = sum((brets[i] - mb) ** 2 for i in range(n)) / n
    vs = sum((rets[i] - ms) ** 2 for i in range(n)) / n
    if vb <= 0 or vs <= 0:
        return None
    return cov / math.sqrt(vb * vs)


def _one_corr(fapi: str, sym: str, interval: str, limit: int, btc: dict[int, float]) -> tuple[str, float | None]:
    try:
        mp = _fetch_closes(fapi, sym, interval, limit)
        return sym, _corr_from_closes(btc, mp)
    except Exception:
        return sym, None


def load_btc_corr_map(
    symbols: list[str],
    fapi: str,
    interval: str = "5m",
    limit: int = 500,
    workers: int = 12,
) -> dict[str, float | None]:
    """Pearson correlation of log returns vs BTCUSDT (same interval, last `limit` bars)."""
    btc = _fetch_closes(fapi, "BTCUSDT", interval, limit)
    out: dict[str, float | None] = {}
    syms = [s for s in symbols if s != "BTCUSDT"]
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_one_corr, fapi, s, interval, limit, btc): s for s in syms}
        for fut in as_completed(futs):
            sym, c = fut.result()
            out[sym] = c
            time.sleep(0.02)
    return out


def open_unrl_by_bucket(
    book: BtcBucketBook,
    opens: list[tuple[str, str, float, float]],
    notional: float,
) -> tuple[int, int, float, float]:
    """opens: (symbol, side, entry_px, mark_px) → (open_dep, open_indep, unrl_dep, unrl_indep)."""
    open_dep = open_indep = 0
    unrl_dep = unrl_indep = 0.0
    for sym, side, entry, mark in opens:
        b = book.bucket(sym)
        if not b:
            continue
        u = unrealized_usd(side, entry, mark, notional)
        if b == "btc_dependent":
            open_dep += 1
            unrl_dep += u
        else:
            open_indep += 1
            unrl_indep += u
    return open_dep, open_indep, unrl_dep, unrl_indep
