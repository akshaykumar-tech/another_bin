#!/usr/bin/env python3
"""
Scan one UTC day of Binance USD-M aggTrades (Vision daily ZIP) for extreme 1-second moves.

Metrics (calendar 1s buckets, UTC second aligned):
  - extreme_net_1s: |close-open|/open >= min_move_pct
  - extreme_amp_1s: max(up_wick, down_wick) >= min_move_pct
  - reversal_1s: up_wick >= min AND down_wick >= min in the same second (6% up + 6% down from open)

Also reports rolling 1s window (bot-style): last 1000ms from each aggTrade tick.
"""

from __future__ import annotations

import argparse
import io
import json
import subprocess
import zipfile
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

VISION_TMPL = (
    "https://data.binance.vision/data/futures/um/daily/aggTrades/"
    "{symbol}/{symbol}-aggTrades-{date}.zip"
)


def pct(a: float, b: float) -> float:
    if a <= 0:
        return 0.0
    return (b - a) / a * 100.0


def download_zip(url: str) -> Optional[bytes]:
    proc = subprocess.run(
        ["curl", "-sfL", "--max-time", "120", url],
        capture_output=True,
    )
    if proc.returncode != 0:
        return None
    return proc.stdout


def iter_agg_trades(data: bytes) -> List[Tuple[int, float, float]]:
    """Return (T_ms, price, quote_notional) sorted by time."""
    rows: List[Tuple[int, float, float]] = []
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        names = [n for n in zf.namelist() if n.endswith(".csv")]
        if not names:
            return rows
        text = zf.read(names[0]).decode("utf-8", errors="replace")
    for line in text.splitlines():
        if not line.strip():
            continue
        parts = line.split(",")
        if len(parts) < 6:
            continue
        try:
            px = float(parts[1])
            qty = float(parts[2])
            ts = int(parts[5])
        except ValueError:
            continue
        if px <= 0:
            continue
        rows.append((ts, px, px * qty))
    rows.sort(key=lambda x: x[0])
    return rows


@dataclass
class SecBar:
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    notional: float = 0.0
    trades: int = 0

    def add(self, px: float, n: float) -> None:
        if self.trades == 0:
            self.open = self.high = self.low = self.close = px
        else:
            if px > self.high:
                self.high = px
            if px < self.low:
                self.low = px
            self.close = px
        self.notional += n
        self.trades += 1


def analyze_calendar_seconds(
    trades: List[Tuple[int, float, float]],
    min_move: float,
    max_move: float,
) -> Tuple[Counter, List[dict]]:
    buckets: Dict[int, SecBar] = {}
    for ts, px, n in trades:
        sec = (ts // 1000) * 1000
        b = buckets.get(sec)
        if b is None:
            b = SecBar()
            buckets[sec] = b
        b.add(px, n)

    counts: Counter = Counter()
    events: List[dict] = []
    for sec, b in sorted(buckets.items()):
        if b.trades < 2 or b.open <= 0 or b.low <= 0:
            continue
        counts["bars_1s"] += 1
        up_wick = max(0.0, pct(b.open, b.high))
        dn_wick = max(0.0, pct(b.low, b.open))
        net = pct(b.open, b.close)
        amp = max(up_wick, dn_wick)

        in_net = min_move <= abs(net) <= max_move if max_move > 0 else abs(net) >= min_move
        in_amp = min_move <= amp <= max_move if max_move > 0 else amp >= min_move
        in_rev = up_wick >= min_move and dn_wick >= min_move
        if max_move > 0:
            in_rev = in_rev and up_wick <= max_move and dn_wick <= max_move

        if in_amp:
            counts["extreme_amp_1s"] += 1
        if in_net:
            counts["extreme_net_1s"] += 1
            if net >= min_move:
                counts["extreme_net_up_1s"] += 1
            else:
                counts["extreme_net_down_1s"] += 1
        if in_rev:
            counts["reversal_1s"] += 1
        # spike >=6% one side but close on the other side of open
        dom_up = up_wick >= dn_wick
        if amp >= min_move and ((dom_up and net < 0) or (not dom_up and net > 0)):
            counts["spike_fade_1s"] += 1

        if in_net or in_amp or in_rev:
            events.append(
                {
                    "sec_ms": sec,
                    "net_pct": round(net, 4),
                    "up_wick_pct": round(up_wick, 4),
                    "down_wick_pct": round(dn_wick, 4),
                    "amp_pct": round(amp, 4),
                    "notional_usdt": round(b.notional, 2),
                    "trades": b.trades,
                    "reversal": in_rev,
                }
            )
    return counts, events


def analyze_rolling_1s(
    symbol: str,
    trades: List[Tuple[int, float, float]],
    min_move: float,
    max_move: float,
) -> Tuple[Counter, List[dict]]:
    """Bot-style rolling 1000ms window (like burst.go sec_ticks). O(n) via monotonic deques."""
    from collections import deque

    counts: Counter = Counter()
    events: List[dict] = []
    uniq_net: set = set()
    uniq_amp: set = set()
    uniq_rev: set = set()
    uniq_fade: set = set()
    if len(trades) < 2:
        return counts, events

    def in_range(v: float) -> bool:
        if max_move > 0:
            return min_move <= v <= max_move
        return v >= min_move

    win: deque = deque()  # (idx, t, p)
    max_d: deque = deque()  # decreasing p, (idx, p)
    min_d: deque = deque()  # increasing p, (idx, p)
    last_report_ms = -10_000

    for i1, (t1, p1, _) in enumerate(trades):
        win.append((i1, t1, p1))
        while max_d and max_d[-1][1] <= p1:
            max_d.pop()
        max_d.append((i1, p1))
        while min_d and min_d[-1][1] >= p1:
            min_d.pop()
        min_d.append((i1, p1))

        while win and win[0][1] < t1 - 1000:
            old_i, _, _ = win.popleft()
            if max_d and max_d[0][0] == old_i:
                max_d.popleft()
            if min_d and min_d[0][0] == old_i:
                min_d.popleft()

        if len(win) < 2 or t1 - win[0][1] < 200:
            continue
        anchor = win[0][2]
        if anchor <= 0 or not max_d or not min_d:
            continue
        hi = max_d[0][1]
        lo = min_d[0][1]
        up_wick = max(0.0, pct(anchor, hi))
        dn_wick = max(0.0, pct(lo, anchor))
        net = pct(anchor, p1)
        amp = max(up_wick, dn_wick)

        hit_net = in_range(abs(net))
        hit_amp = in_range(amp)
        hit_rev = up_wick >= min_move and dn_wick >= min_move
        dom_up = up_wick >= dn_wick
        hit_spike_fade = amp >= min_move and (
            (dom_up and net < 0) or (not dom_up and net > 0)
        )

        sec_key = (symbol, t1 // 1000)
        if hit_net:
            counts["rolling_extreme_net_1s"] += 1
            uniq_net.add(sec_key)
        if hit_amp:
            counts["rolling_extreme_amp_1s"] += 1
            uniq_amp.add(sec_key)
        if hit_rev:
            counts["rolling_reversal_1s"] += 1
            uniq_rev.add(sec_key)
        if hit_spike_fade:
            counts["rolling_spike_fade_1s"] += 1
            uniq_fade.add(sec_key)

        if (hit_net or hit_amp or hit_rev or hit_spike_fade) and t1 - last_report_ms >= 50:
            last_report_ms = t1
            events.append(
                {
                    "t_ms": t1,
                    "net_pct": round(net, 4),
                    "up_wick_pct": round(up_wick, 4),
                    "down_wick_pct": round(dn_wick, 4),
                    "amp_pct": round(amp, 4),
                    "reversal": hit_rev,
                    "spike_fade": hit_spike_fade,
                }
            )

    counts["_uniq_net"] = uniq_net
    counts["_uniq_amp"] = uniq_amp
    counts["_uniq_rev"] = uniq_rev
    counts["_uniq_fade"] = uniq_fade
    return counts, events


def scan_symbol(
    symbol: str,
    date: str,
    min_move: float,
    max_move: float,
    rolling: bool,
) -> dict:
    url = VISION_TMPL.format(symbol=symbol, date=date)
    raw = download_zip(url)
    out = {
        "symbol": symbol,
        "trades": 0,
        "calendar": {},
        "rolling": {},
        "calendar_events": [],
        "missing": raw is None,
    }
    if raw is None:
        return out

    trades = iter_agg_trades(raw)
    out["trades"] = len(trades)
    cal_c, cal_e = analyze_calendar_seconds(trades, min_move, max_move)
    out["calendar"] = dict(cal_c)
    out["calendar_events"] = cal_e

    if rolling:
        roll_c, _ = analyze_rolling_1s(symbol, trades, min_move, max_move)
        out["rolling"] = {k: v for k, v in roll_c.items() if not k.startswith("_uniq")}
        out["rolling_uniq"] = {
            "net": roll_c.get("_uniq_net", set()),
            "amp": roll_c.get("_uniq_amp", set()),
            "rev": roll_c.get("_uniq_rev", set()),
            "fade": roll_c.get("_uniq_fade", set()),
        }
    return out


def load_symbols(path: Path) -> List[str]:
    text = path.read_text()
    if path.suffix == ".json":
        data = json.loads(text)
        if isinstance(data, dict) and "per_symbol_counts" in data:
            return sorted(data["per_symbol_counts"].keys())
        if isinstance(data, list):
            return data
    return [ln.strip() for ln in text.splitlines() if ln.strip()]


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--symbols-file", required=True)
    p.add_argument("--min-move-pct", type=float, default=6.0)
    p.add_argument("--max-move-pct", type=float, default=0.0, help="0 = no upper cap")
    p.add_argument("--workers", type=int, default=6)
    p.add_argument("--rolling", action="store_true")
    p.add_argument("--out", default="")
    args = p.parse_args()

    symbols = load_symbols(Path(args.symbols_file))
    totals_cal: Counter = Counter()
    totals_roll: Counter = Counter()
    global_uniq_net: set = set()
    global_uniq_amp: set = set()
    global_uniq_rev: set = set()
    global_uniq_fade: set = set()
    per_symbol_cal: Dict[str, dict] = {}
    all_cal_events: List[dict] = []
    missing = 0
    ok = 0

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {
            ex.submit(
                scan_symbol,
                sym,
                args.date,
                args.min_move_pct,
                args.max_move_pct,
                args.rolling,
            ): sym
            for sym in symbols
        }
        for fut in as_completed(futs):
            sym = futs[fut]
            r = fut.result()
            if r["missing"]:
                missing += 1
                continue
            ok += 1
            for k, v in r["calendar"].items():
                totals_cal[k] += v
            for k, v in r["rolling"].items():
                totals_roll[k] += v
            ru = r.get("rolling_uniq") or {}
            global_uniq_net |= ru.get("net", set())
            global_uniq_amp |= ru.get("amp", set())
            global_uniq_rev |= ru.get("rev", set())
            global_uniq_fade |= ru.get("fade", set())
            if r["calendar"].get("extreme_net_1s") or r["calendar"].get("reversal_1s") or r["calendar"].get("extreme_amp_1s"):
                per_symbol_cal[sym] = dict(r["calendar"])
            for e in r["calendar_events"]:
                e["symbol"] = sym
                all_cal_events.append(e)

    all_cal_events.sort(key=lambda e: (-e.get("amp_pct", 0), -abs(e.get("net_pct", 0))))

    sync_rev = defaultdict(list)
    for e in all_cal_events:
        if e.get("reversal"):
            sync_rev[e["sec_ms"]].append(e["symbol"])

    report = {
        "date_utc": args.date,
        "min_move_pct": args.min_move_pct,
        "max_move_pct": args.max_move_pct or None,
        "symbols_ok": ok,
        "symbols_missing_zip": missing,
        "calendar_1s": dict(totals_cal),
        "rolling_1s": dict(totals_roll) if args.rolling else None,
        "rolling_unique_symbol_seconds": (
            {
                "net": len(global_uniq_net),
                "amp": len(global_uniq_amp),
                "reversal": len(global_uniq_rev),
                "spike_fade": len(global_uniq_fade),
            }
            if args.rolling
            else None
        ),
        "per_symbol_calendar_hits": per_symbol_cal,
        "top_calendar_events": all_cal_events[:40],
        "sync_reversal_seconds": [
            {"sec_ms": ms, "symbols": syms, "count": len(syms)}
            for ms, syms in sorted(sync_rev.items(), key=lambda x: -len(x[1]))
            if len(syms) >= 2
        ],
    }

    text = json.dumps(report, indent=2)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(text)
    print(text)


if __name__ == "__main__":
    main()
