#!/usr/bin/env python3
"""Count extreme 1m candles for one UTC day via Binance Vision daily ZIPs."""

from __future__ import annotations

import argparse
import io
import json
import subprocess
import sys
import zipfile
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

UTC = timezone.utc
VISION_TMPL = (
    "https://data.binance.vision/data/futures/um/daily/klines/"
    "{symbol}/1m/{symbol}-1m-{date}.zip"
)


def pct(a: float, b: float) -> float:
    if a <= 0:
        return 0.0
    return (b - a) / a * 100.0


def download_zip(url: str) -> bytes | None:
    proc = subprocess.run(
        ["curl", "-sfL", "--max-time", "45", url],
        capture_output=True,
    )
    if proc.returncode != 0:
        return None
    return proc.stdout


def candles_from_zip(data: bytes) -> Iterable[Tuple[int, float, float, float, float]]:
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        names = [n for n in zf.namelist() if n.endswith(".csv")]
        if not names:
            return
        text = zf.read(names[0]).decode("utf-8", errors="replace")
    for line in text.splitlines():
        if not line.strip():
            continue
        row = line.split(",")
        if len(row) < 5:
            continue
        try:
            yield int(row[0]), float(row[1]), float(row[2]), float(row[3]), float(row[4])
        except ValueError:
            continue


def scan_symbol(
    symbol: str,
    date: str,
    min_move: float,
    max_move: float,
) -> Tuple[str, Dict[str, int], List[dict]]:
    url = VISION_TMPL.format(symbol=symbol, date=date)
    raw = download_zip(url)
    counts = {
        "bars": 0,
        "extreme_amp_1m": 0,
        "extreme_net_1m": 0,
        "extreme_net_up_1m": 0,
        "extreme_net_down_1m": 0,
    }
    events: List[dict] = []
    if raw is None:
        return symbol, counts, events

    for ot, o, h, l, c in candles_from_zip(raw):
        if o <= 0 or l <= 0:
            continue
        counts["bars"] += 1
        up_wick = max(0.0, pct(o, h))
        dn_wick = max(0.0, pct(l, o))
        net = pct(o, c)
        amp = max(up_wick, dn_wick)

        if min_move <= amp <= max_move:
            counts["extreme_amp_1m"] += 1
        if min_move <= abs(net) <= max_move:
            counts["extreme_net_1m"] += 1
            if net >= min_move:
                counts["extreme_net_up_1m"] += 1
            else:
                counts["extreme_net_down_1m"] += 1
            events.append(
                {
                    "symbol": symbol,
                    "open_time_ms": ot,
                    "net_pct": round(net, 4),
                    "amp_pct": round(amp, 4),
                    "open": o,
                    "close": c,
                }
            )
    return symbol, counts, events


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True, help="UTC day YYYY-MM-DD")
    p.add_argument("--symbols-file", default="", help="JSON list or one symbol per line")
    p.add_argument("--min-move-pct", type=float, default=6.0)
    p.add_argument("--max-move-pct", type=float, default=20.0)
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--out", default="")
    args = p.parse_args()

    if args.symbols_file:
        path = Path(args.symbols_file)
        text = path.read_text()
        if path.suffix == ".json":
            data = json.loads(text)
            if isinstance(data, list):
                symbols = data
            elif isinstance(data, dict) and "per_symbol_counts" in data:
                symbols = sorted(data["per_symbol_counts"].keys())
            else:
                symbols = sorted(data.keys())
        else:
            symbols = [ln.strip() for ln in text.splitlines() if ln.strip()]
    else:
        print("symbols-file required", file=sys.stderr)
        sys.exit(1)

    totals = Counter()
    all_events: List[dict] = []
    missing = 0
    scanned = 0

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {
            ex.submit(
                scan_symbol, sym, args.date, args.min_move_pct, args.max_move_pct
            ): sym
            for sym in symbols
        }
        for fut in as_completed(futs):
            sym, counts, events = fut.result()
            if counts["bars"] == 0:
                missing += 1
            else:
                scanned += 1
            for k, v in counts.items():
                totals[k] += v
            all_events.extend(events)

    all_events.sort(key=lambda e: -abs(e["net_pct"]))
    sync = defaultdict(list)
    for e in all_events:
        sync[e["open_time_ms"]].append(e["symbol"])

    out = {
        "date_utc": args.date,
        "thresholds": {"min_move_pct": args.min_move_pct, "max_move_pct": args.max_move_pct},
        "symbols_requested": len(symbols),
        "symbols_with_zip": scanned,
        "symbols_missing_zip": missing,
        "overall_counts": dict(totals),
        "events": all_events,
        "sync_minutes_net_ge_min": [
            {"open_time_ms": ms, "symbols": syms, "count": len(syms)}
            for ms, syms in sorted(sync.items(), key=lambda x: -len(x[1]))
            if len(syms) >= 2
        ],
    }

    text = json.dumps(out, indent=2)
    if args.out:
        Path(args.out).write_text(text)
    print(text)


if __name__ == "__main__":
    main()
