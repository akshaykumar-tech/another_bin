#!/usr/bin/env python3
"""Build 1s kline CSVs from Binance USDT-M aggTrades (futures has no 1s kline REST)."""
from __future__ import annotations

import argparse
import csv
import json
import os
import time
import urllib.parse
import urllib.request
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG = ROOT / "config" / "whale-focused.yaml"
OUT_DIR = ROOT / "data" / "klines" / "1s"
FAPI = os.environ.get("FOCUSED_FAPI", "https://fapi.binance.com").rstrip("/")
LIMIT = 1000


def http_json(url: str):
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.load(resp)


def load_symbols(config_path: Path) -> list[str]:
    cfg = yaml.safe_load(config_path.read_text())
    return sorted((cfg.get("symbols") or {}).keys())


def fetch_agg_trades(symbol: str, start_ms: int, end_ms: int) -> list[dict]:
    trades = []
    cur = start_ms
    while cur < end_ms:
        q = urllib.parse.urlencode({"symbol": symbol, "limit": LIMIT, "startTime": cur, "endTime": end_ms})
        batch = http_json(f"{FAPI}/fapi/v1/aggTrades?{q}")
        if not batch:
            break
        trades.extend(batch)
        cur = int(batch[-1]["T"]) + 1
        if len(batch) < LIMIT:
            break
        time.sleep(0.05)
    return trades


def trades_to_bars(trades: list[dict]) -> list[dict]:
    buckets: dict[int, dict] = {}
    for t in trades:
        sec = (int(t["T"]) // 1000) * 1000
        price = float(t["p"])
        qty = float(t["q"])
        b = buckets.get(sec)
        if not b:
            buckets[sec] = {"timestamp_ms": sec, "open": price, "high": price, "low": price, "close": price, "volume": qty}
        else:
            b["high"] = max(b["high"], price)
            b["low"] = min(b["low"], price)
            b["close"] = price
            b["volume"] += qty
    rows = sorted(buckets.values(), key=lambda x: x["timestamp_ms"])
    for r in rows:
        for k in ("open", "high", "low", "close", "volume"):
            r[k] = f"{r[k]:.8f}"
    return rows


def fetch_symbol(symbol: str, hours: int) -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUT_DIR / f"{symbol}.csv"
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - hours * 3600 * 1000
    trades = fetch_agg_trades(symbol, start_ms, end_ms)
    rows = trades_to_bars(trades)
    if not rows:
        print(f"  {symbol}: no trades")
        return 0
    with out.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["timestamp_ms", "open", "high", "low", "close", "volume"])
        w.writeheader()
        for r in rows:
            w.writerow({**r, "timestamp_ms": r["timestamp_ms"]})
    print(f"  {symbol}: {len(trades)} trades -> {len(rows)} bars -> {out}")
    return len(rows)


def main():
    ap = argparse.ArgumentParser(description="Fetch aggTrades and build 1s bars")
    ap.add_argument("--config", default=str(DEFAULT_CONFIG))
    ap.add_argument("--hours", type=int, default=int(os.environ.get("KLINE_FETCH_HOURS", "24")))
    ap.add_argument("--symbols", default="", help="comma-separated override")
    args = ap.parse_args()
    if args.symbols.strip():
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    else:
        symbols = load_symbols(Path(args.config))
    print(f"Fetching aggTrades for {len(symbols)} symbols, last {args.hours}h")
    total = 0
    for sym in symbols:
        try:
            total += fetch_symbol(sym, args.hours)
        except Exception as e:
            print(f"  {sym}: ERROR {e}")
        time.sleep(0.15)
    print(f"Done. {total} total bars.")


if __name__ == "__main__":
    main()
