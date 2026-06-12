#!/usr/bin/env python3
"""Sum Binance USD-M futures income in a time window (IST by default)."""
from __future__ import annotations

import hashlib
import hmac
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]


def load_env():
    env = {}
    p = ROOT / ".env"
    if p.exists():
        for line in p.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k] = v
    env.setdefault("BINANCE_API_KEY", os.getenv("BINANCE_API_KEY", ""))
    env.setdefault("BINANCE_API_SECRET", os.getenv("BINANCE_API_SECRET", ""))
    return env


def signed_get(base: str, key: str, secret: str, path: str, params: dict) -> list:
    params = dict(params)
    params["timestamp"] = str(int(time.time() * 1000))
    params["recvWindow"] = "60000"
    q = urlencode(sorted(params.items()))
    sig = hmac.new(secret.encode(), q.encode(), hashlib.sha256).hexdigest()
    url = f"{base}{path}?{q}&signature={sig}"
    req = Request(url, headers={"X-MBX-APIKEY": key})
    with urlopen(req, timeout=30) as r:
        data = r.read()
    import json
    return json.loads(data)


def fetch_income(key, secret, start_ms, end_ms):
    rows = []
    cur = start_ms
    while cur < end_ms:
        batch = signed_get(
            "https://fapi.binance.com", key, secret, "/fapi/v1/income",
            {"startTime": str(cur), "endTime": str(end_ms), "limit": "1000"},
        )
        if not batch:
            break
        rows.extend(batch)
        last = int(batch[-1]["time"])
        if last <= cur or len(batch) < 1000:
            break
        cur = last + 1
        time.sleep(0.12)
    return [r for r in rows if start_ms <= int(r["time"]) < end_ms]


def main():
    # Usage: python3 scripts/binance_futures_pnl_window.py 2026-06-03 8 20
    if len(sys.argv) < 4:
        print("Usage: binance_futures_pnl_window.py YYYY-MM-DD START_HOUR END_HOUR [IST]")
        sys.exit(1)
    y, m, d = map(int, sys.argv[1].split("-"))
    h0, h1 = int(sys.argv[2]), int(sys.argv[3])
    IST = timezone(timedelta(hours=5, minutes=30))
    tz = IST if len(sys.argv) < 5 or sys.argv[4].upper() == "IST" else timezone.utc
    start = datetime(y, m, d, h0, 0, tzinfo=tz)
    end = datetime(y, m, d, h1, 0, tzinfo=tz)
    start_ms, end_ms = int(start.timestamp() * 1000), int(end.timestamp() * 1000)

    env = load_env()
    key, secret = env["BINANCE_API_KEY"], env["BINANCE_API_SECRET"]
    if not key or not secret:
        print("Missing BINANCE_API_KEY / BINANCE_API_SECRET in .env")
        sys.exit(1)

    rows = fetch_income(key, secret, start_ms, end_ms)
    by_type: dict[str, float] = {}
    for r in rows:
        t = r.get("incomeType", "?")
        by_type[t] = by_type.get(t, 0) + float(r.get("income", 0))

    print(f"Window: {start} → {end}")
    print(f"Events: {len(rows)}")
    for t, v in sorted(by_type.items(), key=lambda x: -abs(x[1])):
        print(f"  {t:20s} {v:+.4f} USDT")
    realized = by_type.get("REALIZED_PNL", 0)
    comm = by_type.get("COMMISSION", 0)
    net = sum(by_type.values())
    print(f"REALIZED_PNL: {realized:+.4f}")
    print(f"COMMISSION:   {comm:+.4f}")
    print(f"NET (all):    {net:+.4f} USDT")


if __name__ == "__main__":
    main()
