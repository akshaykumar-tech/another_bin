#!/usr/bin/env python3
"""
Live paper detector: Binance USDT-M perpetual aggTrade → 1s candles.

- 300 lowest-volume perps (configurable)
- SIGNAL: 1s move >= 2%  |  EVENT: 1s move >= 6%  |  match window: 60s
- Per-symbol cooldown after signal (default 60 min)

Output: data/live/<run_ts>/{signals,events,results}.csv

  python3 scripts/live_paper.py
"""
import asyncio
import csv
import json
import os
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


def load_dotenv(path=".env"):
    p = Path(path)
    if not p.is_file():
        p = Path(__file__).resolve().parent.parent / ".env"
    if not p.is_file():
        return
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key, val = key.strip(), val.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = val


load_dotenv()

try:
    import websockets
except ImportError:
    print("pip install websockets")
    raise


def _env_int(name, default):
    v = os.environ.get(name, "").strip()
    return int(v) if v else default


def _env_float(name, default):
    v = os.environ.get(name, "").strip()
    return float(v) if v else default


WATCHLIST_SIZE = _env_int("LIVE_PAPER_WATCHLIST_SIZE", 300)
WATCHLIST_MODE = os.environ.get("LIVE_PAPER_WATCHLIST_MODE", "lowest_volume").strip().lower()
WS_CHUNK = _env_int("LIVE_PAPER_WS_CHUNK", 80)
PRE_THRESH = _env_float("LIVE_PAPER_PRE_THRESH", 0.02)
EVENT_THRESH = _env_float("LIVE_PAPER_EVENT_THRESH", 0.06)
WINDOW = _env_int("LIVE_PAPER_WINDOW_SEC", 60)
COOLDOWN_SEC = _env_int("LIVE_PAPER_COOLDOWN_SEC", 3600)
WS_ROOT = os.environ.get("LIVE_PAPER_WS_ROOT", "wss://fstream.binance.com").rstrip("/")
FAPI = os.environ.get("LIVE_PAPER_FAPI", "https://fapi.binance.com").rstrip("/")

RUN_TS = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
OUT_DIR = os.environ.get("LIVE_PAPER_OUT_DIR", f"data/live/{RUN_TS}")
os.makedirs(OUT_DIR, exist_ok=True)
SIGNAL_CSV = os.path.join(OUT_DIR, "signals.csv")
EVENT_CSV = os.path.join(OUT_DIR, "events.csv")
RESULTS_CSV = os.path.join(OUT_DIR, "results.csv")

for path, header in [
    (SIGNAL_CSV, ["symbol", "signal_ts_ms", "signal_utc", "direction", "entry_price", "vol_sum_1s"]),
    (EVENT_CSV, ["symbol", "event_ts_ms", "event_utc", "type", "open", "high", "low", "close", "volume"]),
    (
        RESULTS_CSV,
        [
            "symbol", "signal_ts_ms", "signal_utc", "direction", "entry_price",
            "event_found", "event_ts_ms", "event_utc", "event_peak", "move_pct",
        ],
    ),
]:
    with open(path, "w", newline="") as f:
        csv.writer(f).writerow(header)


def _http_json(url):
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.load(resp)


def fetch_lowest_volume_perps(n):
    info = _http_json(f"{FAPI}/fapi/v1/exchangeInfo")
    perps = {
        s["symbol"]
        for s in info.get("symbols", [])
        if s.get("contractType") == "PERPETUAL"
        and s.get("quoteAsset") == "USDT"
        and s.get("status") == "TRADING"
        and "_" not in s.get("symbol", "")
    }
    ranked = []
    for t in _http_json(f"{FAPI}/fapi/v1/ticker/24hr"):
        sym = t.get("symbol", "")
        if sym in perps:
            ranked.append((sym, float(t.get("quoteVolume", 0) or 0)))
    ranked.sort(key=lambda x: (x[1], x[0]))
    return [sym for sym, _ in ranked[:n]]


def resolve_symbols():
    manual = os.environ.get("LIVE_PAPER_SYMBOLS", "").strip()
    if manual:
        return [s.strip().upper() for s in manual.split(",") if s.strip()]
    if WATCHLIST_MODE != "lowest_volume":
        raise ValueError(f"unsupported watchlist mode: {WATCHLIST_MODE!r}")
    return fetch_lowest_volume_perps(WATCHLIST_SIZE)


def chunk_symbols(symbols, size):
    size = max(size, 1)
    return [symbols[i : i + size] for i in range(0, len(symbols), size)]


SYMBOLS = resolve_symbols()
if not SYMBOLS:
    raise SystemExit("no symbols resolved")

buckets = {s: {} for s in SYMBOLS}
signals = []
last_signal_ms = {}  # symbol -> last signal time (ms); 60 min cooldown
stats = {"trades": 0, "signals": 0, "events": 0, "matches": 0, "cooldown_skips": 0}


def utc_iso(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


async def finalize_bucket(symbol, b):
    sec = b["sec"]
    o, h, l, c, vol = b["open"], b["high"], b["low"], b["close"], b["volume"]

    event_type = None
    if o > 0 and (h - o) / o >= EVENT_THRESH:
        event_type = "high"
    if o > 0 and (o - l) / o >= EVENT_THRESH:
        event_type = "low" if event_type is None else event_type

    if event_type:
        amp = (h - o) / o * 100 if event_type == "high" else (o - l) / o * 100
        stats["events"] += 1
        print(f"[EVENT] {symbol} {event_type} {amp:.2f}% @ {utc_iso(sec)}", flush=True)
        with open(EVENT_CSV, "a", newline="") as f:
            csv.writer(f).writerow([symbol, sec, utc_iso(sec), event_type, f"{o:.8f}", f"{h:.8f}", f"{l:.8f}", f"{c:.8f}", f"{vol:.8f}"])
        peak = h if event_type == "high" else l
        for s in list(signals):
            if s["resolved"] or s["symbol"] != symbol:
                continue
            if 0 <= (sec - s["signal_ts"]) <= WINDOW * 1000 and s["direction"] == event_type:
                s["resolved"] = True
                move = (peak - s["entry_price"]) / s["entry_price"] * 100 if s["direction"] == "high" else (s["entry_price"] - peak) / s["entry_price"] * 100
                stats["matches"] += 1
                print(f"[MATCH] {symbol} {s['direction']} move={move:.2f}% lag={(sec - s['signal_ts']) // 1000}s", flush=True)
                with open(RESULTS_CSV, "a", newline="") as f:
                    csv.writer(f).writerow([symbol, s["signal_ts"], utc_iso(s["signal_ts"]), s["direction"], f"{s['entry_price']:.8f}", 1, sec, utc_iso(sec), f"{peak:.8f}", f"{move:.6f}"])
        return

    if o <= 0:
        return
    up = (h - o) / o
    dn = (o - l) / o
    if up < PRE_THRESH and dn < PRE_THRESH:
        return

    prev = last_signal_ms.get(symbol, 0)
    if sec - prev < COOLDOWN_SEC * 1000:
        stats["cooldown_skips"] += 1
        return

    dirc = "high" if up >= PRE_THRESH else "low"
    amp = up * 100 if dirc == "high" else dn * 100
    last_signal_ms[symbol] = sec
    stats["signals"] += 1
    sig = {"symbol": symbol, "signal_ts": sec, "direction": dirc, "entry_price": o, "resolved": False}
    signals.append(sig)
    print(f"[SIGNAL] {symbol} {dirc} {amp:.2f}% @ {utc_iso(sec)}", flush=True)
    with open(SIGNAL_CSV, "a", newline="") as f:
        csv.writer(f).writerow([symbol, sec, utc_iso(sec), dirc, f"{o:.8f}", f"{b.get('vol_sum', 0):.8f}"])


async def on_trade(symbol, price, qty, t_ms):
    stats["trades"] += 1
    sec = (t_ms // 1000) * 1000
    state = buckets.get(symbol)
    if state is None:
        return
    b = state.get("cur")
    if not b or b["sec"] != sec:
        if b:
            await finalize_bucket(symbol, b)
        state["cur"] = {"sec": sec, "open": price, "high": price, "low": price, "close": price, "volume": qty, "vol_sum": qty}
    else:
        b["close"] = price
        b["high"] = max(b["high"], price)
        b["low"] = min(b["low"], price)
        b["volume"] += qty
        b["vol_sum"] += qty


async def ws_handler(conn_id, symbols):
    streams = "/".join(f"{s.lower()}@aggTrade" for s in symbols)
    url = f"{WS_ROOT}/market/stream?streams={streams}"
    print(f"[ws-{conn_id}] connecting {len(symbols)} symbols")
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, max_size=2**22) as ws:
                print(f"[ws-{conn_id}] connected")
                async for msg in ws:
                    data = json.loads(msg).get("data")
                    if not data:
                        continue
                    await on_trade(data["s"], float(data["p"]), float(data["q"]), int(data["T"]))
        except Exception as e:
            print(f"[ws-{conn_id}] reconnect ({e})")
            await asyncio.sleep(3)


async def flush_stale_buckets_loop():
    while True:
        cutoff = (int(time.time()) - 1) * 1000
        for symbol, state in buckets.items():
            b = state.get("cur")
            if b and b["sec"] < cutoff:
                await finalize_bucket(symbol, b)
                state.pop("cur", None)
        await asyncio.sleep(0.5)


async def stats_loop():
    while True:
        await asyncio.sleep(60)
        print(
            f"[stats] trades={stats['trades']} signals={stats['signals']} "
            f"events={stats['events']} matches={stats['matches']} "
            f"cooldown_skips={stats['cooldown_skips']}",
            flush=True,
        )


async def expire_signals_loop():
    while True:
        now_ms = int(time.time() * 1000)
        for s in list(signals):
            if s["resolved"]:
                continue
            if now_ms - s["signal_ts"] > WINDOW * 1000:
                s["resolved"] = True
                with open(RESULTS_CSV, "a", newline="") as f:
                    csv.writer(f).writerow([s["symbol"], s["signal_ts"], utc_iso(s["signal_ts"]), s["direction"], f"{s['entry_price']:.8f}", 0, "", "", "", ""])
        await asyncio.sleep(1.0)


async def main():
    chunks = chunk_symbols(SYMBOLS, WS_CHUNK)
    print(f"live_paper | symbols={len(SYMBOLS)} connections={len(chunks)}")
    print(f"  pre={PRE_THRESH*100:.1f}% event={EVENT_THRESH*100:.1f}% window={WINDOW}s cooldown={COOLDOWN_SEC}s")
    print(f"  out={OUT_DIR}")

    await asyncio.gather(
        *[ws_handler(i, c) for i, c in enumerate(chunks)],
        flush_stale_buckets_loop(),
        stats_loop(),
        expire_signals_loop(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopping")
