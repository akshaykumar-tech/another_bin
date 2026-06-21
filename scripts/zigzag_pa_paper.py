#!/usr/bin/env python3
"""
Dry paper: ZigZag PA harmonic patterns (Pine V4.1 logic).

  python3 scripts/zigzag_pa_paper.py

ZigZag + patterns on 1h; entries/exits on 5m:
  LONG  — bull pattern (new) + close <= Fib 0.236 entry window
  SHORT — bear pattern (new) + close >= Fib 0.236 entry window
  Exit  — Fib TP 0.618 / SL -0.236 (on 5m high/low)

Logs (separate from Alma / sr_chartprime):
  data/zigzag_pa/<run_ts>/zigzag_dry.log
  data/zigzag_pa/<run_ts>/zigzag_dry_trades.csv
"""
from __future__ import annotations

import asyncio
import csv
import json
import os
import sys
import time
import urllib.request
from urllib.parse import quote
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Deque

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))

from zigzag_pa_lib import Bar, detect_patterns, fib_level, zigzag_pivots

try:
    import websockets
except ImportError:
    print("pip install websockets")
    raise

KDIR = ROOT / "data" / "klines" / "1s"
BAR_5M = 300_000
BAR_1H = 3_600_000


def load_dotenv(path: str = ".env") -> None:
    p = Path(path)
    if not p.is_file():
        p = ROOT / ".env"
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


def _env(name: str, default: str) -> str:
    return os.environ.get(name, "").strip() or default


def _env_int(name: str, default: int) -> int:
    return int(_env(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(_env(name, str(default)))


load_dotenv()

WS_ROOT = _env("ZIGZAG_PA_WS_ROOT", "wss://fstream.binance.com").rstrip("/")
FAPI = _env("ZIGZAG_PA_FAPI", "https://fapi.binance.com").rstrip("/")
INTERVAL = _env("ZIGZAG_PA_INTERVAL", "5m")
BAR_MS = BAR_5M if INTERVAL == "5m" else 300_000
WS_CHUNK = _env_int("ZIGZAG_PA_WS_CHUNK", 40)
WATCHLIST_MODE = _env("ZIGZAG_PA_WATCHLIST_MODE", "all_perps").lower()
WATCHLIST_SIZE = _env_int("ZIGZAG_PA_WATCHLIST_SIZE", 500)
NOTIONAL = _env_float("ZIGZAG_PA_NOTIONAL_USDT", 6.0)
FEE_RT = _env_float("ZIGZAG_PA_FEE_RT", 0.0008)
EW_RATE = _env_float("ZIGZAG_PA_EW_FIB", 0.236)
TP_RATE = _env_float("ZIGZAG_PA_TP_FIB", 0.618)
SL_RATE = _env_float("ZIGZAG_PA_SL_FIB", -0.236)
MAX_HOLD_BARS = _env_int("ZIGZAG_PA_MAX_HOLD_BARS", 96)
MAX_OPEN = _env_int("ZIGZAG_PA_MAX_OPEN", 50)
STATS_INTERVAL_SEC = _env_int("ZIGZAG_PA_STATS_INTERVAL_SEC", 1800)
BOOTSTRAP_5M = _env_int("ZIGZAG_PA_BOOTSTRAP_KLINES", 500)
WARMUP_1H = 48

RUN_TS = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
OUT_DIR = Path(_env("ZIGZAG_PA_OUT_DIR", str(ROOT / f"data/zigzag_pa/{RUN_TS}")))
OUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = OUT_DIR / "zigzag_dry.log"
TRADES_CSV = OUT_DIR / "zigzag_dry_trades.csv"

stats = {
    "signals": 0,
    "entries": 0,
    "exits": 0,
    "wins": 0,
    "net_usd": 0.0,
    "tp": 0,
    "sl": 0,
    "timeout": 0,
    "skipped_busy": 0,
    "skipped_max_open": 0,
    "warmup_ready": 0,
    "bull_pat": 0,
    "bear_pat": 0,
    "bars_closed": 0,
}


@dataclass
class ActiveTrade:
    symbol: str
    side: str
    pattern: str
    signal_ms: int
    entry_ms: int
    entry_px: float
    tp_px: float
    sl_px: float
    bars_held: int = 0
    max_fav_pct: float = 0.0
    max_adv_pct: float = 0.0


@dataclass
class SymbolFeed:
    bars_5m: Deque[Bar] = field(default_factory=lambda: deque(maxlen=800))
    bars_1h: Deque[Bar] = field(default_factory=lambda: deque(maxlen=300))
    cur_1h: dict | None = None
    trade: ActiveTrade | None = None
    warmed: bool = False
    prev_bull: bool = False
    prev_bear: bool = False


feeds: dict[str, SymbolFeed] = {}
SYMBOLS: list[str] = []


def log(msg: str) -> None:
    line = f"{datetime.now(timezone.utc).isoformat()} {msg}"
    print(line, flush=True)
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def utc_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def _http_json(url: str, retries: int = 5) -> object:
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read())
        except Exception as e:
            last_err = e
            time.sleep(min(2 ** attempt, 12) + 0.3)
    if last_err is not None:
        raise last_err
    raise RuntimeError("http failed")


def _filter_ascii_symbols(syms: list[str]) -> list[str]:
    return [s for s in syms if s.isascii()]


def resolve_symbols() -> list[str]:
    manual = _env("ZIGZAG_PA_SYMBOLS", "")
    if manual:
        return _filter_ascii_symbols([s.strip().upper() for s in manual.split(",") if s.strip()])
    if WATCHLIST_MODE == "local":
        syms = sorted(p.stem for p in KDIR.glob("*.csv"))
        if not syms:
            syms = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        out = syms[:WATCHLIST_SIZE] if WATCHLIST_SIZE > 0 else syms
        return _filter_ascii_symbols(out)
    if WATCHLIST_MODE == "all_perps":
        info = _http_json(f"{FAPI}/fapi/v1/exchangeInfo")
        out = [
            s["symbol"]
            for s in info["symbols"]
            if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING"
        ][:WATCHLIST_SIZE]
        return _filter_ascii_symbols(out)
    raise ValueError(f"unsupported ZIGZAG_PA_WATCHLIST_MODE: {WIGZAG_PA_WATCHLIST_MODE!r}")


def pnl_usd(side: str, entry: float, exit_px: float) -> float:
    if entry <= 0:
        return 0.0
    g = (exit_px - entry) / entry * 100 if side == "long" else (entry - exit_px) / entry * 100
    return NOTIONAL * g / 100 - NOTIONAL * FEE_RT


def open_count() -> int:
    return sum(1 for f in feeds.values() if f.trade is not None)


def init_trades_csv() -> None:
    if not TRADES_CSV.is_file():
        with TRADES_CSV.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(
                [
                    "symbol", "side", "pattern", "signal_utc", "entry_utc", "exit_utc",
                    "entry_px", "exit_px", "tp_px", "sl_px", "reason", "net_usd",
                    "hold_bars", "max_fav_pct", "max_adv_pct",
                ]
            )


def fetch_klines(symbol: str, interval: str, limit: int) -> list[Bar]:
    url = f"{FAPI}/fapi/v1/klines?symbol={quote(symbol)}&interval={interval}&limit={limit}"
    rows = _http_json(url)
    now_period = (int(time.time() * 1000) // BAR_MS) * BAR_MS
    out: list[Bar] = []
    for r in rows:
        ts = int(r[0])
        if interval == INTERVAL and ts >= now_period:
            continue
        out.append(Bar(ts, float(r[1]), float(r[2]), float(r[3]), float(r[4])))
    return out


def bars_to_1h(bars_5m: list[Bar]) -> list[Bar]:
    buckets: dict[int, list[Bar]] = {}
    for b in bars_5m:
        key = (b.ts // BAR_1H) * BAR_1H
        buckets.setdefault(key, []).append(b)
    out = []
    for ts in sorted(buckets):
        c = buckets[ts]
        out.append(Bar(ts, c[0].o, max(x.h for x in c), min(x.l for x in c), c[-1].c))
    return out


def last5_pivots(feed: SymbolFeed) -> tuple[float, float, float, float, float] | None:
    blist = list(feed.bars_1h)
    if len(blist) < WARMUP_1H:
        return None
    pivots = zigzag_pivots(blist)
    if len(pivots) < 5:
        return None
    prices = [px for _, px in pivots[-5:]]
    return prices[0], prices[1], prices[2], prices[3], prices[4]


def seed_cur_1h(feed: SymbolFeed) -> None:
    """Continue building the in-progress 1h bar from bootstrap 5m history."""
    if not feed.bars_5m:
        return
    last_ts = feed.bars_5m[-1].ts
    period = (last_ts // BAR_1H) * BAR_1H
    hour_bars = [b for b in feed.bars_5m if (b.ts // BAR_1H) * BAR_1H == period]
    if not hour_bars:
        return
    feed.cur_1h = {
        "ts": period,
        "o": hour_bars[0].o,
        "h": max(b.h for b in hour_bars),
        "l": min(b.l for b in hour_bars),
        "c": hour_bars[-1].c,
    }


def finalize_1h(feed: SymbolFeed, bar_5m: Bar) -> None:
    period = (bar_5m.ts // BAR_1H) * BAR_1H
    cur = feed.cur_1h
    if cur is None:
        feed.cur_1h = {"ts": period, "o": bar_5m.o, "h": bar_5m.h, "l": bar_5m.l, "c": bar_5m.c}
        return
    if cur["ts"] != period:
        completed = Bar(cur["ts"], cur["o"], cur["h"], cur["l"], cur["c"])
        if not feed.bars_1h or feed.bars_1h[-1].ts != completed.ts:
            feed.bars_1h.append(completed)
        feed.cur_1h = {"ts": period, "o": bar_5m.o, "h": bar_5m.h, "l": bar_5m.l, "c": bar_5m.c}
        return
    cur["h"] = max(cur["h"], bar_5m.h)
    cur["l"] = min(cur["l"], bar_5m.l)
    cur["c"] = bar_5m.c


def update_mfe_mae(t: ActiveTrade, hi: float, lo: float) -> None:
    ep = t.entry_px
    if t.side == "long":
        t.max_fav_pct = max(t.max_fav_pct, (hi - ep) / ep * 100)
        t.max_adv_pct = max(t.max_adv_pct, (ep - lo) / ep * 100)
    else:
        t.max_fav_pct = max(t.max_fav_pct, (ep - lo) / ep * 100)
        t.max_adv_pct = max(t.max_adv_pct, (hi - ep) / ep * 100)


def check_fib_exit(side: str, hi: float, lo: float, tp: float, sl: float) -> tuple[str, float] | None:
    if side == "long":
        if lo <= sl:
            return "sl", sl
        if hi >= tp:
            return "tp", tp
    else:
        if hi >= sl:
            return "sl", sl
        if lo <= tp:
            return "tp", tp
    return None


def close_trade(feed: SymbolFeed, exit_ms: int, exit_px: float, reason: str) -> None:
    t = feed.trade
    if t is None:
        return
    net = pnl_usd(t.side, t.entry_px, exit_px)
    stats["exits"] += 1
    stats["net_usd"] += net
    if net > 0:
        stats["wins"] += 1
    if reason in stats:
        stats[reason] += 1
    log(
        f"[EXIT] {t.symbol} {t.side.upper()} pattern={t.pattern} reason={reason} "
        f"entry={t.entry_px:.8f} exit={exit_px:.8f} net=${net:+.4f} "
        f"tp={t.tp_px:.8f} sl={t.sl_px:.8f} hold={t.bars_held}bars"
    )
    with TRADES_CSV.open("a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(
            [
                t.symbol, t.side, t.pattern, utc_iso(t.signal_ms), utc_iso(t.entry_ms), utc_iso(exit_ms),
                f"{t.entry_px:.8f}", f"{exit_px:.8f}", f"{t.tp_px:.8f}", f"{t.sl_px:.8f}",
                reason, f"{net:.4f}", t.bars_held, f"{t.max_fav_pct:.2f}", f"{t.max_adv_pct:.2f}",
            ]
        )
    feed.trade = None


def open_trade(symbol: str, feed: SymbolFeed, side: str, pattern: str, ts: int, px: float, tp: float, sl: float) -> None:
    feed.trade = ActiveTrade(symbol, side, pattern, ts, ts, px, tp, sl)
    stats["entries"] += 1
    log(
        f"[ENTRY] {symbol} {side.upper()} pattern={pattern} @ {utc_iso(ts)} price={px:.8f} "
        f"TP={tp:.8f} ({TP_RATE}) SL={sl:.8f} ({SL_RATE})"
    )


def on_5m_close(symbol: str, feed: SymbolFeed, bar: Bar) -> None:
    if feed.bars_5m and feed.bars_5m[-1].ts == bar.ts:
        return
    feed.bars_5m.append(bar)
    prev_1h = len(feed.bars_1h)
    finalize_1h(feed, bar)
    if len(feed.bars_1h) > prev_1h:
        log(f"[1H_BAR] {symbol} closed ts={utc_iso(feed.bars_1h[-1].ts)} count={len(feed.bars_1h)}")

    if len(feed.bars_1h) < WARMUP_1H and feed.cur_1h:
        if len(feed.bars_1h) >= WARMUP_1H - 1:
            pass
    if len(feed.bars_1h) >= WARMUP_1H and not feed.warmed:
        feed.warmed = True
        stats["warmup_ready"] += 1
        log(f"[WARMUP] {symbol} ready 1h_bars={len(feed.bars_1h)}")

    if not feed.warmed:
        return

    pts = last5_pivots(feed)
    if pts is None:
        return
    x, a, bb, c, d = pts
    bull, bear, bull_names, bear_names = detect_patterns(x, a, bb, c, d)
    bull_edge = bull and not feed.prev_bull
    bear_edge = bear and not feed.prev_bear
    feed.prev_bull, feed.prev_bear = bull, bear

    if bull:
        stats["bull_pat"] += int(bull_edge)
    if bear:
        stats["bear_pat"] += int(bear_edge)

    fib_ew = fib_level(d, c, EW_RATE)
    fib_tp = fib_level(d, c, TP_RATE)
    fib_sl = fib_level(d, c, SL_RATE)

    if bull_edge or bear_edge:
        names = ", ".join(bull_names if bull_edge else bear_names)
        log(f"[PATTERN] {symbol} {'BULL' if bull_edge else 'BEAR'} [{names}] @ {utc_iso(bar.ts)} close={bar.c:.8f} ew={fib_ew:.8f}")

    if feed.trade is not None:
        return

    if bull_edge and bar.c <= fib_ew:
        stats["signals"] += 1
        if open_count() >= MAX_OPEN:
            stats["skipped_max_open"] += 1
            log(f"[SIGNAL_SKIP] {symbol} LONG — max_open={MAX_OPEN}")
            return
        pat = bull_names[0] if bull_names else "Bull"
        log(f"[SIGNAL] {symbol} LONG pattern={pat} close={bar.c:.8f} <= ew={fib_ew:.8f}")
        open_trade(symbol, feed, "long", pat, bar.ts, bar.c, fib_tp, fib_sl)
    elif bear_edge and bar.c >= fib_ew:
        stats["signals"] += 1
        if open_count() >= MAX_OPEN:
            stats["skipped_max_open"] += 1
            log(f"[SIGNAL_SKIP] {symbol} SHORT — max_open={MAX_OPEN}")
            return
        pat = bear_names[0] if bear_names else "Bear"
        log(f"[SIGNAL] {symbol} SHORT pattern={pat} close={bar.c:.8f} >= ew={fib_ew:.8f}")
        open_trade(symbol, feed, "short", pat, bar.ts, bar.c, fib_tp, fib_sl)


def on_kline(symbol: str, k: dict) -> None:
    feed = feeds.get(symbol)
    if feed is None:
        return
    hi, lo, cl = float(k["h"]), float(k["l"]), float(k["c"])
    ts = int(k["t"])

    t = feed.trade
    if t:
        update_mfe_mae(t, hi, lo)
        hit = check_fib_exit(t.side, hi, lo, t.tp_px, t.sl_px)
        if hit:
            close_trade(feed, ts, hit[1], hit[0])
        elif k.get("x"):
            t.bars_held += 1
            if t.bars_held >= MAX_HOLD_BARS:
                close_trade(feed, ts, cl, "timeout")

    if k.get("x"):
        stats["bars_closed"] += 1
        on_5m_close(symbol, feed, Bar(ts, float(k["o"]), hi, lo, cl))


def prime_feed(symbol: str) -> int:
    time.sleep(0.15)
    try:
        bars_5m = fetch_klines(symbol, INTERVAL, BOOTSTRAP_5M)
        bars_1h = fetch_klines(symbol, "1h", max(WARMUP_1H + 10, 120))
    except Exception as e:
        log(f"[bootstrap] {symbol} fetch failed: {e}")
        return 0
    feed = feeds[symbol]
    for b in bars_5m:
        feed.bars_5m.append(b)
    for b in bars_1h:
        if not feed.bars_1h or feed.bars_1h[-1].ts != b.ts:
            feed.bars_1h.append(b)
    seed_cur_1h(feed)
    if len(feed.bars_1h) >= WARMUP_1H:
        feed.warmed = True
        stats["warmup_ready"] += 1
        pts = last5_pivots(feed)
        if pts:
            bull, bear, _, _ = detect_patterns(*pts)
            feed.prev_bull, feed.prev_bear = bull, bear
    return len(bars_5m)


async def bootstrap_all() -> None:
    global SYMBOLS
    log(f"[bootstrap] {INTERVAL}×{BOOTSTRAP_5M} + 1h for {len(SYMBOLS)} symbols...")
    sem = asyncio.Semaphore(3)
    loop = asyncio.get_event_loop()

    async def one(sym: str) -> None:
        async with sem:
            await loop.run_in_executor(None, prime_feed, sym)

    await asyncio.gather(*[one(s) for s in SYMBOLS])
    ready = [s for s in SYMBOLS if feeds[s].bars_5m]
    skipped = len(SYMBOLS) - len(ready)
    SYMBOLS = ready
    if not SYMBOLS:
        raise SystemExit("bootstrap failed — no symbols loaded")
    log(f"[bootstrap] warmed={stats['warmup_ready']}/{len(SYMBOLS)} skipped={skipped}")


async def ws_handler(conn_id: int, symbols: list[str]) -> None:
    streams = "/".join(f"{s.lower()}@kline_{INTERVAL}" for s in symbols)
    url = f"{WS_ROOT}/market/stream?streams={streams}"
    log(f"[ws-{conn_id}] connecting {len(symbols)} symbols ({INTERVAL})")
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, max_size=2**22) as ws:
                log(f"[ws-{conn_id}] connected")
                async for msg in ws:
                    wrap = json.loads(msg)
                    data = wrap.get("data") or wrap
                    if data.get("e") != "kline":
                        continue
                    on_kline(data["s"], data["k"])
        except Exception as e:
            log(f"[ws-{conn_id}] reconnect ({e})")
            await asyncio.sleep(3)


async def stats_loop() -> None:
    while True:
        await asyncio.sleep(STATS_INTERVAL_SEC)
        open_n = open_count()
        exits = stats["exits"]
        wr = (stats["wins"] / exits * 100) if exits else 0.0
        log(
            f"[stats] warmed={stats['warmup_ready']}/{len(SYMBOLS)} signals={stats['signals']} "
            f"entries={stats['entries']} exits={exits} wr={wr:.1f}% net=${stats['net_usd']:+.4f} "
            f"tp={stats['tp']} sl={stats['sl']} timeout={stats['timeout']} open={open_n} "
            f"bars_closed={stats['bars_closed']} "
            f"max_open_skips={stats['skipped_max_open']}"
        )


async def main() -> None:
    global SYMBOLS
    init_trades_csv()
    SYMBOLS = resolve_symbols()
    if not SYMBOLS:
        raise SystemExit("no symbols resolved")
    for sym in SYMBOLS:
        feeds[sym] = SymbolFeed()

    log(
        f"zigzag_pa | exec={INTERVAL} zigzag=1h | symbols≤{WATCHLIST_SIZE} mode={WATCHLIST_MODE} "
        f"notional=${NOTIONAL} fib ew={EW_RATE} tp={TP_RATE} sl={SL_RATE} max_open={MAX_OPEN}"
    )
    log(f"  log={LOG_FILE}")
    log(f"  trades={TRADES_CSV}")

    await bootstrap_all()

    chunks = [SYMBOLS[i : i + WS_CHUNK] for i in range(0, len(SYMBOLS), WS_CHUNK)]
    tasks = [asyncio.create_task(ws_handler(i, c)) for i, c in enumerate(chunks)]
    tasks.append(asyncio.create_task(stats_loop()))
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
