#!/usr/bin/env python3
"""
Dry paper: ChartPrime Support/Resistance (High Volume Boxes).

  python3 scripts/sr_chartprime_paper.py

Signals on 5m bar close:
  LONG  — sup_holds, break_res, res_as_sup
  SHORT — res_holds, break_sup, sup_as_res

Logs:
  data/sr_chartprime/<run_ts>/sr_dry.log
  data/sr_chartprime/<run_ts>/sr_dry_trades.csv
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
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))

from sr_chartprime_lib import Bar, SRTracker, entry_side, signal_name

try:
    import websockets
except ImportError:
    print("pip install websockets")
    raise

KDIR = ROOT / "data" / "klines" / "1s"


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

WS_ROOT = _env("SR_CHARTPRIME_WS_ROOT", "wss://fstream.binance.com").rstrip("/")
FAPI = _env("SR_CHARTPRIME_FAPI", "https://fapi.binance.com").rstrip("/")
INTERVAL = _env("SR_CHARTPRIME_INTERVAL", "5m")
BAR_MS = {"1m": 60_000, "5m": 300_000, "15m": 900_000}.get(INTERVAL, 300_000)
WS_CHUNK = _env_int("SR_CHARTPRIME_WS_CHUNK", 40)
WATCHLIST_MODE = _env("SR_CHARTPRIME_WATCHLIST_MODE", "all_perps").lower()
WATCHLIST_SIZE = _env_int("SR_CHARTPRIME_WATCHLIST_SIZE", 500)
LOOKBACK = _env_int("SR_CHARTPRIME_LOOKBACK", 20)
VOL_LEN = _env_int("SR_CHARTPRIME_VOL_LEN", 2)
BOX_WIDTH = _env_float("SR_CHARTPRIME_BOX_WIDTH", 1.0)
SIGNAL_MODE = _env("SR_CHARTPRIME_SIGNAL_MODE", "all").lower()
NOTIONAL = _env_float("SR_CHARTPRIME_NOTIONAL_USDT", 6.0)
FEE_RT = _env_float("SR_CHARTPRIME_FEE_RT", 0.0008)
SL_PCT = _env_float("SR_CHARTPRIME_SL_PCT", 8.0)
TP_PCT = _env_float("SR_CHARTPRIME_TP_PCT", 1.5)
MAX_HOLD_BARS = _env_int("SR_CHARTPRIME_MAX_HOLD_BARS", 96)
MAX_OPEN = _env_int("SR_CHARTPRIME_MAX_OPEN", 50)
STATS_INTERVAL_SEC = _env_int("SR_CHARTPRIME_STATS_INTERVAL_SEC", 1800)
BOOTSTRAP_KLINES = _env_int("SR_CHARTPRIME_BOOTSTRAP_KLINES", 300)
WARMUP_BARS = max(LOOKBACK * 2 + 10, 210)

RUN_TS = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
OUT_DIR = Path(_env("SR_CHARTPRIME_OUT_DIR", str(ROOT / f"data/sr_chartprime/{RUN_TS}")))
OUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = OUT_DIR / "sr_dry.log"
TRADES_CSV = OUT_DIR / "sr_dry_trades.csv"

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
    "sup_boxes": 0,
    "res_boxes": 0,
    "sup_holds": 0,
    "res_holds": 0,
    "break_res": 0,
    "break_sup": 0,
    "res_as_sup": 0,
    "sup_as_res": 0,
}


@dataclass
class ActiveTrade:
    symbol: str
    side: str
    signal: str
    signal_ms: int
    entry_ms: int
    entry_px: float
    sl_px: float
    tp_px: float
    bars_held: int = 0
    max_fav_pct: float = 0.0
    max_adv_pct: float = 0.0


@dataclass
class SymbolFeed:
    tracker: SRTracker = field(default_factory=SRTracker)
    trade: ActiveTrade | None = None
    warmed: bool = False


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
    manual = _env("SR_CHARTPRIME_SYMBOLS", "")
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
    raise ValueError(f"unsupported SR_CHARTPRIME_WATCHLIST_MODE: {WATCHLIST_MODE!r}")


def sl_tp_prices(side: str, entry: float) -> tuple[float, float]:
    if side == "long":
        return entry * (1 - SL_PCT / 100), entry * (1 + TP_PCT / 100)
    return entry * (1 + SL_PCT / 100), entry * (1 - TP_PCT / 100)


def pnl_usd(side: str, entry: float, exit_px: float) -> float:
    if entry <= 0:
        return 0.0
    g = (exit_px - entry) / entry * 100 if side == "long" else (entry - exit_px) / entry * 100
    return NOTIONAL * g / 100 - NOTIONAL * FEE_RT


def check_exit(side: str, hi: float, lo: float, sl: float, tp: float) -> tuple[str, float] | None:
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


def open_count() -> int:
    return sum(1 for f in feeds.values() if f.trade is not None)


def init_trades_csv() -> None:
    if not TRADES_CSV.is_file():
        with TRADES_CSV.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(
                [
                    "symbol", "side", "signal", "signal_utc", "entry_utc", "exit_utc",
                    "entry_px", "exit_px", "reason", "net_usd", "hold_bars", "max_fav_pct", "max_adv_pct",
                ]
            )


def fetch_klines(symbol: str, limit: int) -> list[Bar]:
    url = f"{FAPI}/fapi/v1/klines?symbol={quote(symbol)}&interval={INTERVAL}&limit={limit}"
    rows = _http_json(url)
    now_period = (int(time.time() * 1000) // BAR_MS) * BAR_MS
    out: list[Bar] = []
    for r in rows:
        ts = int(r[0])
        if ts >= now_period:
            continue
        out.append(Bar(ts, float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])))
    return out


def prime_feed(symbol: str) -> int:
    time.sleep(0.12)
    try:
        bars = fetch_klines(symbol, BOOTSTRAP_KLINES)
    except Exception as e:
        log(f"[bootstrap] {symbol} fetch failed: {e}")
        return 0
    feed = feeds[symbol]
    feed.tracker = SRTracker(LOOKBACK, VOL_LEN, BOX_WIDTH)
    if bars:
        feed.tracker.load_history(bars)
    if len(bars) >= WARMUP_BARS:
        feed.warmed = True
        stats["warmup_ready"] += 1
    return len(bars)


async def bootstrap_all() -> None:
    global SYMBOLS
    log(f"[bootstrap] fetching {BOOTSTRAP_KLINES}×{INTERVAL} klines for {len(SYMBOLS)} symbols...")
    sem = asyncio.Semaphore(4)
    loop = asyncio.get_event_loop()

    async def one(sym: str) -> None:
        async with sem:
            n = await loop.run_in_executor(None, prime_feed, sym)
            if 0 < n < WARMUP_BARS:
                log(f"[bootstrap] {sym} only {n} bars (need {WARMUP_BARS})")

    await asyncio.gather(*[one(s) for s in SYMBOLS])
    ready = [s for s in SYMBOLS if feeds[s].tracker.bars]
    skipped = len(SYMBOLS) - len(ready)
    SYMBOLS = ready
    if not SYMBOLS:
        raise SystemExit("bootstrap failed — no symbols loaded (check network / Binance API)")
    log(f"[bootstrap] warmed={stats['warmup_ready']}/{len(SYMBOLS)} skipped={skipped}")
    with_sr = sum(
        1
        for s in SYMBOLS
        if feeds[s].tracker.st.support is not None or feeds[s].tracker.st.resistance is not None
    )
    log(f"[bootstrap] symbols_with_sr_levels={with_sr}/{len(SYMBOLS)}")


def update_mfe_mae(t: ActiveTrade, hi: float, lo: float) -> None:
    ep = t.entry_px
    if t.side == "long":
        t.max_fav_pct = max(t.max_fav_pct, (hi - ep) / ep * 100)
        t.max_adv_pct = max(t.max_adv_pct, (ep - lo) / ep * 100)
    else:
        t.max_fav_pct = max(t.max_fav_pct, (ep - lo) / ep * 100)
        t.max_adv_pct = max(t.max_adv_pct, (hi - ep) / ep * 100)


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
    hold = exit_ms - t.entry_ms
    log(
        f"[EXIT] {t.symbol} {t.side.upper()} signal={t.signal} reason={reason} "
        f"entry={t.entry_px:.8f} exit={exit_px:.8f} net=${net:+.4f} hold={hold // 1000}s "
        f"fav={t.max_fav_pct:.2f}% adv={t.max_adv_pct:.2f}%"
    )
    with TRADES_CSV.open("a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(
            [
                t.symbol, t.side, t.signal, utc_iso(t.signal_ms), utc_iso(t.entry_ms), utc_iso(exit_ms),
                f"{t.entry_px:.8f}", f"{exit_px:.8f}", reason, f"{net:.4f}",
                t.bars_held, f"{t.max_fav_pct:.2f}", f"{t.max_adv_pct:.2f}",
            ]
        )
    feed.trade = None


def open_trade(symbol: str, feed: SymbolFeed, side: str, sig_name: str, signal_ms: int, entry_px: float) -> None:
    sl_px, tp_px = sl_tp_prices(side, entry_px)
    feed.trade = ActiveTrade(symbol, side, sig_name, signal_ms, signal_ms, entry_px, sl_px, tp_px)
    stats["entries"] += 1
    log(
        f"[ENTRY] {symbol} {side.upper()} signal={sig_name} @ {utc_iso(signal_ms)} price={entry_px:.8f} "
        f"SL={sl_px:.8f} ({SL_PCT}%) TP={tp_px:.8f} ({TP_PCT}%)"
    )


def bump_signal_stats(sig_name: str) -> None:
    if sig_name in stats:
        stats[sig_name] += 1


def detect_sr_on_close(symbol: str, feed: SymbolFeed, bar: Bar) -> None:
    st = feed.tracker.st
    prev_sup = st.support
    prev_res = st.resistance

    sig = feed.tracker.on_bar(bar)

    if st.support != prev_sup and st.support is not None:
        stats["sup_boxes"] += 1
        log(f"[SR_BOX] {symbol} SUPPORT @ {st.support:.8f} box_low={st.support_1:.8f} ts={utc_iso(bar.ts)}")
    if st.resistance != prev_res and st.resistance is not None:
        stats["res_boxes"] += 1
        log(f"[SR_BOX] {symbol} RESISTANCE @ {st.resistance:.8f} box_high={st.resistance_1:.8f} ts={utc_iso(bar.ts)}")

    side = entry_side(sig, SIGNAL_MODE)
    if not side:
        return

    sig_name = signal_name(sig)
    bump_signal_stats(sig_name)
    stats["signals"] += 1

    if feed.trade is not None:
        stats["skipped_busy"] += 1
        log(f"[SIGNAL_SKIP] {symbol} {sig_name} {side.upper()} — position open ({feed.trade.side})")
        return
    if open_count() >= MAX_OPEN:
        stats["skipped_max_open"] += 1
        log(f"[SIGNAL_SKIP] {symbol} {sig_name} {side.upper()} — max_open={MAX_OPEN}")
        return

    log(f"[SIGNAL] {symbol} {sig_name} → {side.upper()} @ {utc_iso(bar.ts)} close={bar.c:.8f}")
    open_trade(symbol, feed, side, sig_name, bar.ts, bar.c)


def on_bar_close(symbol: str, bar: Bar) -> None:
    feed = feeds[symbol]
    if feed.tracker.bars and feed.tracker.bars[-1].ts == bar.ts:
        return

    if not feed.warmed and len(feed.tracker.bars) + 1 >= WARMUP_BARS:
        feed.warmed = True
        stats["warmup_ready"] += 1
        log(f"[WARMUP] {symbol} ready bars={len(feed.tracker.bars) + 1}")

    if not feed.warmed:
        feed.tracker.on_bar(bar)
        return

    detect_sr_on_close(symbol, feed, bar)


def on_kline(symbol: str, k: dict) -> None:
    feed = feeds.get(symbol)
    if feed is None:
        return
    hi, lo, cl = float(k["h"]), float(k["l"]), float(k["c"])
    ts = int(k["t"])
    vol = float(k.get("v", 0))

    t = feed.trade
    if t:
        update_mfe_mae(t, hi, lo)
        hit = check_exit(t.side, hi, lo, t.sl_px, t.tp_px)
        if hit:
            reason, px = hit
            close_trade(feed, ts, px, reason)
        elif k.get("x"):
            t.bars_held += 1
            if t.bars_held >= MAX_HOLD_BARS:
                close_trade(feed, ts, cl, "timeout")

    if k.get("x"):
        on_bar_close(symbol, Bar(ts, float(k["o"]), hi, lo, cl, vol))


async def ws_handler(conn_id: int, symbols: list[str]) -> None:
    streams = "/".join(f"{s.lower()}@kline_{INTERVAL}" for s in symbols)
    url = f"{WS_ROOT}/stream?streams={streams}"
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
            f"[stats] warmed={stats['warmup_ready']}/{len(SYMBOLS)} mode={SIGNAL_MODE} "
            f"signals={stats['signals']} entries={stats['entries']} exits={exits} wr={wr:.1f}% "
            f"net=${stats['net_usd']:+.4f} tp={stats['tp']} sl={stats['sl']} timeout={stats['timeout']} "
            f"open={open_n} sup_boxes={stats['sup_boxes']} res_boxes={stats['res_boxes']} "
            f"busy_skips={stats['skipped_busy']} max_open_skips={stats['skipped_max_open']}"
        )


async def main() -> None:
    global SYMBOLS
    init_trades_csv()
    SYMBOLS = resolve_symbols()
    if not SYMBOLS:
        raise SystemExit("no symbols resolved")
    for sym in SYMBOLS:
        feeds[sym] = SymbolFeed(tracker=SRTracker(LOOKBACK, VOL_LEN, BOX_WIDTH))

    log(
        f"sr_chartprime | TF={INTERVAL} lookback={LOOKBACK} vol_len={VOL_LEN} box_width={BOX_WIDTH} | "
        f"symbols={len(SYMBOLS)} mode={WATCHLIST_MODE} notional=${NOTIONAL} "
        f"SL={SL_PCT}% TP={TP_PCT}% max_open={MAX_OPEN} signals={SIGNAL_MODE}"
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
