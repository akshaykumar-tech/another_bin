#!/usr/bin/env python3
"""
Dry paper: SMC Supply zone break → SHORT only (supply_bos + supply_choch).

  python3 scripts/smc_sd_paper.py

  300 perps default | 5m | SL 8% TP 1.5% | max hold 96 bars
"""
from __future__ import annotations

import asyncio
import csv
import json
import os
import sys
import time
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))

from smc_sd_lib import SDSignal, SDState, on_bar_confirmed
from sr_chartprime_lib import Bar

try:
    import websockets
except ImportError:
    print("pip install websockets")
    raise


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

WS_ROOT = _env("SMC_SD_WS_ROOT", "wss://fstream.binance.com").rstrip("/")
FAPI = _env("SMC_SD_FAPI", "https://fapi.binance.com").rstrip("/")
INTERVAL = _env("SMC_SD_INTERVAL", "5m")
BAR_MS = 300_000
WS_CHUNK = _env_int("SMC_SD_WS_CHUNK", 40)
WATCHLIST_MODE = _env("SMC_SD_WATCHLIST_MODE", "all_perps").lower()
WATCHLIST_SIZE = _env_int("SMC_SD_WATCHLIST_SIZE", 300)
SWING_LEN = _env_int("SMC_SD_SWING_LEN", 50)
NOTIONAL = _env_float("SMC_SD_NOTIONAL_USDT", 6.0)
FEE_RT = _env_float("SMC_SD_FEE_RT", 0.0008)
SL_PCT = _env_float("SMC_SD_SL_PCT", 8.0)
TP_PCT = _env_float("SMC_SD_TP_PCT", 1.5)
MAX_HOLD_BARS = _env_int("SMC_SD_MAX_HOLD_BARS", 96)
MAX_OPEN = _env_int("SMC_SD_MAX_OPEN", 0)  # 0 = unlimited
BOOTSTRAP_KLINES = _env_int("SMC_SD_BOOTSTRAP_KLINES", 500)
STATS_INTERVAL_SEC = _env_int("SMC_SD_STATS_INTERVAL_SEC", 1800)
BAR_HISTORY_MAX = _env_int("SMC_SD_BAR_HISTORY_MAX", 500)
WARMUP_BARS = SWING_LEN + 60

OUT_DIR = Path(_env("SMC_SD_OUT_DIR", str(ROOT / "data/smc_sd/dry")))
OUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = OUT_DIR / "smc_sd_dry.log"
TRADES_CSV = OUT_DIR / "smc_sd_dry_trades.csv"

stats = {
    "signals": 0,
    "signals_skipped": 0,
    "entries": 0,
    "exits": 0,
    "wins": 0,
    "net_usd": 0.0,
    "tp": 0,
    "sl": 0,
    "timeout": 0,
    "supply_bos": 0,
    "supply_choch": 0,
    "bars_closed": 0,
    "warmup_ready": 0,
}


@dataclass
class ActiveTrade:
    symbol: str
    side: str
    setup: str
    signal_ms: int
    entry_ms: int
    entry_px: float
    sl_px: float
    tp_px: float
    level: float
    bars_held: int = 0
    max_fav_pct: float = 0.0
    max_adv_pct: float = 0.0


@dataclass
class SymbolFeed:
    bars: list[Bar] = field(default_factory=list)
    state: SDState = field(default_factory=SDState)
    trade: ActiveTrade | None = None
    warmed: bool = False
    last_closed_ts: int = 0


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


def resolve_symbols() -> list[str]:
    manual = _env("SMC_SD_SYMBOLS", "")
    if manual:
        return sorted(s.strip().upper() for s in manual.split(",") if s.strip())
    if WATCHLIST_MODE == "all_perps":
        info = _http_json(f"{FAPI}/fapi/v1/exchangeInfo")
        out = [
            s["symbol"]
            for s in info["symbols"]
            if s.get("contractType") == "PERPETUAL"
            and s.get("quoteAsset") == "USDT"
            and s.get("status") == "TRADING"
            and s["symbol"].isascii()
        ]
        return sorted(out[:WATCHLIST_SIZE] if WATCHLIST_SIZE > 0 else out)
    raise ValueError(f"unsupported SMC_SD_WATCHLIST_MODE: {WATCHLIST_MODE!r}")


def sl_tp_prices(entry: float) -> tuple[float, float]:
    sl = entry * (1 + SL_PCT / 100)
    tp = entry * (1 - TP_PCT / 100)
    return sl, tp


def pnl_usd(entry: float, exit_px: float) -> float:
    if entry <= 0:
        return 0.0
    g = (entry - exit_px) / entry * 100
    return NOTIONAL * g / 100 - NOTIONAL * FEE_RT


def open_count() -> int:
    return sum(1 for f in feeds.values() if f.trade is not None)


def init_trades_csv() -> None:
    if not TRADES_CSV.is_file():
        with TRADES_CSV.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(
                [
                    "symbol", "side", "setup", "level",
                    "signal_utc", "entry_utc", "exit_utc",
                    "entry_px", "exit_px", "tp_px", "sl_px",
                    "reason", "net_usd", "hold_bars", "max_fav_pct", "max_adv_pct",
                ]
            )


def new_sd_state() -> SDState:
    return SDState(swing_len=SWING_LEN)


def accept_signal(sig: SDSignal) -> bool:
    return sig.side == "short" and sig.setup in ("supply_bos", "supply_choch")


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


def replay_bar(symbol: str, feed: SymbolFeed, bar: Bar, allow_trade: bool) -> None:
    if feed.bars and feed.bars[-1].ts == bar.ts:
        return
    feed.bars.append(bar)
    if len(feed.bars) > BAR_HISTORY_MAX:
        feed.bars = feed.bars[-BAR_HISTORY_MAX:]
    i = len(feed.bars) - 1
    sig = on_bar_confirmed(feed.state, feed.bars, i)
    if not allow_trade or not feed.warmed:
        return
    if sig is not None:
        stats["signals"] += 1
        if sig.setup == "supply_bos":
            stats["supply_bos"] += 1
        elif sig.setup == "supply_choch":
            stats["supply_choch"] += 1
    try_open(symbol, feed, sig)


def check_exit(hi: float, lo: float, sl: float, tp: float) -> tuple[str, float] | None:
    if hi >= sl:
        return "sl", sl
    if lo <= tp:
        return "tp", tp
    return None


def update_mfe_mae(t: ActiveTrade, hi: float, lo: float) -> None:
    ep = t.entry_px
    t.max_fav_pct = max(t.max_fav_pct, (ep - lo) / ep * 100)
    t.max_adv_pct = max(t.max_adv_pct, (hi - ep) / ep * 100)


def try_open(symbol: str, feed: SymbolFeed, sig: SDSignal | None) -> None:
    if sig is None or feed.trade is not None:
        return
    if not accept_signal(sig):
        if sig is not None:
            stats["signals_skipped"] += 1
        return
    if MAX_OPEN > 0 and open_count() >= MAX_OPEN:
        stats["signals_skipped"] += 1
        log(f"[SIGNAL_SKIP] {symbol} {sig.setup} — max_open={MAX_OPEN}")
        return
    sl_px, tp_px = sl_tp_prices(sig.entry_px)
    feed.trade = ActiveTrade(
        symbol=symbol,
        side="short",
        setup=sig.setup,
        signal_ms=sig.ts,
        entry_ms=sig.ts,
        entry_px=sig.entry_px,
        sl_px=sl_px,
        tp_px=tp_px,
        level=sig.level,
    )
    stats["entries"] += 1
    log(
        f"[ENTRY] {symbol} SHORT setup={sig.setup} @ {utc_iso(sig.ts)} "
        f"price={sig.entry_px:.8f} level={sig.level:.8f} "
        f"SL={sl_px:.8f} ({SL_PCT}%) TP={tp_px:.8f} ({TP_PCT}%) open={open_count()}"
    )


def close_trade(feed: SymbolFeed, exit_ms: int, exit_px: float, reason: str) -> None:
    t = feed.trade
    if t is None:
        return
    net = pnl_usd(t.entry_px, exit_px)
    stats["exits"] += 1
    stats["net_usd"] += net
    if net > 0:
        stats["wins"] += 1
    if reason in stats:
        stats[reason] += 1
    log(
        f"[EXIT] {t.symbol} SHORT setup={t.setup} reason={reason} "
        f"entry={t.entry_px:.8f} exit={exit_px:.8f} net=${net:+.4f} hold={t.bars_held}bars "
        f"fav={t.max_fav_pct:.2f}% adv={t.max_adv_pct:.2f}%"
    )
    with TRADES_CSV.open("a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(
            [
                t.symbol, t.side, t.setup, f"{t.level:.8f}",
                utc_iso(t.signal_ms), utc_iso(t.entry_ms), utc_iso(exit_ms),
                f"{t.entry_px:.8f}", f"{exit_px:.8f}", f"{t.tp_px:.8f}", f"{t.sl_px:.8f}",
                reason, f"{net:.4f}", t.bars_held, f"{t.max_fav_pct:.2f}", f"{t.max_adv_pct:.2f}",
            ]
        )
    feed.trade = None


def prime_feed(symbol: str) -> int:
    time.sleep(0.08)
    try:
        bars = fetch_klines(symbol, BOOTSTRAP_KLINES)
    except Exception as e:
        log(f"[bootstrap] {symbol} fetch failed: {e}")
        return 0
    feed = feeds[symbol]
    feed.bars = []
    feed.state = new_sd_state()
    for bar in bars:
        replay_bar(symbol, feed, bar, allow_trade=False)
    if len(feed.bars) >= WARMUP_BARS:
        feed.warmed = True
        stats["warmup_ready"] += 1
    return len(feed.bars)


async def bootstrap_all() -> None:
    global SYMBOLS
    log(f"[bootstrap] fetching {BOOTSTRAP_KLINES}×{INTERVAL} for {len(SYMBOLS)} symbols...")
    sem = asyncio.Semaphore(4)
    loop = asyncio.get_running_loop()

    async def one(sym: str) -> None:
        async with sem:
            n = await loop.run_in_executor(None, prime_feed, sym)
            if 0 < n < WARMUP_BARS:
                log(f"[bootstrap] {sym} only {n} bars (need {WARMUP_BARS})")

    await asyncio.gather(*[one(s) for s in SYMBOLS])
    ready = [s for s in SYMBOLS if feeds[s].warmed]
    skipped = len(SYMBOLS) - len(ready)
    SYMBOLS = ready
    if not SYMBOLS:
        raise SystemExit("bootstrap failed — no symbols loaded")
    log(f"[bootstrap] warmed={stats['warmup_ready']}/{len(SYMBOLS)} skipped={skipped}")


def on_kline(symbol: str, k: dict) -> None:
    feed = feeds.get(symbol)
    if feed is None or not feed.warmed:
        return
    hi, lo, cl = float(k["h"]), float(k["l"]), float(k["c"])
    ts = int(k["t"])

    t = feed.trade
    if t:
        update_mfe_mae(t, hi, lo)
        hit = check_exit(hi, lo, t.sl_px, t.tp_px)
        if hit:
            close_trade(feed, ts, hit[1], hit[0])
        elif k.get("x"):
            t.bars_held += 1
            if t.bars_held >= MAX_HOLD_BARS:
                close_trade(feed, ts, cl, "timeout")

    if not k.get("x"):
        return
    if ts == feed.last_closed_ts:
        return
    feed.last_closed_ts = ts

    bar = Bar(ts, float(k["o"]), hi, lo, cl, float(k.get("v", 0)))
    replay_bar(symbol, feed, bar, allow_trade=True)
    stats["bars_closed"] += 1


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
        exits = stats["exits"]
        wr = (stats["wins"] / exits * 100) if exits else 0.0
        log(
            f"[stats] warmed={stats['warmup_ready']}/{len(SYMBOLS)} signals={stats['signals']} "
            f"skip={stats['signals_skipped']} entries={stats['entries']} exits={exits} wr={wr:.1f}% "
            f"net=${stats['net_usd']:+.4f} tp={stats['tp']} sl={stats['sl']} timeout={stats['timeout']} "
            f"open={open_count()} supply_bos={stats['supply_bos']} supply_choch={stats['supply_choch']} "
            f"bars_closed={stats['bars_closed']}"
        )


async def main() -> None:
    global SYMBOLS
    init_trades_csv()
    SYMBOLS = resolve_symbols()
    if not SYMBOLS:
        raise SystemExit("no symbols resolved")
    for sym in SYMBOLS:
        feeds[sym] = SymbolFeed(state=new_sd_state())

    log(
        f"smc_sd_dry | SHORT only supply_bos+supply_choch | TF={INTERVAL} swing={SWING_LEN} "
        f"| symbols={len(SYMBOLS)} mode={WATCHLIST_MODE}"
    )
    log(
        f"  notional=${NOTIONAL} SL={SL_PCT}% TP={TP_PCT}% max_hold={MAX_HOLD_BARS}bars "
        f"max_open={'unlimited' if MAX_OPEN <= 0 else MAX_OPEN}"
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
