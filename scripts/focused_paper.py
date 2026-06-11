#!/usr/bin/env python3
"""
Live paper: 6% 1s events + sig_open_break adaptive direction.

  python3 scripts/focused_paper.py

  FOCUSED_LIVE_TRADE=false             # true = real Binance market orders
  FOCUSED_WATCHLIST_MODE=all_perps     # default ~500 USDT-M perps
  FOCUSED_WATCHLIST_SIZE=500
  FOCUSED_CONFIG=config/whale-focused.yaml

Entry: signal second T excluded → T+1 OPEN. Exit: T+hold_sec CLOSE. Flip on sig_open.

Output: data/focused/<run_ts>/{events,trades,results}.csv
"""
from __future__ import annotations

import asyncio
import csv
import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))

from binance_futures import (
    BinanceFuturesClient,
    close_side_for_dir,
    order_side_for_dir,
    parse_fill,
)
from focused_lib import (
    Bar,
    SigOpenBreakTrade,
    burst_direction,
    dry_pnl_usdt,
    is_six_pct_event,
    side_label,
)

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


def _env_int(name: str, default: int) -> int:
    v = os.environ.get(name, "").strip()
    return int(v) if v else default


def _env_float(name: str, default: float) -> float:
    v = os.environ.get(name, "").strip()
    return float(v) if v else default


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.environ.get(name, "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "on")


load_dotenv()

CONFIG_PATH = Path(os.environ.get("FOCUSED_CONFIG", ROOT / "config" / "whale-focused.yaml"))
WS_ROOT = os.environ.get("FOCUSED_WS_ROOT", "wss://fstream.binance.com").rstrip("/")
FAPI = os.environ.get("FOCUSED_FAPI", "https://fapi.binance.com").rstrip("/")
WS_CHUNK = _env_int("FOCUSED_WS_CHUNK", 80)
STATS_INTERVAL_SEC = _env_int("FOCUSED_STATS_INTERVAL_SEC", 1800)
WATCHLIST_MODE = os.environ.get("FOCUSED_WATCHLIST_MODE", "all_perps").strip().lower()
WATCHLIST_SIZE = _env_int("FOCUSED_WATCHLIST_SIZE", 500)

cfg = yaml.safe_load(CONFIG_PATH.read_text())
EVENT_THRESH_PCT = float(cfg.get("event_thresh_pct", 6.0))
MIN_EVENT_VOL = float(cfg.get("min_event_vol", 100))
HOLD_SEC = _env_int("FOCUSED_HOLD_SEC", int(cfg.get("hold_sec", 300)))
REARM_SEC = int(cfg.get("cooldown_sec", 300))
MARGIN_USDT = float(cfg.get("margin_usdt", 1))
LEVERAGE = float(cfg.get("leverage", 10))
DRY_RUN = bool(cfg.get("dry_run", True))
LATE_MS = _env_int("FOCUSED_LATE_MS", 30)
NOTIONAL = _env_float(
    "FOCUSED_NOTIONAL_USDT",
    float(cfg.get("notional_usdt", MARGIN_USDT * LEVERAGE)),
)

# Live Binance — env overrides yaml; default off
LIVE_TRADE = _env_bool("FOCUSED_LIVE_TRADE", bool(cfg.get("live_trade", False)))
MAX_OPEN_ORDERS = _env_int("FOCUSED_MAX_OPEN_ORDERS", 2)

binance: BinanceFuturesClient | None = None
if LIVE_TRADE:
    binance = BinanceFuturesClient(
        os.environ.get("FOCUSED_BINANCE_API_KEY", ""),
        os.environ.get("FOCUSED_BINANCE_API_SECRET", ""),
        FAPI,
    )
    if not binance.configured():
        raise SystemExit(
            "FOCUSED_LIVE_TRADE requires FOCUSED_BINANCE_API_KEY and FOCUSED_BINANCE_API_SECRET"
        )
    binance.warm_cache()

RUN_TS = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
OUT_DIR = Path(os.environ.get("FOCUSED_OUT_DIR", ROOT / f"data/focused/{RUN_TS}"))
OUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = OUT_DIR / "focused.log"
EVENT_CSV = OUT_DIR / "events.csv"
TRADES_CSV = OUT_DIR / "trades.csv"
RESULTS_CSV = OUT_DIR / "results.csv"


def log(msg: str) -> None:
    print(msg, flush=True)
    ts = datetime.now(timezone.utc).isoformat()
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(f"{ts} {msg}\n")

for path, header in [
    (
        EVENT_CSV,
        [
            "symbol", "event_ts_ms", "event_utc", "burst_dir", "amp_pct",
            "open", "high", "low", "close", "volume",
        ],
    ),
    (
        TRADES_CSV,
        [
            "symbol", "signal_ts_ms", "signal_utc", "burst_dir", "trade_dir", "side",
            "entry_ts_ms", "entry_utc", "entry_price", "sig_open", "amp_pct", "hold_sec",
        ],
    ),
    (
        RESULTS_CSV,
        [
            "symbol", "signal_ts_ms", "signal_utc", "burst_dir", "trade_dir", "side",
            "entry_price", "exit_ts_ms", "exit_utc", "exit_price",
            "hold_sec", "flipped", "flip_sec", "max_fav_pct", "max_adv_pct", "pnl_pct",
            "dry_pnl_usdt", "late_entry_price", "late_exit_price", "late_pnl_pct", "late_dry_pnl_usdt",
        ],
    ),
]:
    with path.open("w", newline="") as f:
        csv.writer(f).writerow(header)

log(f"focused_paper | log_file={LOG_FILE}")


def utc_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def _http_json(url: str):
    with urllib.request.urlopen(url, timeout=60) as resp:
        return json.load(resp)


def fetch_usdt_perps() -> list[str]:
    info = _http_json(f"{FAPI}/fapi/v1/exchangeInfo")
    return sorted(
        s["symbol"]
        for s in info.get("symbols", [])
        if s.get("contractType") == "PERPETUAL"
        and s.get("quoteAsset") == "USDT"
        and s.get("status") == "TRADING"
        and "_" not in s.get("symbol", "")
    )


def fetch_lowest_volume_perps(n: int) -> list[str]:
    perps = set(fetch_usdt_perps())
    ranked = []
    for t in _http_json(f"{FAPI}/fapi/v1/ticker/24hr"):
        sym = t.get("symbol", "")
        if sym in perps:
            ranked.append((sym, float(t.get("quoteVolume", 0) or 0)))
    ranked.sort(key=lambda x: (x[1], x[0]))
    return [sym for sym, _ in ranked[:n]]


def resolve_symbols() -> list[str]:
    manual = os.environ.get("FOCUSED_SYMBOLS", "").strip()
    if manual:
        return [s.strip().upper() for s in manual.split(",") if s.strip()]
    if WATCHLIST_MODE == "config":
        symbols_cfg = cfg.get("symbols") or {}
        return sorted(s for s, c in symbols_cfg.items() if c.get("enabled", True))
    if WATCHLIST_MODE == "lowest_volume":
        return fetch_lowest_volume_perps(WATCHLIST_SIZE)
    if WATCHLIST_MODE == "all_perps":
        perps = fetch_usdt_perps()
        return perps[:WATCHLIST_SIZE] if WATCHLIST_SIZE > 0 else perps
    raise ValueError(f"unsupported FOCUSED_WATCHLIST_MODE: {WATCHLIST_MODE!r}")


SYMBOLS = resolve_symbols()
if not SYMBOLS:
    raise SystemExit("no symbols resolved")

buckets: dict[str, dict] = {s: {} for s in SYMBOLS}
last_event_ms: dict[str, int] = {}
active: dict[str, SigOpenBreakTrade] = {}
stats = {
    "trades": 0,
    "events": 0,
    "entries": 0,
    "exits": 0,
    "flips": 0,
    "cooldown_skips": 0,
    "live_entries": 0,
    "live_exits": 0,
    "live_flips": 0,
    "live_skips": 0,
    "dry_pnl_usdt": 0.0,
    "late_dry_pnl_usdt": 0.0,
}

live_slots: dict[str, dict] = {}


def in_cooldown(symbol: str, sec_ms: int) -> bool:
    prev = last_event_ms.get(symbol, 0)
    return sec_ms - prev < REARM_SEC * 1000


def live_open_count() -> int:
    return len(live_slots)


def can_open_live() -> bool:
    return LIVE_TRADE and live_open_count() < MAX_OPEN_ORDERS


async def live_entry(symbol: str, trade: SigOpenBreakTrade, entry_open: float) -> None:
    if not LIVE_TRADE or binance is None:
        return
    if not can_open_live():
        stats["live_skips"] += 1
        log(f"[LIVE_SKIP] {symbol} max_open={MAX_OPEN_ORDERS}")
        return
    sym = symbol.upper()
    if not binance.symbol_tradable(sym):
        stats["live_skips"] += 1
        log(f"[LIVE_SKIP] {symbol} not tradable")
        return

    def _place():
        lev = binance.set_max_leverage(sym)
        side = order_side_for_dir(trade.trade_dir)
        resp = binance.market_order_notional(sym, side, NOTIONAL)
        entry, qty = parse_fill(resp)
        return lev, entry, qty, side

    try:
        lev, entry, qty, side = await asyncio.get_running_loop().run_in_executor(None, _place)
        if entry <= 0:
            entry = entry_open
        live_slots[sym] = {
            "trade_dir": trade.trade_dir,
            "qty": qty,
            "entry_price": entry,
            "leverage": lev,
        }
        stats["live_entries"] += 1
        log(
            f"[LIVE_ENTRY] {symbol} {side_label(trade.trade_dir)} side={side} "
            f"fill={entry:.8f} qty={qty:.8f} notional=${NOTIONAL:.2f} lev={lev}x"
        )
    except Exception as e:
        log(f"[LIVE_ENTRY_FAIL] {symbol} {e}")


async def live_flip(symbol: str, new_dir: str, ref_price: float) -> None:
    if not LIVE_TRADE or binance is None:
        return
    sym = symbol.upper()
    slot = live_slots.get(sym)
    if not slot:
        return

    def _flip():
        close_side = close_side_for_dir(slot["trade_dir"])
        binance.market_close_qty(sym, close_side, slot["qty"])
        lev = binance.set_max_leverage(sym)
        open_side = order_side_for_dir(new_dir)
        resp = binance.market_order_notional(sym, open_side, NOTIONAL)
        entry, qty = parse_fill(resp)
        return lev, entry, qty, open_side

    try:
        lev, entry, qty, side = await asyncio.get_running_loop().run_in_executor(None, _flip)
        if entry <= 0:
            entry = ref_price
        live_slots[sym] = {
            "trade_dir": new_dir,
            "qty": qty,
            "entry_price": entry,
            "leverage": lev,
        }
        stats["live_flips"] += 1
        log(
            f"[LIVE_FLIP] {symbol} -> {side_label(new_dir)} side={side} "
            f"fill={entry:.8f} qty={qty:.8f} lev={lev}x"
        )
    except Exception as e:
        log(f"[LIVE_FLIP_FAIL] {symbol} {e}")
        live_slots.pop(sym, None)


async def live_exit(symbol: str) -> None:
    if not LIVE_TRADE or binance is None:
        return
    sym = symbol.upper()
    slot = live_slots.pop(sym, None)
    if not slot:
        return

    def _close():
        close_side = close_side_for_dir(slot["trade_dir"])
        return binance.market_close_qty(sym, close_side, slot["qty"])

    try:
        resp = await asyncio.get_running_loop().run_in_executor(None, _close)
        exit_px, _ = parse_fill(resp)
        stats["live_exits"] += 1
        log(
            f"[LIVE_EXIT] {symbol} {side_label(slot['trade_dir'])} "
            f"exit={exit_px:.8f} entry={slot['entry_price']:.8f}"
        )
    except Exception as e:
        log(f"[LIVE_EXIT_FAIL] {symbol} {e}")


def write_trade_entry(t: SigOpenBreakTrade) -> None:
    with TRADES_CSV.open("a", newline="") as f:
        csv.writer(f).writerow(
            [
                t.symbol,
                t.signal_ts_ms,
                utc_iso(t.signal_ts_ms),
                t.burst_dir,
                t.trade_dir,
                side_label(t.trade_dir),
                t.entry_ts_ms,
                utc_iso(t.entry_ts_ms),
                f"{t.entry_price:.8f}",
                f"{t.sig_open:.8f}",
                f"{t.sig_amp_pct:.4f}",
                t.hold_sec,
            ]
        )


def write_trade_result(
    t: SigOpenBreakTrade,
    exit_price: float,
    pnl: float,
    dry_usdt: float,
    late_pnl: float,
    late_usdt: float,
    late_entry: float,
    late_exit: float,
) -> None:
    with RESULTS_CSV.open("a", newline="") as f:
        csv.writer(f).writerow(
            [
                t.symbol,
                t.signal_ts_ms,
                utc_iso(t.signal_ts_ms),
                t.burst_dir,
                t.trade_dir,
                side_label(t.trade_dir),
                f"{t.entry_price:.8f}",
                t.exit_ts_ms,
                utc_iso(t.exit_ts_ms),
                f"{exit_price:.8f}",
                t.hold_sec,
                int(t.flipped),
                t.flip_sec,
                f"{t.max_fav_pct:.4f}",
                f"{t.max_adv_pct:.4f}",
                f"{pnl:.6f}",
                f"{dry_usdt:.6f}",
                f"{late_entry:.8f}",
                f"{late_exit:.8f}",
                f"{late_pnl:.6f}",
                f"{late_usdt:.6f}",
            ]
        )


async def close_trade(symbol: str, exit_price: float) -> None:
    await live_exit(symbol)
    t = active.pop(symbol, None)
    if not t or t.status != "active":
        return
    pnl = t.close_pnl_pct(exit_price)
    late_pnl, late_entry, late_exit = t.late_pnl_pct(exit_price)
    dry_usdt = dry_pnl_usdt(MARGIN_USDT, LEVERAGE, pnl) if DRY_RUN else 0.0
    late_dry_usdt = dry_pnl_usdt(MARGIN_USDT, LEVERAGE, late_pnl) if DRY_RUN else 0.0
    stats["exits"] += 1
    if DRY_RUN:
        stats["dry_pnl_usdt"] += dry_usdt
        stats["late_dry_pnl_usdt"] += late_dry_usdt
    log(
        f"[EXIT] {symbol} {side_label(t.trade_dir)} "
        f"entry={t.entry_price:.8f} exit={exit_price:.8f} "
        f"pnl={pnl:+.2f}% fav={t.max_fav_pct:+.2f}% adv={t.max_adv_pct:+.2f}% "
        f"flip={t.flipped} @ {utc_iso(t.exit_ts_ms)}"
    )
    if DRY_RUN:
        log(
            f"[EXIT_DRY] {symbol} margin={MARGIN_USDT} lev={LEVERAGE:.0f} "
            f"pnl_usdt={dry_usdt:+.4f} cumulative={stats['dry_pnl_usdt']:+.4f}"
        )
    log(
        f"[EXIT_LATE{LATE_MS}ms] {symbol} {side_label(t.trade_dir)} "
        f"entry={late_entry:.8f} exit={late_exit:.8f} pnl={late_pnl:+.2f}%"
    )
    if DRY_RUN:
        log(
            f"[EXIT_LATE{LATE_MS}ms_DRY] {symbol} margin={MARGIN_USDT} lev={LEVERAGE:.0f} "
            f"pnl_usdt={late_dry_usdt:+.4f} cumulative={stats['late_dry_pnl_usdt']:+.4f}"
        )
    write_trade_result(t, exit_price, pnl, dry_usdt, late_pnl, late_dry_usdt, late_entry, late_exit)


def start_event(symbol: str, bar: Bar) -> None:
    burst = burst_direction(bar)
    amp = bar.amp_pct()
    stats["events"] += 1
    last_event_ms[symbol] = bar.sec
    log(
        f"[EVENT] {symbol} {burst} {amp:.2f}% @ {utc_iso(bar.sec)} "
        f"open={bar.o:.8f}"
    )
    with EVENT_CSV.open("a", newline="") as f:
        csv.writer(f).writerow(
            [
                symbol,
                bar.sec,
                utc_iso(bar.sec),
                burst,
                f"{amp:.4f}",
                f"{bar.o:.8f}",
                f"{bar.h:.8f}",
                f"{bar.l:.8f}",
                f"{bar.c:.8f}",
                f"{bar.vol:.8f}",
            ]
        )
    trade = SigOpenBreakTrade(
        symbol=symbol,
        signal_ts_ms=bar.sec,
        burst_dir=burst,
        sig_open=bar.o,
        sig_amp_pct=amp,
        entry_ts_ms=bar.sec + 1000,
        exit_ts_ms=bar.sec + HOLD_SEC * 1000,
        hold_sec=HOLD_SEC,
    )
    active[symbol] = trade


async def process_bar(symbol: str, b: dict) -> None:
    sec = b["sec"]
    o, h, l, c, vol = b["open"], b["high"], b["low"], b["close"], b["volume"]
    bar = Bar(sec=sec, o=o, h=h, l=l, c=c, vol=vol)

    trade = active.get(symbol)
    if trade:
        if trade.status == "pending_entry" and sec == trade.entry_ts_ms:
            trade.on_entry_bar(o)
            stats["entries"] += 1
            write_trade_entry(trade)
            log(
                f"[ENTRY] {symbol} {side_label(trade.trade_dir)} @ {utc_iso(sec)} "
                f"price={o:.8f} burst={trade.burst_dir} sig_open={trade.sig_open:.8f}"
            )
            if DRY_RUN:
                log(
                    f"[ENTRY_DRY] {symbol} margin={MARGIN_USDT} USDT lev={LEVERAGE:.0f} "
                    f"notional={NOTIONAL:.2f} USDT"
                )
            await live_entry(symbol, trade, o)
        elif trade.status == "active":
            prev_flipped = trade.flipped
            trade.on_bar(bar)
            if trade.flipped and not prev_flipped:
                stats["flips"] += 1
                log(
                    f"[FLIP] {symbol} -> {side_label(trade.trade_dir)} @ T+{trade.flip_sec}s "
                    f"close={c:.8f} sig_open={trade.sig_open:.8f}"
                )
                await live_flip(symbol, trade.trade_dir, c)
            if sec >= trade.exit_ts_ms:
                trade.exit_ts_ms = sec
                await close_trade(symbol, c)
                return

    if symbol in active:
        return
    if not is_six_pct_event(bar, EVENT_THRESH_PCT, MIN_EVENT_VOL):
        return
    if in_cooldown(symbol, sec):
        stats["cooldown_skips"] += 1
        return
    start_event(symbol, bar)


async def finalize_bucket(symbol: str, b: dict) -> None:
    await process_bar(symbol, b)


async def on_trade(symbol: str, price: float, qty: float, t_ms: int) -> None:
    stats["trades"] += 1
    trade = active.get(symbol)
    if trade and trade.status in ("pending_entry", "active"):
        trade.on_tick(t_ms, price, LATE_MS)
    sec = (t_ms // 1000) * 1000
    state = buckets.get(symbol)
    if state is None:
        return
    b = state.get("cur")
    if not b or b["sec"] != sec:
        if b:
            await finalize_bucket(symbol, b)
        state["cur"] = {
            "sec": sec,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": qty,
        }
    else:
        b["close"] = price
        b["high"] = max(b["high"], price)
        b["low"] = min(b["low"], price)
        b["volume"] += qty


async def ws_handler(conn_id: int, symbols: list[str]) -> None:
    streams = "/".join(f"{s.lower()}@aggTrade" for s in symbols)
    url = f"{WS_ROOT}/market/stream?streams={streams}"
    log(f"[ws-{conn_id}] connecting {len(symbols)} symbols")
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, max_size=2**22) as ws:
                log(f"[ws-{conn_id}] connected")
                async for msg in ws:
                    data = json.loads(msg).get("data")
                    if not data:
                        continue
                    await on_trade(data["s"], float(data["p"]), float(data["q"]), int(data["T"]))
        except Exception as e:
            log(f"[ws-{conn_id}] reconnect ({e})")
            await asyncio.sleep(3)


async def flush_stale_buckets_loop() -> None:
    while True:
        cutoff = (int(time.time()) - 1) * 1000
        for symbol, state in buckets.items():
            b = state.get("cur")
            if b and b["sec"] < cutoff:
                await finalize_bucket(symbol, b)
                state.pop("cur", None)
        await asyncio.sleep(0.5)


async def stats_loop() -> None:
    while True:
        await asyncio.sleep(STATS_INTERVAL_SEC)
        open_trades = sum(1 for t in active.values() if t.status == "active")
        pending = sum(1 for t in active.values() if t.status == "pending_entry")
        dry_line = ""
        if DRY_RUN:
            dry_line = (
                f" dry_pnl={stats['dry_pnl_usdt']:+.4f} "
                f"late{LATE_MS}ms_dry_pnl={stats['late_dry_pnl_usdt']:+.4f}"
            )
        live_open = live_open_count()
        live_line = ""
        if LIVE_TRADE:
            live_line = (
                f" live_open={live_open}/{MAX_OPEN_ORDERS} "
                f"live_entries={stats['live_entries']} live_flips={stats['live_flips']} "
                f"live_skips={stats['live_skips']}"
            )
        log(
            f"[stats] agg_trades={stats['trades']} events={stats['events']} "
            f"entries={stats['entries']} exits={stats['exits']} flips={stats['flips']} "
            f"open={open_trades} pending={pending} "
            f"cooldown_skips={stats['cooldown_skips']}{dry_line}{live_line}"
        )


async def main() -> None:
    chunks = [SYMBOLS[i : i + WS_CHUNK] for i in range(0, len(SYMBOLS), WS_CHUNK)]
    log(f"focused_paper | strategy=sig_open_break | config={CONFIG_PATH}")
    log(
        f"  symbols={len(SYMBOLS)} mode={WATCHLIST_MODE} connections={len(chunks)} "
        f"event>={EVENT_THRESH_PCT}% hold={HOLD_SEC}s rearm={REARM_SEC}s "
        f"dry={DRY_RUN} notional=${NOTIONAL} margin={MARGIN_USDT} lev={LEVERAGE} late_ms={LATE_MS}"
    )
    log(
        f"  LIVE_TRADE={LIVE_TRADE} max_open={MAX_OPEN_ORDERS} "
        f"(live mirrors paper: entry T+1 / flip / exit T+{HOLD_SEC}s)"
    )
    log(f"  out={OUT_DIR}")
    await asyncio.gather(
        *[ws_handler(i, c) for i, c in enumerate(chunks)],
        flush_stale_buckets_loop(),
        stats_loop(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("Stopping")
