#!/usr/bin/env python3
"""
Live paper: vol5x volume spike + sig_open_break (separate from focused_paper 6%).

  python3 scripts/vol5x_paper.py

  VOL5X_LIVE_TRADE=false          # true = real Binance market orders
  VOL5X_WATCHLIST_MODE=all_perps
  VOL5X_WATCHLIST_SIZE=500
  VOL5X_CONFIG=config/whale-vol5x.yaml

Output: data/vol5x/<run_ts>/{vol5x.log,signals,trades,results}.csv
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
    SymbolState,
    burst_direction,
    is_vol5x_signal,
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

CONFIG_PATH = Path(os.environ.get("VOL5X_CONFIG", ROOT / "config" / "whale-vol5x.yaml"))
WS_ROOT = os.environ.get("VOL5X_WS_ROOT", "wss://fstream.binance.com").rstrip("/")
FAPI = os.environ.get("VOL5X_FAPI", "https://fapi.binance.com").rstrip("/")
WS_CHUNK = _env_int("VOL5X_WS_CHUNK", 80)
STATS_INTERVAL_SEC = _env_int("VOL5X_STATS_INTERVAL_SEC", 1800)
WATCHLIST_MODE = os.environ.get("VOL5X_WATCHLIST_MODE", "all_perps").strip().lower()
WATCHLIST_SIZE = _env_int("VOL5X_WATCHLIST_SIZE", 500)

cfg = yaml.safe_load(CONFIG_PATH.read_text())
NOTIONAL = _env_float("VOL5X_NOTIONAL_USDT", float(cfg.get("notional_usdt", 10)))
FEE_PER_SIDE = float(cfg.get("fee_per_side", 0.0004))
VOL_MULT = float(cfg.get("vol_mult", 5.0))
VOL_LOOKBACK = int(cfg.get("vol_lookback", 30))
MIN_VOL = float(cfg.get("min_vol", 1000))
HOLD_SEC = _env_int("VOL5X_HOLD_SEC", int(cfg.get("hold_sec", 300)))
REARM_SEC = int(cfg.get("cooldown_sec", 3600))

# Live Binance — env overrides yaml; default off
LIVE_TRADE = _env_bool("VOL5X_LIVE_TRADE", bool(cfg.get("live_trade", False)))
MAX_OPEN_ORDERS = _env_int("VOL5X_MAX_OPEN_ORDERS", 2)

binance: BinanceFuturesClient | None = None
if LIVE_TRADE:
    binance = BinanceFuturesClient(
        os.environ.get("BINANCE_API_KEY", ""),
        os.environ.get("BINANCE_API_SECRET", ""),
        FAPI,
    )
    if not binance.configured():
        raise SystemExit("VOL5X_LIVE_TRADE requires BINANCE_API_KEY and BINANCE_API_SECRET")
    binance.warm_cache()

RUN_TS = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
OUT_DIR = Path(os.environ.get("VOL5X_OUT_DIR", ROOT / f"data/vol5x/{RUN_TS}"))
OUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = OUT_DIR / "vol5x.log"
SIGNALS_CSV = OUT_DIR / "signals.csv"
TRADES_CSV = OUT_DIR / "trades.csv"
RESULTS_CSV = OUT_DIR / "results.csv"


def log(msg: str) -> None:
    print(msg, flush=True)
    ts = datetime.now(timezone.utc).isoformat()
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(f"{ts} {msg}\n")


for path, header in [
    (
        SIGNALS_CSV,
        [
            "symbol", "signal_ts_ms", "signal_utc", "burst_dir", "vol_ratio",
            "volume", "open", "high", "low", "close",
        ],
    ),
    (
        TRADES_CSV,
        [
            "symbol", "signal_ts_ms", "signal_utc", "burst_dir", "trade_dir", "side",
            "entry_ts_ms", "entry_utc", "entry_price", "sig_open", "vol_ratio", "hold_sec",
        ],
    ),
    (
        RESULTS_CSV,
        [
            "symbol", "signal_ts_ms", "signal_utc", "burst_dir", "trade_dir", "side",
            "entry_price", "exit_ts_ms", "exit_utc", "exit_price",
            "hold_sec", "flipped", "flip_sec", "max_fav_pct", "max_adv_pct", "pnl_pct",
            "gross_usdt", "fees_usdt", "net_usdt",
        ],
    ),
]:
    with path.open("w", newline="") as f:
        csv.writer(f).writerow(header)

log(f"vol5x_paper | log_file={LOG_FILE}")


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
    manual = os.environ.get("VOL5X_SYMBOLS", "").strip()
    if manual:
        return [s.strip().upper() for s in manual.split(",") if s.strip()]
    if WATCHLIST_MODE == "lowest_volume":
        return fetch_lowest_volume_perps(WATCHLIST_SIZE)
    if WATCHLIST_MODE == "all_perps":
        perps = fetch_usdt_perps()
        return perps[:WATCHLIST_SIZE] if WATCHLIST_SIZE > 0 else perps
    raise ValueError(f"unsupported VOL5X_WATCHLIST_MODE: {WATCHLIST_MODE!r}")


SYMBOLS = resolve_symbols()
if not SYMBOLS:
    raise SystemExit("no symbols resolved")

buckets: dict[str, dict] = {s: {} for s in SYMBOLS}
sym_state: dict[str, SymbolState] = {s: SymbolState() for s in SYMBOLS}
last_signal_ms: dict[str, int] = {}
active: dict[str, SigOpenBreakTrade] = {}
stats = {
    "trades": 0,
    "signals": 0,
    "entries": 0,
    "exits": 0,
    "flips": 0,
    "cooldown_skips": 0,
    "live_entries": 0,
    "live_exits": 0,
    "live_flips": 0,
    "live_skips": 0,
    "net_usdt": 0.0,
    "fees_usdt": 0.0,
}

live_slots: dict[str, dict] = {}


def in_cooldown(symbol: str, sec_ms: int) -> bool:
    prev = last_signal_ms.get(symbol, 0)
    return sec_ms - prev < REARM_SEC * 1000


def fee_usd() -> float:
    return NOTIONAL * FEE_PER_SIDE * 2


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
                f"{t.sig_amp_pct:.2f}",
                t.hold_sec,
            ]
        )


def write_trade_result(
    t: SigOpenBreakTrade,
    exit_price: float,
    pnl: float,
    gross: float,
    fees: float,
    net: float,
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
                f"{gross:.6f}",
                f"{fees:.6f}",
                f"{net:.6f}",
            ]
        )


async def close_trade(symbol: str, exit_price: float) -> None:
    await live_exit(symbol)
    t = active.pop(symbol, None)
    if not t or t.status != "active":
        return
    pnl = t.close_pnl_pct(exit_price)
    fees = fee_usd()
    gross = NOTIONAL * pnl / 100.0
    net = gross - fees
    stats["exits"] += 1
    stats["fees_usdt"] += fees
    stats["net_usdt"] += net
    log(
        f"[EXIT] {symbol} {side_label(t.trade_dir)} "
        f"entry={t.entry_price:.8f} exit={exit_price:.8f} "
        f"pnl={pnl:+.2f}% net=${net:+.4f} flip={t.flipped} @ {utc_iso(t.exit_ts_ms)}"
    )
    write_trade_result(t, exit_price, pnl, gross, fees, net)


def start_signal(symbol: str, bar: Bar, vol_ratio: float) -> None:
    burst = burst_direction(bar)
    stats["signals"] += 1
    last_signal_ms[symbol] = bar.sec
    log(
        f"[SIGNAL] {symbol} {burst} vol5x={vol_ratio:.1f}x vol={bar.vol:.0f} "
        f"@ {utc_iso(bar.sec)} open={bar.o:.8f}"
    )
    with SIGNALS_CSV.open("a", newline="") as f:
        csv.writer(f).writerow(
            [
                symbol,
                bar.sec,
                utc_iso(bar.sec),
                burst,
                f"{vol_ratio:.2f}",
                f"{bar.vol:.4f}",
                f"{bar.o:.8f}",
                f"{bar.h:.8f}",
                f"{bar.l:.8f}",
                f"{bar.c:.8f}",
            ]
        )
    trade = SigOpenBreakTrade(
        symbol=symbol,
        signal_ts_ms=bar.sec,
        burst_dir=burst,
        sig_open=bar.o,
        sig_amp_pct=vol_ratio,
        entry_ts_ms=bar.sec + 1000,
        exit_ts_ms=bar.sec + HOLD_SEC * 1000,
        hold_sec=HOLD_SEC,
    )
    active[symbol] = trade


async def process_bar(symbol: str, b: dict) -> None:
    sec = b["sec"]
    o, h, l, c, vol = b["open"], b["high"], b["low"], b["close"], b["volume"]
    bar = Bar(sec=sec, o=o, h=h, l=l, c=c, vol=vol)
    st = sym_state[symbol]
    prior = list(st.history)

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
                st.push(bar)
                return

    st.push(bar)

    if symbol in active:
        return

    ok, ratio = is_vol5x_signal(bar, prior, VOL_MULT, VOL_LOOKBACK, MIN_VOL)
    if not ok:
        return
    if in_cooldown(symbol, sec):
        stats["cooldown_skips"] += 1
        return
    start_signal(symbol, bar, ratio)


async def finalize_bucket(symbol: str, b: dict) -> None:
    await process_bar(symbol, b)


async def on_trade(symbol: str, price: float, qty: float, t_ms: int) -> None:
    stats["trades"] += 1
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
        live_open = live_open_count()
        log(
            f"[stats] signals={stats['signals']} entries={stats['entries']} exits={stats['exits']} "
            f"flips={stats['flips']} open={open_trades} pending={pending} "
            f"paper_net=${stats['net_usdt']:+.4f} live_open={live_open}/{MAX_OPEN_ORDERS} "
            f"live_entries={stats['live_entries']} live_flips={stats['live_flips']} "
            f"live_skips={stats['live_skips']}"
        )


async def main() -> None:
    chunks = [SYMBOLS[i : i + WS_CHUNK] for i in range(0, len(SYMBOLS), WS_CHUNK)]
    log(f"vol5x_paper | strategy=vol5x_sig_open | config={CONFIG_PATH}")
    log(
        f"  symbols={len(SYMBOLS)} mode={WATCHLIST_MODE} connections={len(chunks)} "
        f"vol>={VOL_MULT}x/{VOL_LOOKBACK}s min_vol={MIN_VOL} hold={HOLD_SEC}s "
        f"cooldown={REARM_SEC}s notional=${NOTIONAL}"
    )
    log(
        f"  LIVE_TRADE={LIVE_TRADE} max_open={MAX_OPEN_ORDERS} "
        f"(live mirrors paper entry T+1 / flip / exit hold)"
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
