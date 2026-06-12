#!/usr/bin/env python3
"""
Live dry paper: 15m structure strategies (Option A + B from backtest sweep).

  python3 scripts/structure_dry_paper.py

Strategies (15m bars from aggTrade, SHORT only):
  price_range_short  — Price Range + double supply (LOOSE, 15m)
  amd_fvg_short      — AMD manipulation + bear FVG retrace (LOOSE, 15m)

Output: data/structure_dry/<run_ts>/{price_range_short,amd_fvg_short}.log
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
from typing import Callable, Optional

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))

from focused_lib import Bar
from structure_lib import (
    AMD_FVG_PARAMS,
    PRICE_RANGE_PARAMS,
    AmdPending,
    EngineState,
    StructureTrade,
    bear_fvg,
    check_amd_manipulation_short,
    check_price_range_short,
    entry_from_fvg,
    net_pnl_pct,
    net_pnl_usdt,
    planned_exit_ms,
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

BAR_MINUTES = _env_int("STRUCTURE_DRY_BAR_MINUTES", 15)
BAR_MS = BAR_MINUTES * 60 * 1000
WS_ROOT = os.environ.get("STRUCTURE_DRY_WS_ROOT", "wss://fstream.binance.com").rstrip("/")
FAPI = os.environ.get("STRUCTURE_DRY_FAPI", "https://fapi.binance.com").rstrip("/")
WS_CHUNK = _env_int("STRUCTURE_DRY_WS_CHUNK", 80)
STATS_INTERVAL_SEC = _env_int("STRUCTURE_DRY_STATS_INTERVAL_SEC", 1800)
WATCHLIST_MODE = os.environ.get("STRUCTURE_DRY_WATCHLIST_MODE", "all_perps").strip().lower()
WATCHLIST_SIZE = _env_int("STRUCTURE_DRY_WATCHLIST_SIZE", 500)
NOTIONAL = _env_float("STRUCTURE_DRY_NOTIONAL_USDT", 10.0)
REARM_SEC = _env_int("STRUCTURE_DRY_REARM_SEC", 3600)
FEE_RT = _env_float("STRUCTURE_DRY_FEE_RT", 0.0008)
LIVE_TRADE = _env_bool("STRUCTURE_DRY_LIVE_TRADE", False)

if LIVE_TRADE:
    raise SystemExit("STRUCTURE_DRY_LIVE_TRADE is not implemented — dry paper only")

RUN_TS = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
OUT_DIR = Path(os.environ.get("STRUCTURE_DRY_OUT_DIR", ROOT / f"data/structure_dry/{RUN_TS}"))
OUT_DIR.mkdir(parents=True, exist_ok=True)

ENGINES: list[EngineState] = [
    EngineState("price_range_short", "price_range_short", PRICE_RANGE_PARAMS, "price_range"),
    EngineState("amd_fvg_short", "amd_fvg_short", AMD_FVG_PARAMS, "amd_fvg"),
]

LOGGERS: dict[str, Callable[[str], None]] = {}
TRADES_CSV: dict[str, Path] = {}

for eng in ENGINES:
    log_path = OUT_DIR / f"{eng.log_name}.log"
    trades_path = OUT_DIR / f"{eng.log_name}_trades.csv"

    def make_log(path: Path):
        def _log(msg: str) -> None:
            print(f"[{path.stem}] {msg}", flush=True)
            ts = datetime.now(timezone.utc).isoformat()
            with path.open("a", encoding="utf-8") as f:
                f.write(f"{ts} {msg}\n")

        return _log

    LOGGERS[eng.name] = make_log(log_path)
    TRADES_CSV[eng.name] = trades_path
    if not trades_path.exists() or trades_path.stat().st_size == 0:
        with trades_path.open("w", newline="") as f:
            csv.writer(f).writerow(
                [
                    "symbol", "signal_utc", "entry_utc", "exit_utc", "side",
                    "entry_price", "exit_price", "exit_reason", "hold_bars",
                    "gross_pct", "net_pct", "net_usd", "meta",
                ]
            )
    LOGGERS[eng.name](f"structure_dry | log_file={log_path}")


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
    manual = os.environ.get("STRUCTURE_DRY_SYMBOLS", "").strip()
    if manual:
        return [s.strip().upper() for s in manual.split(",") if s.strip()]
    if WATCHLIST_MODE == "lowest_volume":
        return fetch_lowest_volume_perps(WATCHLIST_SIZE)
    if WATCHLIST_MODE == "all_perps":
        perps = fetch_usdt_perps()
        return perps[:WATCHLIST_SIZE] if WATCHLIST_SIZE > 0 else perps
    raise ValueError(f"unsupported STRUCTURE_DRY_WATCHLIST_MODE: {WATCHLIST_MODE!r}")


SYMBOLS = resolve_symbols()
if not SYMBOLS:
    raise SystemExit("no symbols resolved")

buckets: dict[str, dict] = {s: {} for s in SYMBOLS}
agg_stats = {"trades": 0}


def write_trade_csv(st: EngineState, t: StructureTrade, gross: float, net: float, usd: float) -> None:
    with TRADES_CSV[st.name].open("a", newline="") as f:
        csv.writer(f).writerow(
            [
                t.symbol,
                utc_iso(t.signal_ms),
                utc_iso(t.entry_ms),
                utc_iso(t.exit_ms),
                t.side,
                f"{t.entry_price:.8f}",
                f"{t.exit_price:.8f}",
                t.exit_reason,
                t.hold_bars,
                f"{gross:.6f}",
                f"{net:.6f}",
                f"{usd:.6f}",
                t.meta,
            ]
        )


def close_trade(st: EngineState, t: StructureTrade, exit_price: float, exit_ms: int, reason: str) -> None:
    log = LOGGERS[st.name]
    t.exit_price = exit_price
    t.exit_ms = exit_ms
    t.exit_reason = reason
    gross = t.gross_pnl_pct()
    net = net_pnl_pct(gross, FEE_RT)
    usd = net_pnl_usdt(gross, NOTIONAL, FEE_RT)
    st.stats["exits"] += 1
    st.stats["net_usd"] += usd
    if net > 0:
        st.stats["wins"] += 1
    log(
        f"[EXIT] {t.symbol} {t.side} @ {utc_iso(exit_ms)} reason={reason} "
        f"signal@{utc_iso(t.signal_ms)} entry@{utc_iso(t.entry_ms)} "
        f"entry={t.entry_price:.8f} exit={exit_price:.8f} "
        f"gross={gross:+.2f}% net={net:+.2f}% ${usd:+.4f} {t.meta}"
    )
    write_trade_csv(st, t, gross, net, usd)
    if reason == "target":
        st.supply_retests[t.symbol] = 0


def open_trade(st: EngineState, t: StructureTrade) -> None:
    log = LOGGERS[st.name]
    st.active[t.symbol] = t
    st.stats["entries"] += 1
    log(
        f"[ENTRY] {t.symbol} {t.side} @ {utc_iso(t.entry_ms)} "
        f"exit@{utc_iso(t.exit_ms)} price={t.entry_price:.8f} "
        f"stop={t.stop:.8f} target={t.target:.8f} {t.meta}"
    )


def manage_active(st: EngineState, symbol: str, bar: Bar) -> None:
    t = st.active.get(symbol)
    if not t or t.status != "active":
        return
    if bar.h >= t.stop:
        close_trade(st, t, t.stop, bar.sec, "stop")
        st.active.pop(symbol, None)
        return
    if bar.l <= t.target:
        close_trade(st, t, t.target, bar.sec, "target")
        st.active.pop(symbol, None)
        return
    if bar.sec >= t.exit_ms:
        close_trade(st, t, bar.c, bar.sec, "time_exit")
        st.active.pop(symbol, None)


def process_amd_pending(st: EngineState, symbol: str, bars: list[Bar], i: int, bar: Bar) -> None:
    log = LOGGERS[st.name]
    p = st.amd_pending.get(symbol)
    if not p:
        return
    p.bars_left -= 1

    if p.phase == "seek_fvg":
        z = bear_fvg(bars, i)
        if z:
            bot, top = z
            mid = (p.rh + p.rl) / 2
            gap_pct = (top - bot) / mid * 100 if mid > 0 else 0
            if gap_pct >= st.params.get("min_fvg_pct", 0):
                p.fvg_bot, p.fvg_top = bot, top
                p.fvg_ms = bar.sec
                p.phase = "seek_entry"
                p.entry_wait_left = st.params["max_entry_wait"]
                log(
                    f"[FVG] {symbol} bear gap @ {utc_iso(bar.sec)} "
                    f"bot={bot:.8f} top={top:.8f} signal@{utc_iso(p.signal_ms)}"
                )
        if p.phase == "seek_fvg" and p.bars_left <= 0:
            log(f"[FVG_MISS] {symbol} no FVG after M @ {utc_iso(p.signal_ms)}")
            st.amd_pending.pop(symbol, None)
        return

    if p.phase == "seek_entry":
        p.entry_wait_left -= 1
        if bar.l <= p.fvg_top and bar.h >= p.fvg_bot:
            entry = entry_from_fvg(bars, i, p.fvg_bot, p.fvg_top, st.params["entry_mode"])
            if entry >= p.stop:
                st.amd_pending.pop(symbol, None)
                return
            hold = st.params["hold_bars"]
            trade = StructureTrade(
                symbol=symbol,
                strategy=st.name,
                signal_ms=p.signal_ms,
                entry_ms=bar.sec,
                exit_ms=planned_exit_ms(bar.sec, hold, BAR_MS),
                side="SHORT",
                entry_price=entry,
                stop=p.stop,
                target=p.rl,
                status="active",
                hold_bars=hold,
                meta=f"M@{utc_iso(p.m_bar_ms)} FVG@{utc_iso(p.fvg_ms)} rh={p.rh:.8f} rl={p.rl:.8f}",
            )
            st.amd_pending.pop(symbol, None)
            open_trade(st, trade)
            return
        if p.entry_wait_left <= 0:
            log(
                f"[ENTRY_MISS] {symbol} no FVG retrace signal@{utc_iso(p.signal_ms)} "
                f"fvg@{utc_iso(p.fvg_ms)}"
            )
            st.amd_pending.pop(symbol, None)


def process_price_range(st: EngineState, symbol: str, bars: list[Bar], i: int, bar: Bar) -> None:
    log = LOGGERS[st.name]
    if symbol in st.active:
        st.stats["overlap_skips"] += 1
        return
    if st.in_cooldown(symbol, bar.sec, REARM_SEC):
        st.stats["cooldown_skips"] += 1
        return
    trade = check_price_range_short(symbol, bars, i, st)
    if not trade:
        return
    st.stats["signals"] += 1
    st.last_signal_ms[symbol] = bar.sec
    exit_ms = trade.exit_ms
    log(
        f"[SIGNAL] {symbol} {trade.side} double_supply @ {utc_iso(bar.sec)} "
        f"entry@{utc_iso(trade.entry_ms)} exit@{utc_iso(exit_ms)} {trade.meta}"
    )
    open_trade(st, trade)


def process_amd_signal(st: EngineState, symbol: str, bars: list[Bar], i: int, bar: Bar) -> None:
    log = LOGGERS[st.name]
    if st.busy(symbol):
        st.stats["overlap_skips"] += 1
        return
    if st.in_cooldown(symbol, bar.sec, REARM_SEC):
        st.stats["cooldown_skips"] += 1
        return
    m = check_amd_manipulation_short(bars, i, st.params)
    if not m:
        return
    rh, rl, stop = m
    st.stats["signals"] += 1
    st.last_signal_ms[symbol] = bar.sec
    hold = st.params["hold_bars"]
    planned_exit = planned_exit_ms(bar.sec + BAR_MS, hold, BAR_MS)
    log(
        f"[SIGNAL] {symbol} SHORT AMD_sweep @ {utc_iso(bar.sec)} "
        f"planned_entry_after_FVG exit@{utc_iso(planned_exit)} rh={rh:.8f} rl={rl:.8f}"
    )
    st.amd_pending[symbol] = AmdPending(
        symbol=symbol,
        signal_ms=bar.sec,
        m_bar_ms=bar.sec,
        rh=rh,
        rl=rl,
        stop=stop,
        bars_left=st.params["fvg_lookahead"],
    )


def process_engine_bar(st: EngineState, symbol: str, bar: Bar) -> None:
    st.hist(symbol).append(bar)
    bars = list(st.hist(symbol))
    i = len(bars) - 1

    manage_active(st, symbol, bar)

    if st.kind == "amd_fvg":
        process_amd_pending(st, symbol, bars, i, bar)

    if symbol in st.active:
        return

    if st.kind == "price_range":
        process_price_range(st, symbol, bars, i, bar)
    else:
        if symbol not in st.amd_pending:
            process_amd_signal(st, symbol, bars, i, bar)


async def process_bar(symbol: str, b: dict) -> None:
    bar = Bar(sec=b["sec"], o=b["open"], h=b["high"], l=b["low"], c=b["close"], vol=b["volume"])
    for st in ENGINES:
        process_engine_bar(st, symbol, bar)


async def finalize_bucket(symbol: str, b: dict) -> None:
    await process_bar(symbol, b)


async def on_trade(symbol: str, price: float, qty: float, t_ms: int) -> None:
    agg_stats["trades"] += 1
    sec = (t_ms // BAR_MS) * BAR_MS
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
    for eng in ENGINES:
        LOGGERS[eng.name](f"[ws-{conn_id}] connecting {len(symbols)} symbols")
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, max_size=2**22) as ws:
                for eng in ENGINES:
                    LOGGERS[eng.name](f"[ws-{conn_id}] connected")
                async for msg in ws:
                    data = json.loads(msg).get("data")
                    if not data:
                        continue
                    await on_trade(data["s"], float(data["p"]), float(data["q"]), int(data["T"]))
        except Exception as e:
            for eng in ENGINES:
                LOGGERS[eng.name](f"[ws-{conn_id}] reconnect ({e})")
            await asyncio.sleep(3)


async def flush_stale_buckets_loop() -> None:
    while True:
        cutoff = (int(time.time() * 1000) // BAR_MS) * BAR_MS - BAR_MS
        for symbol, state in buckets.items():
            b = state.get("cur")
            if b and b["sec"] < cutoff:
                await finalize_bucket(symbol, b)
                state.pop("cur", None)
        await asyncio.sleep(2)


async def stats_loop() -> None:
    while True:
        await asyncio.sleep(STATS_INTERVAL_SEC)
        for st in ENGINES:
            s = st.stats
            open_n = len(st.active)
            pending = len(st.amd_pending)
            exits = s["exits"]
            wr = (s["wins"] / exits * 100) if exits else 0.0
            LOGGERS[st.name](
                f"[stats] agg_trades={agg_stats['trades']} signals={s['signals']} "
                f"entries={s['entries']} exits={exits} wr={wr:.1f}% "
                f"net_usd={s['net_usd']:+.4f} open={open_n} amd_pending={pending} "
                f"cooldown_skips={s['cooldown_skips']} overlap_skips={s['overlap_skips']}"
            )


async def main() -> None:
    chunks = [SYMBOLS[i : i + WS_CHUNK] for i in range(0, len(SYMBOLS), WS_CHUNK)]
    header = (
        f"structure_dry | bar={BAR_MINUTES}m symbols={len(SYMBOLS)} "
        f"connections={len(chunks)} notional=${NOTIONAL} rearm={REARM_SEC}s fee_rt={FEE_RT}"
    )
    for st in ENGINES:
        LOGGERS[st.name](header)
        LOGGERS[st.name](f"  strategy={st.name} kind={st.kind} params={st.params}")
        LOGGERS[st.name](f"  out={OUT_DIR}")
    await asyncio.gather(
        *[ws_handler(i, c) for i, c in enumerate(chunks)],
        flush_stale_buckets_loop(),
        stats_loop(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        for st in ENGINES:
            LOGGERS[st.name]("Stopping")
