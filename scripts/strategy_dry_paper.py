#!/usr/bin/env python3
"""
Live dry paper: 4 strategies in parallel on one aggTrade feed.

  python3 scripts/strategy_dry_paper.py

Strategies (3% 1s burst, 1 position/symbol each, realistic entry timing):
  ll_cont_low_short   — low burst, LL_CONT @ T+30 → SHORT T+31, hold 300s
  trail_lock_300      — burst direction T+1, stepped trail locks, hold 300s
  hh_cont_high_long   — high burst, HH_CONT @ T+30 → LONG T+31, hold 300s
  fade_60s            — opposite of burst T+1, hold 60s

Output: data/strategy_dry/<run_ts>/{ll_cont_low_short,trail_lock_300,hh_cont_high_long,fade_60s}.log
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
from typing import Callable

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))

from focused_lib import Bar, inv_direction, side_label
from strategy_lib import (
    DryTrade,
    PendingConfirm,
    StrategyState,
    amp_burst,
    ll_cont_at,
    net_pnl_pct,
    net_pnl_usdt,
    post_struct,
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

WS_ROOT = os.environ.get("STRATEGY_DRY_WS_ROOT", "wss://fstream.binance.com").rstrip("/")
FAPI = os.environ.get("STRATEGY_DRY_FAPI", "https://fapi.binance.com").rstrip("/")
WS_CHUNK = _env_int("STRATEGY_DRY_WS_CHUNK", 80)
STATS_INTERVAL_SEC = _env_int("STRATEGY_DRY_STATS_INTERVAL_SEC", 1800)
WATCHLIST_MODE = os.environ.get("STRATEGY_DRY_WATCHLIST_MODE", "all_perps").strip().lower()
WATCHLIST_SIZE = _env_int("STRATEGY_DRY_WATCHLIST_SIZE", 500)
EVENT_THRESH_PCT = _env_float("STRATEGY_DRY_EVENT_THRESH_PCT", 3.0)
MIN_EVENT_VOL = _env_float("STRATEGY_DRY_MIN_EVENT_VOL", 100.0)
NOTIONAL = _env_float("STRATEGY_DRY_NOTIONAL_USDT", 10.0)
REARM_SEC = _env_int("STRATEGY_DRY_REARM_SEC", 300)
FEE_RT = _env_float("STRATEGY_DRY_FEE_RT", 0.0008)
LIVE_TRADE = _env_bool("STRATEGY_DRY_LIVE_TRADE", False)

if LIVE_TRADE:
    raise SystemExit("STRATEGY_DRY_LIVE_TRADE is not implemented — dry paper only")

RUN_TS = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
OUT_DIR = Path(os.environ.get("STRATEGY_DRY_OUT_DIR", ROOT / f"data/strategy_dry/{RUN_TS}"))
OUT_DIR.mkdir(parents=True, exist_ok=True)

STRATEGIES: list[StrategyState] = [
    StrategyState("ll_cont_low_short", 300, 31, burst_filter="low", confirm_kind="ll_cont"),
    StrategyState("trail_lock_300", 300, 1, use_trail=True),
    StrategyState("hh_cont_high_long", 300, 31, burst_filter="high", confirm_kind="hh_cont"),
    StrategyState("fade_60s", 60, 1, fade=True),
]

LOGGERS: dict[str, Callable[[str], None]] = {}
TRADES_CSV: dict[str, Path] = {}

for st in STRATEGIES:
    log_path = OUT_DIR / f"{st.name}.log"
    trades_path = OUT_DIR / f"{st.name}_trades.csv"

    def make_log(path: Path):
        def _log(msg: str) -> None:
            print(f"[{path.stem}] {msg}", flush=True)
            ts = datetime.now(timezone.utc).isoformat()
            with path.open("a", encoding="utf-8") as f:
                f.write(f"{ts} {msg}\n")

        return _log

    LOGGERS[st.name] = make_log(log_path)
    TRADES_CSV[st.name] = trades_path
    with trades_path.open("w", newline="") as f:
        csv.writer(f).writerow(
            [
                "symbol", "signal_utc", "entry_utc", "exit_utc", "burst", "side",
                "amp_pct", "entry_price", "exit_price", "hold_sec", "exit_reason",
                "gross_pct", "net_pct", "net_usd", "lock_pct", "max_fav_pct", "max_adv_pct",
            ]
        )
    LOGGERS[st.name](f"strategy_dry | log_file={log_path}")


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
    manual = os.environ.get("STRATEGY_DRY_SYMBOLS", "").strip()
    if manual:
        return [s.strip().upper() for s in manual.split(",") if s.strip()]
    if WATCHLIST_MODE == "lowest_volume":
        return fetch_lowest_volume_perps(WATCHLIST_SIZE)
    if WATCHLIST_MODE == "all_perps":
        perps = fetch_usdt_perps()
        return perps[:WATCHLIST_SIZE] if WATCHLIST_SIZE > 0 else perps
    raise ValueError(f"unsupported STRATEGY_DRY_WATCHLIST_MODE: {WATCHLIST_MODE!r}")


SYMBOLS = resolve_symbols()
if not SYMBOLS:
    raise SystemExit("no symbols resolved")

buckets: dict[str, dict] = {s: {} for s in SYMBOLS}
agg_stats = {"trades": 0}


def write_trade_result(st: StrategyState, t: DryTrade, gross: float, net: float, usd: float) -> None:
    with TRADES_CSV[st.name].open("a", newline="") as f:
        csv.writer(f).writerow(
            [
                t.symbol,
                utc_iso(t.signal_ms),
                utc_iso(t.entry_ms),
                utc_iso(t.exit_ms),
                t.burst,
                side_label(t.trade_dir),
                f"{t.amp_pct:.4f}",
                f"{t.entry_price:.8f}",
                f"{t.exit_price:.8f}",
                t.hold_sec,
                t.exit_reason,
                f"{gross:.6f}",
                f"{net:.6f}",
                f"{usd:.6f}",
                f"{t.lock_pct:.4f}",
                f"{t.best_mfe_pct:.4f}",
                f"{t.max_adv_pct:.4f}",
            ]
        )


def close_trade(st: StrategyState, symbol: str, exit_price: float, reason: str) -> None:
    log = LOGGERS[st.name]
    t = st.active.pop(symbol, None)
    if not t or t.status != "active":
        return
    t.exit_price = exit_price
    t.exit_reason = reason
    t.status = "closed"
    gross = t.close_pnl_pct(exit_price)
    net = net_pnl_pct(gross, FEE_RT)
    usd = net_pnl_usdt(gross, NOTIONAL, FEE_RT)
    st.stats["exits"] += 1
    st.stats["net_usd"] += usd
    if net > 0:
        st.stats["wins"] += 1
    log(
        f"[EXIT] {symbol} {side_label(t.trade_dir)} reason={reason} "
        f"entry={t.entry_price:.8f} exit={exit_price:.8f} "
        f"gross={gross:+.2f}% net={net:+.2f}% ${usd:+.4f} "
        f"lock={t.lock_pct:.1f}% fav={t.best_mfe_pct:+.2f}% adv={t.max_adv_pct:+.2f}%"
    )
    write_trade_result(st, t, gross, net, usd)


def schedule_trade(
    st: StrategyState,
    symbol: str,
    signal_ms: int,
    burst: str,
    amp: float,
    sig_open: float,
    trade_dir: str,
) -> None:
    entry_ms = signal_ms + st.entry_offset * 1000
    exit_ms = entry_ms + (st.hold_sec - 1) * 1000
    st.active[symbol] = DryTrade(
        symbol=symbol,
        strategy=st.name,
        signal_ms=signal_ms,
        burst=burst,  # type: ignore[arg-type]
        trade_dir=trade_dir,  # type: ignore[arg-type]
        sig_open=sig_open,
        amp_pct=amp,
        entry_ms=entry_ms,
        exit_ms=exit_ms,
        hold_sec=st.hold_sec,
    )


def start_immediate_signal(st: StrategyState, symbol: str, bar: Bar, burst: str, amp: float) -> None:
    log = LOGGERS[st.name]
    if st.burst_filter and burst != st.burst_filter:
        return
    if st.busy(symbol):
        st.stats["overlap_skips"] += 1
        return
    if st.in_cooldown(symbol, bar.sec, REARM_SEC):
        st.stats["cooldown_skips"] += 1
        return

    st.stats["signals"] += 1
    st.last_event_ms[symbol] = bar.sec
    trade_dir = inv_direction(burst) if st.fade else burst  # type: ignore[arg-type]
    log(
        f"[SIGNAL] {symbol} {burst} {amp:.2f}% -> {side_label(trade_dir)} "
        f"T+{st.entry_offset} @ {utc_iso(bar.sec)}"
    )
    schedule_trade(st, symbol, bar.sec, burst, amp, bar.o, trade_dir)


def start_delayed_signal(st: StrategyState, symbol: str, bar: Bar, burst: str, amp: float) -> None:
    log = LOGGERS[st.name]
    if st.burst_filter and burst != st.burst_filter:
        return
    if st.busy(symbol):
        st.stats["overlap_skips"] += 1
        return
    if st.in_cooldown(symbol, bar.sec, REARM_SEC):
        st.stats["cooldown_skips"] += 1
        return

    st.stats["signals"] += 1
    st.last_event_ms[symbol] = bar.sec
    confirm_ms = bar.sec + 30 * 1000
    entry_ms = bar.sec + 31 * 1000
    st.pending[symbol] = PendingConfirm(
        symbol=symbol,
        signal_ms=bar.sec,
        burst=burst,  # type: ignore[arg-type]
        sig_open=bar.o,
        amp_pct=amp,
        confirm_ms=confirm_ms,
        entry_ms=entry_ms,
        strategy=st.name,
    )
    log(
        f"[SIGNAL] {symbol} {burst} {amp:.2f}% pending {st.confirm_kind} "
        f"confirm@T+30 entry@T+31 @ {utc_iso(bar.sec)}"
    )


def try_confirm(st: StrategyState, symbol: str, bar: Bar) -> None:
    log = LOGGERS[st.name]
    p = st.pending.get(symbol)
    if not p or bar.sec != p.confirm_ms:
        return

    bars = list(st.hist(symbol))
    sig_i = next((i for i, b in enumerate(bars) if b.sec == p.signal_ms), None)
    if sig_i is None:
        st.pending.pop(symbol, None)
        return

    ok = False
    if st.confirm_kind == "ll_cont":
        ok = ll_cont_at(bars, sig_i)
    elif st.confirm_kind == "hh_cont":
        entry_ref = bars[sig_i + 1].o if sig_i + 1 < len(bars) else p.sig_open
        ok = post_struct(bars, sig_i, p.burst, entry_ref) == "HH_CONT"

    if not ok:
        st.stats["confirm_fail"] += 1
        log(f"[CONFIRM_FAIL] {symbol} {st.confirm_kind} @ {utc_iso(bar.sec)}")
        st.pending.pop(symbol, None)
        return

    st.stats["confirm_pass"] += 1
    trade_dir = "low" if st.confirm_kind == "ll_cont" else "high"
    log(
        f"[CONFIRM_OK] {symbol} {st.confirm_kind} -> {side_label(trade_dir)} "
        f"entry@T+31 @ {utc_iso(bar.sec)}"
    )
    st.pending.pop(symbol, None)
    schedule_trade(st, symbol, p.signal_ms, p.burst, p.amp_pct, p.sig_open, trade_dir)


def process_strategy_bar(st: StrategyState, symbol: str, bar: Bar) -> None:
    log = LOGGERS[st.name]
    st.hist(symbol).append(bar)

    trade = st.active.get(symbol)
    if trade:
        if trade.status == "pending_entry" and bar.sec == trade.entry_ms:
            trade.on_entry(bar.o)
            st.stats["entries"] += 1
            log(
                f"[ENTRY] {symbol} {side_label(trade.trade_dir)} @ {utc_iso(bar.sec)} "
                f"price={bar.o:.8f} burst={trade.burst} amp={trade.amp_pct:.2f}%"
            )
        elif trade.status == "active":
            stop_px = trade.on_bar(bar)
            if stop_px is not None:
                trade.exit_ms = bar.sec
                close_trade(st, symbol, stop_px, "trail_lock")
                return
            if bar.sec >= trade.exit_ms:
                trade.exit_ms = bar.sec
                close_trade(st, symbol, bar.c, "time_exit")
                return

    if symbol in st.active:
        return

    if st.confirm_kind:
        try_confirm(st, symbol, bar)

    if symbol in st.pending or symbol in st.active:
        return

    burst, amp = amp_burst(bar, EVENT_THRESH_PCT, MIN_EVENT_VOL)
    if burst is None:
        return

    if st.confirm_kind:
        start_delayed_signal(st, symbol, bar, burst, amp)
    else:
        start_immediate_signal(st, symbol, bar, burst, amp)


async def process_bar(symbol: str, b: dict) -> None:
    bar = Bar(
        sec=b["sec"],
        o=b["open"],
        h=b["high"],
        l=b["low"],
        c=b["close"],
        vol=b["volume"],
    )
    for st in STRATEGIES:
        process_strategy_bar(st, symbol, bar)


async def finalize_bucket(symbol: str, b: dict) -> None:
    await process_bar(symbol, b)


async def on_trade(symbol: str, price: float, qty: float, t_ms: int) -> None:
    agg_stats["trades"] += 1
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
    for st in STRATEGIES:
        LOGGERS[st.name](f"[ws-{conn_id}] connecting {len(symbols)} symbols")
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, max_size=2**22) as ws:
                for st in STRATEGIES:
                    LOGGERS[st.name](f"[ws-{conn_id}] connected")
                async for msg in ws:
                    data = json.loads(msg).get("data")
                    if not data:
                        continue
                    await on_trade(data["s"], float(data["p"]), float(data["q"]), int(data["T"]))
        except Exception as e:
            for st in STRATEGIES:
                LOGGERS[st.name](f"[ws-{conn_id}] reconnect ({e})")
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
        for st in STRATEGIES:
            s = st.stats
            open_n = sum(1 for t in st.active.values() if t.status == "active")
            pending_n = sum(1 for t in st.active.values() if t.status == "pending_entry")
            pend_conf = len(st.pending)
            exits = s["exits"]
            wr = (s["wins"] / exits * 100) if exits else 0.0
            LOGGERS[st.name](
                f"[stats] agg_trades={agg_stats['trades']} signals={s['signals']} "
                f"confirm_ok={s['confirm_pass']} confirm_fail={s['confirm_fail']} "
                f"entries={s['entries']} exits={exits} wr={wr:.1f}% "
                f"net_usd={s['net_usd']:+.4f} open={open_n} pending_entry={pending_n} "
                f"pending_confirm={pend_conf} cooldown_skips={s['cooldown_skips']} "
                f"overlap_skips={s['overlap_skips']}"
            )


async def main() -> None:
    chunks = [SYMBOLS[i : i + WS_CHUNK] for i in range(0, len(SYMBOLS), WS_CHUNK)]
    header = (
        f"strategy_dry | symbols={len(SYMBOLS)} mode={WATCHLIST_MODE} "
        f"connections={len(chunks)} event>={EVENT_THRESH_PCT}% "
        f"notional=${NOTIONAL} rearm={REARM_SEC}s fee_rt={FEE_RT} dry_only"
    )
    for st in STRATEGIES:
        LOGGERS[st.name](header)
        LOGGERS[st.name](
            f"  {st.name} hold={st.hold_sec}s entry=T+{st.entry_offset} "
            f"burst_filter={st.burst_filter} confirm={st.confirm_kind} fade={st.fade} trail={st.use_trail}"
        )
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
        for st in STRATEGIES:
            LOGGERS[st.name]("Stopping")
