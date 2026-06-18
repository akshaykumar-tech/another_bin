#!/usr/bin/env python3
"""
Dry paper: Alma SD SuperTrend + dual_flip_consensus (Alma+STC) on 15m.

  python3 scripts/alma_st_dry_paper.py

Strategy (dry SL 3% / TP 8%):
  dual_flip_consensus — Alma flip OR (Alma trend + STC buy/sell); entry next 15m open
    optional live mirror on Binance (opposite side, exchange SL 8% / TP 3% at entry)

Logs:
  data/alma_st_dry/<run_ts>/dual_flip_consensus.log + dual_flip_consensus_trades.csv

Live mirror (ALMA_ST_BINANCE_LIVE=true):
  ALMA_ST_NOTIONAL_USDT, ALMA_ST_MAX_OPEN_LIVE, ALMA_ST_MIN_LEVERAGE (default 50)
  ALMA_ST_LIVE_MIRROR_SL_PCT=8, ALMA_ST_LIVE_MIRROR_TP_PCT=3 (restart reapplies TP on open positions)
"""
from __future__ import annotations

import asyncio
import csv
import json
import os
import sys
import time
import urllib.request
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Deque

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))

from alma_st_lib import (
    ALMA_LEN,
    FACTOR,
    OHLC,
    SD_LEN,
    WARMUP_BARS as ALMA_WARMUP,
    check_exit,
    compute_supertrend,
    signal_flip,
    signal_series,
    sl_tp_prices,
)
from stc_lib import WARMUP_BARS as STC_WARMUP, compute_stc, stc_signals
from binance_futures import BinanceFuturesClient, parse_fill

try:
    import websockets
except ImportError:
    print("pip install websockets")
    raise

WARMUP_BARS = max(ALMA_WARMUP, STC_WARMUP)


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


def _env(name: str, fallback: str, default: str) -> str:
    return os.environ.get(name, "").strip() or os.environ.get(fallback, "").strip() or default


def _env_int(name: str, fallback: str, default: int) -> int:
    return int(_env(name, fallback, str(default)))


def _env_float(name: str, fallback: str, default: float) -> float:
    return float(_env(name, fallback, str(default)))


def _env_bool(name: str, fallback: str, default: bool) -> bool:
    v = _env(name, fallback, "true" if default else "false").lower()
    return v in ("1", "true", "yes", "on")


load_dotenv()

WS_ROOT = _env("ALMA_ST_WS_ROOT", "STRATEGY_DRY_WS_ROOT", "wss://fstream.binance.com").rstrip("/")
FAPI = _env("ALMA_ST_FAPI", "STRATEGY_DRY_FAPI", "https://fapi.binance.com").rstrip("/")
WS_CHUNK = _env_int("ALMA_ST_WS_CHUNK", "STRATEGY_DRY_WS_CHUNK", 80)
WATCHLIST_MODE = _env("ALMA_ST_WATCHLIST_MODE", "STRATEGY_DRY_WATCHLIST_MODE", "all_perps").lower()
WATCHLIST_SIZE = _env_int("ALMA_ST_WATCHLIST_SIZE", "STRATEGY_DRY_WATCHLIST_SIZE", 500)
NOTIONAL = _env_float("ALMA_ST_NOTIONAL_USDT", "STRATEGY_DRY_NOTIONAL_USDT", 6.0)
FEE_RT = _env_float("ALMA_ST_FEE_RT", "STRATEGY_DRY_FEE_RT", 0.0008)
SL_PCT = _env_float("ALMA_ST_SL_PCT", "", 3.0)
TP_PCT = _env_float("ALMA_ST_TP_PCT", "", 8.0)
BAR_MS = 900_000
MAX_HOLD_SEC = 96 * 900
STATS_INTERVAL_SEC = _env_int("ALMA_ST_STATS_INTERVAL_SEC", "STRATEGY_DRY_STATS_INTERVAL_SEC", 1800)
EXCLUDE = {s.strip().upper() for s in _env("ALMA_ST_EXCLUDE", "", "SAHARAUSDT").split(",") if s.strip()}

LIVE_TRADE = _env_bool("ALMA_ST_BINANCE_LIVE", "STRATEGY_DRY_LIVE_TRADE", False)
LIVE_MIRROR_SL_PCT = _env_float("ALMA_ST_LIVE_MIRROR_SL_PCT", "", 8.0)
LIVE_MIRROR_TP_PCT = _env_float("ALMA_ST_LIVE_MIRROR_TP_PCT", "", 3.0)
ALGO_WORKING_TYPE = _env("ALMA_ST_ALGO_WORKING_TYPE", "", "CONTRACT_PRICE").upper()
LIVE_RECONCILE_SEC = _env_int("ALMA_ST_LIVE_RECONCILE_SEC", "", 60)
MAX_OPEN_LIVE = _env_int("ALMA_ST_MAX_OPEN_LIVE", "STRATEGY_DRY_MAX_OPEN_LIVE", 30)
MIN_LEVERAGE = _env_int("ALMA_ST_MIN_LEVERAGE", "", 50)
MARGIN_BUFFER = _env_float("ALMA_ST_MARGIN_BUFFER", "STRATEGY_DRY_MARGIN_BUFFER", 1.05)
LIVE_MIRROR_RUNNER = "dual_flip_consensus"

binance: BinanceFuturesClient | None = None
live_slots: dict[str, dict] = {}
live_stats = {"entries": 0, "exits": 0, "skips": 0}
live_mirror_lock = asyncio.Lock()

RUN_TS = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
OUT_DIR = Path(os.environ.get("ALMA_ST_OUT_DIR", ROOT / f"data/alma_st_dry/{RUN_TS}"))
OUT_DIR.mkdir(parents=True, exist_ok=True)
COMBINED_LOG = OUT_DIR / "combined.log"
BOOTSTRAP_KLINES = _env_int("ALMA_ST_BOOTSTRAP_KLINES", "", 120)
BOOTSTRAP_CONCURRENCY = _env_int("ALMA_ST_BOOTSTRAP_CONCURRENCY", "", 20)

agg_stats = {"trades": 0}


@dataclass
class IndicatorSnap:
    bar_ts: int
    bar_close: float
    alma_long: bool
    alma_short: bool
    alma_bull: bool
    alma_bear: bool
    stc_buy: bool
    stc_sell: bool
    stc_val: float


@dataclass
class ActiveTrade:
    symbol: str
    side: str
    signal_ms: int
    entry_ms: int
    entry_price: float
    sl_px: float
    tp_px: float
    max_fav_pct: float = 0.0
    max_adv_pct: float = 0.0


@dataclass
class StratSymState:
    trade: ActiveTrade | None = None
    pending_side: str | None = None
    pending_at_ms: int = 0
    pending_snap: IndicatorSnap | None = None


@dataclass
class SymbolFeed:
    bars_1s_cur: dict | None = None
    bars_15m: Deque[OHLC] = field(default_factory=lambda: deque(maxlen=150))
    cur_15m: dict | None = None
    warmed: bool = False
    prev_alma_sig: int = 0
    strat: dict[str, StratSymState] = field(default_factory=dict)


@dataclass
class StrategyRunner:
    name: str
    log_path: Path
    trades_csv: Path
    signal_fn: Callable[[IndicatorSnap], str | None]
    stats: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.stats = {
            "signals": 0,
            "entries": 0,
            "exits": 0,
            "wins": 0,
            "net_usd": 0.0,
            "sl": 0,
            "tp": 0,
            "timeout": 0,
            "warmup_ready": 0,
            "skipped_busy": 0,
        }
        with self.trades_csv.open("w", newline="") as f:
            csv.writer(f).writerow(
                [
                    "symbol", "side", "signal_utc", "entry_utc", "exit_utc",
                    "entry_price", "exit_price", "sl_px", "tp_px",
                    "hold_sec", "exit_reason", "gross_pct", "net_usd",
                    "max_fav_pct", "max_adv_pct",
                ]
            )

    def log(self, msg: str) -> None:
        line = f"[{self.name}] {msg}"
        print(line, flush=True)
        ts = datetime.now(timezone.utc).isoformat()
        row = f"{ts} {msg}\n"
        with self.log_path.open("a", encoding="utf-8") as f:
            f.write(row)
        with COMBINED_LOG.open("a", encoding="utf-8") as f:
            f.write(f"{ts} [{self.name}] {msg}\n")


def utc_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def net_pnl_usd(side: str, entry: float, exit_px: float) -> float:
    if entry <= 0:
        return 0.0
    gross = (exit_px - entry) / entry * 100 if side == "long" else (entry - exit_px) / entry * 100
    return NOTIONAL * gross / 100 - NOTIONAL * FEE_RT


def signal_dual_flip(snap: IndicatorSnap) -> str | None:
    al = snap.alma_long or (snap.alma_bull and snap.stc_buy)
    sh = snap.alma_short or (snap.alma_bear and snap.stc_sell)
    if al and not sh:
        return "long"
    if sh and not al:
        return "short"
    return None


STRATEGIES: list[StrategyRunner] = [
    StrategyRunner(
        "dual_flip_consensus",
        OUT_DIR / "dual_flip_consensus.log",
        OUT_DIR / "dual_flip_consensus_trades.csv",
        signal_dual_flip,
    ),
]


def fetch_klines_15m(symbol: str, limit: int = BOOTSTRAP_KLINES) -> list[OHLC]:
    url = f"{FAPI}/fapi/v1/klines?symbol={symbol}&interval=15m&limit={limit}"
    rows = _http_json(url)
    now_period = (int(time.time() * 1000) // BAR_MS) * BAR_MS
    out: list[OHLC] = []
    for r in rows:
        ts = int(r[0])
        if ts >= now_period:
            continue
        out.append(OHLC(ts, float(r[1]), float(r[2]), float(r[3]), float(r[4])))
    return out


def prime_feed_from_history(symbol: str, feed: SymbolFeed) -> int:
    bars = fetch_klines_15m(symbol)
    for b in bars:
        feed.bars_15m.append(b)
    if len(feed.bars_15m) >= 2:
        bars_list = list(feed.bars_15m)
        direction = compute_supertrend(bars_list)
        sigs = signal_series(direction)
        feed.prev_alma_sig = sigs[-1] if sigs else 0
    if len(feed.bars_15m) >= WARMUP_BARS:
        feed.warmed = True
    return len(bars)


async def bootstrap_history() -> None:
    if BOOTSTRAP_KLINES <= 0:
        return
    loop = asyncio.get_running_loop()
    sem = asyncio.Semaphore(BOOTSTRAP_CONCURRENCY)
    ready_before = sum(1 for f in feeds.values() if f.warmed)

    async def one(sym: str) -> int:
        async with sem:
            try:
                return await loop.run_in_executor(None, prime_feed_from_history, sym, feeds[sym])
            except Exception as e:
                for runner in STRATEGIES:
                    runner.log(f"[bootstrap_fail] {sym} {e}")
                return 0

    for runner in STRATEGIES:
        runner.log(f"[bootstrap] fetching {BOOTSTRAP_KLINES}×15m klines for {len(SYMBOLS)} symbols...")
    counts = await asyncio.gather(*[one(s) for s in SYMBOLS])
    ready_after = sum(1 for f in feeds.values() if f.warmed)
    for runner in STRATEGIES:
        runner.stats["warmup_ready"] = ready_after
        runner.log(
            f"[bootstrap] done bars_avg={sum(counts)/max(len(counts),1):.0f} "
            f"warmed={ready_after}/{len(SYMBOLS)} (was {ready_before})"
        )


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
    manual = _env("ALMA_ST_SYMBOLS", "STRATEGY_DRY_SYMBOLS", "")
    if manual:
        syms = [s.strip().upper() for s in manual.split(",") if s.strip()]
    elif WATCHLIST_MODE == "lowest_volume":
        syms = fetch_lowest_volume_perps(WATCHLIST_SIZE)
    elif WATCHLIST_MODE == "all_perps":
        perps = fetch_usdt_perps()
        syms = perps[:WATCHLIST_SIZE] if WATCHLIST_SIZE > 0 else perps
    else:
        raise ValueError(f"unsupported watchlist mode: {WATCHLIST_MODE!r}")
    return [s for s in syms if s not in EXCLUDE]


SYMBOLS: list[str] = []
feeds: dict[str, SymbolFeed] = {}


def init_binance_client() -> None:
    global binance
    api_key = _env("ALMA_ST_BINANCE_API_KEY", "STRATEGY_DRY_BINANCE_API_KEY", "")
    if not api_key:
        api_key = _env("BINANCE_API_KEY", "FOCUSED_BINANCE_API_KEY", "")
    api_secret = _env("ALMA_ST_BINANCE_API_SECRET", "STRATEGY_DRY_BINANCE_API_SECRET", "")
    if not api_secret:
        api_secret = _env("BINANCE_API_SECRET", "FOCUSED_BINANCE_API_SECRET", "")
    if not api_key or not api_secret:
        return
    binance = BinanceFuturesClient(api_key, api_secret, FAPI)
    binance.warm_cache()


def filter_symbols_by_leverage(syms: list[str]) -> list[str]:
    if not binance or MIN_LEVERAGE <= 0:
        return syms
    return [s for s in syms if binance.max_leverage(s) >= MIN_LEVERAGE]


def init_feeds() -> None:
    global feeds
    feeds = {}
    for sym in SYMBOLS:
        f = SymbolFeed()
        f.strat = {st.name: StratSymState() for st in STRATEGIES}
        feeds[sym] = f


def mirror_entry_side(dry_side: str) -> str:
    return "SELL" if dry_side == "long" else "BUY"


def mirror_close_side(dry_side: str) -> str:
    return "BUY" if dry_side == "long" else "SELL"


def mirror_live_side(order_side: str) -> str:
    return "long" if order_side == "BUY" else "short"


def mirror_live_tp_px(entry: float, live_side: str) -> float:
    if live_side == "long":
        return entry * (1 + LIVE_MIRROR_TP_PCT / 100)
    return entry * (1 - LIVE_MIRROR_TP_PCT / 100)


def mirror_live_sl_tp(entry: float, live_side: str, ref_px: float) -> tuple[float, float]:
    """Live mirror: SL from fill; TP from fill at ALMA_ST_LIVE_MIRROR_TP_PCT."""
    if live_side == "long":
        sl_px = entry * (1 - LIVE_MIRROR_SL_PCT / 100)
    else:
        sl_px = entry * (1 + LIVE_MIRROR_SL_PCT / 100)
    tp_px = mirror_live_tp_px(entry, live_side)
    return sl_px, tp_px


def mirror_close_side_from_live(live_side: str) -> str:
    return "SELL" if live_side == "long" else "BUY"


def live_tp_hit(live_side: str, px: float, tp_px: float) -> bool:
    if live_side == "long":
        return px >= tp_px
    return px <= tp_px


def live_open_count() -> int:
    if LIVE_TRADE and binance is not None:
        return len(binance.open_position_symbols())
    return len(live_slots)


def _cancel_orphan_algos_sync() -> list[str]:
    if not LIVE_TRADE or binance is None:
        return []
    try:
        return binance.cancel_orphan_algo_orders()
    except Exception:
        return []


def _reconcile_live_slots_sync() -> list[tuple[str, bool]]:
    """Drop stale slots; cancel leftover SL/TP when position is flat."""
    if not LIVE_TRADE or binance is None:
        return []
    open_syms = set(binance.open_position_symbols())
    removed: list[tuple[str, bool]] = []
    for sym in list(live_slots.keys()):
        if sym not in open_syms:
            cancelled = False
            try:
                binance.cancel_all_algo_orders(sym)
                cancelled = True
            except Exception:
                pass
            live_slots.pop(sym, None)
            removed.append((sym, cancelled))
    return removed


def _seed_live_slots_sync() -> int:
    """On restart: track existing exchange positions (no new orders)."""
    if not LIVE_TRADE or binance is None:
        return 0
    seeded = 0
    for sym in binance.open_position_symbols():
        if sym in live_slots:
            continue
        row = binance.position_row(sym)
        if not row:
            continue
        amt = float(row.get("positionAmt") or 0)
        qty = abs(amt)
        if qty <= 0:
            continue
        live_side = "long" if amt > 0 else "short"
        entry = float(row.get("entryPrice") or 0)
        tp_px = mirror_live_tp_px(entry, live_side) if entry > 0 else 0.0
        live_slots[sym] = {
            "dry_side": "unknown",
            "qty": qty,
            "entry_price": entry,
            "leverage": int(float(row.get("leverage") or 0)),
            "live_side": live_side,
            "sl_px": 0.0,
            "tp_px": tp_px,
        }
        seeded += 1
    return seeded


def _refresh_live_tp_orders_sync() -> tuple[list[tuple], list[tuple], list[tuple]]:
    """Apply current LIVE_MIRROR_TP_PCT to open positions (close if hit, else replace TP algo)."""
    if not LIVE_TRADE or binance is None:
        return [], [], []
    closed: list[tuple] = []
    updated: list[tuple] = []
    failed: list[tuple] = []
    for sym in binance.open_position_symbols():
        try:
            row = binance.position_row(sym)
            if not row:
                continue
            amt = float(row.get("positionAmt") or 0)
            qty = abs(amt)
            if qty <= 0:
                continue
            live_side = "long" if amt > 0 else "short"
            slot = live_slots.get(sym, {})
            entry = float(slot.get("entry_price") or 0)
            if entry <= 0:
                entry = float(row.get("entryPrice") or 0)
            if entry <= 0:
                failed.append((sym, "no entry price"))
                continue
            tp_px = mirror_live_tp_px(entry, live_side)
            close_side = mirror_close_side_from_live(live_side)
            px = binance.last_price(sym)
            old_tp = float(slot.get("tp_px") or 0)

            if live_tp_hit(live_side, px, tp_px):
                try:
                    binance.cancel_all_algo_orders(sym)
                except Exception:
                    pass
                binance.market_close_qty(sym, close_side, qty)
                if binance.position_qty(sym) > 0:
                    binance.market_close_qty(sym, close_side, binance.position_qty(sym))
                live_slots.pop(sym, None)
                closed.append((sym, tp_px, px))
                continue

            binance.cancel_tp_algo_orders(sym)
            tp_resp = binance.take_profit_market_reduce(
                sym, close_side, tp_px, qty, working_type=ALGO_WORKING_TYPE
            )
            live_slots[sym] = {
                **slot,
                "qty": qty,
                "entry_price": entry,
                "live_side": live_side,
                "tp_px": tp_px,
            }
            updated.append((sym, old_tp, tp_px, tp_resp.get("algoId", "?")))
        except Exception as e:
            failed.append((sym, str(e)))
    return closed, updated, failed


def dual_flip_runner() -> StrategyRunner | None:
    for runner in STRATEGIES:
        if runner.name == LIVE_MIRROR_RUNNER:
            return runner
    return None


def mirror_log(msg: str) -> None:
    runner = dual_flip_runner()
    if runner:
        runner.log(msg)


async def reconcile_live_slots() -> None:
    if not LIVE_TRADE or binance is None:
        return
    loop = asyncio.get_running_loop()
    removed = await loop.run_in_executor(None, _reconcile_live_slots_sync)
    for sym, algos_cancelled in removed:
        live_stats["exits"] += 1
        extra = " + algos cancelled" if algos_cancelled else ""
        mirror_log(f"[LIVE_MIRROR_RECONCILE] {sym} flat on exchange — slot cleared{extra}")
    orphans = await loop.run_in_executor(None, _cancel_orphan_algos_sync)
    for sym in orphans:
        mirror_log(f"[LIVE_MIRROR_RECONCILE] {sym} orphan SL/TP algos cancelled (no position)")


async def bootstrap_live_mirror() -> None:
    if not LIVE_TRADE or binance is None:
        return
    loop = asyncio.get_running_loop()

    def _boot():
        orphans = _cancel_orphan_algos_sync()
        seeded = _seed_live_slots_sync()
        closed, updated, failed = _refresh_live_tp_orders_sync()
        n_pos = len(binance.open_position_symbols())
        return orphans, seeded, n_pos, closed, updated, failed

    orphans, seeded, n_pos, closed, updated, failed = await loop.run_in_executor(None, _boot)
    mirror_log(
        f"[LIVE_MIRROR_BOOT] exchange_positions={n_pos} seeded_slots={seeded} "
        f"tp_pct={LIVE_MIRROR_TP_PCT}% max_open={MAX_OPEN_LIVE} (no new entry orders on startup)"
    )
    for sym in orphans:
        mirror_log(f"[LIVE_MIRROR_BOOT] {sym} orphan SL/TP algos cancelled")
    for sym, tp_px, px in closed:
        live_stats["exits"] += 1
        mirror_log(
            f"[LIVE_MIRROR_TP_REFRESH] {sym} closed @ {px:.8f} "
            f"(new TP {tp_px:.8f} already hit, pct={LIVE_MIRROR_TP_PCT}%)"
        )
    for sym, old_tp, new_tp, algo_id in updated:
        old_s = f"{old_tp:.8f}" if old_tp > 0 else "n/a"
        mirror_log(
            f"[LIVE_MIRROR_TP_REFRESH] {sym} TP {old_s} -> {new_tp:.8f} "
            f"({LIVE_MIRROR_TP_PCT}%) tp_algo={algo_id}"
        )
    for sym, err in failed:
        mirror_log(f"[LIVE_MIRROR_TP_REFRESH_FAIL] {sym} {err}")


async def live_mirror_entry(symbol: str, dry_side: str, ref_px: float) -> None:
    if not LIVE_TRADE or binance is None:
        return
    sym = symbol.upper()
    try:
        async with live_mirror_lock:
            if sym in live_slots:
                live_stats["skips"] += 1
                mirror_log(f"[LIVE_MIRROR_SKIP] {sym} already open")
                return
            open_syms = set(binance.open_position_symbols())
            if sym in open_syms:
                live_stats["skips"] += 1
                mirror_log(f"[LIVE_MIRROR_SKIP] {sym} exchange position open")
                return
            n_open = len(open_syms)
            if n_open >= MAX_OPEN_LIVE:
                live_stats["skips"] += 1
                mirror_log(f"[LIVE_MIRROR_SKIP] {sym} max_open={MAX_OPEN_LIVE} (exchange={n_open})")
                return
            if not binance.symbol_tradable(sym):
                live_stats["skips"] += 1
                mirror_log(f"[LIVE_MIRROR_SKIP] {sym} not tradable")
                return
            if binance.max_leverage(sym) < MIN_LEVERAGE:
                live_stats["skips"] += 1
                mirror_log(f"[LIVE_MIRROR_SKIP] {sym} lev<{MIN_LEVERAGE}x")
                return

            order_side = mirror_entry_side(dry_side)

            def _place():
                lev = binance.set_max_leverage(sym)
                margin_needed = (NOTIONAL / lev) * MARGIN_BUFFER
                bal = binance.available_usdt()
                if bal < margin_needed:
                    raise RuntimeError(
                        f"insufficient margin: need ${margin_needed:.2f} "
                        f"(notional=${NOTIONAL:.2f} @ {lev}x), available=${bal:.2f}"
                    )
                resp = binance.market_order_notional(sym, order_side, NOTIONAL)
                entry, qty = parse_fill(resp)
                if qty <= 0:
                    qty = binance.position_qty(sym)
                if entry <= 0:
                    entry = ref_px
                live_side = mirror_live_side(order_side)
                sl_px, tp_px = mirror_live_sl_tp(entry, live_side, ref_px)
                close_side = mirror_close_side(dry_side)
                sl_resp = binance.stop_market_reduce(
                    sym, close_side, sl_px, qty, working_type=ALGO_WORKING_TYPE
                )
                tp_resp = binance.take_profit_market_reduce(
                    sym, close_side, tp_px, qty, working_type=ALGO_WORKING_TYPE
                )
                return lev, entry, qty, margin_needed, bal, live_side, sl_px, tp_px, sl_resp, tp_resp

            (
                lev, entry, qty, margin_needed, bal,
                live_side, sl_px, tp_px, sl_resp, tp_resp,
            ) = await asyncio.get_running_loop().run_in_executor(None, _place)
            live_slots[sym] = {
                "dry_side": dry_side,
                "qty": qty,
                "entry_price": entry,
                "leverage": lev,
                "live_side": live_side,
                "sl_px": sl_px,
                "tp_px": tp_px,
            }
            live_stats["entries"] += 1
            mirror_log(
                f"[LIVE_MIRROR_ENTRY] {sym} dry={dry_side.upper()} binance={order_side} "
                f"fill={entry:.8f} qty={qty:.8f} notional=${NOTIONAL:.2f} lev={lev}x "
                f"margin~=${margin_needed:.2f} bal=${bal:.2f} ref={ref_px:.8f} "
                f"SL={sl_px:.8f} ({LIVE_MIRROR_SL_PCT}%) TP={tp_px:.8f} ({LIVE_MIRROR_TP_PCT}%) "
                f"sl_algo={sl_resp.get('algoId', sl_resp.get('clientAlgoId', '?'))} "
                f"tp_algo={tp_resp.get('algoId', tp_resp.get('clientAlgoId', '?'))}"
            )
    except Exception as e:
        live_stats["skips"] += 1
        mirror_log(f"[LIVE_MIRROR_ENTRY_FAIL] {sym} {e}")


async def live_mirror_exit(symbol: str, dry_side: str, reason: str) -> None:
    if not LIVE_TRADE or binance is None:
        return
    sym = symbol.upper()
    slot = live_slots.get(sym)
    close_side = mirror_close_side(dry_side)

    def _close():
        try:
            binance.cancel_all_algo_orders(sym)
        except Exception:
            pass
        pos_qty = binance.position_qty(sym)
        if pos_qty <= 0:
            return 0.0, True
        slot_qty = float(slot["qty"]) if slot else pos_qty
        qty = min(pos_qty, slot_qty)
        resp = binance.market_close_qty(sym, close_side, qty)
        exit_px, _ = parse_fill(resp)
        if exit_px <= 0:
            exit_px = binance.mark_price(sym)
        if binance.position_qty(sym) > 0:
            binance.market_close_qty(sym, close_side, binance.position_qty(sym))
        return exit_px, False

    try:
        async with live_mirror_lock:
            exit_px, already_flat = await asyncio.get_running_loop().run_in_executor(None, _close)
            live_slots.pop(sym, None)
            live_stats["exits"] += 1
            if already_flat:
                mirror_log(f"[LIVE_MIRROR_EXIT] {sym} reason={reason} already_flat")
            else:
                mirror_log(
                    f"[LIVE_MIRROR_EXIT] {sym} reason={reason} side={close_side} "
                    f"exit={exit_px:.8f} dry={dry_side.upper()}"
                )
    except Exception as e:
        mirror_log(f"[LIVE_MIRROR_EXIT_FAIL] {sym} reason={reason} {e}")
        try:
            flat = await asyncio.get_running_loop().run_in_executor(
                None, lambda: binance.position_qty(sym) <= 0
            )
            if flat:
                live_slots.pop(sym, None)
                live_stats["exits"] += 1
                mirror_log(f"[LIVE_MIRROR_EXIT] {sym} reason={reason} flat_after_fail")
        except Exception:
            pass


def schedule_live_mirror_entry(symbol: str, dry_side: str, ref_px: float) -> None:
    try:
        asyncio.get_running_loop().create_task(live_mirror_entry(symbol, dry_side, ref_px))
    except RuntimeError:
        pass


def schedule_live_mirror_exit(symbol: str, dry_side: str, reason: str) -> None:
    try:
        asyncio.get_running_loop().create_task(live_mirror_exit(symbol, dry_side, reason))
    except RuntimeError:
        pass


def build_snap(feed: SymbolFeed, bar: OHLC) -> IndicatorSnap | None:
    feed.bars_15m.append(bar)
    if len(feed.bars_15m) < WARMUP_BARS:
        return None

    bars = list(feed.bars_15m)
    snap, cur_sig = indicator_snap_from_bars(bars, feed.prev_alma_sig)
    feed.prev_alma_sig = cur_sig
    return snap


def indicator_snap_from_bars(bars: list[OHLC], prev_alma_sig: int) -> tuple[IndicatorSnap | None, int]:
    if len(bars) < WARMUP_BARS:
        return None, prev_alma_sig

    closes = [b.c for b in bars]
    direction = compute_supertrend(bars)
    sigs = signal_series(direction)
    i = len(bars) - 1
    prev_sig = sigs[i - 1] if i >= 1 else prev_alma_sig
    cur_sig = sigs[i]

    stc = compute_stc(closes)
    stc_v = stc[i] if i < len(stc) else float("nan")
    sg = stc_signals(stc, i) if stc_v == stc_v else {
        "buy": False, "sell": False,
    }

    return IndicatorSnap(
        bar_ts=bars[i].ts,
        bar_close=bars[i].c,
        alma_long=signal_flip(prev_sig, cur_sig) == "long",
        alma_short=signal_flip(prev_sig, cur_sig) == "short",
        alma_bull=direction[i] < 0,
        alma_bear=direction[i] > 0,
        stc_buy=sg["buy"],
        stc_sell=sg["sell"],
        stc_val=stc_v if stc_v == stc_v else -1.0,
    ), cur_sig


def write_trade_row(runner: StrategyRunner, t: ActiveTrade, exit_ms: int, exit_px: float, reason: str) -> None:
    hold = max(0, (exit_ms - t.entry_ms) // 1000)
    gross = (
        (exit_px - t.entry_price) / t.entry_price * 100
        if t.side == "long"
        else (t.entry_price - exit_px) / t.entry_price * 100
    )
    net = net_pnl_usd(t.side, t.entry_price, exit_px)
    with runner.trades_csv.open("a", newline="") as f:
        csv.writer(f).writerow(
            [
                t.symbol, t.side, utc_iso(t.signal_ms), utc_iso(t.entry_ms), utc_iso(exit_ms),
                f"{t.entry_price:.8f}", f"{exit_px:.8f}", f"{t.sl_px:.8f}", f"{t.tp_px:.8f}",
                hold, reason, f"{gross:.4f}", f"{net:.4f}",
                f"{t.max_fav_pct:.4f}", f"{t.max_adv_pct:.4f}",
            ]
        )


def close_trade(runner: StrategyRunner, st: StratSymState, exit_ms: int, exit_px: float, reason: str) -> None:
    t = st.trade
    if not t:
        return
    if runner.name == LIVE_MIRROR_RUNNER and LIVE_TRADE:
        schedule_live_mirror_exit(t.symbol, t.side, reason)
    net = net_pnl_usd(t.side, t.entry_price, exit_px)
    runner.stats["exits"] += 1
    runner.stats["net_usd"] += net
    if net > 0:
        runner.stats["wins"] += 1
    if reason == "sl":
        runner.stats["sl"] += 1
    elif reason == "tp":
        runner.stats["tp"] += 1
    else:
        runner.stats["timeout"] += 1
    write_trade_row(runner, t, exit_ms, exit_px, reason)
    runner.log(
        f"[EXIT] {t.symbol} {t.side.upper()} reason={reason} "
        f"entry={t.entry_price:.8f} exit={exit_px:.8f} net=${net:+.4f} "
        f"hold={(exit_ms - t.entry_ms) // 1000}s fav={t.max_fav_pct:.2f}% adv={t.max_adv_pct:.2f}%"
    )
    st.trade = None


def open_trade(
    runner: StrategyRunner,
    st: StratSymState,
    symbol: str,
    side: str,
    signal_ms: int,
    entry_ms: int,
    entry_px: float,
    snap: IndicatorSnap | None,
) -> None:
    sl_px, tp_px = sl_tp_prices(entry_px, side, SL_PCT, TP_PCT)
    st.trade = ActiveTrade(symbol, side, signal_ms, entry_ms, entry_px, sl_px, tp_px)
    runner.stats["entries"] += 1
    extra = ""
    if snap:
        extra = (
            f" alma_L={snap.alma_long} alma_S={snap.alma_short} "
            f"bull={snap.alma_bull} stc={snap.stc_val:.1f} stc_buy={snap.stc_buy} stc_sell={snap.stc_sell}"
        )
    runner.log(
        f"[ENTRY] {symbol} {side.upper()} @ {utc_iso(entry_ms)} price={entry_px:.8f} "
        f"SL={sl_px:.8f} ({SL_PCT}%) TP={tp_px:.8f} ({TP_PCT}%) signal@{utc_iso(signal_ms)}{extra}"
    )
    if runner.name == LIVE_MIRROR_RUNNER and LIVE_TRADE:
        schedule_live_mirror_entry(symbol, side, entry_px)


def update_mfe_mae(t: ActiveTrade, bar_h: float, bar_l: float) -> None:
    ep = t.entry_price
    if ep <= 0:
        return
    if t.side == "long":
        t.max_fav_pct = max(t.max_fav_pct, (bar_h - ep) / ep * 100)
        t.max_adv_pct = max(t.max_adv_pct, (ep - bar_l) / ep * 100)
    else:
        t.max_fav_pct = max(t.max_fav_pct, (ep - bar_l) / ep * 100)
        t.max_adv_pct = max(t.max_adv_pct, (bar_h - ep) / ep * 100)


def on_15m_close(symbol: str, feed: SymbolFeed, bar: OHLC) -> None:
    if feed.bars_15m and feed.bars_15m[-1].ts == bar.ts:
        return
    snap = build_snap(feed, bar)
    if snap is None:
        return

    if not feed.warmed:
        feed.warmed = True
        for runner in STRATEGIES:
            runner.stats["warmup_ready"] += 1
            runner.log(f"[WARMUP] {symbol} ready bars_15m={len(feed.bars_15m)}")

    for runner in STRATEGIES:
        side = runner.signal_fn(snap)
        if side is None:
            continue
        if binance and binance.max_leverage(symbol) < MIN_LEVERAGE:
            continue
        st = feed.strat[runner.name]
        if st.trade is not None:
            runner.stats["skipped_busy"] += 1
            runner.log(f"[SIGNAL_SKIP] {symbol} {side.upper()} — position open ({st.trade.side})")
            continue
        runner.stats["signals"] += 1
        st.pending_side = side
        st.pending_at_ms = bar.ts + BAR_MS
        st.pending_snap = snap
        extra = (
            f" alma_L={snap.alma_long} bull+stc={snap.alma_bull and snap.stc_buy} "
            f"stc={snap.stc_val:.1f}"
        )
        runner.log(
            f"[SIGNAL] {symbol} {side.upper()} @ {utc_iso(bar.ts)} close={bar.c:.8f} "
            f"entry_scheduled@{utc_iso(st.pending_at_ms)}{extra}"
        )


def finalize_15m_from_1s(symbol: str, feed: SymbolFeed, bar_1s: dict) -> None:
    period = (bar_1s["sec"] // BAR_MS) * BAR_MS
    cur = feed.cur_15m
    if cur is None:
        feed.cur_15m = {
            "ts": period,
            "o": bar_1s["open"],
            "h": bar_1s["high"],
            "l": bar_1s["low"],
            "c": bar_1s["close"],
        }
        return
    if cur["ts"] != period:
        completed = OHLC(cur["ts"], cur["o"], cur["h"], cur["l"], cur["c"])
        on_15m_close(symbol, feed, completed)
        feed.cur_15m = {
            "ts": period,
            "o": bar_1s["open"],
            "h": bar_1s["high"],
            "l": bar_1s["low"],
            "c": bar_1s["close"],
        }
        return
    cur["h"] = max(cur["h"], bar_1s["high"])
    cur["l"] = min(cur["l"], bar_1s["low"])
    cur["c"] = bar_1s["close"]


def process_1s_bar(symbol: str, bar: dict) -> None:
    feed = feeds[symbol]
    sec = bar["sec"]

    for runner in STRATEGIES:
        st = feed.strat[runner.name]
        snap = st.pending_snap

        if st.pending_side and st.trade is None and sec >= st.pending_at_ms:
            open_trade(
                runner, st, symbol, st.pending_side,
                st.pending_at_ms - BAR_MS, sec, bar["open"], snap,
            )
            st.pending_side = None
            st.pending_at_ms = 0
            st.pending_snap = None

        t = st.trade
        if t:
            update_mfe_mae(t, bar["high"], bar["low"])
            hit = check_exit(t.side, bar["high"], bar["low"], t.sl_px, t.tp_px)
            if hit:
                reason, px = hit
                close_trade(runner, st, sec, px, reason)
            elif sec - t.entry_ms >= MAX_HOLD_SEC * 1000:
                close_trade(runner, st, sec, bar["close"], "timeout")

    finalize_15m_from_1s(symbol, feed, bar)


async def finalize_1s_bucket(symbol: str, bar: dict) -> None:
    process_1s_bar(symbol, bar)


async def on_trade(symbol: str, price: float, qty: float, t_ms: int) -> None:
    agg_stats["trades"] += 1
    sec = (t_ms // 1000) * 1000
    feed = feeds.get(symbol)
    if feed is None:
        return
    b = feed.bars_1s_cur
    if not b or b["sec"] != sec:
        if b:
            await finalize_1s_bucket(symbol, b)
        feed.bars_1s_cur = {
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
    for runner in STRATEGIES:
        runner.log(f"[ws-{conn_id}] connecting {len(symbols)} symbols")
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, max_size=2**22) as ws:
                for runner in STRATEGIES:
                    runner.log(f"[ws-{conn_id}] connected")
                async for msg in ws:
                    data = json.loads(msg).get("data")
                    if not data:
                        continue
                    await on_trade(data["s"], float(data["p"]), float(data["q"]), int(data["T"]))
        except Exception as e:
            for runner in STRATEGIES:
                runner.log(f"[ws-{conn_id}] reconnect ({e})")
            await asyncio.sleep(3)


async def flush_stale_loop() -> None:
    while True:
        cutoff = (int(time.time()) - 1) * 1000
        for symbol, feed in feeds.items():
            b = feed.bars_1s_cur
            if b and b["sec"] < cutoff:
                await finalize_1s_bucket(symbol, b)
                feed.bars_1s_cur = None
        await asyncio.sleep(0.5)


async def live_reconcile_loop() -> None:
    while True:
        await asyncio.sleep(max(15, LIVE_RECONCILE_SEC))
        await reconcile_live_slots()


async def stats_loop() -> None:
    while True:
        await asyncio.sleep(STATS_INTERVAL_SEC)
        for runner in STRATEGIES:
            open_n = sum(1 for f in feeds.values() if f.strat[runner.name].trade is not None)
            pending_n = sum(1 for f in feeds.values() if f.strat[runner.name].pending_side)
            s = runner.stats
            exits = s["exits"]
            wr = (s["wins"] / exits * 100) if exits else 0.0
            runner.log(
                f"[stats] agg={agg_stats['trades']} warmed={s['warmup_ready']}/{len(SYMBOLS)} "
                f"signals={s['signals']} entries={s['entries']} exits={exits} "
                f"wr={wr:.1f}% net=${s['net_usd']:+.4f} "
                f"tp={s['tp']} sl={s['sl']} timeout={s['timeout']} "
                f"open={open_n} pending={pending_n} busy_skips={s['skipped_busy']}"
                + (
                    f" live_open={live_open_count()} live_in={live_stats['entries']} "
                    f"live_out={live_stats['exits']} live_skip={live_stats['skips']}"
                    if runner.name == LIVE_MIRROR_RUNNER and LIVE_TRADE
                    else ""
                )
            )
            if agg_stats["trades"] == 0:
                runner.log("[WARN] agg=0 — no websocket ticks received; check network / WS URL")


async def main() -> None:
    global SYMBOLS
    SYMBOLS = resolve_symbols()
    if LIVE_TRADE or MIN_LEVERAGE > 0:
        init_binance_client()
    if LIVE_TRADE and (binance is None or not binance.configured()):
        raise SystemExit(
            "ALMA_ST_BINANCE_LIVE requires ALMA_ST_BINANCE_API_KEY/SECRET "
            "(or STRATEGY_DRY_BINANCE_API_KEY/SECRET / BINANCE_API_KEY/SECRET)"
        )
    SYMBOLS = filter_symbols_by_leverage(SYMBOLS)
    if not SYMBOLS:
        raise SystemExit("no symbols resolved after leverage filter")
    init_feeds()

    chunks = [SYMBOLS[i : i + WS_CHUNK] for i in range(0, len(SYMBOLS), WS_CHUNK)]
    header = (
        f"alma_st_dry | TF=15m SL={SL_PCT}% TP={TP_PCT}% | symbols={len(SYMBOLS)} "
        f"excluded={EXCLUDE} mode={WATCHLIST_MODE} notional=${NOTIONAL} fee_rt={FEE_RT}"
    )
    for runner in STRATEGIES:
        runner.log(header)
        runner.log("  signal: Alma flip OR (Alma bull+bear + STC buy/sell cross 25/75)")
        runner.log(
            f"  entry: next 15m bar open | warmup={WARMUP_BARS}×15m | max_hold={MAX_HOLD_SEC // 3600}h"
        )
        if LIVE_TRADE:
            runner.log(
                f"  live_mirror: ON opposite side | max_open={MAX_OPEN_LIVE} "
                f"min_lev={MIN_LEVERAGE}x margin_buf={MARGIN_BUFFER} "
                f"exchange SL={LIVE_MIRROR_SL_PCT}% TP={LIVE_MIRROR_TP_PCT}% "
                f"working={ALGO_WORKING_TYPE} reconcile={LIVE_RECONCILE_SEC}s "
                f"(restart refreshes open TP algos)"
            )
        else:
            runner.log("  live_mirror: OFF (ALMA_ST_BINANCE_LIVE=false)")
        runner.log(f"  log={runner.log_path}")
        runner.log(f"  trades_csv={runner.trades_csv}")
    for runner in STRATEGIES:
        runner.log(f"  combined_log={COMBINED_LOG}")
    await bootstrap_history()
    if LIVE_TRADE:
        await bootstrap_live_mirror()
    tasks = [
        *[ws_handler(i, c) for i, c in enumerate(chunks)],
        flush_stale_loop(),
        stats_loop(),
    ]
    if LIVE_TRADE:
        tasks.append(live_reconcile_loop())
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        for runner in STRATEGIES:
            runner.log("Stopping")
