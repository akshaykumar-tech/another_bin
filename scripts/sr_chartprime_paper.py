#!/usr/bin/env python3
"""
Dry paper: ChartPrime SR + God-mode fakeout/breakout (parallel dry runs).
Optional live Binance futures for one god-mode setup only (same side as dry, no mirror).

  python3 scripts/sr_chartprime_paper.py

ChartPrime signals (sr_dry.log / sr_dry_trades.csv) — dry only:
  LONG  — sup_holds, break_res, res_as_sup
  SHORT — res_holds, break_sup, sup_as_res

God-mode signals (godmode_dry.log / godmode_dry_trades.csv) — dry for all setups:
  LONG  — fakeout_sup, breakout_res, retest_res
  SHORT — fakeout_res (breakdown_sup skipped by default)

Live (SR_GODMODE_LIVE_ENABLED=true): fakeout_res only — SHORT on Binance, exchange SL/TP.
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

from binance_futures import BinanceFuturesClient, parse_fill
from sr_chartprime_lib import Bar, SRTracker, entry_side, signal_name
from sr_godmode_lib import GodModeTracker

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


def _env_bool(name: str, default: bool) -> bool:
    v = _env(name, "true" if default else "false").lower()
    return v in ("1", "true", "yes", "on")


def _env_alt(primary: str, fallback: str, default: str) -> str:
    v = os.environ.get(primary, "").strip()
    if v:
        return v
    v2 = os.environ.get(fallback, "").strip()
    return v2 or default


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
STATS_INTERVAL_SEC = _env_int("SR_CHARTPRIME_STATS_INTERVAL_SEC", 1800)
BOOTSTRAP_KLINES = _env_int("SR_CHARTPRIME_BOOTSTRAP_KLINES", 300)
WARMUP_BARS = max(LOOKBACK * 2 + 10, 210)

GODMODE_ENABLED = _env_bool("SR_GODMODE_ENABLED", True)
GOD_LOOKBACK = _env_int("SR_GODMODE_LOOKBACK", 15)
GOD_VOL_MA = _env_int("SR_GODMODE_VOL_MA", 20)
GOD_VOL_SPIKE = _env_float("SR_GODMODE_VOL_SPIKE", 1.4)
GOD_WICK_RATIO = _env_float("SR_GODMODE_WICK_RATIO", 0.55)
GOD_SKIP_BREAKDOWN = _env_bool("SR_GODMODE_SKIP_BREAKDOWN", True)
GOD_MAX_HOLD_BARS = _env_int("SR_GODMODE_MAX_HOLD_BARS", MAX_HOLD_BARS)

LIVE_ENABLED = _env_bool("SR_GODMODE_LIVE_ENABLED", False)
LIVE_SETUP = _env("SR_GODMODE_LIVE_SETUP", "fakeout_res")
LIVE_NOTIONAL = _env_float("SR_GODMODE_LIVE_NOTIONAL_USDT", 6.0)
LIVE_SL_PCT = _env_float("SR_GODMODE_LIVE_SL_PCT", 8.0)
LIVE_TP_PCT = _env_float("SR_GODMODE_LIVE_TP_PCT", 1.5)
LIVE_MAX_OPEN = _env_int("SR_GODMODE_LIVE_MAX_OPEN", 30)
LIVE_MIN_LEVERAGE = _env_int("SR_GODMODE_LIVE_MIN_LEVERAGE", 50)
LIVE_MARGIN_BUFFER = _env_float("SR_GODMODE_LIVE_MARGIN_BUFFER", 1.05)
LIVE_RECONCILE_SEC = _env_int("SR_GODMODE_LIVE_RECONCILE_SEC", 60)
LIVE_ALGO_WORKING_TYPE = _env("SR_GODMODE_LIVE_ALGO_WORKING_TYPE", "CONTRACT_PRICE").upper()

binance: BinanceFuturesClient | None = None
live_slots: dict[str, dict] = {}
live_stats = {"entries": 0, "exits": 0, "skips": 0}
live_lock: asyncio.Lock | None = None

RUN_TS = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
OUT_DIR = Path(_env("SR_CHARTPRIME_OUT_DIR", str(ROOT / f"data/sr_chartprime/{RUN_TS}")))
OUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = OUT_DIR / "sr_dry.log"
TRADES_CSV = OUT_DIR / "sr_dry_trades.csv"
GOD_LOG_FILE = OUT_DIR / "godmode_dry.log"
GOD_TRADES_CSV = OUT_DIR / "godmode_dry_trades.csv"

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
    "warmup_ready": 0,
    "sup_boxes": 0,
    "res_boxes": 0,
    "sup_holds": 0,
    "res_holds": 0,
    "break_res": 0,
    "break_sup": 0,
    "res_as_sup": 0,
    "sup_as_res": 0,
    "bars_closed": 0,
}

god_stats = {
    "signals": 0,
    "entries": 0,
    "exits": 0,
    "wins": 0,
    "net_usd": 0.0,
    "tp": 0,
    "sl": 0,
    "timeout": 0,
    "fakeout_sup": 0,
    "fakeout_res": 0,
    "breakout_res": 0,
    "breakdown_sup": 0,
    "retest_res": 0,
    "retest_sup": 0,
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
    god: GodModeTracker = field(default_factory=GodModeTracker)
    trade: ActiveTrade | None = None
    god_trades: list[ActiveTrade] = field(default_factory=list)
    warmed: bool = False


feeds: dict[str, SymbolFeed] = {}
SYMBOLS: list[str] = []


def log(msg: str) -> None:
    line = f"{datetime.now(timezone.utc).isoformat()} {msg}"
    print(line, flush=True)
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def god_log(msg: str) -> None:
    line = f"{datetime.now(timezone.utc).isoformat()} {msg}"
    with GOD_LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def init_binance_client() -> None:
    global binance
    api_key = _env_alt("SR_GODMODE_BINANCE_API_KEY", "BINANCE_API_KEY", "")
    api_secret = _env_alt("SR_GODMODE_BINANCE_API_SECRET", "BINANCE_API_SECRET", "")
    if not api_key or not api_secret:
        return
    binance = BinanceFuturesClient(api_key, api_secret, FAPI)
    binance.warm_cache()


def _live_lock() -> asyncio.Lock:
    global live_lock
    if live_lock is None:
        live_lock = asyncio.Lock()
    return live_lock


def entry_order_side(dry_side: str) -> str:
    return "BUY" if dry_side == "long" else "SELL"


def close_order_side(dry_side: str) -> str:
    return "SELL" if dry_side == "long" else "BUY"


def order_to_live_side(order_side: str) -> str:
    return "long" if order_side == "BUY" else "short"


def live_tp_px(entry: float, live_side: str) -> float:
    if live_side == "long":
        return entry * (1 + LIVE_TP_PCT / 100)
    return entry * (1 - LIVE_TP_PCT / 100)


def live_sl_tp(entry: float, live_side: str) -> tuple[float, float]:
    if live_side == "long":
        return entry * (1 - LIVE_SL_PCT / 100), entry * (1 + LIVE_TP_PCT / 100)
    return entry * (1 + LIVE_SL_PCT / 100), entry * (1 - LIVE_TP_PCT / 100)


def close_order_side_from_live(live_side: str) -> str:
    return "SELL" if live_side == "long" else "BUY"


def live_tp_hit(live_side: str, px: float, tp_px: float) -> bool:
    if live_side == "long":
        return px >= tp_px
    return px <= tp_px


def live_open_count() -> int:
    if LIVE_ENABLED and binance is not None:
        return len(binance.open_position_symbols())
    return len(live_slots)


def god_setup_open_count(symbol: str, setup: str) -> int:
    feed = feeds.get(symbol)
    if not feed:
        return 0
    return sum(1 for t in feed.god_trades if t.signal == setup)


def _cancel_orphan_algos_sync() -> list[str]:
    if not LIVE_ENABLED or binance is None:
        return []
    try:
        return binance.cancel_orphan_algo_orders()
    except Exception:
        return []


def _reconcile_live_slots_sync() -> list[tuple[str, bool]]:
    if not LIVE_ENABLED or binance is None:
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
    if not LIVE_ENABLED or binance is None:
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
        sl_px, tp_px = live_sl_tp(entry, live_side) if entry > 0 else (0.0, 0.0)
        live_slots[sym] = {
            "dry_side": "unknown",
            "qty": qty,
            "entry_price": entry,
            "leverage": int(float(row.get("leverage") or 0)),
            "live_side": live_side,
            "sl_px": sl_px,
            "tp_px": tp_px,
        }
        seeded += 1
    return seeded


def _refresh_live_tp_orders_sync() -> tuple[list[tuple], list[tuple], list[tuple]]:
    if not LIVE_ENABLED or binance is None:
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
            sl_px, tp_px = live_sl_tp(entry, live_side)
            close_side = close_order_side_from_live(live_side)
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
                closed.append((sym, px, tp_px))
                continue
            binance.cancel_tp_algo_orders(sym)
            tp_resp = binance.take_profit_market_reduce(
                sym, close_side, tp_px, qty, working_type=LIVE_ALGO_WORKING_TYPE
            )
            live_slots[sym] = {
                **slot,
                "qty": qty,
                "entry_price": entry,
                "live_side": live_side,
                "sl_px": sl_px,
                "tp_px": tp_px,
            }
            algo_id = tp_resp.get("algoId", tp_resp.get("clientAlgoId", "?"))
            updated.append((sym, old_tp, tp_px, algo_id))
        except Exception as e:
            failed.append((sym, str(e)))
    return closed, updated, failed


async def reconcile_live_slots() -> None:
    if not LIVE_ENABLED or binance is None:
        return
    loop = asyncio.get_running_loop()
    removed = await loop.run_in_executor(None, _reconcile_live_slots_sync)
    for sym, cancelled in removed:
        live_stats["exits"] += 1
        extra = " + orphan algos cancelled" if cancelled else ""
        god_log(f"[LIVE_RECONCILE] {sym} flat on exchange — slot cleared{extra}")
    for sym in await loop.run_in_executor(None, _cancel_orphan_algos_sync):
        god_log(f"[LIVE_RECONCILE] {sym} orphan SL/TP algos cancelled (no position)")


async def bootstrap_live() -> None:
    if not LIVE_ENABLED or binance is None:
        return
    loop = asyncio.get_running_loop()
    seeded = await loop.run_in_executor(None, _seed_live_slots_sync)
    closed, updated, failed = await loop.run_in_executor(None, _refresh_live_tp_orders_sync)
    n_pos = len(binance.open_position_symbols())
    god_log(
        f"[LIVE_BOOT] setup={LIVE_SETUP} exchange_positions={n_pos} seeded_slots={seeded} "
        f"tp_pct={LIVE_TP_PCT}% sl_pct={LIVE_SL_PCT}% max_open={LIVE_MAX_OPEN} "
        f"(no new entry orders on startup)"
    )
    for sym, px, tp_px in closed:
        live_stats["exits"] += 1
        god_log(f"[LIVE_TP_REFRESH] {sym} closed @ {px:.8f} (TP {tp_px:.8f} already hit)")
    for sym, old_tp, new_tp, algo_id in updated:
        old_s = f"{old_tp:.8f}" if old_tp > 0 else "none"
        god_log(f"[LIVE_TP_REFRESH] {sym} TP {old_s} -> {new_tp:.8f} ({LIVE_TP_PCT}%) tp_algo={algo_id}")
    for sym, err in failed:
        god_log(f"[LIVE_TP_REFRESH_FAIL] {sym} {err}")


async def live_entry(symbol: str, dry_side: str, ref_px: float) -> None:
    if not LIVE_ENABLED or binance is None:
        return
    sym = symbol.upper()
    try:
        async with _live_lock():
            if sym in live_slots:
                live_stats["skips"] += 1
                god_log(f"[LIVE_SKIP] {sym} already in live_slots")
                return
            open_syms = set(binance.open_position_symbols())
            if sym in open_syms:
                live_stats["skips"] += 1
                god_log(f"[LIVE_SKIP] {sym} exchange position already open")
                return
            n_open = len(open_syms)
            if n_open >= LIVE_MAX_OPEN:
                live_stats["skips"] += 1
                god_log(f"[LIVE_SKIP] {sym} max_open={LIVE_MAX_OPEN} (exchange={n_open})")
                return
            if not binance.symbol_tradable(sym):
                live_stats["skips"] += 1
                god_log(f"[LIVE_SKIP] {sym} not tradable")
                return
            if binance.max_leverage(sym) < LIVE_MIN_LEVERAGE:
                live_stats["skips"] += 1
                god_log(f"[LIVE_SKIP] {sym} lev<{LIVE_MIN_LEVERAGE}x")
                return

            order_side = entry_order_side(dry_side)

            def _place():
                lev = binance.set_max_leverage(sym)
                margin_needed = (LIVE_NOTIONAL / lev) * LIVE_MARGIN_BUFFER
                bal = binance.available_usdt()
                if bal < margin_needed:
                    raise RuntimeError(
                        f"insufficient margin: need ${margin_needed:.2f} "
                        f"(notional=${LIVE_NOTIONAL:.2f} @ {lev}x), available=${bal:.2f}"
                    )
                resp = binance.market_order_notional(sym, order_side, LIVE_NOTIONAL)
                entry, qty = parse_fill(resp)
                if qty <= 0:
                    qty = binance.position_qty(sym)
                if entry <= 0:
                    entry = ref_px
                live_side = order_to_live_side(order_side)
                sl_px, tp_px = live_sl_tp(entry, live_side)
                close_side = close_order_side(dry_side)
                sl_resp = binance.stop_market_reduce(
                    sym, close_side, sl_px, qty, working_type=LIVE_ALGO_WORKING_TYPE
                )
                tp_resp = binance.take_profit_market_reduce(
                    sym, close_side, tp_px, qty, working_type=LIVE_ALGO_WORKING_TYPE
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
            god_log(
                f"[LIVE_ENTRY] {sym} setup={LIVE_SETUP} dry={dry_side.upper()} binance={order_side} "
                f"fill={entry:.8f} qty={qty:.8f} notional=${LIVE_NOTIONAL:.2f} lev={lev}x "
                f"margin~=${margin_needed:.2f} bal=${bal:.2f} ref={ref_px:.8f} "
                f"SL={sl_px:.8f} ({LIVE_SL_PCT}%) TP={tp_px:.8f} ({LIVE_TP_PCT}%) "
                f"sl_algo={sl_resp.get('algoId', sl_resp.get('clientAlgoId', '?'))} "
                f"tp_algo={tp_resp.get('algoId', tp_resp.get('clientAlgoId', '?'))}"
            )
    except Exception as e:
        live_stats["skips"] += 1
        god_log(f"[LIVE_ENTRY_FAIL] {sym} {e}")


async def live_exit(symbol: str, dry_side: str, reason: str) -> None:
    if not LIVE_ENABLED or binance is None:
        return
    sym = symbol.upper()
    slot = live_slots.get(sym)
    close_side = close_order_side(dry_side)

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
        async with _live_lock():
            exit_px, already_flat = await asyncio.get_running_loop().run_in_executor(None, _close)
            live_slots.pop(sym, None)
            live_stats["exits"] += 1
            if already_flat:
                god_log(f"[LIVE_EXIT] {sym} reason={reason} already_flat")
            else:
                god_log(
                    f"[LIVE_EXIT] {sym} reason={reason} side={close_side} "
                    f"exit={exit_px:.8f} dry={dry_side.upper()}"
                )
    except Exception as e:
        god_log(f"[LIVE_EXIT_FAIL] {sym} reason={reason} {e}")
        try:
            flat = await asyncio.get_running_loop().run_in_executor(
                None, lambda: binance.position_qty(sym) <= 0
            )
            if flat:
                live_slots.pop(sym, None)
                live_stats["exits"] += 1
                god_log(f"[LIVE_EXIT] {sym} reason={reason} flat_after_fail")
        except Exception:
            pass


def schedule_live_entry(symbol: str, dry_side: str, ref_px: float) -> None:
    try:
        asyncio.get_running_loop().create_task(live_entry(symbol, dry_side, ref_px))
    except RuntimeError:
        pass


def schedule_live_exit(symbol: str, dry_side: str, reason: str) -> None:
    try:
        asyncio.get_running_loop().create_task(live_exit(symbol, dry_side, reason))
    except RuntimeError:
        pass


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


def cp_open_count() -> int:
    return sum(1 for f in feeds.values() if f.trade is not None)


def god_open_count() -> int:
    return sum(len(f.god_trades) for f in feeds.values())


def init_trades_csv() -> None:
    if not TRADES_CSV.is_file():
        with TRADES_CSV.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(
                [
                    "symbol", "side", "signal", "signal_utc", "entry_utc", "exit_utc",
                    "entry_px", "exit_px", "reason", "net_usd", "hold_bars", "max_fav_pct", "max_adv_pct",
                ]
            )


def init_god_trades_csv() -> None:
    if not GOD_TRADES_CSV.is_file():
        with GOD_TRADES_CSV.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(
                [
                    "symbol", "side", "setup", "signal_utc", "entry_utc", "exit_utc",
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
    feed.god = GodModeTracker(GOD_LOOKBACK, GOD_VOL_MA, GOD_VOL_SPIKE, GOD_WICK_RATIO, GOD_SKIP_BREAKDOWN)
    if bars:
        feed.tracker.load_history(bars)
        if GODMODE_ENABLED:
            feed.god.load_history(bars)
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
    if GODMODE_ENABLED:
        god_log(f"[bootstrap] godmode ready symbols={len(SYMBOLS)} skip_breakdown={GOD_SKIP_BREAKDOWN}")


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


def close_god_trade(feed: SymbolFeed, t: ActiveTrade, exit_ms: int, exit_px: float, reason: str) -> None:
    if t not in feed.god_trades:
        return
    net = pnl_usd(t.side, t.entry_px, exit_px)
    god_stats["exits"] += 1
    god_stats["net_usd"] += net
    if net > 0:
        god_stats["wins"] += 1
    if reason in god_stats:
        god_stats[reason] += 1
    god_log(
        f"[EXIT] {t.symbol} {t.side.upper()} setup={t.signal} reason={reason} "
        f"entry={t.entry_px:.8f} exit={exit_px:.8f} net=${net:+.4f} hold={t.bars_held}bars "
        f"fav={t.max_fav_pct:.2f}% adv={t.max_adv_pct:.2f}%"
    )
    with GOD_TRADES_CSV.open("a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(
            [
                t.symbol, t.side, t.signal, utc_iso(t.signal_ms), utc_iso(t.entry_ms), utc_iso(exit_ms),
                f"{t.entry_px:.8f}", f"{exit_px:.8f}", reason, f"{net:.4f}",
                t.bars_held, f"{t.max_fav_pct:.2f}", f"{t.max_adv_pct:.2f}",
            ]
        )
    sym, side, setup = t.symbol, t.side, t.signal
    feed.god_trades.remove(t)
    if LIVE_ENABLED and setup == LIVE_SETUP and god_setup_open_count(sym, LIVE_SETUP) == 0:
        schedule_live_exit(sym, side, reason)


def open_trade(symbol: str, feed: SymbolFeed, side: str, sig_name: str, signal_ms: int, entry_px: float) -> None:
    sl_px, tp_px = sl_tp_prices(side, entry_px)
    feed.trade = ActiveTrade(symbol, side, sig_name, signal_ms, signal_ms, entry_px, sl_px, tp_px)
    stats["entries"] += 1
    log(
        f"[ENTRY] {symbol} {side.upper()} signal={sig_name} @ {utc_iso(signal_ms)} price={entry_px:.8f} "
        f"SL={sl_px:.8f} ({SL_PCT}%) TP={tp_px:.8f} ({TP_PCT}%)"
    )


def open_god_trade(symbol: str, feed: SymbolFeed, side: str, setup: str, signal_ms: int, entry_px: float) -> None:
    sl_px, tp_px = sl_tp_prices(side, entry_px)
    t = ActiveTrade(symbol, side, setup, signal_ms, signal_ms, entry_px, sl_px, tp_px)
    feed.god_trades.append(t)
    god_stats["entries"] += 1
    if setup in god_stats:
        god_stats[setup] += 1
    god_log(
        f"[ENTRY] {symbol} {side.upper()} setup={setup} @ {utc_iso(signal_ms)} price={entry_px:.8f} "
        f"SL={sl_px:.8f} ({SL_PCT}%) TP={tp_px:.8f} ({TP_PCT}%) open={len(feed.god_trades)}"
    )
    if LIVE_ENABLED and setup == LIVE_SETUP:
        schedule_live_entry(symbol, side, entry_px)


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

    log(f"[SIGNAL] {symbol} {sig_name} → {side.upper()} @ {utc_iso(bar.ts)} close={bar.c:.8f}")
    open_trade(symbol, feed, side, sig_name, bar.ts, bar.c)


def detect_god_on_close(symbol: str, feed: SymbolFeed, bar: Bar) -> None:
    if not GODMODE_ENABLED:
        return
    gsig = feed.god.on_bar(bar)
    if gsig is None:
        return
    god_stats["signals"] += 1
    god_log(
        f"[SIGNAL] {symbol} {gsig.setup} → {gsig.side.upper()} @ {utc_iso(bar.ts)} "
        f"entry={gsig.entry_px:.8f} close={bar.c:.8f}"
    )
    open_god_trade(symbol, feed, gsig.side, gsig.setup, bar.ts, gsig.entry_px)


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
        if GODMODE_ENABLED:
            feed.god.on_bar(bar)
        return

    detect_sr_on_close(symbol, feed, bar)
    detect_god_on_close(symbol, feed, bar)


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

    for gt in list(feed.god_trades):
        update_mfe_mae(gt, hi, lo)
        hit = check_exit(gt.side, hi, lo, gt.sl_px, gt.tp_px)
        if hit:
            close_god_trade(feed, gt, ts, hit[1], hit[0])
        elif k.get("x"):
            gt.bars_held += 1
            if gt.bars_held >= GOD_MAX_HOLD_BARS:
                close_god_trade(feed, gt, ts, cl, "timeout")

    if k.get("x"):
        stats["bars_closed"] += 1
        on_bar_close(symbol, Bar(ts, float(k["o"]), hi, lo, cl, vol))


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
            f"[stats] chartprime warmed={stats['warmup_ready']}/{len(SYMBOLS)} mode={SIGNAL_MODE} "
            f"signals={stats['signals']} entries={stats['entries']} exits={exits} wr={wr:.1f}% "
            f"net=${stats['net_usd']:+.4f} tp={stats['tp']} sl={stats['sl']} timeout={stats['timeout']} "
            f"open={cp_open_count()} bars_closed={stats['bars_closed']} busy_skips={stats['skipped_busy']}"
        )
        if GODMODE_ENABLED:
            g_exits = god_stats["exits"]
            g_wr = (god_stats["wins"] / g_exits * 100) if g_exits else 0.0
            god_log(
                f"[stats] godmode signals={god_stats['signals']} entries={god_stats['entries']} "
                f"exits={g_exits} wr={g_wr:.1f}% net=${god_stats['net_usd']:+.4f} "
                f"tp={god_stats['tp']} sl={god_stats['sl']} timeout={god_stats['timeout']} "
                f"open={god_open_count()} fakeout_sup={god_stats['fakeout_sup']} "
                f"breakout_res={god_stats['breakout_res']} fakeout_res={god_stats['fakeout_res']}"
                + (
                    f" | live_setup={LIVE_SETUP} live_open={live_open_count()} "
                    f"live_in={live_stats['entries']} live_out={live_stats['exits']} "
                    f"live_skip={live_stats['skips']}"
                    if LIVE_ENABLED
                    else ""
                )
            )


async def live_reconcile_loop() -> None:
    while True:
        await asyncio.sleep(max(15, LIVE_RECONCILE_SEC))
        await reconcile_live_slots()


async def main() -> None:
    global SYMBOLS
    init_trades_csv()
    if GODMODE_ENABLED:
        init_god_trades_csv()
    if LIVE_ENABLED:
        init_binance_client()
        if binance is None or not binance.configured():
            raise SystemExit("SR_GODMODE_LIVE_ENABLED=true but Binance API keys missing")
    SYMBOLS = resolve_symbols()
    if not SYMBOLS:
        raise SystemExit("no symbols resolved")
    for sym in SYMBOLS:
        feeds[sym] = SymbolFeed(
            tracker=SRTracker(LOOKBACK, VOL_LEN, BOX_WIDTH),
            god=GodModeTracker(GOD_LOOKBACK, GOD_VOL_MA, GOD_VOL_SPIKE, GOD_WICK_RATIO, GOD_SKIP_BREAKDOWN),
        )

    log(
        f"sr_chartprime | TF={INTERVAL} lookback={LOOKBACK} vol_len={VOL_LEN} box_width={BOX_WIDTH} | "
        f"symbols={len(SYMBOLS)} mode={WATCHLIST_MODE} notional=${NOTIONAL} "
        f"SL={SL_PCT}% TP={TP_PCT}% signals={SIGNAL_MODE} max_open=unlimited"
    )
    log(f"  log={LOG_FILE}")
    log(f"  trades={TRADES_CSV}")
    if GODMODE_ENABLED:
        god_log(
            f"godmode | TF={INTERVAL} lookback={GOD_LOOKBACK} vol_spike={GOD_VOL_SPIKE} "
            f"wick={GOD_WICK_RATIO} skip_breakdown={GOD_SKIP_BREAKDOWN} | symbols={len(SYMBOLS)} "
            f"notional=${NOTIONAL} SL={SL_PCT}% TP={TP_PCT}% max_hold={GOD_MAX_HOLD_BARS}bars dry_only"
        )
        god_log(f"  log={GOD_LOG_FILE}")
        god_log(f"  trades={GOD_TRADES_CSV}")
        if LIVE_ENABLED:
            god_log(
                f"  live: ON setup={LIVE_SETUP} same side as dry | notional=${LIVE_NOTIONAL} "
                f"max_open={LIVE_MAX_OPEN} min_lev={LIVE_MIN_LEVERAGE}x max_lev on entry "
                f"SL={LIVE_SL_PCT}% TP={LIVE_TP_PCT}% working={LIVE_ALGO_WORKING_TYPE} "
                f"reconcile={LIVE_RECONCILE_SEC}s margin_buf={LIVE_MARGIN_BUFFER}"
            )
        else:
            god_log("  live: OFF (SR_GODMODE_LIVE_ENABLED=false)")

    await bootstrap_all()
    if LIVE_ENABLED:
        await bootstrap_live()

    chunks = [SYMBOLS[i : i + WS_CHUNK] for i in range(0, len(SYMBOLS), WS_CHUNK)]
    tasks = [asyncio.create_task(ws_handler(i, c)) for i, c in enumerate(chunks)]
    tasks.append(asyncio.create_task(stats_loop()))
    if LIVE_ENABLED:
        tasks.append(asyncio.create_task(live_reconcile_loop()))
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
