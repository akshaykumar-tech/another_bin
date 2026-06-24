#!/usr/bin/env python3
"""
Dry paper + optional live Binance: Flux Charts Liquidity Grabs — 500-symbol scan.

  Buyside grab (sweep pivot high, close below) → SHORT (default)
  Sellside grab → LONG (optional, off by default)

  python3 scripts/liquidity_grabs_paper.py

Live (LQ_GRABS_LIVE_ENABLED=true): same side as dry, exchange SL/TP.
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

from liquidity_grabs_lib import Bar, GrabState, GrabSignal, on_bar_confirmed
from binance_futures import BinanceFuturesClient, parse_fill
from paper_stats_lib import TypeStatsBook, unrealized_usd

try:
    import websockets
except ImportError:
    print("pip install websockets")
    raise

RUN_TS = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


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

WS_ROOT = _env("LQ_GRABS_WS_ROOT", "wss://fstream.binance.com").rstrip("/")
FAPI = _env("LQ_GRABS_FAPI", "https://fapi.binance.com").rstrip("/")
INTERVAL = _env("LQ_GRABS_INTERVAL", "5m")
BAR_MS = 300_000
WS_CHUNK = _env_int("LQ_GRABS_WS_CHUNK", 40)
WATCHLIST_MODE = _env("LQ_GRABS_WATCHLIST_MODE", "all_perps").lower()
WATCHLIST_SIZE = _env_int("LQ_GRABS_WATCHLIST_SIZE", 500)
PIVOT_LEN = _env_int("LQ_GRABS_PIVOT_LEN", 25)
WBR = _env_float("LQ_GRABS_WBR", 0.5)
COOLDOWN = _env_int("LQ_GRABS_COOLDOWN", 3)
LIQ_ZONE_COUNT = _env_int("LQ_GRABS_LIQ_ZONE_COUNT", 5)
NOTIONAL = _env_float("LQ_GRABS_NOTIONAL_USDT", 6.0)
FEE_RT = _env_float("LQ_GRABS_FEE_RT", 0.0008)
SL_PCT = _env_float("LQ_GRABS_SL_PCT", 8.0)
TP_PCT = _env_float("LQ_GRABS_TP_PCT", 1.5)
MAX_HOLD_BARS = _env_int("LQ_GRABS_MAX_HOLD_BARS", 96)
SHORTS_ONLY = _env_bool("LQ_GRABS_SHORTS_ONLY", True)
MIN_GRAB_SIZE = _env_int("LQ_GRABS_MIN_GRAB_SIZE", 1)
MAX_OPEN = _env_int("LQ_GRABS_MAX_OPEN", 0)  # 0 = unlimited
BOOTSTRAP_KLINES = _env_int("LQ_GRABS_BOOTSTRAP_KLINES", 300)
STATS_INTERVAL_SEC = _env_int("LQ_GRABS_STATS_INTERVAL_SEC", 1800)
BAR_HISTORY_MAX = _env_int("LQ_GRABS_BAR_HISTORY_MAX", 400)
WARMUP_BARS = 2 * PIVOT_LEN + 50

LIVE_ENABLED = _env_bool("LQ_GRABS_LIVE_ENABLED", False)
LIVE_NOTIONAL = _env_float("LQ_GRABS_LIVE_NOTIONAL_USDT", 6.0)
LIVE_SL_PCT = _env_float("LQ_GRABS_LIVE_SL_PCT", 8.0)
LIVE_TP_PCT = _env_float("LQ_GRABS_LIVE_TP_PCT", 1.5)
LIVE_MAX_OPEN = _env_int("LQ_GRABS_LIVE_MAX_OPEN", 30)
LIVE_MIN_LEVERAGE = _env_int("LQ_GRABS_LIVE_MIN_LEVERAGE", 50)
LIVE_MARGIN_BUFFER = _env_float("LQ_GRABS_LIVE_MARGIN_BUFFER", 1.05)
LIVE_RECONCILE_SEC = _env_int("LQ_GRABS_LIVE_RECONCILE_SEC", 60)
LIVE_ALGO_WORKING_TYPE = _env("LQ_GRABS_LIVE_ALGO_WORKING_TYPE", "CONTRACT_PRICE").upper()

binance: BinanceFuturesClient | None = None
live_slots: dict[str, dict] = {}
live_stats = {"entries": 0, "exits": 0, "skips": 0}
live_lock: asyncio.Lock | None = None

OUT_DIR = Path(_env("LQ_GRABS_OUT_DIR", str(ROOT / "data/liq_grabs/dry")))
OUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = OUT_DIR / "liq_grabs_dry.log"
TRADES_CSV = OUT_DIR / "liq_grabs_dry_trades.csv"

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
    "buyside": 0,
    "sellside": 0,
    "grab_sz1": 0,
    "grab_sz2": 0,
    "grab_sz3": 0,
    "bars_closed": 0,
    "warmup_ready": 0,
}

type_stats = TypeStatsBook()


@dataclass
class ActiveTrade:
    symbol: str
    side: str
    grab_type: str
    grab_size: int
    liq_level: float
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
    bars: list[Bar] = field(default_factory=list)
    state: GrabState = field(default_factory=GrabState)
    trade: ActiveTrade | None = None
    warmed: bool = False
    last_closed_ts: int = 0
    last_close: float = 0.0


feeds: dict[str, SymbolFeed] = {}
SYMBOLS: list[str] = []


def log(msg: str) -> None:
    line = f"{datetime.now(timezone.utc).isoformat()} {msg}"
    print(line, flush=True)
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def init_binance_client() -> None:
    global binance
    api_key = _env_alt("LQ_GRABS_BINANCE_API_KEY", "BINANCE_API_KEY", "")
    api_secret = _env_alt("LQ_GRABS_BINANCE_API_SECRET", "BINANCE_API_SECRET", "")
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
        log(f"[LIVE_RECONCILE] {sym} flat on exchange — slot cleared{extra}")
    for sym in await loop.run_in_executor(None, _cancel_orphan_algos_sync):
        log(f"[LIVE_RECONCILE] {sym} orphan SL/TP algos cancelled (no position)")


async def bootstrap_live() -> None:
    if not LIVE_ENABLED or binance is None:
        return
    loop = asyncio.get_running_loop()
    seeded = await loop.run_in_executor(None, _seed_live_slots_sync)
    closed, updated, failed = await loop.run_in_executor(None, _refresh_live_tp_orders_sync)
    n_pos = len(binance.open_position_symbols())
    log(
        f"[LIVE_BOOT] exchange_positions={n_pos} seeded_slots={seeded} "
        f"tp_pct={LIVE_TP_PCT}% sl_pct={LIVE_SL_PCT}% max_open={LIVE_MAX_OPEN} "
        f"(no new entry orders on startup)"
    )
    for sym, px, tp_px in closed:
        live_stats["exits"] += 1
        log(f"[LIVE_TP_REFRESH] {sym} closed @ {px:.8f} (TP {tp_px:.8f} already hit)")
    for sym, old_tp, new_tp, algo_id in updated:
        old_s = f"{old_tp:.8f}" if old_tp > 0 else "none"
        log(f"[LIVE_TP_REFRESH] {sym} TP {old_s} -> {new_tp:.8f} ({LIVE_TP_PCT}%) tp_algo={algo_id}")
    for sym, err in failed:
        log(f"[LIVE_TP_REFRESH_FAIL] {sym} {err}")


async def live_entry(symbol: str, dry_side: str, ref_px: float) -> None:
    if not LIVE_ENABLED or binance is None:
        return
    sym = symbol.upper()
    try:
        async with _live_lock():
            if sym in live_slots:
                live_stats["skips"] += 1
                log(f"[LIVE_SKIP] {sym} already in live_slots")
                return
            open_syms = set(binance.open_position_symbols())
            if sym in open_syms:
                live_stats["skips"] += 1
                log(f"[LIVE_SKIP] {sym} exchange position already open")
                return
            n_open = len(open_syms)
            if n_open >= LIVE_MAX_OPEN:
                live_stats["skips"] += 1
                log(f"[LIVE_SKIP] {sym} max_open={LIVE_MAX_OPEN} (exchange={n_open})")
                return
            if not binance.symbol_tradable(sym):
                live_stats["skips"] += 1
                log(f"[LIVE_SKIP] {sym} not tradable")
                return
            if binance.max_leverage(sym) < LIVE_MIN_LEVERAGE:
                live_stats["skips"] += 1
                log(f"[LIVE_SKIP] {sym} lev<{LIVE_MIN_LEVERAGE}x")
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
            log(
                f"[LIVE_ENTRY] {sym} dry={dry_side.upper()} binance={order_side} "
                f"fill={entry:.8f} qty={qty:.8f} notional=${LIVE_NOTIONAL:.2f} lev={lev}x "
                f"margin~=${margin_needed:.2f} bal=${bal:.2f} ref={ref_px:.8f} "
                f"SL={sl_px:.8f} ({LIVE_SL_PCT}%) TP={tp_px:.8f} ({LIVE_TP_PCT}%) "
                f"sl_algo={sl_resp.get('algoId', sl_resp.get('clientAlgoId', '?'))} "
                f"tp_algo={tp_resp.get('algoId', tp_resp.get('clientAlgoId', '?'))}"
            )
    except Exception as e:
        live_stats["skips"] += 1
        log(f"[LIVE_ENTRY_FAIL] {sym} {e}")


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
                log(f"[LIVE_EXIT] {sym} reason={reason} already_flat")
            else:
                log(
                    f"[LIVE_EXIT] {sym} reason={reason} side={close_side} "
                    f"exit={exit_px:.8f} dry={dry_side.upper()}"
                )
    except Exception as e:
        log(f"[LIVE_EXIT_FAIL] {sym} reason={reason} {e}")
        try:
            flat = await asyncio.get_running_loop().run_in_executor(
                None, lambda: binance.position_qty(sym) <= 0
            )
            if flat:
                live_slots.pop(sym, None)
                live_stats["exits"] += 1
                log(f"[LIVE_EXIT] {sym} reason={reason} flat_after_fail")
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


def resolve_symbols() -> list[str]:
    manual = _env("LQ_GRABS_SYMBOLS", "")
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
    raise ValueError(f"unsupported LQ_GRABS_WATCHLIST_MODE: {WATCHLIST_MODE!r}")


def sl_tp_prices(side: str, entry: float) -> tuple[float, float]:
    if side == "long":
        return entry * (1 - SL_PCT / 100), entry * (1 + TP_PCT / 100)
    return entry * (1 + SL_PCT / 100), entry * (1 - TP_PCT / 100)


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
                    "symbol", "side", "grab_type", "grab_size", "liq_level",
                    "signal_utc", "entry_utc", "exit_utc",
                    "entry_px", "exit_px", "tp_px", "sl_px",
                    "reason", "net_usd", "hold_bars", "max_fav_pct", "max_adv_pct",
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


def new_grab_state() -> GrabState:
    return GrabState(
        pivot_len=PIVOT_LEN,
        liq_zone_count=LIQ_ZONE_COUNT,
        wbr=WBR,
        cooldown=COOLDOWN,
        tp_pct=TP_PCT,
    )


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
        type_stats.get(sig.grab_type).signals += 1
        if sig.grab_type == "buyside":
            stats["buyside"] += 1
        else:
            stats["sellside"] += 1
        sz_key = f"grab_sz{sig.grab_size}"
        if sz_key in stats:
            stats[sz_key] += 1
    try_open(symbol, feed, sig)


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


def update_mfe_mae(t: ActiveTrade, hi: float, lo: float) -> None:
    ep = t.entry_px
    if t.side == "long":
        t.max_fav_pct = max(t.max_fav_pct, (hi - ep) / ep * 100)
        t.max_adv_pct = max(t.max_adv_pct, (ep - lo) / ep * 100)
    else:
        t.max_fav_pct = max(t.max_fav_pct, (ep - lo) / ep * 100)
        t.max_adv_pct = max(t.max_adv_pct, (hi - ep) / ep * 100)


def accept_signal(sig: GrabSignal) -> bool:
    if SHORTS_ONLY and sig.side != "short":
        return False
    if sig.grab_size < MIN_GRAB_SIZE:
        return False
    return True


def try_open(symbol: str, feed: SymbolFeed, sig: GrabSignal | None) -> None:
    if sig is None:
        return
    key = sig.grab_type
    if feed.trade is not None:
        type_stats.get(key).skipped += 1
        return
    if not accept_signal(sig):
        type_stats.get(key).skipped += 1
        stats["signals_skipped"] += 1
        return
    if MAX_OPEN > 0 and open_count() >= MAX_OPEN:
        type_stats.get(key).skipped += 1
        stats["signals_skipped"] += 1
        log(f"[SIGNAL_SKIP] {symbol} {sig.side} {sig.grab_type} — max_open={MAX_OPEN}")
        return
    sl_px, tp_px = sl_tp_prices(sig.side, sig.entry_px)
    feed.trade = ActiveTrade(
        symbol=symbol,
        side=sig.side,
        grab_type=sig.grab_type,
        grab_size=sig.grab_size,
        liq_level=sig.liq_level,
        signal_ms=sig.ts,
        entry_ms=sig.ts,
        entry_px=sig.entry_px,
        sl_px=sl_px,
        tp_px=tp_px,
    )
    stats["entries"] += 1
    type_stats.get(key).entries += 1
    sym = feed.trade.symbol
    log(
        f"[ENTRY] {sym} {sig.side.upper()} grab={sig.grab_type} sz={sig.grab_size} "
        f"@ {utc_iso(sig.ts)} price={sig.entry_px:.8f} liq={sig.liq_level:.8f} "
        f"SL={sl_px:.8f} ({SL_PCT}%) TP={tp_px:.8f} ({TP_PCT}%) open={open_count()}"
    )
    if LIVE_ENABLED:
        schedule_live_entry(sym, sig.side, sig.entry_px)


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
    type_stats.get(t.grab_type).record_exit(reason, net)
    log(
        f"[EXIT] {t.symbol} {t.side.upper()} grab={t.grab_type} sz={t.grab_size} reason={reason} "
        f"entry={t.entry_px:.8f} exit={exit_px:.8f} net=${net:+.4f} hold={t.bars_held}bars "
        f"fav={t.max_fav_pct:.2f}% adv={t.max_adv_pct:.2f}%"
    )
    with TRADES_CSV.open("a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(
            [
                t.symbol, t.side, t.grab_type, t.grab_size, f"{t.liq_level:.8f}",
                utc_iso(t.signal_ms), utc_iso(t.entry_ms), utc_iso(exit_ms),
                f"{t.entry_px:.8f}", f"{exit_px:.8f}", f"{t.tp_px:.8f}", f"{t.sl_px:.8f}",
                reason, f"{net:.4f}", t.bars_held, f"{t.max_fav_pct:.2f}", f"{t.max_adv_pct:.2f}",
            ]
        )
    sym, side = t.symbol, t.side
    feed.trade = None
    if LIVE_ENABLED:
        schedule_live_exit(sym, side, reason)


def prime_feed(symbol: str) -> int:
    time.sleep(0.1)
    try:
        bars = fetch_klines(symbol, BOOTSTRAP_KLINES)
    except Exception as e:
        log(f"[bootstrap] {symbol} fetch failed: {e}")
        return 0
    feed = feeds[symbol]
    feed.bars = []
    feed.state = new_grab_state()
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
        t.symbol = symbol
        update_mfe_mae(t, hi, lo)
        hit = check_exit(t.side, hi, lo, t.sl_px, t.tp_px)
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
    feed.last_close = cl

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
            f"open={open_count()} buyside={stats['buyside']} sellside={stats['sellside']} "
            f"sz1={stats['grab_sz1']} sz2={stats['grab_sz2']} sz3={stats['grab_sz3']} "
            f"bars_closed={stats['bars_closed']}"
            + (
                f" | live_open={live_open_count()} live_in={live_stats['entries']} "
                f"live_out={live_stats['exits']} live_skip={live_stats['skips']}"
                if LIVE_ENABLED
                else ""
            )
        )
        open_counts: dict[str, int] = {}
        unrealized_by: dict[str, float] = {}
        for sym, feed in feeds.items():
            if not feed.trade:
                continue
            key = feed.trade.grab_type
            open_counts[key] = open_counts.get(key, 0) + 1
            mark = feed.last_close or feed.trade.entry_px
            unrealized_by[key] = unrealized_by.get(key, 0.0) + unrealized_usd(
                feed.trade.side, feed.trade.entry_px, mark, NOTIONAL
            )
        for line in type_stats.format_lines(open_counts, unrealized_by):
            log(f"[stats_by_type]{line}")


async def live_reconcile_loop() -> None:
    while True:
        await asyncio.sleep(max(15, LIVE_RECONCILE_SEC))
        await reconcile_live_slots()


async def main() -> None:
    global SYMBOLS
    init_trades_csv()
    if LIVE_ENABLED:
        init_binance_client()
        if binance is None or not binance.configured():
            raise SystemExit("LQ_GRABS_LIVE_ENABLED=true but Binance API keys missing")
    SYMBOLS = resolve_symbols()
    if not SYMBOLS:
        raise SystemExit("no symbols resolved")
    for sym in SYMBOLS:
        feeds[sym] = SymbolFeed(state=new_grab_state())

    mode = "SHORT only (buyside grab)" if SHORTS_ONLY else "buyside SHORT + sellside LONG"
    log(
        f"liq_grabs_dry | TF={INTERVAL} pivot={PIVOT_LEN} WBR={WBR} cooldown={COOLDOWN} "
        f"zones={LIQ_ZONE_COUNT} | symbols={len(SYMBOLS)} mode={WATCHLIST_MODE} | {mode}"
    )
    log(
        f"  notional=${NOTIONAL} SL={SL_PCT}% TP={TP_PCT}% max_hold={MAX_HOLD_BARS}bars "
        f"min_grab_sz={MIN_GRAB_SIZE} max_open={'unlimited' if MAX_OPEN <= 0 else MAX_OPEN}"
    )
    log(f"  log={LOG_FILE}")
    log(f"  trades={TRADES_CSV}")
    if LIVE_ENABLED:
        log(
            f"  live: ON same side as dry | notional=${LIVE_NOTIONAL} max_open={LIVE_MAX_OPEN} "
            f"min_lev={LIVE_MIN_LEVERAGE}x SL={LIVE_SL_PCT}% TP={LIVE_TP_PCT}% "
            f"working={LIVE_ALGO_WORKING_TYPE} reconcile={LIVE_RECONCILE_SEC}s"
        )
    else:
        log("  live: OFF (LQ_GRABS_LIVE_ENABLED=false)")

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
