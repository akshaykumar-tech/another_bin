#!/usr/bin/env python3
"""
Dry paper: Alma SD SuperTrend + dual_flip_consensus (Alma+STC) on 15m.

  python3 scripts/alma_st_dry_paper.py

Strategies (parallel, SL 3% / TP 8%):
  alma_only           — Alma signal flip only
  dual_flip_consensus — Alma flip OR (Alma trend + STC buy/sell)

Entry: next 15m bar open | Exit: SL/TP on 1s bars

Logs (terminal + file per strategy):
  data/alma_st_dry/<run_ts>/alma_only.log
  data/alma_st_dry/<run_ts>/dual_flip_consensus.log
  + matching *_trades.csv
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

DUAL_MAX_HOLD_SEC = _env_int("ALMA_ST_DUAL_MAX_HOLD_SEC", "", 20 * 60)
SL_HEAVY_ENTRY_PRICE_MAX = _env_float("ALMA_ST_SL_HEAVY_ENTRY_PRICE_MAX", "", 0.01)
SL_HEAVY_STC_VAL_MAX = _env_float("ALMA_ST_SL_HEAVY_STC_VAL_MAX", "", 40.0)

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
    max_hold_sec: int | None = MAX_HOLD_SEC
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
            "filtered_skips": 0,
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


def signal_sl_heavy_mirror(snap: IndicatorSnap) -> str | None:
    # Same base signal family, but long-only side is required for mirror bucket.
    side = signal_dual_flip(snap)
    if side == "long":
        return "long"
    return None


STRATEGIES: list[StrategyRunner] = [
    StrategyRunner(
        "dual_flip_consensus",
        OUT_DIR / "dual_flip_consensus.log",
        OUT_DIR / "dual_flip_consensus_trades.csv",
        signal_dual_flip,
        max_hold_sec=DUAL_MAX_HOLD_SEC,
    ),
    StrategyRunner(
        "sl_heavy_mirror",
        OUT_DIR / "sl_heavy_mirror.log",
        OUT_DIR / "sl_heavy_mirror_trades.csv",
        signal_sl_heavy_mirror,
        max_hold_sec=None,
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


SYMBOLS = resolve_symbols()
if not SYMBOLS:
    raise SystemExit("no symbols resolved")

feeds: dict[str, SymbolFeed] = {}
for sym in SYMBOLS:
    f = SymbolFeed()
    f.strat = {st.name: StratSymState() for st in STRATEGIES}
    feeds[sym] = f


def build_snap(feed: SymbolFeed, bar: OHLC) -> IndicatorSnap | None:
    feed.bars_15m.append(bar)
    if len(feed.bars_15m) < WARMUP_BARS:
        return None

    bars = list(feed.bars_15m)
    closes = [b.c for b in bars]
    direction = compute_supertrend(bars)
    sigs = signal_series(direction)
    i = len(bars) - 1
    prev_sig = sigs[i - 1] if i >= 1 else feed.prev_alma_sig
    cur_sig = sigs[i]
    feed.prev_alma_sig = cur_sig

    stc = compute_stc(closes)
    stc_v = stc[i] if i < len(stc) else float("nan")
    sg = stc_signals(stc, i) if stc_v == stc_v else {
        "buy": False, "sell": False,
    }

    return IndicatorSnap(
        bar_ts=bar.ts,
        bar_close=bar.c,
        alma_long=signal_flip(prev_sig, cur_sig) == "long",
        alma_short=signal_flip(prev_sig, cur_sig) == "short",
        alma_bull=direction[i] < 0,
        alma_bear=direction[i] > 0,
        stc_buy=sg["buy"],
        stc_sell=sg["sell"],
        stc_val=stc_v if stc_v == stc_v else -1.0,
    )


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
    if runner.name == "dual_flip_consensus" and snap:
        extra = (
            f" alma_L={snap.alma_long} alma_S={snap.alma_short} "
            f"bull={snap.alma_bull} stc={snap.stc_val:.1f} stc_buy={snap.stc_buy} stc_sell={snap.stc_sell}"
        )
    runner.log(
        f"[ENTRY] {symbol} {side.upper()} @ {utc_iso(entry_ms)} price={entry_px:.8f} "
        f"SL={sl_px:.8f} ({SL_PCT}%) TP={tp_px:.8f} ({TP_PCT}%) signal@{utc_iso(signal_ms)}{extra}"
    )


def entry_filter_for_runner(
    runner_name: str,
    side: str,
    snap: IndicatorSnap | None,
    entry_px: float,
) -> bool:
    if runner_name != "sl_heavy_mirror":
        return True
    if side.upper() != "LONG":
        return False
    if SL_HEAVY_ENTRY_PRICE_MAX > 0 and entry_px > SL_HEAVY_ENTRY_PRICE_MAX:
        return False
    if SL_HEAVY_STC_VAL_MAX > 0:
        if snap is None:
            return False
        v = snap.stc_val
        # snap.stc_val uses -1.0 sentinel when invalid.
        if v != v or v < 0:
            return False
        if v > SL_HEAVY_STC_VAL_MAX:
            return False
    return True


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
        st = feed.strat[runner.name]
        if st.trade is not None:
            runner.stats["skipped_busy"] += 1
            runner.log(f"[SIGNAL_SKIP] {symbol} {side.upper()} — position open ({st.trade.side})")
            continue
        runner.stats["signals"] += 1
        st.pending_side = side
        st.pending_at_ms = bar.ts + BAR_MS
        st.pending_snap = snap
        extra = ""
        if runner.name == "dual_flip_consensus":
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
            entry_px = bar["open"]
            if entry_filter_for_runner(runner.name, st.pending_side, snap, entry_px):
                open_trade(
                    runner, st, symbol, st.pending_side,
                    st.pending_at_ms - BAR_MS, sec, entry_px, snap,
                )
            else:
                runner.stats["filtered_skips"] += 1
                if snap:
                    runner.log(
                        f"[FILTER_SKIP] {symbol} {st.pending_side.upper()} "
                        f"entry_px={entry_px:.8f} stc_val={snap.stc_val:.1f}"
                    )
                else:
                    runner.log(f"[FILTER_SKIP] {symbol} {st.pending_side.upper()} entry_px={entry_px:.8f}")
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
            elif runner.max_hold_sec is not None and sec - t.entry_ms >= runner.max_hold_sec * 1000:
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
                f"open={open_n} pending={pending_n} busy_skips={s['skipped_busy']} "
                f"filtered_skips={s['filtered_skips']}"
            )
            if agg_stats["trades"] == 0:
                runner.log("[WARN] agg=0 — no websocket ticks received; check network / WS URL")


async def main() -> None:
    chunks = [SYMBOLS[i : i + WS_CHUNK] for i in range(0, len(SYMBOLS), WS_CHUNK)]
    header = (
        f"alma_st_dry | TF=15m SL={SL_PCT}% TP={TP_PCT}% | symbols={len(SYMBOLS)} "
        f"excluded={EXCLUDE} mode={WATCHLIST_MODE} notional=${NOTIONAL} fee_rt={FEE_RT}"
    )
    for runner in STRATEGIES:
        runner.log(header)
        if runner.name == "dual_flip_consensus":
            runner.log(
                "  signal: Alma flip OR (Alma bull+bear + STC buy/sell cross 25/75)"
            )
        else:
            runner.log(
                f"  signal: SL-heavy mirror bucket (dual_flip long-only + entry_px<={SL_HEAVY_ENTRY_PRICE_MAX:g} + stc<={SL_HEAVY_STC_VAL_MAX:g})"
            )
        hold_msg = "none" if runner.max_hold_sec is None else f"{runner.max_hold_sec}s"
        runner.log(f"  warmup={WARMUP_BARS}×15m | max_hold={hold_msg}")
        runner.log(f"  log={runner.log_path}")
        runner.log(f"  trades_csv={runner.trades_csv}")
    for runner in STRATEGIES:
        runner.log(f"  combined_log={COMBINED_LOG}")
    await bootstrap_history()
    await asyncio.gather(
        *[ws_handler(i, c) for i, c in enumerate(chunks)],
        flush_stale_loop(),
        stats_loop(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        for runner in STRATEGIES:
            runner.log("Stopping")
