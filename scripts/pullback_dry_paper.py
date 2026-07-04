#!/usr/bin/env python3
"""Strong Pullback Signals dry paper bot — 1h Pine parity (trading1.log).

Breakout → arm → limit pullback fill on confirmed 1h close.
Entry at signal candle close (market fill at log time).
HTF 4h EMA50 filter; SL swing+buffer; TP1/2/3 at 1R/2R/3R.
Exits checked on 1h kline hi/lo (intrabar updates).
"""
from __future__ import annotations

import asyncio
import csv
import io
import json
import os
import sys
import time
import urllib.request
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote

import websockets

from pullback_engine import (
    Bar,
    SymbolState,
    check_exit,
    min_start_i,
    on_closed_bar,
)

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))
IST = timezone(timedelta(hours=5, minutes=30))
VISION = "https://data.binance.vision/data/futures/um/daily/klines"


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

WS_ROOT = _env("PULLBACK_DRY_WS_ROOT", "wss://fstream.binance.com").rstrip("/")
FAPI = _env("PULLBACK_DRY_FAPI", "https://fapi.binance.com").rstrip("/")
INTERVAL = _env("PULLBACK_DRY_INTERVAL", "1h")
INTERVAL_H4 = _env("PULLBACK_DRY_H4_INTERVAL", "4h")
BAR_MS = 3_600_000 if INTERVAL == "1h" else 300_000
H4_MS = 4 * 3_600_000
WS_CHUNK = _env_int("PULLBACK_DRY_WS_CHUNK", 40)
WATCHLIST_MODE = _env("PULLBACK_DRY_WATCHLIST_MODE", "all_perps").lower()
WATCHLIST_SIZE = _env_int("PULLBACK_DRY_WATCHLIST_SIZE", 300)

NOTIONAL = _env_float("PULLBACK_DRY_NOTIONAL_USDT", 6.0)
BANKROLL = _env_float("PULLBACK_DRY_BANKROLL_USDT", 100.0)
FEE_RT = _env_float("PULLBACK_DRY_FEE_RT", 0.0008)
SLIPPAGE_BPS = _env_float("PULLBACK_DRY_SLIPPAGE_BPS", 1.0)

BOOTSTRAP_KLINES = _env_int("PULLBACK_DRY_BOOTSTRAP_KLINES", 300)
BOOTSTRAP_H4 = _env_int("PULLBACK_DRY_BOOTSTRAP_H4", 80)
BAR_HISTORY_MAX = _env_int("PULLBACK_DRY_BAR_HISTORY_MAX", 400)
STATS_INTERVAL_SEC = _env_int("PULLBACK_DRY_STATS_INTERVAL_SEC", 1800)
RUN_DAYS = _env_float("PULLBACK_DRY_RUN_DAYS", 7.0)

OUT_DIR = Path(_env("PULLBACK_DRY_OUT_DIR", str(ROOT / "data/aws/pullback_dry")))
OUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = OUT_DIR / "pullback_dry.log"
STATS_FILE = OUT_DIR / "pullback_dry_stats.log"
TRADES_CSV = OUT_DIR / "pullback_dry_trades.csv"

RUN_START = datetime.now(timezone.utc)
RUN_END = RUN_START + timedelta(days=RUN_DAYS)


@dataclass
class SymbolFeed:
    symbol: str
    state: SymbolState = field(default_factory=SymbolState)
    warmed: bool = False
    mintick: float = 0.0
    last_closed_ts: int = 0
    last_close: float = 0.0
    last_h4_closed_ts: int = 0


@dataclass
class Stats:
    signals: int = 0
    blocked: int = 0
    ent: int = 0
    tp1: int = 0
    tp2: int = 0
    tp3: int = 0
    sl: int = 0
    real: float = 0.0


feeds: dict[str, SymbolFeed] = {}
SYMBOLS: list[str] = []
stats = Stats()
meta = {"bars_closed": 0, "warmup_ready": 0, "bars_h4_closed": 0}


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


def resolve_symbols_and_ticks() -> tuple[list[str], dict[str, float]]:
    manual = _env("PULLBACK_DRY_SYMBOLS", "")
    info = _http_json(f"{FAPI}/fapi/v1/exchangeInfo")
    ticks: dict[str, float] = {}
    perps: list[str] = []
    for s in info["symbols"]:
        sym = s["symbol"]
        if not (
            s.get("contractType") == "PERPETUAL"
            and s.get("quoteAsset") == "USDT"
            and s.get("status") == "TRADING"
            and sym.isascii()
        ):
            continue
        perps.append(sym)
        for f in s.get("filters", []):
            if f.get("filterType") == "PRICE_FILTER":
                ticks[sym] = float(f.get("tickSize") or 0) or 0.0
    if manual:
        want = sorted(x.strip().upper() for x in manual.split(",") if x.strip())
        return want, ticks
    perps.sort()
    out = perps[:WATCHLIST_SIZE] if WATCHLIST_SIZE > 0 else perps
    return out, ticks


def net_pnl_r(side: int, entry: float, risk: float, res_r: float) -> float:
    if entry <= 0 or risk <= 0:
        return 0.0
    pct = (risk / entry) * 100 * res_r
    return NOTIONAL * pct / 100 - NOTIONAL * FEE_RT


def unrealized(side: int, entry: float, mark: float) -> float:
    if entry <= 0:
        return 0.0
    move = ((mark - entry) / entry * 100) if side == 1 else ((entry - mark) / entry * 100)
    return NOTIONAL * move / 100


def init_trades_csv() -> None:
    if not TRADES_CSV.is_file():
        with TRADES_CSV.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(
                [
                    "symbol", "side", "entry_utc", "exit_utc",
                    "entry_px", "exit_px", "sl_px", "tp1_px", "tp2_px", "tp3_px",
                    "reason", "res_r", "net_usd",
                ]
            )


def _bar_ms(interval: str) -> int:
    if interval == "1h":
        return 3_600_000
    if interval == "4h":
        return H4_MS
    if interval == "5m":
        return 300_000
    return BAR_MS


def _fetch_klines_fapi(symbol: str, limit: int, interval: str) -> list[Bar]:
    bar_ms = _bar_ms(interval)
    url = f"{FAPI}/fapi/v1/klines?symbol={quote(symbol)}&interval={interval}&limit={limit}"
    rows = _http_json(url, retries=2)
    now_period = (int(time.time() * 1000) // bar_ms) * bar_ms
    out: list[Bar] = []
    for r in rows:
        ts = int(r[0])
        if ts >= now_period:
            continue
        out.append(Bar(ts, float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])))
    return out


def _fetch_day_vision(symbol: str, day, interval: str) -> list[Bar]:
    url = f"{VISION}/{symbol}/{interval}/{symbol}-{interval}-{day.isoformat()}.zip"
    req = urllib.request.Request(url, headers={"User-Agent": "pullback-dry"})
    with urllib.request.urlopen(req, timeout=60) as r:
        data = r.read()
    out: list[Bar] = []
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        name = z.namelist()[0]
        for rec in csv.reader(io.TextIOWrapper(z.open(name), newline="")):
            if not rec or rec[0] == "open_time":
                continue
            out.append(Bar(int(rec[0]), float(rec[1]), float(rec[2]), float(rec[3]), float(rec[4]), float(rec[5])))
    return out


def _fetch_klines_vision(symbol: str, limit: int, interval: str) -> list[Bar]:
    bar_ms = _bar_ms(interval)
    now_period = (int(time.time() * 1000) // bar_ms) * bar_ms
    need_ms = (limit + 1) * bar_ms
    d0 = datetime.fromtimestamp((now_period - need_ms) / 1000, timezone.utc).date()
    d1 = datetime.fromtimestamp(now_period / 1000, timezone.utc).date()
    rows: list[Bar] = []
    d = d0
    while d <= d1:
        try:
            rows.extend(_fetch_day_vision(symbol, d, interval))
        except Exception:
            pass
        d += timedelta(days=1)
    rows = [b for b in rows if b.ts < now_period]
    rows.sort(key=lambda b: b.ts)
    return rows[-limit:]


def fetch_klines(symbol: str, limit: int, interval: str) -> list[Bar]:
    try:
        bars = _fetch_klines_fapi(symbol, limit, interval)
        if bars:
            return bars
    except Exception:
        pass
    return _fetch_klines_vision(symbol, limit, interval)


def trim_bars(st: SymbolState) -> None:
    if len(st.bars) > BAR_HISTORY_MAX:
        drop = len(st.bars) - BAR_HISTORY_MAX
        st.bars = st.bars[drop:]
        if st.arm_i >= 0:
            st.arm_i = max(0, st.arm_i - drop)
        if st.last_sig_i >= 0:
            st.last_sig_i = max(-1, st.last_sig_i - drop)
    if len(st.bars_4h) > 120:
        st.bars_4h = st.bars_4h[-120:]


def close_trade(feed: SymbolFeed, exit_ms: int, exit_px: float, reason: str, res_r: float) -> None:
    t = feed.state.trade
    feed.state.trade = None
    if t is None:
        return
    net = net_pnl_r(t.side, t.entry, t.risk, res_r)
    stats.real += net
    if reason == "tp1":
        stats.tp1 += 1
    elif reason == "tp2":
        stats.tp2 += 1
    elif reason == "tp3":
        stats.tp3 += 1
    elif reason == "sl":
        stats.sl += 1
    side_s = "LONG" if t.side == 1 else "SHORT"
    log(
        f"[EXIT] {feed.symbol} {side_s} reason={reason} res_r={res_r:+.1f} "
        f"entry={t.entry:.8f} exit={exit_px:.8f} net=${net:+.4f}"
    )
    with TRADES_CSV.open("a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(
            [
                feed.symbol, t.side, utc_iso(t.entry_ts), utc_iso(exit_ms),
                f"{t.entry:.8f}", f"{exit_px:.8f}", f"{t.sl:.8f}",
                f"{t.tp1:.8f}", f"{t.tp2:.8f}", f"{t.tp3:.8f}",
                reason, f"{res_r:.2f}", f"{net:.4f}",
            ]
        )


def check_open_exit(feed: SymbolFeed, hi: float, lo: float, ts: int) -> None:
    t = feed.state.trade
    if t is None or ts <= t.entry_ts:
        return
    ex = check_exit(t, hi, lo, ts)
    if ex:
        close_trade(feed, ex.ts, ex.exit_px, ex.reason, ex.res_r)


def replay_closed_bar(symbol: str, feed: SymbolFeed, bar: Bar, *, allow_trade: bool) -> None:
    st = feed.state
    had_trade = st.trade is not None
    sig = on_closed_bar(st, bar, allow_entry=allow_trade and not had_trade)
    if had_trade and sig is None:
        # armed logic may still run; new entry blocked while trade open
        pass
    trim_bars(st)
    if sig:
        stats.signals += 1
        stats.ent += 1
        side_s = "LONG" if sig.side == 1 else "SHORT"
        log(
            f"[ENTRY] {symbol} {side_s} entry={sig.entry:.8f} (signal_close) sl={sig.sl:.8f} "
            f"tp1={sig.tp1:.8f} tp2={sig.tp2:.8f} tp3={sig.tp3:.8f} ts={utc_iso(sig.ts)}"
        )


def bootstrap_symbol(sym: str, feed: SymbolFeed) -> None:
    bars = fetch_klines(sym, BOOTSTRAP_KLINES, INTERVAL)
    h4 = fetch_klines(sym, BOOTSTRAP_H4, INTERVAL_H4)
    if len(bars) < min_start_i() + 5:
        return
    feed.state.bars_4h = h4
    for b in bars:
        on_closed_bar(feed.state, b, allow_entry=False)
    if bars:
        feed.last_closed_ts = bars[-1].ts
        feed.last_close = bars[-1].c
    feed.warmed = len(feed.state.bars) >= min_start_i()
    if feed.warmed:
        meta["warmup_ready"] += 1


def on_kline_1h(symbol: str, k: dict) -> None:
    feed = feeds.get(symbol)
    if feed is None or not feed.warmed:
        return
    hi, lo, cl = float(k["h"]), float(k["l"]), float(k["c"])
    ts = int(k["t"])
    check_open_exit(feed, hi, lo, ts)
    if not k.get("x"):
        return
    if ts == feed.last_closed_ts:
        return
    feed.last_closed_ts = ts
    feed.last_close = cl
    bar = Bar(ts, float(k["o"]), hi, lo, cl, float(k.get("v", 0)))
    replay_closed_bar(symbol, feed, bar, allow_trade=True)
    meta["bars_closed"] += 1


def on_kline_4h(symbol: str, k: dict) -> None:
    feed = feeds.get(symbol)
    if feed is None:
        return
    if not k.get("x"):
        return
    ts = int(k["t"])
    if ts == feed.last_h4_closed_ts:
        return
    feed.last_h4_closed_ts = ts
    bar = Bar(ts, float(k["o"]), float(k["h"]), float(k["l"]), float(k["c"]), float(k.get("v", 0)))
    st = feed.state
    if st.bars_4h and st.bars_4h[-1].ts == ts:
        st.bars_4h[-1] = bar
    else:
        st.bars_4h.append(bar)
    meta["bars_h4_closed"] += 1


def open_count() -> int:
    return sum(1 for f in feeds.values() if f.state.trade is not None)


def format_stats_table() -> str:
    opn = open_count()
    unrl = 0.0
    for f in feeds.values():
        t = f.state.trade
        if t:
            mark = f.last_close or t.entry
            unrl += unrealized(t.side, t.entry, mark)
    real = stats.real
    comb = real + unrl
    wins = stats.tp1 + stats.tp2 + stats.tp3
    losses = stats.sl
    wr = (wins / (wins + losses) * 100) if (wins + losses) else 0.0
    pp = real / BANKROLL * 100 if BANKROLL else 0.0
    cp = comb / BANKROLL * 100 if BANKROLL else 0.0
    now = datetime.now(timezone.utc)
    return "\n".join([
        "Strong Pullback dry — 1h Pine (trading1.log)",
        f"symbols={meta['warmup_ready']}/{len(SYMBOLS)} | notional=${NOTIONAL} bankroll=${BANKROLL}",
        f"TF={INTERVAL} HTF={INTERVAL_H4} EMA50 | entry=signal_close | tradeActive=1/symbol",
        f"run_utc: {RUN_START:%Y-%m-%d %H:%M} → {RUN_END:%Y-%m-%d %H:%M} (now {now:%Y-%m-%d %H:%M})",
        f"run_ist: {RUN_START.astimezone(IST):%Y-%m-%d %H:%M} → {RUN_END.astimezone(IST):%Y-%m-%d %H:%M}",
        "",
        f"signals={stats.signals} blocked={stats.blocked}",
        f"TOTAL  ent={stats.ent}  tp1={stats.tp1} tp2={stats.tp2} tp3={stats.tp3}  sl={stats.sl}  open={opn}  wr={wr:.1f}%",
        f"real=${real:+.2f} ({pp:+.1f}%)  unrl=${unrl:+.2f}  comb=${comb:+.2f} ({cp:+.1f}%)",
        f"bars_closed={meta['bars_closed']} h4_closed={meta['bars_h4_closed']}",
    ])


def emit_stats(label: str = "stats") -> None:
    text = format_stats_table()
    block = f"\n--- {label} {datetime.now(timezone.utc).isoformat()} ---\n{text}\n"
    log(block)
    with STATS_FILE.open("a", encoding="utf-8") as f:
        f.write(block)


async def ws_handler_1h(conn_id: int, symbols: list[str]) -> None:
    streams = "/".join(f"{s.lower()}@kline_{INTERVAL}" for s in symbols)
    url = f"{WS_ROOT}/market/stream?streams={streams}"
    log(f"[ws-1h-{conn_id}] connecting {len(symbols)} symbols")
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, max_size=2**22) as ws:
                log(f"[ws-1h-{conn_id}] connected")
                async for msg in ws:
                    wrap = json.loads(msg)
                    data = wrap.get("data") or wrap
                    if data.get("e") != "kline":
                        continue
                    on_kline_1h(data["s"], data["k"])
        except Exception as e:
            log(f"[ws-1h-{conn_id}] reconnect ({e})")
            await asyncio.sleep(3)


async def ws_handler_4h(conn_id: int, symbols: list[str]) -> None:
    streams = "/".join(f"{s.lower()}@kline_{INTERVAL_H4}" for s in symbols)
    url = f"{WS_ROOT}/market/stream?streams={streams}"
    log(f"[ws-4h-{conn_id}] connecting {len(symbols)} symbols")
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, max_size=2**22) as ws:
                log(f"[ws-4h-{conn_id}] connected")
                async for msg in ws:
                    wrap = json.loads(msg)
                    data = wrap.get("data") or wrap
                    if data.get("e") != "kline":
                        continue
                    on_kline_4h(data["s"], data["k"])
        except Exception as e:
            log(f"[ws-4h-{conn_id}] reconnect ({e})")
            await asyncio.sleep(3)


async def stats_loop() -> None:
    emit_stats("startup")
    while datetime.now(timezone.utc) < RUN_END:
        await asyncio.sleep(STATS_INTERVAL_SEC)
        if datetime.now(timezone.utc) >= RUN_END:
            break
        emit_stats("periodic")
    emit_stats("final")
    log(f"[done] {RUN_DAYS}-day pullback dry run complete — exiting")


def chunk_symbols(symbols: list[str], size: int) -> list[list[str]]:
    return [symbols[i : i + size] for i in range(0, len(symbols), size)]


async def main() -> None:
    global SYMBOLS
    init_trades_csv()
    SYMBOLS, ticks = resolve_symbols_and_ticks()
    if not SYMBOLS:
        raise SystemExit("no symbols resolved")
    for sym in SYMBOLS:
        feeds[sym] = SymbolFeed(symbol=sym, mintick=ticks.get(sym, 0.0))

    log(f"pullback_dry | TF={INTERVAL} HTF={INTERVAL_H4} | symbols={len(SYMBOLS)} mode={WATCHLIST_MODE}")
    log(f"  notional=${NOTIONAL} bankroll=${BANKROLL} fee_rt={FEE_RT} slip={SLIPPAGE_BPS}bps")
    log(f"  stats_every={STATS_INTERVAL_SEC}s run_days={RUN_DAYS}")
    log(f"  log={LOG_FILE} stats={STATS_FILE} trades={TRADES_CSV}")

    for i, sym in enumerate(SYMBOLS):
        try:
            bootstrap_symbol(sym, feeds[sym])
        except Exception as e:
            log(f"[bootstrap] {sym} failed: {e}")
        if (i + 1) % 50 == 0:
            log(f"[bootstrap] {i + 1}/{len(SYMBOLS)} warmed={meta['warmup_ready']}")

    log(f"[bootstrap] done warmed={meta['warmup_ready']}/{len(SYMBOLS)}")

    chunks = chunk_symbols(SYMBOLS, WS_CHUNK)
    tasks: list[asyncio.Task] = []
    for cid, chunk in enumerate(chunks):
        tasks.append(asyncio.create_task(ws_handler_1h(cid, chunk)))
        tasks.append(asyncio.create_task(ws_handler_4h(cid, chunk)))
    tasks.append(asyncio.create_task(stats_loop()))
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
