#!/usr/bin/env python3
"""IFVG dry paper bot — live WS parity with backtest_ifvg_btc_parity.py.

Runs the exact IFVG (inversion FVG) strategy on 300 perps over Binance
futures WS, bar-by-bar with no lookahead, so dry-run PnL tracks the parity
backtest and real Binance execution. Additive/standalone — does not touch
the SNR dry bot.
"""
from __future__ import annotations

import asyncio
import csv
import io
import json
import os
import time
import urllib.request
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote

import websockets

ROOT = Path(__file__).resolve().parent.parent
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


def _env_bool(name: str, default: bool) -> bool:
    v = _env(name, "true" if default else "false").lower()
    return v in ("1", "true", "yes", "on")


load_dotenv()

WS_ROOT = _env("IFVG_DRY_WS_ROOT", "wss://fstream.binance.com").rstrip("/")
FAPI = _env("IFVG_DRY_FAPI", "https://fapi.binance.com").rstrip("/")
INTERVAL = _env("IFVG_DRY_INTERVAL", "5m")
BAR_MS = 300_000
WS_CHUNK = _env_int("IFVG_DRY_WS_CHUNK", 40)
WATCHLIST_MODE = _env("IFVG_DRY_WATCHLIST_MODE", "all_perps").lower()
WATCHLIST_SIZE = _env_int("IFVG_DRY_WATCHLIST_SIZE", 300)

NOTIONAL = _env_float("IFVG_DRY_NOTIONAL_USDT", 100.0)
BANKROLL = _env_float("IFVG_DRY_BANKROLL_USDT", 100.0)
FEE_RT = _env_float("IFVG_DRY_FEE_RT", 0.0008)
SLIPPAGE_BPS = _env_float("IFVG_DRY_SLIPPAGE_BPS", 1.0)

# IFVG strategy params (match parity backtest defaults / Pine "Balanced").
ATR_LEN = _env_int("IFVG_DRY_ATR_LEN", 14)
SL_ATR_MULT = _env_float("IFVG_DRY_SL_ATR_MULT", 1.5)
TP_RR = _env_float("IFVG_DRY_TP_RR", 3.0)
MAX_HIDDEN_FVG = _env_int("IFVG_DRY_MAX_HIDDEN_FVG", 120)
MAX_FVG_AGE = _env_int("IFVG_DRY_MAX_FVG_AGE", 60)
Q_GAP_ATR = _env_float("IFVG_DRY_Q_GAP_ATR", 0.25)
Q_BODY_RATIO = _env_float("IFVG_DRY_Q_BODY_RATIO", 0.50)
Q_RANGE_ATR = _env_float("IFVG_DRY_Q_RANGE_ATR", 0.60)
INV_BUF_ATR = _env_float("IFVG_DRY_INV_BUF_ATR", 0.05)

BOOTSTRAP_KLINES = _env_int("IFVG_DRY_BOOTSTRAP_KLINES", 300)
BAR_HISTORY_MAX = _env_int("IFVG_DRY_BAR_HISTORY_MAX", 400)
STATS_INTERVAL_SEC = _env_int("IFVG_DRY_STATS_INTERVAL_SEC", 1800)
RUN_DAYS = _env_float("IFVG_DRY_RUN_DAYS", 7.0)
WARMUP_BARS = max(ATR_LEN + 4, 20)

OUT_DIR = Path(_env("IFVG_DRY_OUT_DIR", str(ROOT / "data/aws/sr_chartprime/ifvg_dry")))
OUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = OUT_DIR / "ifvg_dry.log"
STATS_FILE = OUT_DIR / "ifvg_dry_stats.log"
TRADES_CSV = OUT_DIR / "ifvg_dry_trades.csv"

RUN_START = datetime.now(timezone.utc)
RUN_END = RUN_START + timedelta(days=RUN_DAYS)


@dataclass
class Bar:
    ts: int
    o: float
    h: float
    l: float
    c: float
    v: float


@dataclass
class ActiveTrade:
    symbol: str
    side: int  # 1 long, -1 short
    entry_ms: int
    entry_px: float
    sl_px: float
    tp_px: float


@dataclass
class SymbolFeed:
    bars: list[Bar] = field(default_factory=list)
    raw: list[dict] = field(default_factory=list)  # pending (hidden) FVGs
    trade: ActiveTrade | None = None
    warmed: bool = False
    mintick: float = 0.0
    last_closed_ts: int = 0
    last_close: float = 0.0


@dataclass
class Stats:
    signals: int = 0
    blocked: int = 0
    filtered: int = 0
    ent: int = 0
    tp: int = 0
    sl: int = 0
    to: int = 0  # (unused for IFVG; kept for symmetry)
    real: float = 0.0


feeds: dict[str, SymbolFeed] = {}
SYMBOLS: list[str] = []
stats = Stats()
meta = {"bars_closed": 0, "warmup_ready": 0}


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
    manual = _env("IFVG_DRY_SYMBOLS", "")
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


def round_to_tick(px: float, tick: float) -> float:
    if tick <= 0:
        return px
    return round(px / tick) * tick


def atr_last(bars: list[Bar], n: int) -> float | None:
    i = len(bars) - 1
    if i < n:
        return None
    tr = []
    for j in range(i - n + 1, i + 1):
        prev = bars[j - 1].c
        b = bars[j]
        tr.append(max(b.h - b.l, abs(b.h - prev), abs(b.l - prev)))
    return sum(tr) / len(tr)


def quality_pass(gap_atr: float, body_ratio: float, range_atr: float) -> bool:
    return gap_atr >= Q_GAP_ATR and body_ratio >= Q_BODY_RATIO and range_atr >= Q_RANGE_ATR


def net_pnl(side: int, entry: float, exit_px: float) -> float:
    if entry <= 0:
        return 0.0
    move = ((exit_px - entry) / entry * 100) if side == 1 else ((entry - exit_px) / entry * 100)
    return NOTIONAL * move / 100 - NOTIONAL * FEE_RT


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
                    "entry_px", "exit_px", "tp_px", "sl_px",
                    "reason", "net_usd",
                ]
            )


def _fetch_klines_fapi(symbol: str, limit: int) -> list[Bar]:
    url = f"{FAPI}/fapi/v1/klines?symbol={quote(symbol)}&interval={INTERVAL}&limit={limit}"
    rows = _http_json(url, retries=2)
    now_period = (int(time.time() * 1000) // BAR_MS) * BAR_MS
    out: list[Bar] = []
    for r in rows:
        ts = int(r[0])
        if ts >= now_period:
            continue
        out.append(Bar(ts, float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])))
    return out


def _fetch_day_vision(symbol: str, day) -> list[Bar]:
    url = f"{VISION}/{symbol}/{INTERVAL}/{symbol}-{INTERVAL}-{day.isoformat()}.zip"
    req = urllib.request.Request(url, headers={"User-Agent": "ifvg-dry"})
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


def _fetch_klines_vision(symbol: str, limit: int) -> list[Bar]:
    now_period = (int(time.time() * 1000) // BAR_MS) * BAR_MS
    need_ms = (limit + 1) * BAR_MS
    d0 = datetime.fromtimestamp((now_period - need_ms) / 1000, timezone.utc).date()
    d1 = datetime.fromtimestamp(now_period / 1000, timezone.utc).date()
    rows: list[Bar] = []
    d = d0
    while d <= d1:
        try:
            rows.extend(_fetch_day_vision(symbol, d))
        except Exception:
            pass  # a missing day (e.g. today not yet published) is fine
        d += timedelta(days=1)
    rows = [b for b in rows if b.ts < now_period]
    rows.sort(key=lambda b: b.ts)
    return rows[-limit:]


def fetch_klines(symbol: str, limit: int) -> list[Bar]:
    try:
        bars = _fetch_klines_fapi(symbol, limit)
        if bars:
            return bars
    except Exception:
        pass
    return _fetch_klines_vision(symbol, limit)


def close_trade(feed: SymbolFeed, exit_ms: int, exit_px: float, reason: str) -> None:
    t = feed.trade
    if t is None:
        return
    net = net_pnl(t.side, t.entry_px, exit_px)
    stats.real += net
    if reason == "tp":
        stats.tp += 1
    elif reason == "sl":
        stats.sl += 1
    side_s = "LONG" if t.side == 1 else "SHORT"
    log(
        f"[EXIT] {t.symbol} {side_s} reason={reason} entry={t.entry_px:.8f} "
        f"exit={exit_px:.8f} net=${net:+.4f}"
    )
    sym = t.symbol
    feed.trade = None
    with TRADES_CSV.open("a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(
            [
                sym, t.side, utc_iso(t.entry_ms), utc_iso(exit_ms),
                f"{t.entry_px:.8f}", f"{exit_px:.8f}", f"{t.tp_px:.8f}", f"{t.sl_px:.8f}",
                reason, f"{net:.4f}",
            ]
        )


def detect_signal(feed: SymbolFeed) -> tuple[float, float, int, float] | None:
    """Bar-by-bar IFVG detection on the just-closed last bar. Mutates feed.raw.

    Returns (top, bot, direction, safe_atr) for an inversion signal, else None.
    Mirrors run_day() in backtest_ifvg_btc_parity.py exactly.
    """
    bars = feed.bars
    n = len(bars)
    if n < 4:
        return None
    i = n - 1
    b = bars[i]
    mintick = feed.mintick or 1e-8

    # Age & prune pending FVGs.
    for r in feed.raw:
        r["age"] += 1
    feed.raw = [r for r in feed.raw if r["age"] <= MAX_FVG_AGE]

    a = atr_last(bars, ATR_LEN)
    safe_atr = a if (a and a > 0) else mintick
    c_range = max(b.h - b.l, mintick)
    body_ratio = abs(b.c - b.o) / c_range
    range_atr = c_range / safe_atr

    # New FVG formed by 3-bar imbalance (i-2 vs i).
    if b.l > bars[i - 2].h:
        feed.raw.append({
            "top": b.l, "bot": bars[i - 2].h, "dir": 1, "age": 0,
            "gap_atr": (b.l - bars[i - 2].h) / safe_atr,
            "body_ratio": body_ratio, "range_atr": range_atr,
        })
    if b.h < bars[i - 2].l:
        feed.raw.append({
            "top": bars[i - 2].l, "bot": b.h, "dir": -1, "age": 0,
            "gap_atr": (bars[i - 2].l - b.h) / safe_atr,
            "body_ratio": body_ratio, "range_atr": range_atr,
        })
    if len(feed.raw) > MAX_HIDDEN_FVG:
        feed.raw = feed.raw[-MAX_HIDDEN_FVG:]

    # Inversion detection (most recent first, one signal per bar).
    buf = safe_atr * INV_BUF_ATR
    new_sig = None
    for idx in range(len(feed.raw) - 1, -1, -1):
        r = feed.raw[idx]
        bull_inv = r["dir"] == -1 and b.c > r["top"] + buf
        bear_inv = r["dir"] == 1 and b.c < r["bot"] - buf
        if bull_inv or bear_inv:
            if quality_pass(r["gap_atr"], r["body_ratio"], r["range_atr"]):
                new_sig = (r["top"], r["bot"], 1 if bull_inv else -1, safe_atr)
            else:
                stats.filtered += 1
            feed.raw.pop(idx)
            break
    return new_sig


def try_open(symbol: str, feed: SymbolFeed, sig: tuple[float, float, int, float], ts: int) -> None:
    top, bot, direction, safe_atr = sig
    stats.signals += 1
    if feed.trade is not None:
        stats.blocked += 1
        return
    mintick = feed.mintick or 1e-8
    # Broken-boundary entry (Pine default): top for long, bot for short.
    entry = top if direction == 1 else bot
    slip = entry * (SLIPPAGE_BPS / 10000.0)
    entry = entry + slip if direction == 1 else entry - slip
    entry = round_to_tick(entry, mintick)
    risk = safe_atr * SL_ATR_MULT
    sl = entry - risk if direction == 1 else entry + risk
    tp = entry + risk * TP_RR if direction == 1 else entry - risk * TP_RR
    feed.trade = ActiveTrade(
        symbol=symbol,
        side=direction,
        entry_ms=ts,
        entry_px=round_to_tick(entry, mintick),
        sl_px=round_to_tick(sl, mintick),
        tp_px=round_to_tick(tp, mintick),
    )
    stats.ent += 1
    side_s = "LONG" if direction == 1 else "SHORT"
    log(
        f"[ENTRY] {symbol} {side_s} @ {utc_iso(ts)} price={feed.trade.entry_px:.8f} "
        f"SL={feed.trade.sl_px:.8f} TP={feed.trade.tp_px:.8f} open={open_count()}"
    )


def open_count() -> int:
    return sum(1 for f in feeds.values() if f.trade is not None)


def replay_closed_bar(symbol: str, feed: SymbolFeed, bar: Bar, allow_trade: bool) -> None:
    if feed.bars and feed.bars[-1].ts == bar.ts:
        return
    feed.bars.append(bar)
    if len(feed.bars) > BAR_HISTORY_MAX:
        feed.bars = feed.bars[-BAR_HISTORY_MAX:]
    sig = detect_signal(feed)
    if not allow_trade or not feed.warmed or sig is None:
        return
    try_open(symbol, feed, sig, bar.ts)


def prime_feed(symbol: str) -> int:
    time.sleep(0.05)
    try:
        bars = fetch_klines(symbol, BOOTSTRAP_KLINES)
    except Exception as e:
        log(f"[bootstrap] {symbol} fetch failed: {e}")
        return 0
    feed = feeds[symbol]
    feed.bars = []
    feed.raw = []
    for bar in bars:
        replay_closed_bar(symbol, feed, bar, allow_trade=False)
    if len(feed.bars) >= WARMUP_BARS:
        feed.warmed = True
        meta["warmup_ready"] += 1
    if feed.bars:
        feed.last_closed_ts = feed.bars[-1].ts
        feed.last_close = feed.bars[-1].c
    return len(feed.bars)


async def bootstrap_all() -> None:
    global SYMBOLS
    log(f"[bootstrap] fetching {BOOTSTRAP_KLINES}×{INTERVAL} for {len(SYMBOLS)} symbols...")
    sem = asyncio.Semaphore(6)
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
    log(f"[bootstrap] warmed={meta['warmup_ready']}/{len(SYMBOLS)} skipped={skipped}")


def on_kline(symbol: str, k: dict) -> None:
    feed = feeds.get(symbol)
    if feed is None or not feed.warmed:
        return
    hi, lo, cl = float(k["h"]), float(k["l"]), float(k["c"])
    ts = int(k["t"])

    # Intrabar exit check (SL priority if both touched, matching parity backtest).
    t = feed.trade
    if t:
        if t.side == 1:
            hit_sl = lo <= t.sl_px
            hit_tp = hi >= t.tp_px
        else:
            hit_sl = hi >= t.sl_px
            hit_tp = lo <= t.tp_px
        if hit_sl:
            close_trade(feed, ts, t.sl_px, "sl")
        elif hit_tp:
            close_trade(feed, ts, t.tp_px, "tp")

    if not k.get("x"):
        return
    if ts == feed.last_closed_ts:
        return
    feed.last_closed_ts = ts
    feed.last_close = cl
    bar = Bar(ts, float(k["o"]), hi, lo, cl, float(k.get("v", 0)))
    replay_closed_bar(symbol, feed, bar, allow_trade=True)
    meta["bars_closed"] += 1


def format_stats_table() -> str:
    opn = open_count()
    unrl = 0.0
    for f in feeds.values():
        t = f.trade
        if t:
            mark = f.last_close or t.entry_px
            unrl += unrealized(t.side, t.entry_px, mark)
    real = stats.real
    comb = real + unrl
    wr = (stats.tp / (stats.tp + stats.sl) * 100) if (stats.tp + stats.sl) else 0.0
    pp = real / BANKROLL * 100 if BANKROLL else 0.0
    cp = comb / BANKROLL * 100 if BANKROLL else 0.0
    now = datetime.now(timezone.utc)
    lines = [
        "IFVG dry — inversion FVG (Balanced) parity bot",
        f"symbols={meta['warmup_ready']}/{len(SYMBOLS)} | notional=${NOTIONAL} bankroll=${BANKROLL}",
        f"ATR_len={ATR_LEN} SL={SL_ATR_MULT}xATR TP={TP_RR}RR max_fvg_age={MAX_FVG_AGE} slip={SLIPPAGE_BPS}bps",
        f"run_utc: {RUN_START:%Y-%m-%d %H:%M} → {RUN_END:%Y-%m-%d %H:%M} (now {now:%Y-%m-%d %H:%M})",
        f"run_ist: {RUN_START.astimezone(IST):%Y-%m-%d %H:%M} → {RUN_END.astimezone(IST):%Y-%m-%d %H:%M}",
        "",
        f"signals={stats.signals} blocked={stats.blocked} filtered={stats.filtered} bars_closed={meta['bars_closed']}",
        f"TOTAL  ent={stats.ent}  tp={stats.tp}  sl={stats.sl}  open={opn}  wr={wr:.1f}%  "
        f"real=${real:+.2f} ({pp:+.1f}%)  unrl=${unrl:+.2f}  comb=${comb:+.2f} ({cp:+.1f}%)",
    ]
    opens = [
        (sym, "LONG" if f.trade.side == 1 else "SHORT", f.trade.entry_px, f.last_close or f.trade.entry_px)
        for sym, f in feeds.items()
        if f.trade
    ]
    if opens:
        lines.append("")
        lines.append(f"open positions ({len(opens)}):")
        for sym, side, ep, mk in sorted(opens):
            lines.append(f"  {sym:<14} {side:<5} entry={ep:.8f} mark={mk:.8f}")
    return "\n".join(lines)


def emit_stats(label: str = "stats") -> None:
    text = format_stats_table()
    block = f"\n--- {label} {datetime.now(timezone.utc).isoformat()} ---\n{text}\n"
    log(block)
    with STATS_FILE.open("a", encoding="utf-8") as f:
        f.write(block)


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
    emit_stats("startup")
    while datetime.now(timezone.utc) < RUN_END:
        await asyncio.sleep(STATS_INTERVAL_SEC)
        if datetime.now(timezone.utc) >= RUN_END:
            break
        emit_stats("periodic")
    emit_stats("final")
    log(f"[done] {RUN_DAYS}-day IFVG dry run complete — exiting")


async def main() -> None:
    global SYMBOLS
    init_trades_csv()
    SYMBOLS, ticks = resolve_symbols_and_ticks()
    if not SYMBOLS:
        raise SystemExit("no symbols resolved")
    for sym in SYMBOLS:
        feeds[sym] = SymbolFeed(mintick=ticks.get(sym, 0.0))

    log(f"ifvg_dry | inversion FVG | TF={INTERVAL} | symbols={len(SYMBOLS)} mode={WATCHLIST_MODE}")
    log(
        f"  notional=${NOTIONAL} bankroll=${BANKROLL} SL={SL_ATR_MULT}xATR TP={TP_RR}RR "
        f"atr_len={ATR_LEN} max_fvg_age={MAX_FVG_AGE} slip={SLIPPAGE_BPS}bps "
        f"stats_every={STATS_INTERVAL_SEC}s run_days={RUN_DAYS}"
    )
    log(f"  log={LOG_FILE}")
    log(f"  stats={STATS_FILE}")
    log(f"  trades={TRADES_CSV}")

    await bootstrap_all()

    chunks = [SYMBOLS[i : i + WS_CHUNK] for i in range(0, len(SYMBOLS), WS_CHUNK)]
    ws_tasks = [asyncio.create_task(ws_handler(i, c)) for i, c in enumerate(chunks)]
    stats_task = asyncio.create_task(stats_loop())
    _done, pending = await asyncio.wait([stats_task], return_when=asyncio.FIRST_COMPLETED)
    for t in pending:
        t.cancel()
    for t in ws_tasks:
        t.cancel()
    await asyncio.gather(*ws_tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
