#!/usr/bin/env python3
"""
Dry paper: Smarter SnR snr_cross_tp15_sl8 — 300-symbol futures scan.

  python3 scripts/smarter_snr_paper.py

Every SNR_DRY_STATS_INTERVAL_SEC (default 30m) logs per-signal stats:
  signal, ent, tp, sl, to, open, real, unrl, comb

Runs for SNR_DRY_RUN_DAYS (default 7) then exits with final snapshot.
"""
from __future__ import annotations

import asyncio
import csv
import json
import os
import sys
import time
import urllib.request
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))

from smarter_snr_lib import Bar, SNRConfig, scan_signals  # noqa: E402
from btc_corr_lib import BtcBucketBook, load_btc_corr_map, open_unrl_by_bucket  # noqa: E402
from paper_stats_lib import unrealized_usd  # noqa: E402

try:
    import websockets
except ImportError:
    print("pip install websockets")
    raise

IST = timezone(timedelta(hours=5, minutes=30))


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

WS_ROOT = _env("SNR_DRY_WS_ROOT", "wss://fstream.binance.com").rstrip("/")
FAPI = _env("SNR_DRY_FAPI", "https://fapi.binance.com").rstrip("/")
INTERVAL = _env("SNR_DRY_INTERVAL", "5m")
BAR_MS = 300_000
WS_CHUNK = _env_int("SNR_DRY_WS_CHUNK", 40)
WATCHLIST_MODE = _env("SNR_DRY_WATCHLIST_MODE", "all_perps").lower()
WATCHLIST_SIZE = _env_int("SNR_DRY_WATCHLIST_SIZE", 300)
NOTIONAL = _env_float("SNR_DRY_NOTIONAL_USDT", 6.0)
BANKROLL = _env_float("SNR_DRY_BANKROLL_USDT", 100.0)
FEE_RT = _env_float("SNR_DRY_FEE_RT", 0.0008)
SL_PCT = _env_float("SNR_DRY_SL_PCT", 8.0)
TP_PCT = _env_float("SNR_DRY_TP_PCT", 1.5)
MAX_HOLD_BARS = _env_int("SNR_DRY_MAX_HOLD_BARS", 288)
BOOTSTRAP_KLINES = _env_int("SNR_DRY_BOOTSTRAP_KLINES", 300)
BAR_HISTORY_MAX = _env_int("SNR_DRY_BAR_HISTORY_MAX", 400)
STATS_INTERVAL_SEC = _env_int("SNR_DRY_STATS_INTERVAL_SEC", 1800)
RUN_DAYS = _env_float("SNR_DRY_RUN_DAYS", 7.0)
WARMUP_BARS = 80
BTC_CORR_LINKED_MIN = _env_float("SNR_DRY_BTC_CORR_LINKED_MIN", 0.60)
BTC_CORR_INDEP_MAX = _env_float("SNR_DRY_BTC_CORR_INDEP_MAX", 0.40)
BTC_CORR_KLINES = _env_int("SNR_DRY_BTC_CORR_KLINES", 500)

SNR_CFG = SNRConfig(signals="snr_cross")

OUT_DIR = Path(_env("SNR_DRY_OUT_DIR", str(ROOT / "data/aws/sr_chartprime/snr_dry")))
OUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = OUT_DIR / "snr_dry.log"
STATS_FILE = OUT_DIR / "snr_dry_stats.log"
TRADES_CSV = OUT_DIR / "snr_dry_trades.csv"

RUN_START = datetime.now(timezone.utc)
RUN_END = RUN_START + timedelta(days=RUN_DAYS)


@dataclass
class TypeStats:
    ent: int = 0
    tp: int = 0
    sl: int = 0
    to: int = 0
    real: float = 0.0


type_stats: dict[str, TypeStats] = defaultdict(TypeStats)
stats_meta = {"bars_closed": 0, "warmup_ready": 0, "signals_seen": 0, "signals_skipped": 0}
btc_bucket_book = BtcBucketBook(BTC_CORR_LINKED_MIN, BTC_CORR_INDEP_MAX)


@dataclass
class ActiveTrade:
    symbol: str
    sig_type: str
    side: str
    entry_ms: int
    entry_px: float
    sl_px: float
    tp_px: float
    bars_held: int = 0


@dataclass
class SymbolFeed:
    bars: list[Bar] = field(default_factory=list)
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
    manual = _env("SNR_DRY_SYMBOLS", "")
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
    raise ValueError(f"unsupported SNR_DRY_WATCHLIST_MODE: {WATCHLIST_MODE!r}")


def sl_tp_prices(side: str, entry: float) -> tuple[float, float]:
    if side == "long":
        return entry * (1 - SL_PCT / 100), entry * (1 + TP_PCT / 100)
    return entry * (1 + SL_PCT / 100), entry * (1 - TP_PCT / 100)


def pnl_usd(side: str, entry: float, exit_px: float) -> float:
    if entry <= 0:
        return 0.0
    g = (exit_px - entry) / entry * 100 if side == "long" else (entry - exit_px) / entry * 100
    return NOTIONAL * g / 100 - NOTIONAL * FEE_RT


def open_by_type() -> dict[str, int]:
    out: dict[str, int] = defaultdict(int)
    for feed in feeds.values():
        if feed.trade:
            out[feed.trade.sig_type] += 1
    return out


def unrealized_by_type() -> dict[str, float]:
    out: dict[str, float] = defaultdict(float)
    for feed in feeds.values():
        t = feed.trade
        if not t:
            continue
        mark = feed.last_close or t.entry_px
        out[t.sig_type] += unrealized_usd(t.side, t.entry_px, mark, NOTIONAL)
    return out


def open_count() -> int:
    return sum(1 for f in feeds.values() if f.trade is not None)


def init_trades_csv() -> None:
    if not TRADES_CSV.is_file():
        with TRADES_CSV.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(
                [
                    "symbol", "signal", "side", "entry_utc", "exit_utc",
                    "entry_px", "exit_px", "tp_px", "sl_px",
                    "reason", "net_usd", "hold_bars",
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


def signals_on_last_bar(bars: list[Bar]) -> list:
    if len(bars) < WARMUP_BARS:
        return []
    i = len(bars) - 1
    sigs = scan_signals(bars, SNR_CFG)
    return [s for s in sigs if s.bar_i == i]


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


def close_trade(feed: SymbolFeed, exit_ms: int, exit_px: float, reason: str) -> None:
    t = feed.trade
    if t is None:
        return
    net = pnl_usd(t.side, t.entry_px, exit_px)
    st = type_stats[t.sig_type]
    st.real += net
    btc_bucket_book.record_exit(t.symbol, reason, net)
    if reason == "tp":
        st.tp += 1
    elif reason == "sl":
        st.sl += 1
    elif reason == "timeout":
        st.to += 1
    log(
        f"[EXIT] {t.symbol} {t.sig_type} {t.side.upper()} reason={reason} "
        f"entry={t.entry_px:.8f} exit={exit_px:.8f} net=${net:+.4f} hold={t.bars_held}bars"
    )
    with TRADES_CSV.open("a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(
            [
                t.symbol, t.sig_type, t.side,
                utc_iso(t.entry_ms), utc_iso(exit_ms),
                f"{t.entry_px:.8f}", f"{exit_px:.8f}", f"{t.tp_px:.8f}", f"{t.sl_px:.8f}",
                reason, f"{net:.4f}", t.bars_held,
            ]
        )
    feed.trade = None


def try_open(symbol: str, feed: SymbolFeed, sig) -> None:
    if feed.trade is not None:
        stats_meta["signals_skipped"] += 1
        return
    key = sig.sig_type
    stats_meta["signals_seen"] += 1
    sl_px, tp_px = sl_tp_prices(sig.side, sig.entry)
    feed.trade = ActiveTrade(
        symbol=symbol,
        sig_type=key,
        side=sig.side,
        entry_ms=sig.ts,
        entry_px=sig.entry,
        sl_px=sl_px,
        tp_px=tp_px,
    )
    type_stats[key].ent += 1
    btc_bucket_book.record_entry(symbol)
    log(
        f"[ENTRY] {symbol} {key} {sig.side.upper()} @ {utc_iso(sig.ts)} "
        f"price={sig.entry:.8f} SL={sl_px:.8f} TP={tp_px:.8f} open={open_count()}"
    )


def replay_bar(symbol: str, feed: SymbolFeed, bar: Bar, allow_trade: bool) -> None:
    if feed.bars and feed.bars[-1].ts == bar.ts:
        return
    feed.bars.append(bar)
    if len(feed.bars) > BAR_HISTORY_MAX:
        feed.bars = feed.bars[-BAR_HISTORY_MAX:]
    if not allow_trade or not feed.warmed:
        return
    for sig in signals_on_last_bar(feed.bars):
        try_open(symbol, feed, sig)
        break


def prime_feed(symbol: str) -> int:
    time.sleep(0.1)
    try:
        bars = fetch_klines(symbol, BOOTSTRAP_KLINES)
    except Exception as e:
        log(f"[bootstrap] {symbol} fetch failed: {e}")
        return 0
    feed = feeds[symbol]
    feed.bars = []
    for bar in bars:
        replay_bar(symbol, feed, bar, allow_trade=False)
    if len(feed.bars) >= WARMUP_BARS:
        feed.warmed = True
        stats_meta["warmup_ready"] += 1
    if feed.bars:
        feed.last_closed_ts = feed.bars[-1].ts
        feed.last_close = feed.bars[-1].c
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
    log(f"[bootstrap] warmed={stats_meta['warmup_ready']}/{len(SYMBOLS)} skipped={skipped}")
    log(f"[bootstrap] loading BTC correlation ({BTC_CORR_KLINES}×{INTERVAL})...")
    loop = asyncio.get_running_loop()
    corr = await loop.run_in_executor(
        None,
        lambda: load_btc_corr_map(SYMBOLS, FAPI, INTERVAL, BTC_CORR_KLINES),
    )
    btc_bucket_book.corr = corr
    n_dep = sum(1 for c in corr.values() if c is not None and c >= BTC_CORR_LINKED_MIN)
    n_indep = sum(1 for c in corr.values() if c is not None and c < BTC_CORR_INDEP_MAX)
    log(f"[bootstrap] btc_dep={n_dep} btc_indep={n_indep}")


def on_kline(symbol: str, k: dict) -> None:
    feed = feeds.get(symbol)
    if feed is None or not feed.warmed:
        return
    hi, lo, cl = float(k["h"]), float(k["l"]), float(k["c"])
    ts = int(k["t"])

    t = feed.trade
    if t:
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
    stats_meta["bars_closed"] += 1


def format_stats_table() -> str:
    open_counts = open_by_type()
    unrl_by = unrealized_by_type()
    keys = sorted(set(type_stats) | set(open_counts) | set(unrl_by))

    ent = sum(s.ent for s in type_stats.values())
    tp = sum(s.tp for s in type_stats.values())
    sl = sum(s.sl for s in type_stats.values())
    to = sum(s.to for s in type_stats.values())
    opn = open_count()
    real = sum(s.real for s in type_stats.values())
    unrl = sum(unrl_by.values())
    comb = real + unrl
    pp = real / BANKROLL * 100 if BANKROLL else 0.0
    cp = comb / BANKROLL * 100 if BANKROLL else 0.0

    now = datetime.now(timezone.utc)
    lines = [
        "Smarter SnR dry — snr_cross_tp15_sl8 signal breakdown",
        f"symbols={stats_meta['warmup_ready']}/{len(SYMBOLS)} | notional=${NOTIONAL} bankroll=${BANKROLL}",
        f"TP={TP_PCT}% SL={SL_PCT}% max_hold={MAX_HOLD_BARS}bars",
        f"run_utc: {RUN_START:%Y-%m-%d %H:%M} → {RUN_END:%Y-%m-%d %H:%M} (now {now:%Y-%m-%d %H:%M})",
        f"run_ist: {RUN_START.astimezone(IST):%Y-%m-%d %H:%M} → {RUN_END.astimezone(IST):%Y-%m-%d %H:%M}",
        "",
        f"TOTAL  ent={ent}  tp={tp}  sl={sl}  to={to}  open={opn}  "
        f"real=${real:+.2f} ({pp:+.1f}%)  unrl=${unrl:+.2f}  comb=${comb:+.2f} ({cp:+.1f}%)",
        "",
        f"{'signal':<10} {'ent':>5} {'tp':>5} {'sl':>5} {'to':>5} {'open':>5} {'real':>8} {'unrl':>8} {'comb':>8}",
        "-" * 72,
    ]

    rows: list[tuple[float, str, TypeStats, int, float, float]] = []
    for key in keys:
        s = type_stats[key]
        o = open_counts.get(key, 0)
        u = unrl_by.get(key, 0.0)
        if s.ent == 0 and o == 0 and abs(s.real) < 1e-9 and abs(u) < 1e-9:
            continue
        rows.append((s.real + u, key, s, o, u, s.real + u))

    rows.sort(key=lambda x: -x[0])
    for _, key, s, o, u, c in rows:
        lines.append(
            f"{key:<10} {s.ent:>5} {s.tp:>5} {s.sl:>5} {s.to:>5} {o:>5} "
            f"{s.real:>+8.2f} {u:>+8.2f} {c:>+8.2f}"
        )

    lines.extend([
        "-" * 72,
        f"{'TOTAL':<10} {ent:>5} {tp:>5} {sl:>5} {to:>5} {opn:>5} "
        f"{real:>+8.2f} {unrl:>+8.2f} {comb:>+8.2f}",
        "",
    ])
    opens: list[tuple[str, str, float, float]] = []
    for sym, feed in feeds.items():
        if not feed.trade:
            continue
        px = feed.last_close or feed.trade.entry_px
        opens.append((sym, feed.trade.side, feed.trade.entry_px, px))
    o_dep, o_indep, u_dep, u_indep = open_unrl_by_bucket(btc_bucket_book, opens, NOTIONAL)
    for line in btc_bucket_book.format_lines(o_dep, o_indep, u_dep, u_indep):
        lines.append(f"[stats_by_btc]{line}")
    lines.extend([
        "",
        "Signals: s_co=support cross up LONG | s_cu=support cross down SHORT",
        "         r_co=resistance cross up LONG | r_cu=resistance cross down SHORT",
    ])
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
    log(f"[done] {RUN_DAYS}-day dry run complete — exiting")


async def main() -> None:
    global SYMBOLS
    init_trades_csv()
    SYMBOLS = resolve_symbols()
    if not SYMBOLS:
        raise SystemExit("no symbols resolved")
    for sym in SYMBOLS:
        feeds[sym] = SymbolFeed()

    log(
        f"snr_dry | snr_cross_tp15_sl8 | TF={INTERVAL} | symbols={len(SYMBOLS)} mode={WATCHLIST_MODE}"
    )
    log(
        f"  notional=${NOTIONAL} bankroll=${BANKROLL} SL={SL_PCT}% TP={TP_PCT}% "
        f"max_hold={MAX_HOLD_BARS}bars stats_every={STATS_INTERVAL_SEC}s run_days={RUN_DAYS}"
    )
    log(f"  log={LOG_FILE}")
    log(f"  stats={STATS_FILE}")
    log(f"  trades={TRADES_CSV}")

    await bootstrap_all()

    chunks = [SYMBOLS[i : i + WS_CHUNK] for i in range(0, len(SYMBOLS), WS_CHUNK)]
    ws_tasks = [asyncio.create_task(ws_handler(i, c)) for i, c in enumerate(chunks)]
    stats_task = asyncio.create_task(stats_loop())
    done, pending = await asyncio.wait([stats_task], return_when=asyncio.FIRST_COMPLETED)
    for t in pending:
        t.cancel()
    for t in ws_tasks:
        t.cancel()
    await asyncio.gather(*ws_tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
