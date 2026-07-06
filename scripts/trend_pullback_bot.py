#!/usr/bin/env python3
"""
Trend + Pullback bot:
  DRY  = original signal (long/short) tracked on paper
  LIVE = sync-mirror opposite side + TP@signal SL / STOP@signal TP

4H timeframe. Run: python3 scripts/trend_pullback_bot.py
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import sys
import time
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path

import websockets

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))

from live_config_lib import live_notional_usdt, live_max_open
from trend_pullback_engine import (
    Bar,
    Signal,
    MAX_HOLD_BARS,
    dry_check_exit,
    levels,
    mirror_side,
    pnl_usd,
    signal_on_closed_bar,
)
from trend_pullback_live_lib import TrendPullbackMirrorLive

IST = timezone(timedelta(hours=5, minutes=30))
BAR_MS = 4 * 3_600_000


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

FAPI = _env("TPB_FAPI", "https://fapi.binance.com").rstrip("/")
WS_ROOT = _env("TPB_WS_ROOT", "wss://fstream.binance.com").rstrip("/")
OUT_DIR = Path(_env("TPB_DRY_OUT_DIR", str(ROOT / "data/aws/trend_pullback")))
OUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = OUT_DIR / "trend_pullback.log"
STATS_FILE = OUT_DIR / "trend_pullback_stats.log"
TRADES_CSV = OUT_DIR / "trend_pullback_dry_trades.csv"
STATE_FILE = OUT_DIR / "trend_pullback_open.json"

WATCHLIST_MODE = _env("TPB_WATCHLIST_MODE", "all_perps").lower()
WATCHLIST_SIZE = _env_int("TPB_WATCHLIST_SIZE", 300)
WS_CHUNK = _env_int("TPB_WS_CHUNK", 40)
WATCHLIST: list[str] = []
WATCHLIST_SET: set[str] = set()
NOTIONAL = _env_float("TPB_DRY_NOTIONAL_USDT", live_notional_usdt(6.0))
FEE_RT = _env_float("TPB_FEE_RT", 0.0008)
SLIP_BPS = _env_float("TPB_SLIP_BPS", 1.0)
MAX_TRADES_DAY = _env_int("TPB_MAX_TRADES_DAY", 5)
BOOTSTRAP = _env_int("TPB_BOOTSTRAP_BARS", 250)
LIVE_ENABLED = _env_bool("TPB_LIVE_ENABLED", False)
POLL_SEC = _env_int("TPB_MARK_POLL_SEC", 45)
STATS_INTERVAL_SEC = _env_int("TPB_STATS_INTERVAL_SEC", 1800)
BANKROLL = _env_float("TPB_BANKROLL_USDT", 100.0)

RUN_START = datetime.now(timezone.utc)

live_trader: TrendPullbackMirrorLive | None = None
bars: dict[str, list[Bar]] = {}
last_closed_ts: dict[str, int] = {}
day_trades: dict[str, int] = {}
stats = {
    "signals": 0,
    "skipped": 0,
    "dry_entries": 0,
    "dry_exits": 0,
    "dry_pnl": 0.0,
    "tp": 0,
    "sl": 0,
    "timeout": 0,
    "bars_closed": 0,
}


@dataclass
class DryPos:
    sym: str
    side: str
    signal_side: str
    entry_day: str
    entry_px: float
    sl: float
    tp: float
    entry_ts: int
    hold_deadline_ms: int = 0


open_dry: dict[str, DryPos] = {}


def log(msg: str) -> None:
    line = f"{datetime.now(timezone.utc).isoformat()} {msg}"
    print(line, flush=True)
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def today_ist() -> str:
    return datetime.now(timezone.utc).astimezone(IST).strftime("%Y-%m-%d")


def _http_json(url: str, retries: int = 3) -> object:
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "tpb"})
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read())
        except Exception as e:
            last_err = e
            time.sleep(min(2 ** attempt, 8) + 0.2)
    if last_err is not None:
        raise last_err
    raise RuntimeError("http failed")


def resolve_symbols() -> list[str]:
    manual = _env("TPB_SYMBOLS", "") or _env("TPB_WATCHLIST", "")
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
    raise ValueError(f"unsupported TPB_WATCHLIST_MODE: {WATCHLIST_MODE!r}")


def fetch_klines(sym: str, limit: int = 250) -> list[Bar]:
    url = f"{FAPI}/fapi/v1/klines?symbol={sym}&interval=4h&limit={limit}"
    req = urllib.request.Request(url, headers={"User-Agent": "tpb"})
    with urllib.request.urlopen(req, timeout=60) as r:
        data = json.loads(r.read())
    return [Bar(int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4])) for k in data]


def fetch_mark(sym: str) -> float:
    url = f"{FAPI}/fapi/v1/ticker/price?symbol={sym}"
    req = urllib.request.Request(url, headers={"User-Agent": "tpb"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return float(json.loads(r.read())["price"])


def save_state() -> None:
    STATE_FILE.write_text(json.dumps([asdict(p) for p in open_dry.values()], indent=2))


def load_state() -> None:
    global open_dry
    if not STATE_FILE.is_file():
        return
    try:
        rows = json.loads(STATE_FILE.read_text())
        open_dry = {r["sym"]: DryPos(**r) for r in rows}
        for pos in open_dry.values():
            if pos.hold_deadline_ms <= 0:
                pos.hold_deadline_ms = pos.entry_ts + (MAX_HOLD_BARS + 1) * BAR_MS
    except Exception as e:
        log(f"[STATE] load failed: {e}")


def init_trades_csv() -> None:
    if not TRADES_CSV.is_file():
        with TRADES_CSV.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                "entry_day", "exit_day", "symbol", "signal_side", "dry_side",
                "entry_px", "exit_px", "sl", "tp", "pnl_usd", "exit_reason", "live_mirror",
            ])


def load_realized_from_csv() -> None:
    """Restore realized PnL and exit counts after restart."""
    if not TRADES_CSV.is_file():
        return
    try:
        with TRADES_CSV.open(newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                pnl = float(row.get("pnl_usd") or 0)
                stats["dry_pnl"] += pnl
                stats["dry_exits"] += 1
                reason = (row.get("exit_reason") or "").lower()
                if reason == "tp2r":
                    stats["tp"] += 1
                elif reason == "sl":
                    stats["sl"] += 1
                elif reason == "timeout":
                    stats["timeout"] += 1
    except Exception as e:
        log(f"[STATS] csv reload failed: {e}")


def unrealized(side: str, entry: float, mark: float) -> float:
    if entry <= 0:
        return 0.0
    move = ((mark - entry) / entry * 100) if side == "long" else ((entry - mark) / entry * 100)
    return NOTIONAL * move / 100


def format_stats_table() -> str:
    unrl = 0.0
    for sym, pos in open_dry.items():
        bl = bars.get(sym, [])
        mark = bl[-1].c if bl else pos.entry_px
        unrl += unrealized(pos.side, pos.entry_px, mark)
    real = stats["dry_pnl"]
    comb = real + unrl
    closed = stats["tp"] + stats["sl"] + stats["timeout"]
    wr = (stats["tp"] / closed * 100) if closed else 0.0
    pp = real / BANKROLL * 100 if BANKROLL else 0.0
    cp = comb / BANKROLL * 100 if BANKROLL else 0.0
    now = datetime.now(timezone.utc)
    day = today_ist()
    return "\n".join([
        "Trend + Pullback dry — 4H 200/21 EMA + RSI (original side)",
        f"symbols={len(WATCHLIST)} mode={WATCHLIST_MODE} | notional=${NOTIONAL} bankroll=${BANKROLL}",
        f"max/day={MAX_TRADES_DAY} fee_rt={FEE_RT} slip_bps={SLIP_BPS} live={'on' if LIVE_ENABLED else 'off'}",
        f"started_utc: {RUN_START:%Y-%m-%d %H:%M} (now {now:%Y-%m-%d %H:%M})",
        f"started_ist: {RUN_START.astimezone(IST):%Y-%m-%d %H:%M} (now {now.astimezone(IST):%Y-%m-%d %H:%M})",
        f"ist_day={day} entries_today={day_trades.get(day, 0)}",
        "",
        f"signals={stats['signals']} skipped={stats['skipped']} bars_closed={stats['bars_closed']}",
        f"TOTAL  ent={stats['dry_entries']}  tp={stats['tp']}  sl={stats['sl']}  "
        f"to={stats['timeout']}  open={len(open_dry)}  wr={wr:.1f}%",
        f"real=${real:+.2f} ({pp:+.1f}%)  unrl=${unrl:+.2f}  comb=${comb:+.2f} ({cp:+.1f}%)",
    ])


def emit_stats(label: str = "stats") -> None:
    text = format_stats_table()
    block = f"\n--- {label} {datetime.now(timezone.utc).isoformat()} ---\n{text}\n"
    log(block)
    with STATS_FILE.open("a", encoding="utf-8") as f:
        f.write(block)


def bootstrap() -> None:
    for sym in WATCHLIST:
        bars[sym] = fetch_klines(sym, BOOTSTRAP)
        if bars[sym]:
            # REST last kline is the forming candle — mark last fully closed bar
            last_closed_ts[sym] = bars[sym][-2].ts if len(bars[sym]) >= 2 else bars[sym][-1].ts
        time.sleep(0.05)
    log(f"[BOOT] {len(WATCHLIST)} symbols × {BOOTSTRAP} 4h bars")


def on_signal(sig: Signal) -> None:
    global stats
    day = today_ist()
    if day_trades.get(day, 0) >= MAX_TRADES_DAY:
        stats["skipped"] += 1
        log(f"[SKIP] max_trades/day={MAX_TRADES_DAY}")
        return
    if sig.sym in open_dry:
        stats["skipped"] += 1
        log(f"[SKIP] {sig.sym} dry already open")
        return

    stats["signals"] += 1
    day_trades[day] = day_trades.get(day, 0) + 1

    slip = SLIP_BPS / 10000.0
    ent = sig.entry * (1 + slip) if sig.signal_side == "long" else sig.entry * (1 - slip)

    open_dry[sig.sym] = DryPos(
        sym=sig.sym,
        side=sig.signal_side,
        signal_side=sig.signal_side,
        entry_day=day,
        entry_px=ent,
        sl=sig.sl,
        tp=sig.tp,
        entry_ts=sig.bar_ts,
        hold_deadline_ms=int(time.time() * 1000) + MAX_HOLD_BARS * BAR_MS,
    )
    stats["dry_entries"] += 1
    save_state()

    ms = mirror_side(sig.signal_side)
    log(
        f"[DRY_ENTRY] {sig.sym} ORIG={sig.signal_side.upper()} @ {ent:.6f} "
        f"SL={sig.sl:.6f} TP={sig.tp:.6f} | LIVE_MIRROR→{ms.upper()}"
    )
    if live_trader and live_trader.enabled:
        live_trader.schedule_mirror(sig)


def close_dry(sym: str, exit_px: float, reason: str) -> None:
    global stats
    pos = open_dry.pop(sym, None)
    if not pos:
        return
    pnl = pnl_usd(pos.side, pos.entry_px, exit_px, NOTIONAL, FEE_RT, SLIP_BPS)
    stats["dry_exits"] += 1
    stats["dry_pnl"] += pnl
    if reason == "tp2r":
        stats["tp"] += 1
    elif reason == "sl":
        stats["sl"] += 1
    elif reason == "timeout":
        stats["timeout"] += 1
    save_state()
    with TRADES_CSV.open("a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([
            pos.entry_day, today_ist(), sym, pos.signal_side, pos.side,
            f"{pos.entry_px:.8f}", f"{exit_px:.8f}", f"{pos.sl:.8f}", f"{pos.tp:.8f}",
            f"{pnl:.4f}", reason, "yes" if LIVE_ENABLED else "no",
        ])
    log(f"[DRY_EXIT] {sym} {pos.side.upper()} {reason} exit={exit_px:.6f} pnl=${pnl:+.3f} total=${stats['dry_pnl']:+.2f}")


def check_bar_close(sym: str) -> None:
    b = bars.get(sym, [])
    if len(b) < 220:
        return
    i = len(b) - 1
    if b[i].ts == last_closed_ts.get(sym):
        return
    last_closed_ts[sym] = b[i].ts
    stats["bars_closed"] += 1

    if sym in open_dry:
        pos = open_dry[sym]
        if int(time.time() * 1000) >= pos.hold_deadline_ms:
            try:
                px = fetch_mark(sym)
            except Exception:
                px = b[i].c
            close_dry(sym, px, "timeout")

    sig = signal_on_closed_bar(sym, b, i)
    if not sig:
        return
    try:
        sig.entry = fetch_mark(sym)
    except Exception:
        pass
    lv = levels(sig.signal_side, sig.entry, sig.pull_low, sig.pull_high, sig.atr_val)
    if lv:
        sig.sl, sig.tp, sig.risk = lv
    on_signal(sig)


def apply_kline(sym: str, k: dict) -> None:
    ts = int(k["t"])
    bar = Bar(ts, float(k["o"]), float(k["h"]), float(k["l"]), float(k["c"]))
    bl = bars.setdefault(sym, [])
    if bl and bl[-1].ts == ts:
        bl[-1] = bar
    else:
        bl.append(bar)
        if len(bl) > 400:
            del bl[:-400]
    if k.get("x"):
        check_bar_close(sym)


async def ws_handler(conn_id: int, symbols: list[str]) -> None:
    streams = "/".join(f"{s.lower()}@kline_4h" for s in symbols)
    url = f"{WS_ROOT}/market/stream?streams={streams}"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=60, max_size=2**22) as ws:
                log(f"[WS-{conn_id}] connected {len(symbols)} streams 4h")
                async for raw in ws:
                    msg = json.loads(raw)
                    data = msg.get("data", msg)
                    k = data.get("k")
                    if not k:
                        continue
                    sym = data.get("s", k.get("s", "")).upper()
                    if sym in WATCHLIST_SET:
                        apply_kline(sym, k)
        except Exception as e:
            log(f"[WS-{conn_id}] reconnect after {e}")
            await asyncio.sleep(5)


async def mark_poll_loop() -> None:
    """Dry original exits via mark price (sync path for SL/TP)."""
    while True:
        await asyncio.sleep(max(10, POLL_SEC))
        for sym, pos in list(open_dry.items()):
            try:
                px = await asyncio.get_running_loop().run_in_executor(None, fetch_mark, sym)
            except Exception as e:
                log(f"[POLL] {sym} mark err {e}")
                continue
            hit = dry_check_exit(pos.side, pos.sl, pos.tp, px, px)
            if hit:
                close_dry(sym, hit[0], hit[1])


async def stats_loop() -> None:
    emit_stats("startup")
    while True:
        await asyncio.sleep(STATS_INTERVAL_SEC)
        emit_stats("periodic")


async def scan_last_closed() -> None:
    """Evaluate the latest fully closed 4h bar once (REST bootstrap path)."""
    n = 0
    for sym in WATCHLIST:
        b = bars.get(sym, [])
        if len(b) < 220:
            continue
        i = len(b) - 2
        sig = signal_on_closed_bar(sym, b, i)
        if sig:
            n += 1
            try:
                sig.entry = fetch_mark(sym)
            except Exception:
                pass
            lv = levels(sig.signal_side, sig.entry, sig.pull_low, sig.pull_high, sig.atr_val)
            if lv:
                sig.sl, sig.tp, sig.risk = lv
            on_signal(sig)
        time.sleep(0.01)
    if n:
        log(f"[SCAN] last closed 4h bar → {n} signal(s)")
    else:
        log("[SCAN] last closed 4h bar → no signals")


async def scan_once() -> None:
    """Scan last closed 4h bar on all symbols (for --once test)."""
    for sym in WATCHLIST:
        bars[sym] = fetch_klines(sym, BOOTSTRAP)
        if len(bars[sym]) < 220:
            continue
        i = len(bars[sym]) - 2
        sig = signal_on_closed_bar(sym, bars[sym], i)
        if sig:
            on_signal(sig)
        time.sleep(0.05)


async def main_loop(run_once: bool = False) -> None:
    global live_trader, WATCHLIST, WATCHLIST_SET
    init_trades_csv()
    load_state()
    load_realized_from_csv()
    WATCHLIST = resolve_symbols()
    if not WATCHLIST:
        raise SystemExit("no symbols resolved")
    WATCHLIST_SET = set(WATCHLIST)
    bootstrap()

    live_trader = TrendPullbackMirrorLive(FAPI, log, enabled=LIVE_ENABLED)
    live_trader.init_client()
    if live_trader.enabled and live_trader.configured():
        await live_trader.bootstrap()
        asyncio.create_task(live_trader.reconcile_loop())
        log("[LIVE] mirror enabled — opposite entry, TP@sig SL, STOP@sig TP")
    elif LIVE_ENABLED:
        log("[LIVE] TPB_LIVE_ENABLED=true but API keys missing — dry only")
    else:
        log("[DRY] live disabled — original paper only")

    log(
        f"[CONFIG] symbols={len(WATCHLIST)} mode={WATCHLIST_MODE} notional=${NOTIONAL} "
        f"max/day={MAX_TRADES_DAY} fee={FEE_RT} open_dry={len(open_dry)} "
        f"stats_every={STATS_INTERVAL_SEC}s"
    )
    log(f"  log={LOG_FILE}")
    log(f"  stats={STATS_FILE}")
    log(f"  trades={TRADES_CSV}")

    if run_once:
        await scan_once()
        emit_stats("once")
        return

    await scan_last_closed()
    asyncio.create_task(mark_poll_loop())
    asyncio.create_task(stats_loop())
    chunks = [WATCHLIST[i : i + WS_CHUNK] for i in range(0, len(WATCHLIST), WS_CHUNK)]
    ws_tasks = [asyncio.create_task(ws_handler(i, c)) for i, c in enumerate(chunks)]
    await asyncio.gather(*ws_tasks)


def main() -> None:
    ap = argparse.ArgumentParser(description="Trend Pullback: dry original + live mirror")
    ap.add_argument("--once", action="store_true", help="scan last 4h bar once and exit")
    args = ap.parse_args()
    asyncio.run(main_loop(run_once=args.once))


if __name__ == "__main__":
    main()
