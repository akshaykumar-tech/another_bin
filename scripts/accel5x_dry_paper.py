#!/usr/bin/env python3
"""5x acceleration dry paper bot — ALL mix (same direction as backtest).

Daily at IST 05:30 (UTC 00:00):
  - Exit yesterday's positions @ market
  - Scan 5x signals across all USDT perps, top 30 by multiplier
  - DRY: enter same direction as signal (backtest parity)
  - LIVE (optional): BTC prev GREEN -> same | BTC prev RED -> mirror
  - No SL/TP on live (market in / market out at day close)

Uses LIVE_NOTIONAL_USDT, LIVE_MAX_OPEN, LIVE_MIN_LEVERAGE,
LIVE_MARGIN_BUFFER, LIVE_RECONCILE_SEC from .env when live enabled.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))

from accel5x_engine import (
    exec_side,
    entry_px,
    exit_px,
    list_all_syms,
    next_utc_midnight_ms,
    pnl_usd,
    scan_universe,
    today_ist,
)
from accel5x_live_lib import Accel5xLiveTrader
from live_config_lib import live_max_open, live_notional_usdt

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


def _env_bool(name: str, default: bool) -> bool:
    v = _env(name, "true" if default else "false").lower()
    return v in ("1", "true", "yes", "on")


load_dotenv()

FAPI = _env("ACCEL5X_FAPI", "https://fapi.binance.com").rstrip("/")
OUT_DIR = Path(_env("ACCEL5X_DRY_OUT_DIR", str(ROOT / "data/aws/accel5x_dry")))
OUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = OUT_DIR / "accel5x_dry.log"
TRADES_CSV = OUT_DIR / "accel5x_dry_trades.csv"
STATE_FILE = OUT_DIR / "accel5x_open.json"

NOTIONAL = live_notional_usdt(_env_float("ACCEL5X_DRY_NOTIONAL_USDT", live_notional_usdt(6.0)))
MAX_TRADES = min(_env_int("ACCEL5X_MAX_TRADES", 30), live_max_open(30))
MIN_MULT = _env_float("ACCEL5X_MIN_MULT", 5.0)
MIN_BASE_PCT = _env_float("ACCEL5X_MIN_BASE_PCT", 0.3)
FEE_RT = _env_float("ACCEL5X_FEE_RT", 0.0008)
SLIP_BPS = _env_float("ACCEL5X_SLIP_BPS", 1.0)
LIVE_ENABLED = _env_bool("ACCEL5X_LIVE_ENABLED", False)
SCAN_SLEEP = _env_float("ACCEL5X_SCAN_SLEEP", 0.006)


@dataclass
class OpenPosition:
    sym: str
    side: str
    signal_side: str
    entry_day: str
    entry_px: float
    mult: float
    base_pct: float
    prev_pct: float
    btc_prev_green: bool | None


stats = {"entries": 0, "exits": 0, "day_pnl": 0.0, "total_pnl": 0.0}
open_positions: dict[str, OpenPosition] = {}
live_trader: Accel5xLiveTrader | None = None


def log(msg: str) -> None:
    line = f"{datetime.now(timezone.utc).isoformat()} {msg}"
    print(line, flush=True)
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def init_trades_csv() -> None:
    if not TRADES_CSV.is_file():
        with TRADES_CSV.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                "entry_day", "exit_day", "symbol", "signal_side", "exec_side",
                "btc_prev", "mult", "base_pct", "prev_pct",
                "entry_px", "exit_px", "pnl_usd", "mode",
            ])


def save_state() -> None:
    STATE_FILE.write_text(json.dumps([asdict(p) for p in open_positions.values()], indent=2))


def load_state() -> None:
    global open_positions
    if not STATE_FILE.is_file():
        return
    try:
        rows = json.loads(STATE_FILE.read_text())
        open_positions = {r["sym"]: OpenPosition(**r) for r in rows}
    except Exception as e:
        log(f"[STATE] load failed: {e}")


def fetch_mark_price(sym: str) -> float:
    import urllib.request
    url = f"{FAPI}/fapi/v1/ticker/price?symbol={sym}"
    req = urllib.request.Request(url, headers={"User-Agent": "accel5x"})
    with urllib.request.urlopen(req, timeout=30) as r:
        data = json.loads(r.read())
    return float(data["price"])


def close_dry_positions(exit_day: str) -> float:
    day_pnl = 0.0
    for sym, pos in list(open_positions.items()):
        try:
            mark = fetch_mark_price(sym)
        except Exception as e:
            log(f"[EXIT_FAIL] {sym} price: {e}")
            continue
        ex = exit_px(mark, pos.side, SLIP_BPS)
        pnl = pnl_usd(pos.side, pos.entry_px, ex, NOTIONAL, FEE_RT)
        day_pnl += pnl
        stats["exits"] += 1
        stats["total_pnl"] += pnl
        btc_tag = "GREEN" if pos.btc_prev_green else ("RED" if pos.btc_prev_green is False else "?")
        with TRADES_CSV.open("a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                pos.entry_day, exit_day, sym, pos.signal_side, pos.side,
                btc_tag, f"{pos.mult:.1f}", f"{pos.base_pct:.2f}", f"{pos.prev_pct:.2f}",
                f"{pos.entry_px:.8f}", f"{ex:.8f}", f"{pnl:.4f}", "dry",
            ])
        log(f"[DRY_EXIT] {sym} {pos.side.upper()} entry={pos.entry_px:.6f} exit={ex:.6f} pnl=${pnl:+.3f}")
        del open_positions[sym]
    save_state()
    return day_pnl


async def close_live_positions() -> None:
    if live_trader and live_trader.enabled:
        await live_trader.exit_all(reason="day_close")


async def run_day_open(entry_day: str) -> None:
    global stats
    log(f"[DAY_OPEN] entry_day={entry_day} IST — closing prior positions")
    day_pnl = close_dry_positions(entry_day)
    await close_live_positions()
    if day_pnl != 0:
        log(f"[DAY_CLOSE_PNL] prior book pnl=${day_pnl:+.2f} total=${stats['total_pnl']:+.2f}")

    log(f"[SCAN] loading universe...")
    syms = list_all_syms(FAPI)
    sigs, btc_green, btc_body = scan_universe(
        syms, entry_day,
        min_mult=MIN_MULT, min_base_pct=MIN_BASE_PCT, max_trades=MAX_TRADES,
        fapi=FAPI, sleep=SCAN_SLEEP,
    )
    btc_tag = "GREEN" if btc_green else ("RED" if btc_green is False else "DOJI")
    body_s = f"{btc_body:+.2f}%" if btc_body is not None else "n/a"
    log(
        f"[SCAN] {len(sigs)} signals | BTC prev {btc_tag} {body_s} "
        f"| dry=same_dir live={'same' if btc_green else 'mirror' if btc_green is False else 'same'}"
    )

    n_ent = 0
    for sig in sigs:
        if len(open_positions) >= MAX_TRADES:
            break
        dry_side = exec_side(sig.signal_side, btc_green, live_mode=False)
        live_side = exec_side(sig.signal_side, btc_green, live_mode=True)
        ref = sig.entry_open if sig.entry_open > 0 else fetch_mark_price(sig.sym)
        ent = entry_px(ref, dry_side, SLIP_BPS)

        open_positions[sig.sym] = OpenPosition(
            sym=sig.sym,
            side=dry_side,
            signal_side=sig.signal_side,
            entry_day=entry_day,
            entry_px=ent,
            mult=sig.mult,
            base_pct=sig.base_pct,
            prev_pct=sig.prev_pct,
            btc_prev_green=btc_green,
        )
        stats["entries"] += 1
        n_ent += 1
        log(
            f"[DRY_ENTRY] {sig.sym} signal={sig.signal_side.upper()} exec={dry_side.upper()} "
            f"{sig.mult:.1f}x base={sig.base_pct:+.1f}% prev={sig.prev_pct:+.1f}% @ {ent:.6f}"
        )
        if live_trader and live_trader.enabled:
            live_trader.schedule_entry(
                sig.sym, live_side, ref,
                tag=f"sig={sig.signal_side} live={live_side} mult={sig.mult:.1f}x",
            )

    save_state()
    log(f"[DAY_OPEN] opened {n_ent} positions | open={len(open_positions)}")


async def main_loop(run_once: bool = False) -> None:
    global live_trader
    init_trades_csv()
    load_state()

    live_trader = Accel5xLiveTrader(FAPI, log, enabled=LIVE_ENABLED)
    live_trader.init_client()
    if live_trader.enabled and live_trader.configured():
        await live_trader.bootstrap()
        asyncio.create_task(live_trader.reconcile_loop())
        log("[LIVE] enabled — BTC green=same, BTC red=mirror, no SL/TP")
    elif LIVE_ENABLED:
        log("[LIVE] ACCEL5X_LIVE_ENABLED=true but API keys missing — dry only")
    else:
        log("[DRY] live disabled — same direction as backtest (ALL mix)")

    log(
        f"[CONFIG] notional=${NOTIONAL} max_trades={MAX_TRADES} min_mult={MIN_MULT}x "
        f"min_base={MIN_BASE_PCT}% fee={FEE_RT} slip={SLIP_BPS}bps"
    )

    if run_once:
        await run_day_open(today_ist())
        return

    while True:
        nxt = next_utc_midnight_ms()
        wait_s = max(1, (nxt - int(time.time() * 1000)) / 1000)
        nxt_ist = datetime.fromtimestamp(nxt / 1000, tz=timezone.utc).astimezone(IST)
        log(f"[SLEEP] next day open {nxt_ist:%Y-%m-%d %H:%M:%S} IST in {wait_s/3600:.1f}h")
        await asyncio.sleep(wait_s)
        await run_day_open(today_ist())


def main() -> None:
    ap = argparse.ArgumentParser(description="5x accel dry paper + optional live")
    ap.add_argument("--once", action="store_true", help="scan and enter now (test)")
    args = ap.parse_args()
    asyncio.run(main_loop(run_once=args.once))


if __name__ == "__main__":
    main()
