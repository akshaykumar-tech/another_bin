#!/usr/bin/env python3
"""5x acceleration dry paper + optional live.

Daily at IST 05:30 (UTC 00:00):
  - Exit yesterday's positions @ market
  - Scan 5x signals, top 30 by multiplier
  - BTC twist (dry + live): prev GREEN=same | prev RED=mirror
  - Entry: HYB5 (default) or immediate open
    HYB5: if price moves adverse% against exec side first -> enter @ trigger;
          else enter @ day open ref after open_after_sec
  - No SL/TP — exit next day @ market
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
    adverse_trigger_px,
    entry_px,
    exec_side,
    exit_px,
    list_all_syms,
    next_utc_midnight_ms,
    pnl_usd,
    scan_universe,
    today_ist,
)
from accel5x_hyb import HybEntryWatcher, PendingHyb
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
HYB_STATE = OUT_DIR / "accel5x_hyb_pending.json"

NOTIONAL = live_notional_usdt(_env_float("ACCEL5X_DRY_NOTIONAL_USDT", live_notional_usdt(6.0)))
MAX_TRADES = min(_env_int("ACCEL5X_MAX_TRADES", 30), live_max_open(30))
MIN_MULT = _env_float("ACCEL5X_MIN_MULT", 5.0)
MIN_BASE_PCT = _env_float("ACCEL5X_MIN_BASE_PCT", 0.3)
FEE_RT = _env_float("ACCEL5X_FEE_RT", 0.0008)
SLIP_BPS = _env_float("ACCEL5X_SLIP_BPS", 1.0)
LIVE_ENABLED = _env_bool("ACCEL5X_LIVE_ENABLED", False)
SCAN_SLEEP = _env_float("ACCEL5X_SCAN_SLEEP", 0.006)
ENTRY_MODE = _env("ACCEL5X_ENTRY_MODE", "hyb5").lower()
HYB_PCT = _env_float("ACCEL5X_HYB_ADVERSE_PCT", 5.0)
HYB_POLL = _env_float("ACCEL5X_HYB_POLL_SEC", 30.0)
HYB_OPEN_AFTER = _env_float("ACCEL5X_HYB_OPEN_AFTER_SEC", 0.0)


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
    entry_tag: str = "OPEN"


stats = {"entries": 0, "exits": 0, "day_pnl": 0.0, "total_pnl": 0.0}
open_positions: dict[str, OpenPosition] = {}
live_trader: Accel5xLiveTrader | None = None
hyb_watcher: HybEntryWatcher | None = None


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
                "entry_px", "exit_px", "pnl_usd", "entry_tag", "mode",
            ])


def save_state() -> None:
    STATE_FILE.write_text(json.dumps([asdict(p) for p in open_positions.values()], indent=2))


def load_state() -> None:
    global open_positions
    if not STATE_FILE.is_file():
        return
    try:
        rows = json.loads(STATE_FILE.read_text())
        open_positions = {}
        for r in rows:
            if "entry_tag" not in r:
                r["entry_tag"] = "OPEN"
            open_positions[r["sym"]] = OpenPosition(**r)
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
                f"{pos.entry_px:.8f}", f"{ex:.8f}", f"{pnl:.4f}", pos.entry_tag, "dry",
            ])
        log(
            f"[DRY_EXIT] {sym} {pos.side.upper()} {pos.entry_tag} "
            f"entry={pos.entry_px:.6f} exit={ex:.6f} pnl=${pnl:+.3f}"
        )
        del open_positions[sym]
    save_state()
    return day_pnl


async def close_live_positions() -> None:
    if live_trader and live_trader.enabled:
        await live_trader.exit_all(reason="day_close")


def _open_count() -> int:
    n = len(open_positions)
    if hyb_watcher:
        n += len(hyb_watcher.pending)
    return n


def record_entry(
    sym: str,
    side: str,
    signal_side: str,
    entry_day: str,
    ent: float,
    mult: float,
    base_pct: float,
    prev_pct: float,
    btc_green: bool | None,
    entry_tag: str,
    ref: float,
) -> bool:
    if len(open_positions) >= MAX_TRADES:
        return False
    if sym in open_positions:
        return False
    open_positions[sym] = OpenPosition(
        sym=sym,
        side=side,
        signal_side=signal_side,
        entry_day=entry_day,
        entry_px=ent,
        mult=mult,
        base_pct=base_pct,
        prev_pct=prev_pct,
        btc_prev_green=btc_green,
        entry_tag=entry_tag,
    )
    stats["entries"] += 1
    save_state()
    log(
        f"[DRY_ENTRY] {sym} signal={signal_side.upper()} exec={side.upper()} "
        f"{entry_tag} {mult:.1f}x base={base_pct:+.1f}% prev={prev_pct:+.1f}% "
        f"ref={ref:.6f} @ {ent:.6f}"
    )
    if live_trader and live_trader.enabled:
        live_trader.schedule_entry(
            sym, side, ent,
            tag=f"sig={signal_side} exec={side} {entry_tag} mult={mult:.1f}x ref={ref:.4f}",
        )
    return True


def on_hyb_fill(p: PendingHyb, ent: float, tag: str) -> None:
    if len(open_positions) >= MAX_TRADES:
        log(f"[HYB_SKIP] {p.sym} max_trades={MAX_TRADES}")
        return
    record_entry(
        p.sym, p.side, p.signal_side, p.entry_day, ent,
        p.mult, p.base_pct, p.prev_pct, p.btc_prev_green, tag, p.ref,
    )


async def run_day_open(entry_day: str) -> None:
    global stats, hyb_watcher
    log(f"[DAY_OPEN] entry_day={entry_day} IST — closing prior positions")
    if hyb_watcher:
        skipped = len(hyb_watcher.pending)
        if skipped:
            log(f"[HYB] cancel {skipped} unfilled pending from prior day")
        await hyb_watcher.stop()
        hyb_watcher.clear()

    day_pnl = close_dry_positions(entry_day)
    await close_live_positions()
    if day_pnl != 0:
        log(f"[DAY_CLOSE_PNL] prior book pnl=${day_pnl:+.2f} total=${stats['total_pnl']:+.2f}")

    log("[SCAN] loading universe...")
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
        f"| twist=green:same red:mirror | entry={ENTRY_MODE}"
    )

    day_start_ms = day_ms_utc(entry_day)
    n_ent = 0
    use_hyb = ENTRY_MODE in ("hyb5", "hyb", "hybrid5", "hybrid")

    if use_hyb:
        hyb_watcher = HybEntryWatcher(
            FAPI,
            adverse_pct=HYB_PCT,
            poll_sec=HYB_POLL,
            open_after_sec=HYB_OPEN_AFTER,
            slip_bps=SLIP_BPS,
            log=log,
            on_fill=on_hyb_fill,
            state_file=HYB_STATE,
        )
        hyb_watcher.clear()

    for sig in sigs:
        if _open_count() >= MAX_TRADES:
            break
        side = exec_side(sig.signal_side, btc_green, live_mode=True)
        ref = sig.entry_open if sig.entry_open > 0 else fetch_mark_price(sig.sym)

        if use_hyb:
            if sig.sym in open_positions:
                continue
            hyb_watcher.add(PendingHyb(
                sym=sig.sym,
                side=side,
                signal_side=sig.signal_side,
                ref=ref,
                entry_day=entry_day,
                mult=sig.mult,
                base_pct=sig.base_pct,
                prev_pct=sig.prev_pct,
                btc_prev_green=btc_green,
                day_start_ms=day_start_ms,
            ))
            n_ent += 1
            log(
                f"[HYB_PENDING] {sig.sym} signal={sig.signal_side.upper()} exec={side.upper()} "
                f"{sig.mult:.1f}x ref={ref:.6f} adv={HYB_PCT}%"
            )
        else:
            ent = entry_px(ref, side, SLIP_BPS)
            if record_entry(
                sig.sym, side, sig.signal_side, entry_day, ent,
                sig.mult, sig.base_pct, sig.prev_pct, btc_green, "OPEN", ref,
            ):
                n_ent += 1

    if use_hyb and hyb_watcher and hyb_watcher.pending:
        hyb_watcher.start()

    log(
        f"[DAY_OPEN] queued/opened {n_ent} | open_pos={len(open_positions)} "
        f"hyb_pending={len(hyb_watcher.pending) if hyb_watcher else 0}"
    )


def day_ms_utc(day: str) -> int:
    y, m, d = map(int, day.split("-"))
    return int(datetime(y, m, d, tzinfo=timezone.utc).timestamp() * 1000)


async def main_loop(run_once: bool = False) -> None:
    global live_trader, hyb_watcher
    init_trades_csv()
    load_state()

    live_trader = Accel5xLiveTrader(FAPI, log, enabled=LIVE_ENABLED)
    live_trader.init_client()
    if live_trader.enabled and live_trader.configured():
        await live_trader.bootstrap()
        asyncio.create_task(live_trader.reconcile_loop())
        log("[LIVE] enabled — BTC twist + HYB5, no SL/TP")
    elif LIVE_ENABLED:
        log("[LIVE] ACCEL5X_LIVE_ENABLED=true but API keys missing — dry only")
    else:
        log("[DRY] live disabled — BTC twist + HYB5 dry only")

    if ENTRY_MODE in ("hyb5", "hyb", "hybrid5", "hybrid") and HYB_STATE.is_file():
        hyb_watcher = HybEntryWatcher(
            FAPI,
            adverse_pct=HYB_PCT,
            poll_sec=HYB_POLL,
            open_after_sec=HYB_OPEN_AFTER,
            slip_bps=SLIP_BPS,
            log=log,
            on_fill=on_hyb_fill,
            state_file=HYB_STATE,
        )
        hyb_watcher.load()
        if hyb_watcher.pending:
            log(f"[HYB] resume {len(hyb_watcher.pending)} pending from state")
            hyb_watcher.start()

    log(
        f"[CONFIG] notional=${NOTIONAL} max_trades={MAX_TRADES} entry={ENTRY_MODE} "
        f"hyb_adv={HYB_PCT}% hyb_poll={HYB_POLL}s open_after={HYB_OPEN_AFTER}s "
        f"min_mult={MIN_MULT}x fee={FEE_RT}"
    )

    if run_once:
        await run_day_open(today_ist())
        if hyb_watcher and hyb_watcher.pending:
            log("[HYB] --once: waiting up to 120s for pending fills...")
            await asyncio.sleep(120)
        return

    while True:
        nxt = next_utc_midnight_ms()
        wait_s = max(1, (nxt - int(time.time() * 1000)) / 1000)
        nxt_ist = datetime.fromtimestamp(nxt / 1000, tz=timezone.utc).astimezone(IST)
        log(f"[SLEEP] next day open {nxt_ist:%Y-%m-%d %H:%M:%S} IST in {wait_s/3600:.1f}h")
        await asyncio.sleep(wait_s)
        await run_day_open(today_ist())


def main() -> None:
    ap = argparse.ArgumentParser(description="5x accel dry paper + optional live (HYB5)")
    ap.add_argument("--once", action="store_true", help="scan and enter now (test)")
    args = ap.parse_args()
    asyncio.run(main_loop(run_once=args.once))


if __name__ == "__main__":
    main()
