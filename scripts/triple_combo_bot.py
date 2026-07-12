#!/usr/bin/env python3
"""Triple-combo overnight bot — dry paper + optional live.

Day boundary 05:30 IST (00:00 UTC):
  1. During entry day: build top-mover watchlist (7d-clean).
  2. At day rollover: exit prior overnight holds (held through entry day).
  3. Evaluate completed entry-day daily candle → leg 1/2/3 signal.
  4. Enter at rollover; exit next rollover (~24h hold, matches backtest).

Legs @ $10/trade (flat notional):
  LEG1: entry GREEN → SHORT
  LEG2: TOP5_GAIN + entry RED + close chg vs signal <= -15% → SHORT
  LEG3: TOP5_LOSS + entry RED + close chg vs signal <= -5% → LONG
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))

from binance_futures import BinanceFuturesClient, parse_fill
from live_config_lib import binance_api_key, binance_api_secret
from orb30_engine import mark_price, utc_today
from triple_combo_engine import (
    OvernightSignal,
    build_overnight_signals,
    fetch_day_close,
    pnl_pct,
    pnl_usd,
    scan_watchlist_for_day,
)

from orb30_engine import MoverSignal  # noqa: E402


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

FAPI = _env("TC_FAPI", "https://fapi.binance.com").rstrip("/")
OUT_DIR = Path(_env("TC_OUT_DIR", str(ROOT / "data/aws/triple_combo")))
LIVE = _env_bool("TC_LIVE_ENABLED", False)
NOTIONAL = _env_float("TC_NOTIONAL_USDT", 10.0)
LOOKBACK = _env_int("TC_LOOKBACK_DAYS", 7)
GAIN_FADE_PCT = _env_float("TC_GAIN_FADE_PCT", -15.0)
LOSS_BOUNCE_PCT = _env_float("TC_LOSS_BOUNCE_PCT", -5.0)
MAX_OPEN = _env_int("TC_MAX_OPEN_POSITIONS", 25)
POLL_SEC = _env_float("TC_POLL_SEC", 30.0)
SCAN_DELAY_SEC = _env_float("TC_SCAN_DELAY_SEC", 120.0)
EVAL_DELAY_SEC = _env_float("TC_EVAL_DELAY_SEC", 90.0)
FEE_RT = _env_float("TC_FEE_RT", 0.0008)
LEVERAGE_CAP = _env_int("TC_LEVERAGE_CAP", 20)
FLATTEN_RETRIES = _env_int("TC_FLATTEN_RETRIES", 4)
MAX_DAILY_LOSS = _env_float("TC_MAX_DAILY_LOSS_USDT", 15.0)

LOG_FILE = OUT_DIR / ("triple_combo_live.log" if LIVE else "triple_combo_dry.log")
TRADES_CSV = OUT_DIR / ("triple_combo_live_trades.csv" if LIVE else "triple_combo_dry_trades.csv")
DAILY_CSV = OUT_DIR / ("triple_combo_live_daily.csv" if LIVE else "triple_combo_dry_daily.csv")
SIGNALS_CSV = OUT_DIR / ("triple_combo_live_signals.csv" if LIVE else "triple_combo_dry_signals.csv")

MODE = "LIVE" if LIVE else "DRY"


@dataclass
class Position:
    sym: str
    side: str
    leg: str
    bucket: str
    entry_date: str
    entry: float
    qty: float = 0.0


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def log(msg: str) -> None:
    line = f"{utc_now()} [{MODE}] {msg}"
    print(line, flush=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with LOG_FILE.open("a") as f:
        f.write(line + "\n")


def append_csv(path: Path, row: dict, fieldnames: list[str]) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    new = not path.is_file()
    with path.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if new:
            w.writeheader()
        w.writerow(row)


def entry_order_side(side: str) -> str:
    return "BUY" if side == "long" else "SELL"


def close_order_side(side: str) -> str:
    return "SELL" if side == "long" else "BUY"


class TripleComboBot:
    def __init__(self) -> None:
        self.client: BinanceFuturesClient | None = None
        self.trade_date = ""
        self.watch: dict[str, MoverSignal] = {}
        self.positions: dict[str, Position] = {}
        self.session_pnl = 0.0
        self.day_pnl = 0.0
        self.day_trades = 0
        self.day_wins = 0
        self._scanned = False
        self._eval_done_for: str = ""
        self._lock = asyncio.Lock()

    def init_live(self) -> None:
        if not LIVE:
            return
        key, sec = binance_api_key(), binance_api_secret()
        if not key or not sec:
            raise RuntimeError("BINANCE_API_KEY / BINANCE_API_SECRET missing")
        self.client = BinanceFuturesClient(key, sec, FAPI)
        self.client.warm_cache()

    async def run(self) -> None:
        self.init_live()
        log(
            f"start | ${NOTIONAL}/trade | legs: GREEN→SHORT, "
            f"GAIN fade<={GAIN_FADE_PCT}%, LOSS bounce<={LOSS_BOUNCE_PCT}% "
            f"max_open={MAX_OPEN} lookback={LOOKBACK}d"
        )
        while True:
            if self.session_pnl <= -MAX_DAILY_LOSS:
                log(f"[STOP] session loss ${self.session_pnl:.2f}")
                await asyncio.sleep(120)
                continue
            await self._tick_day()
            await self._poll_once()
            await asyncio.sleep(POLL_SEC)

    async def _tick_day(self) -> None:
        today = utc_today()
        if today == self.trade_date:
            return

        if self.trade_date:
            log(f"[ROLLOVER] {self.trade_date} → {today}")
            await asyncio.sleep(EVAL_DELAY_SEC)
            await self._exit_all("NEXT_DAY_EXIT")
            await self._evaluate_and_enter(self.trade_date)
            self._log_day_end()

        self.trade_date = today
        self.watch = {}
        self._scanned = False
        self._eval_done_for = ""
        self.session_pnl = 0.0
        self.day_pnl = 0.0
        self.day_trades = 0
        self.day_wins = 0
        log(f"[NEW_DAY] {today} (05:30 IST) — building watchlist for entry day")

    async def _poll_once(self) -> None:
        if not self._scanned:
            elapsed = time.time() - _day_start_epoch(self.trade_date)
            if elapsed >= SCAN_DELAY_SEC:
                await self._load_watchlist()

    async def _load_watchlist(self) -> None:
        if not self.trade_date:
            return
        loop = asyncio.get_running_loop()
        try:
            sigs: list[MoverSignal] = await loop.run_in_executor(
                None,
                lambda: scan_watchlist_for_day(
                    self.trade_date, lookback=LOOKBACK, fapi=FAPI
                ),
            )
        except Exception as e:
            log(f"[SCAN_ERR] {e}")
            return
        self._scanned = True
        self.watch = {s.sym: s for s in sigs if s.trade_date == self.trade_date}
        log(f"[WATCH] {len(self.watch)} symbols on entry day {self.trade_date}")
        for s in sigs[:8]:
            log(f"  {s.sym} {s.bucket} signal_day={s.signal_date} sig={s.signal_pct:+.1f}%")

    async def _evaluate_and_enter(self, entry_date: str) -> None:
        if self._eval_done_for == entry_date:
            return
        if not self.watch:
            log(f"[EVAL_SKIP] no watchlist for entry day {entry_date}")
            self._eval_done_for = entry_date
            return

        loop = asyncio.get_running_loop()
        watchlist = list(self.watch.values())
        try:
            signals: list[OvernightSignal] = await loop.run_in_executor(
                None,
                lambda: build_overnight_signals(
                    entry_date,
                    watchlist,
                    gain_fade_pct=GAIN_FADE_PCT,
                    loss_bounce_pct=LOSS_BOUNCE_PCT,
                    fapi=FAPI,
                ),
            )
        except Exception as e:
            log(f"[EVAL_ERR] {entry_date} {e}")
            return

        self._eval_done_for = entry_date
        log(f"[EVAL] entry day {entry_date} → {len(signals)} overnight signals")
        for sig in signals:
            append_csv(
                SIGNALS_CSV,
                {
                    "ts": utc_now(),
                    "entry_date": sig.entry_date,
                    "sym": sig.sym,
                    "bucket": sig.bucket,
                    "leg": sig.leg,
                    "side": sig.side,
                    "signal_close": sig.signal_close,
                    "entry_open": sig.entry_open,
                    "entry_close": sig.entry_close,
                    "entry_chg_pct": round(sig.entry_chg_pct, 4),
                    "entry_candle": sig.entry_candle,
                },
                [
                    "ts", "entry_date", "sym", "bucket", "leg", "side",
                    "signal_close", "entry_open", "entry_close",
                    "entry_chg_pct", "entry_candle",
                ],
            )
            log(
                f"  {sig.leg} {sig.sym} {sig.side.upper()} "
                f"entry={sig.entry_close:.8g} chg={sig.entry_chg_pct:+.2f}% {sig.entry_candle}"
            )

        entered = 0
        for sig in signals:
            if len(self.positions) >= MAX_OPEN:
                log(f"[SKIP] max open {MAX_OPEN} reached")
                break
            if sig.sym in self.positions:
                continue
            ok = await self._enter(sig)
            if ok:
                entered += 1
        log(f"[ENTER] {entered} positions for hold through next UTC day")

    async def _enter(self, sig: OvernightSignal) -> bool:
        loop = asyncio.get_running_loop()
        px = sig.entry_close
        if px <= 0:
            try:
                px = await loop.run_in_executor(None, mark_price, sig.sym, FAPI)
            except Exception as e:
                log(f"[ENTRY_ERR] {sig.sym} price: {e}")
                return False
        if px <= 0:
            return False

        qty = 0.0
        if LIVE:
            assert self.client is not None
            async with self._lock:
                if len(self.positions) >= MAX_OPEN:
                    return False
                try:
                    if not self.client.symbol_tradable(sig.sym):
                        return False
                    lev = await loop.run_in_executor(
                        None, self.client.set_max_leverage, sig.sym, LEVERAGE_CAP
                    )
                    margin = (NOTIONAL / lev) * 1.1
                    if self.client.available_usdt() < margin:
                        log(f"[SKIP] {sig.sym} need margin ${margin:.2f}")
                        return False
                    resp = await loop.run_in_executor(
                        None,
                        self.client.market_order_notional,
                        sig.sym,
                        entry_order_side(sig.side),
                        NOTIONAL,
                    )
                    fill_px, qty = parse_fill(resp)
                    row = await loop.run_in_executor(
                        None, self.client.position_row, sig.sym
                    )
                    if row:
                        pq = abs(float(row.get("positionAmt") or 0))
                        pe = float(row.get("entryPrice") or 0)
                        if pq > 0:
                            qty = pq
                        if pe > 0:
                            fill_px = pe
                    if fill_px <= 0 or qty <= 0:
                        return False
                    px = fill_px
                except Exception as e:
                    log(f"[ENTRY_ERR] {sig.sym} {e}")
                    return False

        self.positions[sig.sym] = Position(
            sig.sym, sig.side, sig.leg, sig.bucket, sig.entry_date, px, qty
        )
        log(
            f"[ENTRY] {sig.sym} {sig.side.upper()} {sig.leg} @ {px:.8g} "
            f"{sig.bucket} entry_day={sig.entry_date}"
        )
        return True

    async def _exit_all(self, reason: str) -> None:
        for sym in list(self.positions.keys()):
            await self._exit(sym, reason)

    async def _exit(self, sym: str, reason: str) -> None:
        pos = self.positions.get(sym)
        if not pos:
            return
        loop = asyncio.get_running_loop()
        exit_px = 0.0
        exit_day = self.trade_date
        if not LIVE and exit_day:
            try:
                close = await loop.run_in_executor(
                    None, fetch_day_close, sym, exit_day, FAPI
                )
                if close and close > 0:
                    exit_px = close
            except Exception:
                pass
        if exit_px <= 0:
            try:
                exit_px = await loop.run_in_executor(None, mark_price, sym, FAPI)
            except Exception:
                exit_px = pos.entry

        if LIVE:
            assert self.client is not None
            close = close_order_side(pos.side)
            for _ in range(FLATTEN_RETRIES):
                qty = await loop.run_in_executor(None, self.client.position_qty, sym)
                if qty <= 0:
                    break
                try:
                    await loop.run_in_executor(
                        None, self.client.market_close_qty, sym, close, qty
                    )
                except Exception as e:
                    log(f"[FLATTEN_ERR] {sym} {e}")
                self.client.invalidate_position_cache()
                await asyncio.sleep(0.35)
            try:
                exit_px = await loop.run_in_executor(None, self.client.mark_price, sym)
            except Exception:
                pass

        pnl = pnl_usd(pos.side, pos.entry, exit_px, NOTIONAL, FEE_RT)
        self.session_pnl += pnl
        self.day_pnl += pnl
        self.day_trades += 1
        if pnl > 0:
            self.day_wins += 1

        append_csv(
            TRADES_CSV,
            {
                "ts": utc_now(),
                "entry_date": pos.entry_date,
                "sym": sym,
                "bucket": pos.bucket,
                "leg": pos.leg,
                "side": pos.side,
                "entry": pos.entry,
                "exit": exit_px,
                "reason": reason,
                "pnl_pct": round(pnl_pct(pos.side, pos.entry, exit_px), 4),
                "pnl_usd": round(pnl, 4),
                "session_pnl": round(self.session_pnl, 4),
            },
            [
                "ts", "entry_date", "sym", "bucket", "leg", "side",
                "entry", "exit", "reason", "pnl_pct", "pnl_usd", "session_pnl",
            ],
        )
        log(
            f"[EXIT] {sym} {reason} {pos.leg} @ {exit_px:.8g} "
            f"pnl=${pnl:.4f} session=${self.session_pnl:.4f}"
        )
        del self.positions[sym]

    def _log_day_end(self) -> None:
        if not self.trade_date:
            return
        log(
            f"[DAY_END] {self.trade_date} | trades={self.day_trades} "
            f"wins={self.day_wins} losses={self.day_trades - self.day_wins} "
            f"pnl=${self.day_pnl:+.4f} open={len(self.positions)}"
        )
        append_csv(
            DAILY_CSV,
            {
                "trade_date": self.trade_date,
                "trades": self.day_trades,
                "wins": self.day_wins,
                "losses": self.day_trades - self.day_wins,
                "pnl_usd": round(self.day_pnl, 4),
                "open_held": len(self.positions),
            },
            ["trade_date", "trades", "wins", "losses", "pnl_usd", "open_held"],
        )


def _day_start_epoch(trade_date: str) -> float:
    if not trade_date:
        return time.time()
    y, m, d = map(int, trade_date.split("-"))
    return datetime(y, m, d, tzinfo=timezone.utc).timestamp()


async def main() -> None:
    bot = TripleComboBot()
    try:
        await bot.run()
    finally:
        if bot.trade_date and (bot.day_trades > 0 or bot.positions):
            bot._log_day_end()


if __name__ == "__main__":
    argparse.ArgumentParser(description="Triple-combo overnight bot (dry/live)").parse_args()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("[STOP] interrupted")
