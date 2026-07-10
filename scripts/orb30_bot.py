#!/usr/bin/env python3
"""ORB 30m breakout on top-mover next-days — dry paper + optional live.

Day (05:30 IST = 00:00 UTC):
  1. Scan yesterday top5 gain/loss (7d-clean) → today's watchlist
  2. Build 30m ORB (05:30–06:00 IST)
  3. Breakout long/short, TP 10% / SL 10%, max 3 trades/symbol/day
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
from orb30_engine import (
    ORB_MS,
    Bracket,
    MoverSignal,
    bracket_prices,
    day_start_ms_now,
    mark_price,
    orb_from_5m,
    pnl_pct,
    pnl_usd,
    scan_yesterday_signals,
    utc_today,
)


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

FAPI = _env("ORB30_FAPI", "https://fapi.binance.com").rstrip("/")
OUT_DIR = Path(_env("ORB30_OUT_DIR", str(ROOT / "data/aws/orb30")))
LIVE = _env_bool("ORB30_LIVE_ENABLED", False)
NOTIONAL = _env_float("ORB30_NOTIONAL_USDT", 10.0)
TP_PCT = _env_float("ORB30_TP_PCT", 10.0)
SL_PCT = _env_float("ORB30_SL_PCT", 10.0)
MAX_TRADES_SYM = _env_int("ORB30_MAX_TRADES_PER_SYM", 3)
MAX_OPEN = _env_int("ORB30_MAX_OPEN_POSITIONS", 3)
LOOKBACK = _env_int("ORB30_LOOKBACK_DAYS", 7)
POLL_SEC = _env_float("ORB30_POLL_SEC", 3.0)
SCAN_DELAY_SEC = _env_float("ORB30_SCAN_DELAY_SEC", 120.0)
FEE_RT = _env_float("ORB30_FEE_RT", 0.0008)
LEVERAGE_CAP = _env_int("ORB30_LEVERAGE_CAP", 20)
FLATTEN_RETRIES = _env_int("ORB30_FLATTEN_RETRIES", 4)
MAX_DAILY_LOSS = _env_float("ORB30_MAX_DAILY_LOSS_USDT", 10.0)

LOG_FILE = OUT_DIR / ("orb30_live.log" if LIVE else "orb30_dry.log")
TRADES_CSV = OUT_DIR / ("orb30_live_trades.csv" if LIVE else "orb30_dry_trades.csv")

MODE = "LIVE" if LIVE else "DRY"


@dataclass
class Position:
    sym: str
    side: str
    entry: float
    tp_px: float
    sl_px: float
    qty: float = 0.0
    bucket: str = ""


@dataclass
class SymState:
    sym: str
    bucket: str
    signal_pct: float
    orb_high: float = 0.0
    orb_low: float = 0.0
    day_open: float = 0.0
    orb_locked: bool = False
    trades_today: int = 0
    pos: Position | None = None


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def log(msg: str) -> None:
    line = f"{utc_now()} [{MODE}] {msg}"
    print(line, flush=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with LOG_FILE.open("a") as f:
        f.write(line + "\n")


def append_trade(row: dict) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    new = not TRADES_CSV.is_file()
    with TRADES_CSV.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if new:
            w.writeheader()
        w.writerow(row)


def entry_order_side(side: str) -> str:
    return "BUY" if side == "long" else "SELL"


def close_order_side(side: str) -> str:
    return "SELL" if side == "long" else "BUY"


class Orb30Bot:
    def __init__(self) -> None:
        self.client: BinanceFuturesClient | None = None
        self.trade_date = ""
        self.day_start_ms = 0
        self.watch: dict[str, SymState] = {}
        self.session_pnl = 0.0
        self._lock = asyncio.Lock()
        self._scanned = False

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
            f"start | ${NOTIONAL} tp={TP_PCT}% sl={SL_PCT}% "
            f"max_trades/sym={MAX_TRADES_SYM} max_open={MAX_OPEN} orb=30m"
        )
        while True:
            if self.session_pnl <= -MAX_DAILY_LOSS:
                log(f"[STOP] daily loss ${self.session_pnl:.2f}")
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
            await self._flatten_all("DAY_ROLLOVER")
        self.trade_date = today
        self.day_start_ms = day_start_ms_now()
        self.watch = {}
        self._scanned = False
        log(f"[NEW_DAY] {today} (05:30 IST open)")

    async def _poll_once(self) -> None:
        now_ms = int(time.time() * 1000)
        elapsed = now_ms - self.day_start_ms

        if not self._scanned and elapsed >= int(SCAN_DELAY_SEC * 1000):
            await self._load_watchlist()

        if not self.watch:
            return

        if not self._all_orb_locked() and elapsed >= ORB_MS:
            await self._lock_all_orb()

        if not self._all_orb_locked():
            await self._build_orb_live(elapsed)
            return

        await self._trade_loop()

    async def _load_watchlist(self) -> None:
        loop = asyncio.get_running_loop()
        try:
            sigs: list[MoverSignal] = await loop.run_in_executor(
                None, lambda: scan_yesterday_signals(lookback=LOOKBACK, fapi=FAPI)
            )
        except Exception as e:
            log(f"[SCAN_ERR] {e}")
            return
        self._scanned = True
        for s in sigs:
            if s.trade_date != self.trade_date:
                continue
            self.watch[s.sym] = SymState(s.sym, s.bucket, s.signal_pct)
        log(f"[WATCH] {len(self.watch)} symbols for {self.trade_date}")
        for s in sigs[:8]:
            log(f"  {s.sym} {s.bucket} sig={s.signal_pct:+.1f}%")

    def _all_orb_locked(self) -> bool:
        return bool(self.watch) and all(st.orb_locked for st in self.watch.values())

    async def _build_orb_live(self, elapsed_ms: int) -> None:
        loop = asyncio.get_running_loop()
        for st in self.watch.values():
            if st.orb_locked:
                continue
            try:
                px = await loop.run_in_executor(None, mark_price, st.sym, FAPI)
            except Exception:
                continue
            if st.day_open <= 0:
                st.day_open = px
            if st.orb_high <= 0:
                st.orb_high = px
                st.orb_low = px
            else:
                st.orb_high = max(st.orb_high, px)
                st.orb_low = min(st.orb_low, px)

    async def _lock_all_orb(self) -> None:
        loop = asyncio.get_running_loop()
        for st in self.watch.values():
            if st.orb_locked:
                continue
            try:
                row = await loop.run_in_executor(None, orb_from_5m, st.sym, self.trade_date, FAPI)
                if row:
                    st.day_open, st.orb_high, st.orb_low = row
                elif st.orb_high > 0:
                    pass
                else:
                    continue
                st.orb_locked = True
                log(
                    f"[ORB] {st.sym} hi={st.orb_high:.8g} lo={st.orb_low:.8g} "
                    f"open={st.day_open:.8g}"
                )
            except Exception as e:
                log(f"[ORB_ERR] {st.sym} {e}")

    def _open_count(self) -> int:
        return sum(1 for st in self.watch.values() if st.pos is not None)

    async def _trade_loop(self) -> None:
        for st in self.watch.values():
            if st.pos:
                await self._monitor(st)
            elif st.trades_today < MAX_TRADES_SYM and self._open_count() < MAX_OPEN:
                await self._try_entry(st)

    async def _try_entry(self, st: SymState) -> None:
        loop = asyncio.get_running_loop()
        try:
            px = await loop.run_in_executor(None, mark_price, st.sym, FAPI)
        except Exception:
            return
        side = ""
        entry = 0.0
        if px >= st.orb_high:
            side, entry = "long", st.orb_high
        elif px <= st.orb_low:
            side, entry = "short", st.orb_low
        else:
            return

        br = bracket_prices(side, entry, TP_PCT, SL_PCT)
        if LIVE:
            ok = await self._live_enter(st, side, entry, br)
            if not ok:
                return
        else:
            st.pos = Position(st.sym, side, entry, br.tp_px, br.sl_px, bucket=st.bucket)
        log(
            f"[ENTRY] {st.sym} {side.upper()} @ {entry:.8g} "
            f"tp={br.tp_px:.8g} sl={br.sl_px:.8g} {st.bucket} "
            f"trades={st.trades_today + 1}/{MAX_TRADES_SYM}"
        )

    async def _live_enter(self, st: SymState, side: str, entry: float, br: Bracket) -> bool:
        assert self.client is not None
        async with self._lock:
            if self._open_count() >= MAX_OPEN:
                return False
            loop = asyncio.get_running_loop()
            sym = st.sym
            try:
                if not self.client.symbol_tradable(sym):
                    return False
                lev = await loop.run_in_executor(
                    None, self.client.set_max_leverage, sym, LEVERAGE_CAP
                )
                margin = (NOTIONAL / lev) * 1.1
                if self.client.available_usdt() < margin:
                    log(f"[SKIP] {sym} margin need=${margin:.2f}")
                    return False
                resp = await loop.run_in_executor(
                    None,
                    self.client.market_order_notional,
                    sym,
                    entry_order_side(side),
                    NOTIONAL,
                )
                fill_px, qty = parse_fill(resp)
                row = await loop.run_in_executor(None, self.client.position_row, sym)
                if row:
                    pq = abs(float(row.get("positionAmt") or 0))
                    pe = float(row.get("entryPrice") or 0)
                    if pq > 0:
                        qty = pq
                    if pe > 0:
                        fill_px = pe
                if fill_px <= 0 or qty <= 0:
                    return False
                br2 = bracket_prices(side, fill_px, TP_PCT, SL_PCT)
                st.pos = Position(sym, side, fill_px, br2.tp_px, br2.sl_px, qty, st.bucket)
                return True
            except Exception as e:
                log(f"[ENTRY_ERR] {sym} {e}")
                return False

    async def _monitor(self, st: SymState) -> None:
        pos = st.pos
        if not pos:
            return
        loop = asyncio.get_running_loop()
        try:
            px = await loop.run_in_executor(None, mark_price, pos.sym, FAPI)
        except Exception:
            return

        reason = ""
        if pos.side == "long":
            if px <= pos.sl_px:
                reason = "SL"
            elif px >= pos.tp_px:
                reason = "TP"
        else:
            if px >= pos.sl_px:
                reason = "SL"
            elif px <= pos.tp_px:
                reason = "TP"
        if not reason:
            return
        await self._exit(st, reason, px)

    async def _exit(self, st: SymState, reason: str, mark: float) -> None:
        pos = st.pos
        if not pos:
            return
        exit_px = mark
        if LIVE:
            assert self.client is not None
            loop = asyncio.get_running_loop()
            close = close_order_side(pos.side)
            for _ in range(FLATTEN_RETRIES):
                qty = await loop.run_in_executor(None, self.client.position_qty, pos.sym)
                if qty <= 0:
                    break
                try:
                    await loop.run_in_executor(
                        None, self.client.market_close_qty, pos.sym, close, qty
                    )
                except Exception as e:
                    log(f"[FLATTEN_ERR] {pos.sym} {e}")
                self.client.invalidate_position_cache()
                await asyncio.sleep(0.35)
            exit_px = await loop.run_in_executor(None, self.client.mark_price, pos.sym)

        pnl = pnl_usd(pos.side, pos.entry, exit_px, NOTIONAL, FEE_RT)
        self.session_pnl += pnl
        st.trades_today += 1
        st.pos = None
        append_trade(
            {
                "ts": utc_now(),
                "trade_date": self.trade_date,
                "sym": pos.sym,
                "bucket": pos.bucket,
                "side": pos.side,
                "entry": pos.entry,
                "exit": exit_px,
                "reason": reason,
                "pnl_usd": round(pnl, 4),
                "pnl_pct": round(pnl_pct(pos.side, pos.entry, exit_px), 4),
                "session_pnl": round(self.session_pnl, 4),
            }
        )
        log(
            f"[EXIT] {pos.sym} {reason} @ {exit_px:.8g} "
            f"pnl=${pnl:.4f} session=${self.session_pnl:.4f}"
        )

    async def _flatten_all(self, reason: str) -> None:
        for st in list(self.watch.values()):
            if not st.pos:
                continue
            loop = asyncio.get_running_loop()
            try:
                px = await loop.run_in_executor(None, mark_price, st.pos.sym, FAPI)
            except Exception:
                px = st.pos.entry
            await self._exit(st, reason, px)


async def main() -> None:
    bot = Orb30Bot()
    await bot.run()


if __name__ == "__main__":
    argparse.ArgumentParser(description="ORB 30m top-mover bot (dry/live)").parse_args()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("[STOP] interrupted")
