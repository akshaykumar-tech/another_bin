#!/usr/bin/env python3
"""C60 triple bot — dry paper + optional live.

Strategy (EARLY combine, 1 entry / symbol / UTC day):
  CUM60:  prior 2d |move| > 60% → CONT @ open ±9% pullback (full day)
  L40:    prior 1d |c2c| > 40% → CONT @ open ±9% pullback (full day)
  DOWN15: prior 1d c2c < −15% → SHORT @ open
  Same (sym,day): earliest fill wins (open beats pullback).

Dry:  C60_LIVE_ENABLED=false
Live: C60_LIVE_ENABLED=true + BINANCE_API_KEY/SECRET

Run:
  python3 scripts/c60_triple_bot.py
  python3 scripts/c60_triple_bot.py --backtest 2026-05-01 2026-07-18
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))

from binance_futures import BinanceFuturesClient
from c60_triple_engine import (
    DEFAULT_CUM_THR,
    DEFAULT_DOWN_THR,
    DEFAULT_L40_THR,
    DEFAULT_PB,
    WatchItem,
    attach_open_levels,
    backtest_range,
    build_setups_for_trade_day,
    prefer_live_watch,
    try_fill_on_bar,
)
from live_config_lib import binance_api_key, binance_api_secret
from orb30_engine import (
    bars_5m_day,
    day_ms,
    fetch_daily_range,
    latest_5m_bar,
    list_syms,
    mark_price,
    pnl_pct,
    pnl_usd,
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

FAPI = _env("C60_FAPI", "https://fapi.binance.com").rstrip("/")
OUT_DIR = Path(_env("C60_OUT_DIR", str(ROOT / "data/aws/c60_triple")))
LIVE = _env_bool("C60_LIVE_ENABLED", False)
NOTIONAL = _env_float("C60_NOTIONAL_USDT", 6.0)
CUM_THR = _env_float("C60_CUM_THR", DEFAULT_CUM_THR)
L40_THR = _env_float("C60_L40_THR", DEFAULT_L40_THR)
DOWN_THR = _env_float("C60_DOWN_THR", DEFAULT_DOWN_THR)
PB_PCT = _env_float("C60_PB_PCT", DEFAULT_PB)
MAX_OPEN = _env_int("C60_MAX_OPEN_POSITIONS", 25)
POLL_SEC = _env_float("C60_POLL_SEC", 15.0)
SCAN_DELAY_SEC = _env_float("C60_SCAN_DELAY_SEC", 90.0)
FEE_RT = _env_float("C60_FEE_RT", 0.0008)
LEVERAGE_CAP = _env_int("C60_LEVERAGE_CAP", 20)
FLATTEN_RETRIES = _env_int("C60_FLATTEN_RETRIES", 4)
MAX_DAILY_LOSS = _env_float("C60_MAX_DAILY_LOSS_USDT", 20.0)
DAILY_LOOKBACK = _env_int("C60_DAILY_LOOKBACK_DAYS", 10)

LOG_FILE = OUT_DIR / ("c60_live.log" if LIVE else "c60_dry.log")
TRADES_CSV = OUT_DIR / ("c60_live_trades.csv" if LIVE else "c60_dry_trades.csv")
DAILY_CSV = OUT_DIR / ("c60_live_daily.csv" if LIVE else "c60_dry_daily.csv")
WATCH_CSV = OUT_DIR / ("c60_live_watch.csv" if LIVE else "c60_dry_watch.csv")
MODE = "LIVE" if LIVE else "DRY"


@dataclass
class Position:
    sym: str
    side: str
    leg: str
    entry_date: str
    entry: float
    level: float
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


def _day_start_epoch(d: str) -> float:
    return day_ms(d) / 1000.0


class C60TripleBot:
    def __init__(self) -> None:
        self.client: BinanceFuturesClient | None = None
        self.trade_date = ""
        self.watch: dict[str, WatchItem] = {}
        self.setups: dict = {}
        self.positions: dict[str, Position] = {}
        self.filled_today: set[str] = set()
        self.session_pnl = 0.0
        self.day_pnl = 0.0
        self.day_trades = 0
        self.day_wins = 0
        self._scanned = False
        self._lock = asyncio.Lock()
        self._daily_cache: dict[str, list] = {}

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
            f"start | ${NOTIONAL}/trade | cum>{CUM_THR}% pb{PB_PCT} + "
            f"1d>{L40_THR}% pb{PB_PCT} + down>{DOWN_THR}%@open | "
            f"EARLY | max_open={MAX_OPEN} | EOD"
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
            log(f"[ROLLOVER] {self.trade_date} → {today} — flatten EOD")
            await self._exit_all("EOD")
            self._log_day_end()
        self.trade_date = today
        self.watch = {}
        self.setups = {}
        self.filled_today = set()
        self._scanned = False
        self._daily_cache = {}
        self.session_pnl = 0.0
        self.day_pnl = 0.0
        self.day_trades = 0
        self.day_wins = 0
        log(f"[NEW_DAY] {today} UTC — scan after {SCAN_DELAY_SEC:.0f}s")

    async def _poll_once(self) -> None:
        if not self.trade_date:
            return
        if not self._scanned:
            if time.time() - _day_start_epoch(self.trade_date) >= SCAN_DELAY_SEC:
                await self._load_watch()
            return
        now_ms = int(time.time() * 1000)
        day_end = day_ms(self.trade_date) + 86_400_000 - 60_000
        if now_ms >= day_end and self.positions:
            await self._exit_all("EOD_GUARD")
        await self._scan_fills()

    async def _load_daily_cache(self) -> None:
        loop = asyncio.get_running_loop()
        end = self.trade_date
        start = (
            datetime.strptime(end, "%Y-%m-%d").date() - timedelta(days=DAILY_LOOKBACK)
        ).isoformat()

        def _load():
            out = {}
            for sym in list_syms(FAPI):
                try:
                    bars = fetch_daily_range(sym, start, end, FAPI)
                except Exception:
                    continue
                if bars:
                    out[sym] = bars
            return out

        self._daily_cache = await loop.run_in_executor(None, _load)

    async def _load_watch(self) -> None:
        loop = asyncio.get_running_loop()
        try:
            if not self._daily_cache:
                await self._load_daily_cache()
            setups = build_setups_for_trade_day(
                self.trade_date,
                self._daily_cache,
                cum_thr=CUM_THR,
                l40_thr=L40_THR,
                down_thr=DOWN_THR,
                pb_pct=PB_PCT,
            )
        except Exception as e:
            log(f"[SCAN_ERR] {e}")
            return

        self._scanned = True
        self.setups = setups
        ready: dict[str, WatchItem] = {}
        for sym, setup in setups.items():
            try:
                bars = await loop.run_in_executor(
                    None, lambda s=sym: bars_5m_day(s, self.trade_date, FAPI)
                )
            except Exception:
                continue
            if not bars:
                continue
            w2 = prefer_live_watch(setup.candidates, bars)
            if not w2:
                continue
            ready[sym] = w2
            append_csv(
                WATCH_CSV,
                {
                    "ts": utc_now(),
                    "trade_date": w2.trade_date,
                    "sym": w2.sym,
                    "leg": w2.leg,
                    "direction": w2.direction,
                    "side": w2.side,
                    "move_pct": round(w2.move_pct, 4),
                    "open": w2.open_px,
                    "level": w2.level,
                    "open_entry": w2.open_entry,
                    "n_cands": len(setup.candidates),
                },
                [
                    "ts", "trade_date", "sym", "leg", "direction", "side",
                    "move_pct", "open", "level", "open_entry", "n_cands",
                ],
            )

        self.watch = ready
        n_c = sum(1 for w in ready.values() if w.leg == "CUM60")
        n_l = sum(1 for w in ready.values() if w.leg == "L40_PB9")
        n_d = sum(1 for w in ready.values() if w.leg == "DOWN15_OPEN")
        log(
            f"[WATCH] {len(ready)} syms (cum60={n_c} l40={n_l} down15={n_d}) "
            f"day={self.trade_date}"
        )
        for w in list(ready.values())[:12]:
            log(
                f"  {w.leg} {w.sym} {w.side.upper()} move={w.move_pct:+.1f}% "
                f"lvl={w.level:.8g} open_entry={w.open_entry}"
            )

        if LIVE:
            await self._place_entries()
        await self._catchup_fills()

    async def _catchup_fills(self) -> None:
        loop = asyncio.get_running_loop()
        for sym, w in list(self.watch.items()):
            if sym in self.positions or sym in self.filled_today:
                continue
            if len(self.positions) >= MAX_OPEN:
                break
            try:
                bars = await loop.run_in_executor(
                    None, lambda s=sym: bars_5m_day(s, self.trade_date, FAPI)
                )
            except Exception:
                continue
            if not bars:
                continue
            # EARLY: re-pick among all candidates if setup known
            setup = self.setups.get(sym)
            if setup:
                from c60_triple_engine import pick_early

                picked = pick_early(setup.candidates, bars)
                if picked:
                    w2, _ei, _entry = picked
                    self.watch[sym] = w2
                    w = w2
            for b in bars:
                if try_fill_on_bar(w, b, self.trade_date):
                    await self._enter(w)
                    break

    async def _place_entries(self) -> None:
        assert self.client is not None
        loop = asyncio.get_running_loop()
        for w in self.watch.values():
            if w.sym in self.positions or w.sym in self.filled_today:
                continue
            try:
                if not self.client.symbol_tradable(w.sym):
                    continue
                await loop.run_in_executor(
                    None, self.client.set_max_leverage, w.sym, LEVERAGE_CAP
                )
                side = entry_order_side(w.side)
                if w.open_entry:
                    # Near open: market for live open-entry leg
                    await loop.run_in_executor(
                        None,
                        self.client.market_order_notional,
                        w.sym,
                        side,
                        NOTIONAL,
                    )
                    log(f"[MKT] {w.sym} {w.side} @ open-leg ({w.leg})")
                else:
                    await loop.run_in_executor(
                        None,
                        self.client.limit_order_notional,
                        w.sym,
                        side,
                        NOTIONAL,
                        w.level,
                    )
                    log(f"[LIMIT] {w.sym} {w.side} @ {w.level:.8g} ({w.leg})")
            except Exception as e:
                log(f"[ENTRY_PLACE_ERR] {w.sym}: {e}")

    async def _scan_fills(self) -> None:
        if not self.watch:
            return
        loop = asyncio.get_running_loop()
        for sym, w in list(self.watch.items()):
            if sym in self.positions or sym in self.filled_today:
                continue
            if len(self.positions) >= MAX_OPEN:
                break
            try:
                bar = await loop.run_in_executor(None, latest_5m_bar, sym, FAPI)
            except Exception:
                continue
            if not bar or not try_fill_on_bar(w, bar, self.trade_date):
                continue
            await self._enter(w)

    async def _enter(self, w: WatchItem) -> bool:
        loop = asyncio.get_running_loop()
        async with self._lock:
            if w.sym in self.positions or w.sym in self.filled_today:
                return False
            if len(self.positions) >= MAX_OPEN:
                return False

            entry = w.level
            qty = 0.0
            if LIVE:
                assert self.client is not None
                try:
                    qty = await loop.run_in_executor(
                        None, self.client.position_qty, w.sym
                    )
                    row = await loop.run_in_executor(
                        None, self.client.position_row, w.sym
                    )
                    if qty <= 0:
                        if w.open_entry:
                            # market may still be in flight
                            log(f"[WAIT_MKT] {w.sym} flat after open-leg")
                        else:
                            log(f"[WAIT_LIMIT] {w.sym} through but flat — keep GTC")
                        return False
                    if row:
                        pe = float(row.get("entryPrice") or 0)
                        if pe > 0:
                            entry = pe
                except Exception as e:
                    log(f"[ENTER_ERR] {w.sym}: {e}")
                    return False

            self.positions[w.sym] = Position(
                sym=w.sym,
                side=w.side,
                leg=w.leg,
                entry_date=self.trade_date,
                entry=entry if LIVE else w.level,
                level=w.level,
                qty=qty,
            )
            self.filled_today.add(w.sym)
            # Cancel sibling limits if any
            if LIVE and self.client and not w.open_entry:
                pass  # already only one active watch per sym
            log(
                f"[FILL] {w.leg} {w.sym} {w.side.upper()} @ {entry:.8g} "
                f"(lvl={w.level:.8g} move={w.move_pct:+.1f}%)"
            )
            return True

    async def _exit_all(self, reason: str) -> None:
        for sym in list(self.positions.keys()):
            await self._exit_one(sym, reason)

    async def _exit_one(self, sym: str, reason: str) -> None:
        pos = self.positions.get(sym)
        if not pos:
            return
        loop = asyncio.get_running_loop()
        exit_px = pos.entry
        try:
            exit_px = await loop.run_in_executor(None, mark_price, sym, FAPI)
        except Exception:
            try:
                bars = await loop.run_in_executor(
                    None, lambda: bars_5m_day(sym, pos.entry_date, FAPI)
                )
                if bars:
                    exit_px = bars[-1].c
            except Exception:
                pass

        if LIVE and self.client:
            async with self._lock:
                for attempt in range(FLATTEN_RETRIES):
                    try:
                        await loop.run_in_executor(
                            None, self.client.cancel_all_open_orders, sym
                        )
                        qty = await loop.run_in_executor(
                            None, self.client.position_qty, sym
                        )
                        if qty > 0:
                            await loop.run_in_executor(
                                None,
                                self.client.market_close_qty,
                                sym,
                                close_order_side(pos.side),
                                qty,
                            )
                        break
                    except Exception as e:
                        log(f"[FLATTEN_ERR] {sym} try{attempt+1}: {e}")
                        await asyncio.sleep(1.0)

        usd = pnl_usd(pos.side, pos.entry, exit_px, NOTIONAL, FEE_RT)
        pct = pnl_pct(pos.side, pos.entry, exit_px)
        self.session_pnl += usd
        self.day_pnl += usd
        self.day_trades += 1
        if usd > 0:
            self.day_wins += 1
        append_csv(
            TRADES_CSV,
            {
                "ts": utc_now(),
                "trade_date": pos.entry_date,
                "sym": sym,
                "leg": pos.leg,
                "side": pos.side,
                "entry": pos.entry,
                "exit": exit_px,
                "pnl_usd": round(usd, 6),
                "pnl_pct": round(pct, 6),
                "reason": reason,
                "mode": MODE,
            },
            [
                "ts", "trade_date", "sym", "leg", "side", "entry", "exit",
                "pnl_usd", "pnl_pct", "reason", "mode",
            ],
        )
        log(
            f"[EXIT] {pos.leg} {sym} {pos.side} entry={pos.entry:.8g} "
            f"exit={exit_px:.8g} pnl=${usd:+.3f} ({pct:+.2f}%) {reason}"
        )
        del self.positions[sym]

    def _log_day_end(self) -> None:
        wr = 100.0 * self.day_wins / self.day_trades if self.day_trades else 0.0
        append_csv(
            DAILY_CSV,
            {
                "date": self.trade_date,
                "trades": self.day_trades,
                "wins": self.day_wins,
                "wr": round(wr, 2),
                "pnl": round(self.day_pnl, 4),
                "mode": MODE,
            },
            ["date", "trades", "wins", "wr", "pnl", "mode"],
        )
        log(
            f"[DAY_END] {self.trade_date} trades={self.day_trades} "
            f"WR={wr:.0f}% pnl=${self.day_pnl:+.2f}"
        )


def run_backtest(start: str, end: str) -> None:
    print(
        f"C60 triple backtest {start}→{end} | ${NOTIONAL}/trade | "
        f"cum>{CUM_THR} pb{PB_PCT} + 1d>{L40_THR} pb{PB_PCT} + down>{DOWN_THR}@open"
    )
    trades = backtest_range(
        start,
        end,
        cum_thr=CUM_THR,
        l40_thr=L40_THR,
        down_thr=DOWN_THR,
        pb_pct=PB_PCT,
        notional=NOTIONAL,
        fee_rt=FEE_RT,
        fapi=FAPI,
    )
    n = len(trades)
    if not n:
        print("No trades")
        return
    tot = sum(t.pnl for t in trades)
    wr = 100 * sum(1 for t in trades if t.pnl > 0) / n
    from collections import Counter

    legs = Counter(t.leg for t in trades)
    print(
        f"N={n} legs={dict(legs)} total=${tot:+.2f} WR={wr:.1f}% "
        f"$/100=${tot/n*100:+.2f}"
    )
    for pref, label in [
        ("2026-05", "May"),
        ("2026-06", "Jun"),
        ("2026-07", "Jul"),
    ]:
        xs = [t for t in trades if t.signal_date.startswith(pref)]
        if xs:
            print(f"{label} n={len(xs)} ${sum(t.pnl for t in xs):+.2f}")
    out = OUT_DIR / f"c60_backtest_{start}_to_{end}.csv"
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "signal_date", "trade_date", "sym", "leg", "direction", "side",
                "move_pct", "entry", "exit", "entry_bar_i", "pnl",
            ],
        )
        w.writeheader()
        for t in trades:
            w.writerow(
                {
                    "signal_date": t.signal_date,
                    "trade_date": t.trade_date,
                    "sym": t.sym,
                    "leg": t.leg,
                    "direction": t.direction,
                    "side": t.side,
                    "move_pct": round(t.move_pct, 4),
                    "entry": t.entry,
                    "exit": t.exit,
                    "entry_bar_i": t.entry_bar_i,
                    "pnl": round(t.pnl, 6),
                }
            )
    print(f"wrote {out}")


def main() -> None:
    ap = argparse.ArgumentParser(description="C60 triple dry/live bot")
    ap.add_argument("--backtest", nargs=2, metavar=("START", "END"))
    args = ap.parse_args()
    if args.backtest:
        run_backtest(args.backtest[0], args.backtest[1])
        return
    asyncio.run(C60TripleBot().run())


if __name__ == "__main__":
    main()
