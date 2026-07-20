#!/usr/bin/env python3
"""PB9 combo bot — dry paper + optional live.

Final strategy:
  A (priority): yesterday |c2c| > 50% → today CONT @ open ±9% pullback, FIRST 4h only
  B (fill):     2-day cum |move| > 50% → today CONT @ open ±9% pullback, full day
                only if (sym, day) has no A watch/entry

Realistic rules (paper = live):
  - Fill only on 5m trade-through of pullback level
  - Entry price = exact level (LIMIT @ level; dry uses level, live GTC LIMIT)
  - One open position per symbol per UTC day
  - Exit flatten at next UTC midnight (EOD)

Dry:  PB9_LIVE_ENABLED=false
Live: PB9_LIVE_ENABLED=true + BINANCE_API_KEY/SECRET

Run:
  python3 scripts/pb9_combo_bot.py
  python3 scripts/pb9_combo_bot.py --backtest 2026-06-01 2026-07-18
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

from binance_futures import BinanceFuturesClient
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
from pb9_combo_engine import (
    DEFAULT_PB,
    DEFAULT_THR,
    WatchItem,
    attach_open_levels,
    backtest_range,
    build_setups_for_trade_day,
    first4h_end_ms,
    try_fill_on_bar,
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

FAPI = _env("PB9_FAPI", "https://fapi.binance.com").rstrip("/")
OUT_DIR = Path(_env("PB9_OUT_DIR", str(ROOT / "data/aws/pb9_combo")))
LIVE = _env_bool("PB9_LIVE_ENABLED", False)
NOTIONAL = _env_float("PB9_NOTIONAL_USDT", 6.0)
THR_A = _env_float("PB9_THR_A", DEFAULT_THR)
THR_B = _env_float("PB9_THR_B", DEFAULT_THR)
PB_PCT = _env_float("PB9_PB_PCT", DEFAULT_PB)
MAX_OPEN = _env_int("PB9_MAX_OPEN_POSITIONS", 20)
POLL_SEC = _env_float("PB9_POLL_SEC", 15.0)
SCAN_DELAY_SEC = _env_float("PB9_SCAN_DELAY_SEC", 90.0)
FEE_RT = _env_float("PB9_FEE_RT", 0.0008)
LEVERAGE_CAP = _env_int("PB9_LEVERAGE_CAP", 20)
FLATTEN_RETRIES = _env_int("PB9_FLATTEN_RETRIES", 4)
MAX_DAILY_LOSS = _env_float("PB9_MAX_DAILY_LOSS_USDT", 15.0)
DAILY_LOOKBACK = _env_int("PB9_DAILY_LOOKBACK_DAYS", 10)

LOG_FILE = OUT_DIR / ("pb9_live.log" if LIVE else "pb9_dry.log")
TRADES_CSV = OUT_DIR / ("pb9_live_trades.csv" if LIVE else "pb9_dry_trades.csv")
DAILY_CSV = OUT_DIR / ("pb9_live_daily.csv" if LIVE else "pb9_dry_daily.csv")
WATCH_CSV = OUT_DIR / ("pb9_live_watch.csv" if LIVE else "pb9_dry_watch.csv")

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
    order_id: str = ""


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


class Pb9ComboBot:
    def __init__(self) -> None:
        self.client: BinanceFuturesClient | None = None
        self.trade_date = ""
        self.watch: dict[str, WatchItem] = {}
        self.setups: dict = {}  # sym -> SymDaySetup
        self.positions: dict[str, Position] = {}
        self.filled_today: set[str] = set()
        self.session_pnl = 0.0
        self.day_pnl = 0.0
        self.day_trades = 0
        self.day_wins = 0
        self._scanned = False
        self._after_first4h = False
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
            f"start | ${NOTIONAL}/trade | A:1d>{THR_A}% pb{PB_PCT}% first4h | "
            f"B:cum2d>{THR_B}% pb{PB_PCT}% fullday | max_open={MAX_OPEN} | EOD exit"
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
        self._after_first4h = False
        self._daily_cache = {}
        self.session_pnl = 0.0
        self.day_pnl = 0.0
        self.day_trades = 0
        self.day_wins = 0
        log(f"[NEW_DAY] {today} UTC — will scan signals after {SCAN_DELAY_SEC:.0f}s")

    async def _poll_once(self) -> None:
        if not self.trade_date:
            return
        if not self._scanned:
            elapsed = time.time() - _day_start_epoch(self.trade_date)
            if elapsed >= SCAN_DELAY_SEC:
                await self._load_watch()
            return

        # EOD flatten guard (if rollover missed)
        now_ms = int(time.time() * 1000)
        day_end = day_ms(self.trade_date) + 86_400_000 - 60_000
        if now_ms >= day_end and self.positions:
            await self._exit_all("EOD_GUARD")

        # After first 4h: drop unfilled A; promote B if eligible (research: no A fill)
        a_end = first4h_end_ms(self.trade_date)
        if now_ms >= a_end and not self._after_first4h:
            self._after_first4h = True
            await self._promote_b_after_first4h()

        await self._scan_fills()

    async def _promote_b_after_first4h(self) -> None:
        promoted = 0
        for sym, setup in list(self.setups.items()):
            if sym in self.positions or sym in self.filled_today:
                continue
            had_a = self.watch.get(sym) and self.watch[sym].leg == "A_1D"
            if setup.a and had_a:
                log(f"[A_WINDOW_END] {sym} no A fill — drop A")
                if LIVE and self.client:
                    try:
                        await asyncio.get_running_loop().run_in_executor(
                            None, self.client.cancel_all_open_orders, sym
                        )
                    except Exception:
                        pass
            if setup.b and sym not in self.filled_today:
                try:
                    bars = await asyncio.get_running_loop().run_in_executor(
                        None, lambda s=sym: bars_5m_day(s, self.trade_date, FAPI)
                    )
                except Exception:
                    bars = []
                if not bars:
                    self.watch.pop(sym, None)
                    continue
                wb = attach_open_levels([setup.b], bars)[0]
                self.watch[sym] = wb
                promoted += 1
                log(
                    f"[B_PROMOTE] {sym} {wb.side.upper()} lvl={wb.level:.8g} "
                    f"move={wb.move_pct:+.1f}%"
                )
                if LIVE:
                    await self._place_one_limit(wb)
                # Research B scans full day — catch fills that happened during first4h
                for b in bars:
                    if try_fill_on_bar(wb, b, self.trade_date):
                        await self._enter(wb)
                        break
            elif had_a:
                self.watch.pop(sym, None)
        log(f"[FIRST4H_DONE] B promoted={promoted}")

    async def _load_daily_cache(self) -> None:
        loop = asyncio.get_running_loop()
        end = self.trade_date
        # need ~3+ days history
        from datetime import datetime, timedelta

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
                thr_a=THR_A,
                thr_b=THR_B,
                pb_pct=PB_PCT,
            )
        except Exception as e:
            log(f"[SCAN_ERR] {e}")
            return

        self._scanned = True
        self.setups = setups
        # Active: A if present else B (B promote after first4h if A misses)
        ready: dict[str, WatchItem] = {}
        for sym, setup in setups.items():
            w0 = setup.a or setup.b
            if not w0:
                continue
            try:
                bars = await loop.run_in_executor(
                    None, lambda s=sym: bars_5m_day(s, self.trade_date, FAPI)
                )
            except Exception:
                continue
            if not bars:
                continue
            w2 = attach_open_levels([w0], bars)[0]
            ready[w2.sym] = w2
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
                    "pb_pct": w2.pb_pct,
                    "first4h_only": w2.first4h_only,
                    "has_b_fallback": bool(setup.b) and w2.leg == "A_1D",
                },
                [
                    "ts", "trade_date", "sym", "leg", "direction", "side",
                    "move_pct", "open", "level", "pb_pct", "first4h_only",
                    "has_b_fallback",
                ],
            )

        self.watch = ready
        n_a = sum(1 for w in ready.values() if w.leg == "A_1D")
        n_b = sum(1 for w in ready.values() if w.leg == "B_CUM2D")
        log(f"[WATCH] {len(ready)} syms (A={n_a} B={n_b}) trade_day={self.trade_date}")
        for w in list(ready.values())[:10]:
            win = "first4h" if w.first4h_only else "fullday"
            log(
                f"  {w.leg} {w.sym} {w.side.upper()} move={w.move_pct:+.1f}% "
                f"open={w.open_px:.8g} lvl={w.level:.8g} [{win}]"
            )

        if LIVE:
            await self._place_entry_limits()

        # Catch-up: if bot started mid-day, fill on any already-through bar
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
            for b in bars:
                if try_fill_on_bar(w, b, self.trade_date):
                    await self._enter(w)
                    break

    async def _place_one_limit(self, w: WatchItem) -> None:
        assert self.client is not None
        loop = asyncio.get_running_loop()
        if w.sym in self.positions or w.sym in self.filled_today:
            return
        try:
            if not self.client.symbol_tradable(w.sym):
                return
            await loop.run_in_executor(
                None, self.client.set_max_leverage, w.sym, LEVERAGE_CAP
            )
            await loop.run_in_executor(
                None,
                self.client.limit_order_notional,
                w.sym,
                entry_order_side(w.side),
                NOTIONAL,
                w.level,
            )
            log(f"[LIMIT] {w.sym} {w.side} @ {w.level:.8g} ({w.leg})")
        except Exception as e:
            log(f"[LIMIT_ERR] {w.sym}: {e}")

    async def _place_entry_limits(self) -> None:
        for w in self.watch.values():
            await self._place_one_limit(w)

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
            if not bar:
                continue
            if not try_fill_on_bar(w, bar, self.trade_date):
                continue
            await self._enter(w)

    async def _enter(self, w: WatchItem) -> bool:
        loop = asyncio.get_running_loop()
        async with self._lock:
            if w.sym in self.positions or w.sym in self.filled_today:
                return False
            if len(self.positions) >= MAX_OPEN:
                return False

            # Dry PnL always at exact pullback level (LIMIT fill parity).
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
                        # Limit not filled yet but bar traded through — do NOT market-chase
                        # (keeps live aligned with LIMIT@level research rule). Re-check next poll.
                        log(f"[WAIT_LIMIT] {w.sym} through level but flat — keep GTC")
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
        f"PB9 combo backtest {start}→{end} | $ {NOTIONAL}/trade | "
        f"A>{THR_A}% first4h pb{PB_PCT} + B cum2d>{THR_B}% pb{PB_PCT}"
    )
    trades = backtest_range(
        start,
        end,
        thr_a=THR_A,
        thr_b=THR_B,
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
    n_a = sum(1 for t in trades if t.leg == "A_1D")
    n_b = sum(1 for t in trades if t.leg == "B_CUM2D")
    jun = [t for t in trades if t.trade_date.startswith("2026-06")]
    jul = [t for t in trades if t.trade_date.startswith("2026-07")]
    print(f"N={n} (A={n_a} B={n_b}) total=${tot:+.2f} WR={wr:.1f}% $/100=${tot/n*100:+.2f}")
    if jun:
        print(f"Jun n={len(jun)} ${sum(t.pnl for t in jun):+.2f}")
    if jul:
        print(f"Jul n={len(jul)} ${sum(t.pnl for t in jul):+.2f}")
    out = OUT_DIR / f"pb9_backtest_{start}_to_{end}.csv"
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
    ap = argparse.ArgumentParser(description="PB9 combo dry/live bot")
    ap.add_argument(
        "--backtest",
        nargs=2,
        metavar=("START", "END"),
        help="Run historical backtest instead of live loop",
    )
    args = ap.parse_args()
    if args.backtest:
        run_backtest(args.backtest[0], args.backtest[1])
        return
    bot = Pb9ComboBot()
    asyncio.run(bot.run())


if __name__ == "__main__":
    main()
