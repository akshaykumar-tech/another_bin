#!/usr/bin/env python3
"""RSI FADE UP70 reclaim bot — dry paper + optional live.

Strategy:
  RSI(7) crosses ≥70 → wait leave below → reclaim ≥70 → SHORT next open
  Hold HOLD_DAYS (default 5; set 8 for research max tot), exit at close

Dry:  RSI_LIVE_ENABLED=false
Live: RSI_LIVE_ENABLED=true + BINANCE_API_KEY/SECRET

Run:
  python3 scripts/rsi_reclaim_bot.py
  python3 scripts/rsi_reclaim_bot.py --once
  python3 scripts/rsi_reclaim_bot.py --backtest 2026-05-01 2026-07-27
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))

from binance_futures import BinanceFuturesClient
from live_config_lib import binance_api_key, binance_api_secret
from orb30_engine import (
    DayBar,
    bars_5m_day,
    day_ms,
    fetch_daily_range,
    list_syms,
    mark_price,
    pnl_pct,
    pnl_usd,
    utc_today,
)
from rsi_reclaim_engine import (
    DEFAULT_FEE_RT,
    DEFAULT_HOLD_DAYS,
    DEFAULT_MAX_ARM_DAYS,
    DEFAULT_NOTIONAL,
    DEFAULT_RSI_PERIOD,
    DEFAULT_RSI_THR,
    STRATEGY_NAME,
    Signal,
    backtest_range,
    scan_for_entry_day,
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

FAPI = _env("RSI_FAPI", "https://fapi.binance.com").rstrip("/")
OUT_DIR = Path(_env("RSI_OUT_DIR", str(ROOT / "data/aws/rsi_reclaim")))
LIVE = _env_bool("RSI_LIVE_ENABLED", False)
NOTIONAL = _env_float("RSI_NOTIONAL_USDT", DEFAULT_NOTIONAL)
RSI_PERIOD = _env_int("RSI_PERIOD", DEFAULT_RSI_PERIOD)
RSI_THR = _env_float("RSI_THR", DEFAULT_RSI_THR)
HOLD_DAYS = _env_int("RSI_HOLD_DAYS", DEFAULT_HOLD_DAYS)
MAX_ARM = _env_int("RSI_MAX_ARM_DAYS", DEFAULT_MAX_ARM_DAYS)
MAX_OPEN = _env_int("RSI_MAX_OPEN_POSITIONS", 40)
POLL_SEC = _env_float("RSI_POLL_SEC", 30.0)
SCAN_DELAY_SEC = _env_float("RSI_SCAN_DELAY_SEC", 90.0)
EXIT_BEFORE_MIDNIGHT_SEC = _env_float("RSI_EXIT_BEFORE_MIDNIGHT_SEC", 120.0)
FEE_RT = _env_float("RSI_FEE_RT", DEFAULT_FEE_RT)
LEVERAGE_CAP = _env_int("RSI_LEVERAGE_CAP", 20)
FLATTEN_RETRIES = _env_int("RSI_FLATTEN_RETRIES", 4)
DAILY_LOOKBACK = _env_int("RSI_DAILY_LOOKBACK_DAYS", 60)
UNIVERSE_LIMIT = _env_int("RSI_UNIVERSE_LIMIT", 0)

LOG_FILE = OUT_DIR / ("rsi_live.log" if LIVE else "rsi_dry.log")
TRADES_CSV = OUT_DIR / ("rsi_live_trades.csv" if LIVE else "rsi_dry_trades.csv")
DAILY_CSV = OUT_DIR / ("rsi_live_daily.csv" if LIVE else "rsi_dry_daily.csv")
WATCH_CSV = OUT_DIR / ("rsi_live_watch.csv" if LIVE else "rsi_dry_watch.csv")
STATE_FILE = OUT_DIR / ("rsi_live_state.json" if LIVE else "rsi_dry_state.json")
MODE = "LIVE" if LIVE else "DRY"


@dataclass
class Position:
    sym: str
    side: str
    signal_date: str
    entry_date: str
    exit_date: str
    entry: float
    rsi: float
    thr: float
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


def _shift(d: str, n: int) -> str:
    return (datetime.strptime(d, "%Y-%m-%d").date() + timedelta(days=n)).isoformat()


class RsiReclaimBot:
    def __init__(self) -> None:
        self.client: BinanceFuturesClient | None = None
        self.trade_date = ""
        self.positions: dict[str, Position] = {}
        self.entered_today: set[str] = set()
        self.exited_today: set[str] = set()
        self._scanned = False
        self._exits_done = False
        self._daily_cache: dict[str, list[DayBar]] = {}
        self.day_pnl = 0.0
        self.day_trades = 0
        self.day_wins = 0
        self._lock = asyncio.Lock()

    def init_live(self) -> None:
        if not LIVE:
            return
        key, sec = binance_api_key(), binance_api_secret()
        if not key or not sec:
            raise RuntimeError("BINANCE_API_KEY / BINANCE_API_SECRET missing")
        self.client = BinanceFuturesClient(key, sec, FAPI)
        self.client.warm_cache()

    def _save_state(self) -> None:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        payload = {
            "strategy": STRATEGY_NAME,
            "trade_date": self.trade_date,
            "positions": [asdict(p) for p in self.positions.values()],
            "entered_today": sorted(self.entered_today),
            "exited_today": sorted(self.exited_today),
            "scanned": self._scanned,
            "exits_done": self._exits_done,
        }
        STATE_FILE.write_text(json.dumps(payload, indent=2))

    def _load_state(self) -> None:
        if not STATE_FILE.is_file():
            return
        try:
            payload = json.loads(STATE_FILE.read_text())
        except Exception as e:
            log(f"[STATE_ERR] {e}")
            return
        if payload.get("strategy") and payload.get("strategy") != STRATEGY_NAME:
            log(f"[STATE] ignore old strategy={payload.get('strategy')}")
            return
        self.trade_date = str(payload.get("trade_date") or "")
        self.positions = {}
        for row in payload.get("positions") or []:
            try:
                self.positions[row["sym"]] = Position(**row)
            except Exception:
                continue
        self.entered_today = set(payload.get("entered_today") or [])
        self.exited_today = set(payload.get("exited_today") or [])
        self._scanned = bool(payload.get("scanned"))
        self._exits_done = bool(payload.get("exits_done"))
        log(
            f"[STATE] loaded positions={len(self.positions)} "
            f"trade_date={self.trade_date or '-'}"
        )

    async def run(self) -> None:
        self.init_live()
        self._load_state()
        log(
            f"start {MODE} | {STRATEGY_NAME} | RSI({RSI_PERIOD})≥{RSI_THR} reclaim "
            f"→ SHORT | hold={HOLD_DAYS}d | ${NOTIONAL}/trade | max_open={MAX_OPEN}"
        )
        while True:
            await self._tick_day()
            await self._poll_once()
            self._save_state()
            await asyncio.sleep(POLL_SEC)

    async def run_once(self) -> None:
        self.init_live()
        self._load_state()
        await self._tick_day()
        await self._poll_once()
        self._save_state()
        log("once done")

    async def _tick_day(self) -> None:
        today = utc_today()
        if today == self.trade_date:
            return
        if self.trade_date:
            await self._exit_due(force_reason="ROLLOVER")
            self._log_day_end()
        self.trade_date = today
        self.entered_today = set()
        self.exited_today = set()
        self._scanned = False
        self._exits_done = False
        self._daily_cache = {}
        self.day_pnl = 0.0
        self.day_trades = 0
        self.day_wins = 0
        log(f"[NEW_DAY] {today} UTC — scan after {SCAN_DELAY_SEC:.0f}s")

    async def _poll_once(self) -> None:
        if not self.trade_date:
            return
        now = time.time()
        day_start = _day_start_epoch(self.trade_date)
        elapsed = now - day_start
        day_end = day_start + 86_400.0
        if not self._scanned and elapsed >= SCAN_DELAY_SEC:
            await self._scan_and_enter()
        if not self._exits_done and now >= day_end - EXIT_BEFORE_MIDNIGHT_SEC:
            await self._exit_due(force_reason="EOD_HOLD")
            self._exits_done = True

    async def _load_daily_cache(self) -> None:
        loop = asyncio.get_running_loop()
        end = self.trade_date
        start = (
            datetime.strptime(end, "%Y-%m-%d").date() - timedelta(days=DAILY_LOOKBACK)
        ).isoformat()

        def _load() -> dict[str, list[DayBar]]:
            syms = list_syms(FAPI)
            if UNIVERSE_LIMIT > 0:
                syms = syms[:UNIVERSE_LIMIT]
            out: dict[str, list[DayBar]] = {}

            def one(sym: str) -> tuple[str, list[DayBar] | None]:
                try:
                    return sym, fetch_daily_range(sym, start, end, FAPI)
                except Exception:
                    return sym, None

            with ThreadPoolExecutor(max_workers=12) as ex:
                futs = [ex.submit(one, s) for s in syms]
                for fut in as_completed(futs):
                    sym, bars = fut.result()
                    if bars:
                        out[sym] = bars
            return out

        self._daily_cache = await loop.run_in_executor(None, _load)
        log(f"[DAILY] loaded {len(self._daily_cache)} symbols")

    async def _scan_and_enter(self) -> None:
        try:
            if not self._daily_cache:
                await self._load_daily_cache()
            cands = scan_for_entry_day(
                self._daily_cache,
                self.trade_date,
                rsi_period=RSI_PERIOD,
                thr=RSI_THR,
                hold_days=HOLD_DAYS,
                max_arm=MAX_ARM,
            )
        except Exception as e:
            log(f"[SCAN_ERR] {e}")
            return

        self._scanned = True
        busy = {p.sym: p.exit_date for p in self.positions.values()}
        eligible: list[Signal] = []
        for s in cands:
            if s.sym in busy and s.entry_date < busy[s.sym]:
                continue
            if s.sym in self.positions or s.sym in self.entered_today:
                continue
            eligible.append(s)

        log(
            f"[SCAN] {STRATEGY_NAME} reclaim→entry signals={len(cands)} "
            f"eligible={len(eligible)}"
        )
        for s in eligible:
            append_csv(
                WATCH_CSV,
                {
                    "ts": utc_now(),
                    "trade_date": self.trade_date,
                    "sym": s.sym,
                    "signal_date": s.signal_date,
                    "exit_date": s.exit_date,
                    "rsi": round(s.rsi, 4),
                    "thr": s.thr,
                    "entry_ref": s.entry,
                },
                [
                    "ts", "trade_date", "sym", "signal_date", "exit_date",
                    "rsi", "thr", "entry_ref",
                ],
            )
            if len(self.positions) >= MAX_OPEN:
                log(f"[SKIP_MAX] {s.sym} open={len(self.positions)}")
                continue
            await self._enter(s)

    async def _enter(self, s: Signal) -> bool:
        loop = asyncio.get_running_loop()
        async with self._lock:
            if s.sym in self.positions or s.sym in self.entered_today:
                return False
            if len(self.positions) >= MAX_OPEN:
                return False

            entry = s.entry
            qty = 0.0
            if LIVE:
                assert self.client is not None
                try:
                    await loop.run_in_executor(
                        None, self.client.set_max_leverage, s.sym, LEVERAGE_CAP
                    )
                    await loop.run_in_executor(
                        None,
                        self.client.market_order_notional,
                        s.sym,
                        entry_order_side("short"),
                        NOTIONAL,
                    )
                    qty = await loop.run_in_executor(
                        None, self.client.position_qty, s.sym
                    )
                    row = await loop.run_in_executor(
                        None, self.client.position_row, s.sym
                    )
                    if qty <= 0:
                        log(f"[ENTER_FLAT] {s.sym} qty=0")
                        return False
                    if row:
                        pe = float(row.get("entryPrice") or 0)
                        if pe > 0:
                            entry = pe
                except Exception as e:
                    log(f"[ENTER_ERR] {s.sym}: {e}")
                    return False
            else:
                try:
                    bars = await loop.run_in_executor(
                        None, lambda: bars_5m_day(s.sym, self.trade_date, FAPI)
                    )
                    if bars and bars[0].o > 0:
                        entry = bars[0].o
                    else:
                        entry = mark_price(s.sym, FAPI) or s.entry
                except Exception:
                    try:
                        entry = mark_price(s.sym, FAPI) or s.entry
                    except Exception:
                        entry = s.entry

            exit_date = s.exit_date
            bars = self._daily_cache.get(s.sym) or []
            idx = {b.date: i for i, b in enumerate(bars)}
            if self.trade_date in idx:
                ei = idx[self.trade_date]
                exi = ei + (HOLD_DAYS - 1)
                if exi < len(bars):
                    exit_date = bars[exi].date
                else:
                    exit_date = _shift(self.trade_date, HOLD_DAYS - 1)

            self.positions[s.sym] = Position(
                sym=s.sym,
                side="short",
                signal_date=s.signal_date,
                entry_date=self.trade_date,
                exit_date=exit_date,
                entry=entry,
                rsi=s.rsi,
                thr=s.thr,
                qty=qty,
            )
            self.entered_today.add(s.sym)
            log(
                f"[FILL] {s.sym} SHORT @ {entry:.8g} reclaim_rsi={s.rsi:.1f} "
                f"sig={s.signal_date} hold={HOLD_DAYS}d exit={exit_date}"
            )
            return True

    async def _exit_due(self, force_reason: str = "EOD_HOLD") -> None:
        today = self.trade_date
        due = [
            sym
            for sym, p in self.positions.items()
            if p.exit_date <= today and sym not in self.exited_today
        ]
        for sym in due:
            await self._exit_one(sym, force_reason)

    async def _exit_one(self, sym: str, reason: str) -> None:
        pos = self.positions.get(sym)
        if not pos:
            return
        loop = asyncio.get_running_loop()
        exit_px = pos.entry
        try:
            bars = await loop.run_in_executor(
                None, lambda: bars_5m_day(sym, self.trade_date, FAPI)
            )
            if bars:
                exit_px = bars[-1].c
            else:
                exit_px = await loop.run_in_executor(None, mark_price, sym, FAPI)
        except Exception:
            try:
                exit_px = await loop.run_in_executor(None, mark_price, sym, FAPI)
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
        self.day_pnl += usd
        self.day_trades += 1
        if usd > 0:
            self.day_wins += 1
        append_csv(
            TRADES_CSV,
            {
                "ts": utc_now(),
                "signal_date": pos.signal_date,
                "entry_date": pos.entry_date,
                "exit_date": pos.exit_date,
                "sym": sym,
                "side": pos.side,
                "entry": pos.entry,
                "exit": exit_px,
                "pnl_usd": round(usd, 6),
                "pnl_pct": round(pct, 6),
                "rsi": round(pos.rsi, 4),
                "thr": pos.thr,
                "hold_days": HOLD_DAYS,
                "reason": reason,
                "mode": MODE,
            },
            [
                "ts", "signal_date", "entry_date", "exit_date", "sym", "side",
                "entry", "exit", "pnl_usd", "pnl_pct", "rsi", "thr",
                "hold_days", "reason", "mode",
            ],
        )
        log(
            f"[EXIT] {sym} SHORT entry={pos.entry:.8g} exit={exit_px:.8g} "
            f"pnl=${usd:+.3f} ({pct:+.2f}%) {reason}"
        )
        self.exited_today.add(sym)
        self.positions.pop(sym, None)

    def _log_day_end(self) -> None:
        wr = (100.0 * self.day_wins / self.day_trades) if self.day_trades else 0.0
        append_csv(
            DAILY_CSV,
            {
                "date": self.trade_date,
                "trades": self.day_trades,
                "wins": self.day_wins,
                "wr": round(wr, 2),
                "pnl": round(self.day_pnl, 4),
                "open_left": len(self.positions),
                "mode": MODE,
            },
            ["date", "trades", "wins", "wr", "pnl", "open_left", "mode"],
        )
        log(
            f"[DAY_END] {self.trade_date} trades={self.day_trades} "
            f"pnl=${self.day_pnl:+.3f} open={len(self.positions)}"
        )


def run_backtest(start: str, end: str) -> None:
    print(f"backtest {STRATEGY_NAME} {start}..{end} hold={HOLD_DAYS}")
    start_lb = (
        datetime.strptime(start, "%Y-%m-%d").date() - timedelta(days=DAILY_LOOKBACK)
    ).isoformat()
    syms = list_syms(FAPI)
    if UNIVERSE_LIMIT > 0:
        syms = syms[:UNIVERSE_LIMIT]
    sym_bars: dict[str, list[DayBar]] = {}

    def one(sym: str) -> tuple[str, list[DayBar] | None]:
        try:
            return sym, fetch_daily_range(sym, start_lb, end, FAPI)
        except Exception:
            return sym, None

    with ThreadPoolExecutor(max_workers=12) as ex:
        futs = [ex.submit(one, s) for s in syms]
        for fut in as_completed(futs):
            sym, bars = fut.result()
            if bars:
                sym_bars[sym] = bars
    print(f"loaded {len(sym_bars)} syms")
    trades = backtest_range(
        sym_bars,
        start,
        end,
        rsi_period=RSI_PERIOD,
        thr=RSI_THR,
        hold_days=HOLD_DAYS,
        max_arm=MAX_ARM,
        notional=NOTIONAL,
        fee_rt=FEE_RT,
    )
    tot = sum(t.pnl_usd for t in trades)
    wins = sum(1 for t in trades if t.pnl_usd > 0)
    by_m: dict[str, float] = {}
    n_m: dict[str, int] = {}
    for t in trades:
        m = t.entry_date[:7]
        by_m[m] = by_m.get(m, 0.0) + t.pnl_usd
        n_m[m] = n_m.get(m, 0) + 1
    print(f"n={len(trades)} tot=${tot:.2f} wr={100*wins/len(trades) if trades else 0:.1f}%")
    for m in sorted(by_m):
        print(f"  {m}: n={n_m[m]} tot=${by_m[m]:.2f}")
    out = OUT_DIR / f"rsi_reclaim_bt_{start}_to_{end}.csv"
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "sym", "signal_date", "entry_date", "exit_date", "side",
                "entry", "exit", "rsi", "thr", "pnl_usd", "reason",
            ],
        )
        w.writeheader()
        for t in trades:
            w.writerow(asdict(t))
    print(f"wrote {out}")


def main() -> None:
    ap = argparse.ArgumentParser(description=f"{STRATEGY_NAME} dry/live")
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--backtest", nargs=2, metavar=("START", "END"))
    args = ap.parse_args()
    if args.backtest:
        run_backtest(args.backtest[0], args.backtest[1])
        return
    bot = RsiReclaimBot()
    if args.once:
        asyncio.run(bot.run_once())
    else:
        asyncio.run(bot.run())


if __name__ == "__main__":
    main()
