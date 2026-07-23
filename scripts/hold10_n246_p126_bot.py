#!/usr/bin/env python3
"""hold10_n246_p126 bot — dry paper + optional live.

Strategy (research: c22_s2_top_uw | delay30m | hold10 ≈ n229 / +$135):
  Peak day D: ≥2 up days, cum≥22%, close_loc≥0.75, upper wick≥2%
  Entry: SHORT @ D+1 open+30m (6×5m bar open)
  Exit:  hold 10 daily bars (close of entry+9)

Dry:  H10_LIVE_ENABLED=false
Live: H10_LIVE_ENABLED=true + BINANCE_API_KEY/SECRET

Run:
  python3 scripts/hold10_n246_p126_bot.py
  python3 scripts/hold10_n246_p126_bot.py --backtest 2026-05-01 2026-07-18
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import pickle
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))

from binance_futures import BinanceFuturesClient
from hold10_n246_p126_engine import (
    DEFAULT_CLOSE_LOC_MIN,
    DEFAULT_CUM_PCT,
    DEFAULT_ENTRY_DELAY_BARS,
    DEFAULT_FEE_RT,
    DEFAULT_HOLD_DAYS,
    DEFAULT_NOTIONAL,
    DEFAULT_STREAK_MIN,
    DEFAULT_UW_PCT,
    Signal,
    backtest_range,
    scan_yesterday_for_entries,
)
from live_config_lib import binance_api_key, binance_api_secret
from orb30_engine import (
    BAR_MS,
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

FAPI = _env("H10_FAPI", "https://fapi.binance.com").rstrip("/")
OUT_DIR = Path(_env("H10_OUT_DIR", str(ROOT / "data/aws/hold10_n246_p126")))
LIVE = _env_bool("H10_LIVE_ENABLED", False)
NOTIONAL = _env_float("H10_NOTIONAL_USDT", DEFAULT_NOTIONAL)
CUM_PCT = _env_float("H10_CUM_PCT", DEFAULT_CUM_PCT)
STREAK_MIN = _env_int("H10_STREAK_MIN", DEFAULT_STREAK_MIN)
CLOSE_LOC_MIN = _env_float("H10_CLOSE_LOC_MIN", DEFAULT_CLOSE_LOC_MIN)
UW_PCT = _env_float("H10_UW_PCT", DEFAULT_UW_PCT)
HOLD_DAYS = _env_int("H10_HOLD_DAYS", DEFAULT_HOLD_DAYS)
ENTRY_DELAY_BARS = _env_int("H10_ENTRY_DELAY_BARS", DEFAULT_ENTRY_DELAY_BARS)
ENTRY_DELAY_SEC = ENTRY_DELAY_BARS * (BAR_MS / 1000.0)
MAX_OPEN = _env_int("H10_MAX_OPEN_POSITIONS", 40)
POLL_SEC = _env_float("H10_POLL_SEC", 30.0)
# Enter no earlier than entry-delay (default 30m); SCAN_DELAY can only push later.
SCAN_DELAY_SEC = max(_env_float("H10_SCAN_DELAY_SEC", 90.0), ENTRY_DELAY_SEC)
EXIT_BEFORE_MIDNIGHT_SEC = _env_float("H10_EXIT_BEFORE_MIDNIGHT_SEC", 120.0)
FEE_RT = _env_float("H10_FEE_RT", DEFAULT_FEE_RT)
LEVERAGE_CAP = _env_int("H10_LEVERAGE_CAP", 20)
FLATTEN_RETRIES = _env_int("H10_FLATTEN_RETRIES", 4)
DAILY_LOOKBACK = _env_int("H10_DAILY_LOOKBACK_DAYS", 45)
UNIVERSE_LIMIT = _env_int("H10_UNIVERSE_LIMIT", 0)  # 0 = all

LOG_FILE = OUT_DIR / ("h10_live.log" if LIVE else "h10_dry.log")
TRADES_CSV = OUT_DIR / ("h10_live_trades.csv" if LIVE else "h10_dry_trades.csv")
DAILY_CSV = OUT_DIR / ("h10_live_daily.csv" if LIVE else "h10_dry_daily.csv")
WATCH_CSV = OUT_DIR / ("h10_live_watch.csv" if LIVE else "h10_dry_watch.csv")
STATE_FILE = OUT_DIR / ("h10_live_state.json" if LIVE else "h10_dry_state.json")

MODE = "LIVE" if LIVE else "DRY"
CACHE_DAILY = ROOT / "data" / "cache" / "52w_daily"


@dataclass
class Position:
    sym: str
    side: str
    signal_date: str
    entry_date: str
    exit_date: str
    entry: float
    streak: int
    cum_pct: float
    close_loc: float
    uw_pct: float
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


class Hold10Bot:
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
            f"start | ${NOTIONAL}/trade | SHORT cum≥{CUM_PCT}% s≥{STREAK_MIN} "
            f"top≥{CLOSE_LOC_MIN} uw≥{UW_PCT}% | hold={HOLD_DAYS}d | "
            f"delay={ENTRY_DELAY_BARS*5}m | max_open={MAX_OPEN}"
        )
        while True:
            await self._tick_day()
            await self._poll_once()
            self._save_state()
            await asyncio.sleep(POLL_SEC)

    async def _tick_day(self) -> None:
        today = utc_today()
        if today == self.trade_date:
            return
        if self.trade_date:
            # Safety: flatten any due exits missed
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

        # Entries after scan delay (near open)
        if not self._scanned and elapsed >= SCAN_DELAY_SEC:
            await self._scan_and_enter()

        # Exits near UTC midnight on exit_date
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
            for sym in syms:
                try:
                    bars = fetch_daily_range(sym, start, end, FAPI)
                except Exception:
                    continue
                if bars:
                    out[sym] = bars
            return out

        self._daily_cache = await loop.run_in_executor(None, _load)
        log(f"[DAILY] loaded {len(self._daily_cache)} symbols")

    async def _scan_and_enter(self) -> None:
        loop = asyncio.get_running_loop()
        try:
            if not self._daily_cache:
                await self._load_daily_cache()
            cands = scan_yesterday_for_entries(
                self._daily_cache,
                self.trade_date,
                cum_pct=CUM_PCT,
                streak_min=STREAK_MIN,
                close_loc_min=CLOSE_LOC_MIN,
                uw_pct=UW_PCT,
                hold_days=HOLD_DAYS,
            )
        except Exception as e:
            log(f"[SCAN_ERR] {e}")
            return

        self._scanned = True
        # Overlap vs open positions: treat open exit_date as busy
        busy = {p.sym: p.exit_date for p in self.positions.values()}
        eligible: list[Signal] = []
        for s in sorted(cands, key=lambda x: (-x.cum_pct, x.sym)):
            if s.sym in busy and s.signal_date < busy[s.sym]:
                continue
            if s.sym in self.positions or s.sym in self.entered_today:
                continue
            eligible.append(s)

        log(f"[SCAN] signals={len(cands)} eligible={len(eligible)}")
        for s in eligible:
            append_csv(
                WATCH_CSV,
                {
                    "ts": utc_now(),
                    "trade_date": self.trade_date,
                    "sym": s.sym,
                    "signal_date": s.signal_date,
                    "exit_date": s.exit_date,
                    "streak": s.streak,
                    "cum_pct": round(s.cum_pct, 4),
                    "close_loc": round(s.close_loc, 4),
                    "uw_pct": round(s.uw_pct, 4),
                    "entry_ref": s.entry,
                },
                [
                    "ts", "trade_date", "sym", "signal_date", "exit_date",
                    "streak", "cum_pct", "close_loc", "uw_pct", "entry_ref",
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
                    # Prefer live open: market short
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
                        log(f"[ENTER_FLAT] {s.sym} market filled? qty=0")
                        return False
                    if row:
                        pe = float(row.get("entryPrice") or 0)
                        if pe > 0:
                            entry = pe
                except Exception as e:
                    log(f"[ENTER_ERR] {s.sym}: {e}")
                    return False
            else:
                # Dry: fill at delayed 5m open (bar[ENTRY_DELAY_BARS])
                try:
                    bars = await loop.run_in_executor(
                        None, lambda: bars_5m_day(s.sym, self.trade_date, FAPI)
                    )
                    delay = max(0, int(ENTRY_DELAY_BARS))
                    if bars and delay < len(bars) and bars[delay].o > 0:
                        entry = bars[delay].o
                    elif bars:
                        entry = bars[0].o
                    else:
                        entry = mark_price(s.sym, FAPI) or s.entry
                except Exception:
                    try:
                        entry = mark_price(s.sym, FAPI) or s.entry
                    except Exception:
                        entry = s.entry

            # Resolve exit_date from daily series if possible
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
                streak=s.streak,
                cum_pct=s.cum_pct,
                close_loc=s.close_loc,
                uw_pct=s.uw_pct,
                qty=qty,
            )
            self.entered_today.add(s.sym)
            log(
                f"[FILL] {s.sym} SHORT @ {entry:.8g} sig={s.signal_date} "
                f"cum={s.cum_pct:.1f}% s={s.streak} cl={s.close_loc:.2f} "
                f"uw={s.uw_pct:.1f}% delay={ENTRY_DELAY_BARS*5}m exit={exit_date}"
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
            # Prefer last 5m close of exit day
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
                "streak": pos.streak,
                "cum_pct": round(pos.cum_pct, 4),
                "close_loc": round(pos.close_loc, 4),
                "uw_pct": round(pos.uw_pct, 4),
                "reason": reason,
                "mode": MODE,
            },
            [
                "ts", "signal_date", "entry_date", "exit_date", "sym", "side",
                "entry", "exit", "pnl_usd", "pnl_pct", "streak", "cum_pct",
                "close_loc", "uw_pct", "reason", "mode",
            ],
        )
        log(
            f"[EXIT] {sym} SHORT entry={pos.entry:.8g} exit={exit_px:.8g} "
            f"pnl=${usd:+.3f} ({pct:+.2f}%) {reason}"
        )
        self.exited_today.add(sym)
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
                "open_positions": len(self.positions),
                "mode": MODE,
            },
            ["date", "trades", "wins", "wr", "pnl", "open_positions", "mode"],
        )
        log(
            f"[DAY_END] {self.trade_date} exits={self.day_trades} "
            f"WR={wr:.0f}% pnl=${self.day_pnl:+.2f} still_open={len(self.positions)}"
        )


def _best_daily(sym: str, start: str) -> list[DayBar] | None:
    best = None
    score = (-1, -1)
    for f in CACHE_DAILY.glob(f"{sym}_*_*.pkl"):
        parts = f.stem.rsplit("_", 2)
        if len(parts) < 3:
            continue
        s, e = parts[1], parts[2]
        if e < start:
            continue
        try:
            bars = pickle.loads(f.read_bytes())
        except Exception:
            continue
        if not bars:
            continue
        hist = sum(1 for b in bars if b.date < start)
        sc = (hist, len(bars))
        if sc > score:
            score = sc
            best = bars
    return best


def run_backtest(start: str, end: str) -> None:
    print(
        f"hold10_n246_p126 backtest {start}→{end} | ${NOTIONAL}/trade | "
        f"cum≥{CUM_PCT}% s≥{STREAK_MIN} cl≥{CLOSE_LOC_MIN} uw≥{UW_PCT}% "
        f"hold{HOLD_DAYS} delay{ENTRY_DELAY_BARS*5}m"
    )
    daily: dict[str, list[DayBar]] = {}
    for sym in sorted({f.name.split("_")[0] for f in CACHE_DAILY.glob("*.pkl")}):
        bars = _best_daily(sym, start)
        if not bars or len(bars) < 20:
            continue
        m = {b.date: b for b in bars}
        daily[sym] = sorted(m.values(), key=lambda b: b.date)
    print(f"symbols={len(daily)}", flush=True)

    trades = backtest_range(
        daily,
        start,
        end,
        cum_pct=CUM_PCT,
        streak_min=STREAK_MIN,
        close_loc_min=CLOSE_LOC_MIN,
        uw_pct=UW_PCT,
        hold_days=HOLD_DAYS,
        entry_delay_bars=ENTRY_DELAY_BARS,
        notional=NOTIONAL,
        fee_rt=FEE_RT,
    )
    # Only count trades whose signal_date in window (entry may spill)
    trades = [t for t in trades if start <= t.signal_date <= end]

    by_m: dict[str, list[float]] = {"May": [], "Jun": [], "Jul": []}
    for t in trades:
        m = None
        if t.signal_date.startswith("2026-05"):
            m = "May"
        elif t.signal_date.startswith("2026-06"):
            m = "Jun"
        elif t.signal_date.startswith("2026-07"):
            m = "Jul"
        if m:
            by_m[m].append(t.pnl_usd)

    tot = sum(t.pnl_usd for t in trades)
    n = len(trades)
    wr = 100.0 * sum(1 for t in trades if t.pnl_usd > 0) / n if n else 0.0
    print(f"N={n} Tot=${tot:+.2f} WR={wr:.1f}%")
    for m in ("May", "Jun", "Jul"):
        xs = by_m[m]
        if not xs:
            print(f"  {m}: n0")
            continue
        mw = 100.0 * sum(1 for x in xs if x > 0) / len(xs)
        print(f"  {m}: n{len(xs)} ${sum(xs):+.2f} WR{mw:.0f}%")

    out = OUT_DIR / f"h10_backtest_{start}_to_{end}.csv"
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "sym", "signal_date", "entry_date", "exit_date", "side",
                "entry", "exit", "streak", "cum_pct", "close_loc", "uw_pct",
                "pnl_usd", "reason",
            ],
        )
        w.writeheader()
        for t in trades:
            w.writerow(asdict(t))
    print(f"wrote {out}")


def main() -> None:
    ap = argparse.ArgumentParser(description="hold10_n246_p126 dry/live bot")
    ap.add_argument(
        "--backtest",
        nargs=2,
        metavar=("START", "END"),
        help="Run historical backtest on cached daily bars",
    )
    args = ap.parse_args()
    if args.backtest:
        run_backtest(args.backtest[0], args.backtest[1])
        return
    try:
        asyncio.run(Hold10Bot().run())
    except KeyboardInterrupt:
        log("stopped")


if __name__ == "__main__":
    main()
