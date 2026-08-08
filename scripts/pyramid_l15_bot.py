#!/usr/bin/env python3
"""Pyramid L15 bot — dry paper + optional live (no practical leg limit).

Strategy (canonical):
  L15 first-touch ≥30% → SHORT next-day open
  Pyramid +$NOTIONAL each +10% from anchor
  Exit: daily close ≤ anchor (until-recovery); optional PYR_MAX_HOLD_DAYS
  Max legs default 99 (= no practical limit)

Dry:  PYR_LIVE_ENABLED=false
Live: PYR_LIVE_ENABLED=true + BINANCE_API_KEY/SECRET

Run:
  python3 scripts/pyramid_l15_bot.py
  python3 scripts/pyramid_l15_bot.py --backtest 2026-01-01 2026-07-29
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))

from binance_futures import BinanceFuturesClient
from live_config_lib import binance_api_key, binance_api_secret, live_notional_usdt
from orb30_engine import DayBar, day_ms, fetch_daily_range, list_syms, mark_price, pnl_usd, utc_today
from pyramid_l15_engine import (
    DEFAULT_FEE_RT,
    DEFAULT_LOOKBACK,
    DEFAULT_MAX_HOLD,
    DEFAULT_MAX_LEGS,
    DEFAULT_MIN_LIFE,
    DEFAULT_NOTIONAL,
    DEFAULT_START_PCT,
    DEFAULT_STEP_PCT,
    Book,
    Leg,
    Signal,
    backtest_universe,
    book_pnl,
    hold_days,
    open_book_from_signal,
    rungs_crossed,
    scan_yesterday_for_entries,
    should_exit_anchor,
    should_exit_max_hold,
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

FAPI = _env("PYR_FAPI", "https://fapi.binance.com").rstrip("/")
OUT_DIR = Path(_env("PYR_OUT_DIR", str(ROOT / "data/aws/pyramid_l15")))
LIVE = _env_bool("PYR_LIVE_ENABLED", False)
NOTIONAL = _env_float("PYR_NOTIONAL_USDT", live_notional_usdt(DEFAULT_NOTIONAL))
LOOKBACK = _env_int("PYR_LOOKBACK", DEFAULT_LOOKBACK)
START_PCT = _env_float("PYR_START_PCT", DEFAULT_START_PCT)
STEP_PCT = _env_float("PYR_STEP_PCT", DEFAULT_STEP_PCT)
MAX_LEGS = _env_int("PYR_MAX_LEGS", DEFAULT_MAX_LEGS)
MAX_HOLD = _env_int("PYR_MAX_HOLD_DAYS", DEFAULT_MAX_HOLD)  # 0 = until recovery
MIN_LIFE = _env_int("PYR_MIN_LIFE_DAYS", DEFAULT_MIN_LIFE)
MAX_OPEN = _env_int("PYR_MAX_OPEN_BOOKS", 80)
POLL_SEC = _env_float("PYR_POLL_SEC", 60.0)
SCAN_DELAY_SEC = _env_float("PYR_SCAN_DELAY_SEC", 120.0)
MANAGE_DELAY_SEC = _env_float("PYR_MANAGE_DELAY_SEC", 90.0)
FEE_RT = _env_float("PYR_FEE_RT", DEFAULT_FEE_RT)
LEVERAGE_CAP = _env_int("PYR_LEVERAGE_CAP", 50)
DAILY_LOOKBACK = _env_int("PYR_DAILY_LOOKBACK_DAYS", 400)
UNIVERSE_LIMIT = _env_int("PYR_UNIVERSE_LIMIT", 0)

MODE = "LIVE" if LIVE else "DRY"
LOG_FILE = OUT_DIR / ("pyr_live.log" if LIVE else "pyr_dry.log")
TRADES_CSV = OUT_DIR / ("pyr_live_trades.csv" if LIVE else "pyr_dry_trades.csv")
DAILY_CSV = OUT_DIR / ("pyr_live_daily.csv" if LIVE else "pyr_dry_daily.csv")
WATCH_CSV = OUT_DIR / ("pyr_live_watch.csv" if LIVE else "pyr_dry_watch.csv")
STATE_FILE = OUT_DIR / ("pyr_live_state.json" if LIVE else "pyr_dry_state.json")
STRATEGY_NAME = f"L{LOOKBACK}_S{START_PCT:g}_T{STEP_PCT:g}_ML{MAX_LEGS}" + (
    f"_H{MAX_HOLD}" if MAX_HOLD > 0 else "_REC"
)


@dataclass
class PosLeg:
    entry_px: float
    entry_date: str
    level_idx: int
    qty: float = 0.0


@dataclass
class Position:
    sym: str
    signal_date: str
    entry_date: str
    anchor: float
    ladder: list[float]
    next_li: int
    legs: list[PosLeg] = field(default_factory=list)
    max_hold: int = 0

    @property
    def n_legs(self) -> int:
        return len(self.legs)


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


def _day_start_epoch(d: str) -> float:
    return day_ms(d) / 1000.0


class PyramidL15Bot:
    def __init__(self) -> None:
        self.client: BinanceFuturesClient | None = None
        self.trade_date = ""
        self.positions: dict[str, Position] = {}
        self.entered_today: set[str] = set()
        self.added_today: set[str] = set()
        self.exited_today: set[str] = set()
        self._scanned = False
        self._managed = False
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
        log(f"LIVE client ready | leverage_cap={LEVERAGE_CAP}")

    def _save_state(self) -> None:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        payload = {
            "trade_date": self.trade_date,
            "positions": [
                {
                    **{k: v for k, v in asdict(p).items() if k != "legs"},
                    "legs": [asdict(lg) for lg in p.legs],
                }
                for p in self.positions.values()
            ],
            "entered_today": sorted(self.entered_today),
            "added_today": sorted(self.added_today),
            "exited_today": sorted(self.exited_today),
            "scanned": self._scanned,
            "managed": self._managed,
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
                legs = [PosLeg(**lg) for lg in (row.get("legs") or [])]
                self.positions[row["sym"]] = Position(
                    sym=row["sym"],
                    signal_date=row["signal_date"],
                    entry_date=row["entry_date"],
                    anchor=float(row["anchor"]),
                    ladder=list(row.get("ladder") or []),
                    next_li=int(row.get("next_li") or len(legs)),
                    legs=legs,
                    max_hold=int(row.get("max_hold") or MAX_HOLD),
                )
            except Exception:
                continue
        self.entered_today = set(payload.get("entered_today") or [])
        self.added_today = set(payload.get("added_today") or [])
        self.exited_today = set(payload.get("exited_today") or [])
        self._scanned = bool(payload.get("scanned"))
        self._managed = bool(payload.get("managed"))
        nlegs = sum(p.n_legs for p in self.positions.values())
        log(f"[STATE] books={len(self.positions)} legs={nlegs} date={self.trade_date or '-'}")

    async def run(self) -> None:
        self.init_live()
        self._load_state()
        hold = f"H{MAX_HOLD}" if MAX_HOLD > 0 else "until-recovery"
        log(
            f"start {MODE} | {STRATEGY_NAME} | ${NOTIONAL}/leg | {hold} | "
            f"life≥{MIN_LIFE}d | max_books={MAX_OPEN} | poll={POLL_SEC}s"
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
            # End-of-prior-day safety manage if missed
            if not self._managed:
                await self._manage_books(force=True)
            self._log_day_end()
        self.trade_date = today
        self.entered_today = set()
        self.added_today = set()
        self.exited_today = set()
        self._scanned = False
        self._managed = False
        self._daily_cache = {}
        self.day_pnl = 0.0
        self.day_trades = 0
        self.day_wins = 0
        log(f"[NEW_DAY] {today} UTC — manage@{MANAGE_DELAY_SEC:.0f}s scan@{SCAN_DELAY_SEC:.0f}s")

    async def _poll_once(self) -> None:
        if not self.trade_date:
            return
        now = time.time()
        elapsed = now - _day_start_epoch(self.trade_date)

        # 1) Manage existing books first (adds at today's open / exits on prior close logic)
        if not self._managed and elapsed >= MANAGE_DELAY_SEC:
            await self._manage_books()
            self._managed = True

        # 2) New entries from yesterday signals
        if not self._scanned and elapsed >= SCAN_DELAY_SEC:
            await self._scan_and_enter()
            self._scanned = True

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
            for i, sym in enumerate(syms, 1):
                try:
                    bars = fetch_daily_range(sym, start, end, FAPI)
                except Exception:
                    continue
                if bars:
                    out[sym] = bars
                if i % 100 == 0:
                    log(f"[DAILY] fetched {i}/{len(syms)}")
            return out

        self._daily_cache = await loop.run_in_executor(None, _load)
        log(f"[DAILY] loaded {len(self._daily_cache)} symbols")

    def _prior_close(self, sym: str) -> float | None:
        bars = self._daily_cache.get(sym) or []
        # Prefer day before trade_date
        for b in reversed(bars):
            if b.date < self.trade_date and b.c > 0:
                return b.c
        return None

    async def _manage_books(self, force: bool = False) -> None:
        if not self.positions:
            log("[MANAGE] no open books")
            return
        if not self._daily_cache:
            await self._load_daily_cache()

        for sym in list(self.positions.keys()):
            pos = self.positions.get(sym)
            if not pos or sym in self.exited_today:
                continue
            prior = self._prior_close(sym)
            if prior is None:
                continue

            # Exit checks vs prior close (signal available at today's open)
            if should_exit_anchor(prior, pos.anchor):
                await self._exit_book(pos, prior, "anchor")
                continue
            if should_exit_max_hold(pos.entry_date, self.trade_date, pos.max_hold):
                px = mark_price(sym, FAPI) or prior
                await self._exit_book(pos, px, "maxhold")
                continue

            # Pyramid adds: prior close crossed new rungs → fill at today's open/mark
            if pos.n_legs >= MAX_LEGS or sym in self.added_today:
                continue
            ret = 100.0 * (prior / pos.anchor - 1.0) if pos.anchor > 0 else 0.0
            add = rungs_crossed(ret, pos.ladder, pos.next_li, MAX_LEGS)
            if add <= 0:
                continue
            await self._add_legs(pos, add)

    async def _fill_px(self, sym: str) -> float:
        if LIVE and self.client:
            try:
                loop = asyncio.get_running_loop()
                return await loop.run_in_executor(None, self.client.mark_price, sym)
            except Exception:
                pass
        try:
            return mark_price(sym, FAPI) or 0.0
        except Exception:
            return 0.0

    async def _market_short_leg(self, sym: str) -> tuple[float, float]:
        """Returns (fill_px, leg_qty). Dry: mark price, qty=0."""
        loop = asyncio.get_running_loop()
        if LIVE:
            assert self.client is not None
            await loop.run_in_executor(None, self.client.set_max_leverage, sym, LEVERAGE_CAP)
            before = await loop.run_in_executor(None, self.client.position_qty, sym)
            out = await loop.run_in_executor(
                None, self.client.market_order_notional, sym, "SELL", NOTIONAL
            )
            after = await loop.run_in_executor(None, self.client.position_qty, sym)
            px = float(out.get("avgPrice") or out.get("price") or 0) or await self._fill_px(sym)
            qty = max(0.0, abs(after) - abs(before))
            if qty <= 0:
                qty = abs(float(out.get("executedQty") or 0))
            return px, qty
        px = await self._fill_px(sym)
        return px, 0.0

    async def _flatten_short(self, sym: str) -> None:
        if not LIVE or not self.client:
            return
        loop = asyncio.get_running_loop()
        for attempt in range(4):
            try:
                await loop.run_in_executor(None, self.client.cancel_all_open_orders, sym)
                qty = await loop.run_in_executor(None, self.client.position_qty, sym)
                if abs(qty) < 1e-12:
                    return
                # close short = BUY reduceOnly
                await loop.run_in_executor(
                    None, self.client.market_close_qty, sym, "BUY", abs(qty)
                )
            except Exception as e:
                log(f"[FLAT_ERR] {sym} try={attempt+1}: {e}")
                await asyncio.sleep(0.4)

    async def _add_legs(self, pos: Position, n_add: int) -> None:
        async with self._lock:
            if pos.sym not in self.positions:
                return
            opened = 0
            for k in range(n_add):
                if pos.n_legs >= MAX_LEGS:
                    break
                try:
                    px, qty = await self._market_short_leg(pos.sym)
                except Exception as e:
                    log(f"[ADD_ERR] {pos.sym}: {e}")
                    break
                if px <= 0:
                    break
                pos.legs.append(
                    PosLeg(
                        entry_px=px,
                        entry_date=self.trade_date,
                        level_idx=pos.next_li + k,
                        qty=qty,
                    )
                )
                opened += 1
            if opened:
                pos.next_li += opened
                self.added_today.add(pos.sym)
                log(f"[ADD] {pos.sym} +{opened} legs now={pos.n_legs} next_li={pos.next_li}")

    async def _scan_and_enter(self) -> None:
        try:
            if not self._daily_cache:
                await self._load_daily_cache()
            cands = scan_yesterday_for_entries(
                self._daily_cache,
                self.trade_date,
                lookback=LOOKBACK,
                start_pct=START_PCT,
                step_pct=STEP_PCT,
                max_legs=MAX_LEGS,
                min_life=MIN_LIFE,
            )
        except Exception as e:
            log(f"[SCAN_ERR] {e}")
            return

        eligible = [
            s
            for s in cands
            if s.sym not in self.positions and s.sym not in self.entered_today
        ]
        log(f"[SCAN] signals={len(cands)} eligible={len(eligible)} open_books={len(self.positions)}")
        for s in eligible:
            append_csv(
                WATCH_CSV,
                {
                    "ts": utc_now(),
                    "trade_date": self.trade_date,
                    "sym": s.sym,
                    "signal_date": s.signal_date,
                    "anchor": round(s.anchor, 8),
                    "signal_ret": round(s.signal_ret, 4),
                    "n_init": s.n_init_legs,
                },
                ["ts", "trade_date", "sym", "signal_date", "anchor", "signal_ret", "n_init"],
            )
            if len(self.positions) >= MAX_OPEN:
                log(f"[SKIP_MAX] {s.sym} books={len(self.positions)}")
                continue
            await self._enter(s)

    async def _enter(self, s: Signal) -> bool:
        async with self._lock:
            if s.sym in self.positions or s.sym in self.entered_today:
                return False
            if len(self.positions) >= MAX_OPEN:
                return False

            legs: list[PosLeg] = []
            for i in range(s.n_init_legs):
                try:
                    px, qty = await self._market_short_leg(s.sym)
                except Exception as e:
                    log(f"[ENTER_ERR] {s.sym} leg={i+1}: {e}")
                    break
                if px <= 0:
                    break
                legs.append(
                    PosLeg(entry_px=px, entry_date=self.trade_date, level_idx=i, qty=qty)
                )

            if not legs:
                # If partial live failure with leftover qty, try flatten
                if LIVE:
                    await self._flatten_short(s.sym)
                return False

            self.positions[s.sym] = Position(
                sym=s.sym,
                signal_date=s.signal_date,
                entry_date=self.trade_date,
                anchor=s.anchor,
                ladder=list(s.ladder),
                next_li=len(legs),
                legs=legs,
                max_hold=MAX_HOLD,
            )
            self.entered_today.add(s.sym)
            avg = sum(lg.entry_px for lg in legs) / len(legs)
            log(
                f"[ENTER] {s.sym} SHORT legs={len(legs)} avg={avg:.6g} "
                f"anchor={s.anchor:.6g} sig_ret={s.signal_ret:.1f}%"
            )
            return True

    async def _exit_book(self, pos: Position, exit_px: float, reason: str) -> None:
        async with self._lock:
            if pos.sym not in self.positions:
                return
            if LIVE:
                await self._flatten_short(pos.sym)
                try:
                    loop = asyncio.get_running_loop()
                    assert self.client is not None
                    mp = await loop.run_in_executor(None, self.client.mark_price, pos.sym)
                    if mp > 0:
                        exit_px = mp
                except Exception:
                    pass

            book = Book(
                sym=pos.sym,
                signal_date=pos.signal_date,
                entry_date=pos.entry_date,
                anchor=pos.anchor,
                ladder=pos.ladder,
                next_li=pos.next_li,
                legs=[
                    Leg(entry_px=lg.entry_px, entry_date=lg.entry_date, level_idx=lg.level_idx)
                    for lg in pos.legs
                ],
                max_hold=pos.max_hold,
            )
            pnl = book_pnl(book, exit_px, NOTIONAL, FEE_RT)
            hd = hold_days(pos.entry_date, self.trade_date)
            self.day_pnl += pnl
            self.day_trades += 1
            if pnl > 0:
                self.day_wins += 1
            append_csv(
                TRADES_CSV,
                {
                    "ts": utc_now(),
                    "mode": MODE,
                    "sym": pos.sym,
                    "signal_date": pos.signal_date,
                    "entry_date": pos.entry_date,
                    "exit_date": self.trade_date,
                    "legs": pos.n_legs,
                    "anchor": round(pos.anchor, 8),
                    "exit_px": round(exit_px, 8),
                    "pnl_usd": round(pnl, 4),
                    "hold_days": hd,
                    "reason": reason,
                },
                [
                    "ts", "mode", "sym", "signal_date", "entry_date", "exit_date",
                    "legs", "anchor", "exit_px", "pnl_usd", "hold_days", "reason",
                ],
            )
            log(
                f"[EXIT] {pos.sym} reason={reason} legs={pos.n_legs} "
                f"pnl=${pnl:+.2f} hold={hd}d"
            )
            self.positions.pop(pos.sym, None)
            self.exited_today.add(pos.sym)

    def _log_day_end(self) -> None:
        n = self.day_trades
        wr = (100.0 * self.day_wins / n) if n else 0.0
        append_csv(
            DAILY_CSV,
            {
                "date": self.trade_date,
                "mode": MODE,
                "trades": n,
                "wins": self.day_wins,
                "wr_pct": round(wr, 2),
                "pnl_usd": round(self.day_pnl, 4),
                "open_books": len(self.positions),
                "open_legs": sum(p.n_legs for p in self.positions.values()),
            },
            ["date", "mode", "trades", "wins", "wr_pct", "pnl_usd", "open_books", "open_legs"],
        )
        log(
            f"[DAY_END] {self.trade_date} trades={n} WR={wr:.0f}% "
            f"pnl=${self.day_pnl:+.2f} open_books={len(self.positions)}"
        )


def run_backtest(start: str, end: str) -> None:
    log(f"[BACKTEST] {start} → {end} | {STRATEGY_NAME} | ${NOTIONAL}/leg")
    cache_d = ROOT / "data" / "cache" / "52w_daily"
    daily: dict[str, list[DayBar]] = {}
    if cache_d.is_dir():
        from indicator_hunt_hold2d_v2 import load_daily

        def daily_sym(p: Path) -> str:
            parts = p.stem.split("_")
            return "_".join(parts[:-2]) if len(parts) >= 3 else parts[0]

        syms = sorted({daily_sym(p) for p in cache_d.glob("*.pkl")})
        if UNIVERSE_LIMIT > 0:
            syms = syms[:UNIVERSE_LIMIT]
        for i, sym in enumerate(syms, 1):
            bars = load_daily(sym)
            if bars:
                daily[sym] = bars  # type: ignore[assignment]
            if i % 100 == 0 or i == len(syms):
                log(f"[BACKTEST] cache load {i}/{len(syms)}")
        log(f"[BACKTEST] cache symbols={len(daily)}")
    else:
        syms = list_syms(FAPI)
        if UNIVERSE_LIMIT > 0:
            syms = syms[:UNIVERSE_LIMIT]
        pad = (
            datetime.strptime(start, "%Y-%m-%d").date()
            - timedelta(days=LOOKBACK + MIN_LIFE + 5)
        ).isoformat()
        for i, sym in enumerate(syms, 1):
            try:
                bars = fetch_daily_range(sym, pad, end, FAPI)
            except Exception:
                continue
            if bars:
                daily[sym] = bars
            if i % 50 == 0:
                log(f"[BACKTEST] fetch {i}/{len(syms)}")

    trades = backtest_universe(
        daily,
        start,
        end,
        lookback=LOOKBACK,
        start_pct=START_PCT,
        step_pct=STEP_PCT,
        max_legs=MAX_LEGS,
        max_hold=MAX_HOLD,
        min_life=MIN_LIFE,
        notional=NOTIONAL,
        fee_rt=FEE_RT,
    )
    n = len(trades)
    tot = sum(t.pnl for t in trades)
    wins = sum(1 for t in trades if t.pnl > 0)
    wr = 100.0 * wins / n if n else 0.0
    log(f"[BACKTEST] n={n} WR={wr:.1f}% tot=${tot:+.2f} legs={sum(t.legs for t in trades)}")
    out = OUT_DIR / f"backtest_{start}_to_{end}.csv"
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "sym", "signal_date", "entry_date", "exit_date", "legs",
                "hold_days", "reason", "pnl",
            ],
        )
        w.writeheader()
        for t in trades:
            w.writerow(
                {
                    "sym": t.sym,
                    "signal_date": t.signal_date,
                    "entry_date": t.entry_date,
                    "exit_date": t.exit_date,
                    "legs": t.legs,
                    "hold_days": t.hold_days,
                    "reason": t.reason,
                    "pnl": round(t.pnl, 4),
                }
            )
    log(f"[BACKTEST] wrote {out}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Pyramid L15 dry/live bot")
    ap.add_argument("--backtest", nargs=2, metavar=("START", "END"), help="YYYY-MM-DD YYYY-MM-DD")
    args = ap.parse_args()
    if args.backtest:
        run_backtest(args.backtest[0], args.backtest[1])
        return
    bot = PyramidL15Bot()
    asyncio.run(bot.run())


if __name__ == "__main__":
    main()
