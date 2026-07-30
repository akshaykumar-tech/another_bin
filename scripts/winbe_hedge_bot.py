#!/usr/bin/env python3
"""Quiet3 ladder TP30 bot — dry paper + optional live (replaces WinBE hedge).

Strategy (research Best +$246):
  quiet3 + prev |c2c| ≥ 30% → same-side ladder @ open ±8/10/12/15%
  TP 30%, no SL, max 3/level, EOD flat. Next-bar fill after touch.

Dry:  WINBE_LIVE_ENABLED=false
Live: WINBE_LIVE_ENABLED=true + BINANCE_API_KEY/SECRET

Run:
  python3 scripts/winbe_hedge_bot.py
  python3 scripts/winbe_hedge_bot.py --once
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
from dataclasses import asdict
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
    utc_today,
)
from winbe_hedge_engine import (
    DEFAULT_FEE_RT,
    DEFAULT_LEVELS,
    DEFAULT_MAX_PER_LEVEL,
    DEFAULT_NOTIONAL,
    DEFAULT_PREV_THR,
    DEFAULT_QUIET_MAX,
    DEFAULT_TP_PCT,
    STRATEGY_NAME,
    LadderLeg,
    Signal,
    SymbolDayState,
    active_open_legs,
    apply_bar_dry,
    close_eod_legs,
    make_day_state,
    scan_signals_for_trade_day,
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


def _parse_levels(raw: str) -> tuple[float, ...]:
    parts = [float(x.strip()) for x in raw.split(",") if x.strip()]
    return tuple(parts) if parts else DEFAULT_LEVELS


load_dotenv()

FAPI = _env("WINBE_FAPI", "https://fapi.binance.com").rstrip("/")
OUT_DIR = Path(_env("WINBE_OUT_DIR", str(ROOT / "data/aws/winbe_hedge")))
LIVE = _env_bool("WINBE_LIVE_ENABLED", False)
NOTIONAL = _env_float("WINBE_NOTIONAL_USDT", DEFAULT_NOTIONAL)
PREV_THR = _env_float("WINBE_PREV_THR", DEFAULT_PREV_THR)
QUIET_MAX = _env_float("WINBE_QUIET_MAX", DEFAULT_QUIET_MAX)
TP_PCT = _env_float("WINBE_TP_PCT", DEFAULT_TP_PCT)
MAX_PER_LEVEL = _env_int("WINBE_MAX_PER_LEVEL", DEFAULT_MAX_PER_LEVEL)
LEVELS = _parse_levels(_env("WINBE_LEVELS", "8,10,12,15"))
FEE_RT = _env_float("WINBE_FEE_RT", DEFAULT_FEE_RT)
MAX_OPEN_LEGS = _env_int("WINBE_MAX_OPEN_LEGS", 80)
POLL_SEC = _env_float("WINBE_POLL_SEC", 30.0)
SCAN_DELAY_SEC = _env_float("WINBE_SCAN_DELAY_SEC", 90.0)
EXIT_BEFORE_MIDNIGHT_SEC = _env_float("WINBE_EXIT_BEFORE_MIDNIGHT_SEC", 120.0)
DAILY_LOOKBACK = _env_int("WINBE_DAILY_LOOKBACK_DAYS", 12)
UNIVERSE_LIMIT = _env_int("WINBE_UNIVERSE_LIMIT", 0)
LEVERAGE_CAP = _env_int("WINBE_LEVERAGE_CAP", 20)
FLATTEN_RETRIES = _env_int("WINBE_FLATTEN_RETRIES", 4)
REPLAY_TODAY = _env_bool("WINBE_REPLAY_TODAY", False)

MODE = "LIVE" if LIVE else "DRY"
LOG_FILE = OUT_DIR / ("winbe_live.log" if LIVE else "winbe_dry.log")
TRADES_CSV = OUT_DIR / ("winbe_live_trades.csv" if LIVE else "winbe_dry_trades.csv")
DAILY_CSV = OUT_DIR / ("winbe_live_daily.csv" if LIVE else "winbe_dry_daily.csv")
WATCH_CSV = OUT_DIR / ("winbe_live_watch.csv" if LIVE else "winbe_dry_watch.csv")
STATE_FILE = OUT_DIR / ("winbe_live_state.json" if LIVE else "winbe_dry_state.json")


def entry_order_side(side: str) -> str:
    return "BUY" if side == "long" else "SELL"


def close_order_side(side: str) -> str:
    return "SELL" if side == "long" else "BUY"


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def log(msg: str) -> None:
    line = f"{utc_now()} {msg}"
    print(line, flush=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with LOG_FILE.open("a") as f:
        f.write(line + "\n")


def append_csv(path: Path, row: dict, fields: list[str]) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    new = not path.is_file()
    with path.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if new:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in fields})


def _day_start_epoch(trade_date: str) -> float:
    return day_ms(trade_date) / 1000.0


class LadderTP30Bot:
    def __init__(self) -> None:
        self.client: BinanceFuturesClient | None = None
        if LIVE:
            key, secret = binance_api_key(), binance_api_secret()
            self.client = BinanceFuturesClient(key, secret, FAPI)
            if not self.client.configured():
                raise SystemExit("LIVE requires BINANCE_API_KEY/SECRET")
        self.trade_date = ""
        self.books: dict[str, SymbolDayState] = {}  # sym -> state
        self.signals_today: list[Signal] = []
        self._scanned = False
        self._eod_done = False
        self._daily_cache: dict[str, list[DayBar]] = {}
        self._bar_cursor: dict[str, int] = {}
        self.day_pnl = 0.0
        self.day_trades = 0
        self.day_wins = 0
        self._lock = asyncio.Lock()
        # live: pending fills waiting for next bar after touch
        self._live_pending: dict[str, dict[float, int]] = {}  # sym -> {level: touch_bar}

    def _total_open_legs(self) -> int:
        return sum(len(active_open_legs(st)) for st in self.books.values())

    def _save_state(self) -> None:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        payload = {
            "strategy": STRATEGY_NAME,
            "trade_date": self.trade_date,
            "scanned": self._scanned,
            "eod_done": self._eod_done,
            "day_pnl": self.day_pnl,
            "day_trades": self.day_trades,
            "day_wins": self.day_wins,
            "bar_cursor": self._bar_cursor,
            "live_pending": self._live_pending,
            "books": {},
        }
        for sym, st in self.books.items():
            payload["books"][sym] = {
                "sym": st.sym,
                "side": st.side,
                "signal_date": st.signal_date,
                "trade_date": st.trade_date,
                "move_pct": st.move_pct,
                "day_open": st.day_open,
                "levels": list(st.levels),
                "max_per": st.max_per,
                "tp_pct": st.tp_pct,
                "armed": {str(k): v for k, v in st.armed.items()},
                "pending": {str(k): v for k, v in st.pending.items()},
                "leg_seq": st.leg_seq,
                "open_legs": [asdict(x) for x in st.open_legs],
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
            log(
                f"[STATE] ignore old strategy={payload.get('strategy')} "
                f"(want {STRATEGY_NAME}) — starting fresh"
            )
            return
        self.trade_date = str(payload.get("trade_date") or "")
        self._scanned = bool(payload.get("scanned"))
        self._eod_done = bool(payload.get("eod_done"))
        self.day_pnl = float(payload.get("day_pnl") or 0)
        self.day_trades = int(payload.get("day_trades") or 0)
        self.day_wins = int(payload.get("day_wins") or 0)
        self._bar_cursor = dict(payload.get("bar_cursor") or {})
        self._live_pending = {
            s: {float(k): int(v) for k, v in d.items()}
            for s, d in (payload.get("live_pending") or {}).items()
        }
        self.books = {}
        for sym, row in (payload.get("books") or {}).items():
            try:
                st = SymbolDayState(
                    sym=row["sym"],
                    side=row["side"],
                    signal_date=row["signal_date"],
                    trade_date=row["trade_date"],
                    move_pct=float(row["move_pct"]),
                    day_open=float(row["day_open"]),
                    levels=tuple(float(x) for x in row.get("levels") or LEVELS),
                    max_per=int(row.get("max_per") or MAX_PER_LEVEL),
                    tp_pct=float(row.get("tp_pct") or TP_PCT),
                    armed={float(k): bool(v) for k, v in (row.get("armed") or {}).items()},
                    pending={
                        float(k): (None if v is None else int(v))
                        for k, v in (row.get("pending") or {}).items()
                    },
                    leg_seq=int(row.get("leg_seq") or 0),
                    open_legs=[],
                )
                for lg in row.get("open_legs") or []:
                    st.open_legs.append(LadderLeg(**{
                        k: lg[k] for k in LadderLeg.__dataclass_fields__ if k in lg
                    }))
                self.books[sym] = st
            except Exception as e:
                log(f"[STATE_BOOK_ERR] {sym}: {e}")
        n_open = self._total_open_legs()
        log(f"[STATE] loaded books={len(self.books)} open_legs={n_open} date={self.trade_date or '-'}")

    async def run(self, once: bool = False) -> None:
        if LIVE and self.client:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self.client.load_exchange_info)
            log(f"LIVE client ready | leverage_cap={LEVERAGE_CAP}")
        self._load_state()
        log(
            f"start {MODE} | {STRATEGY_NAME} | prev≥{PREV_THR}% quiet≤{QUIET_MAX}% | "
            f"levels={list(LEVELS)} TP={TP_PCT}% max/level={MAX_PER_LEVEL} | "
            f"${NOTIONAL}/leg max_legs={MAX_OPEN_LEGS} | poll={POLL_SEC}s | "
            f"replay_today={REPLAY_TODAY}"
        )
        while True:
            await self._tick_day()
            await self._poll_once()
            self._save_state()
            if once:
                log("[ONCE] done")
                return
            await asyncio.sleep(POLL_SEC)

    async def _tick_day(self) -> None:
        today = utc_today()
        if today == self.trade_date:
            return
        if self.trade_date:
            if self._total_open_legs() > 0:
                await self._flatten_all("ROLLOVER")
            if not self._eod_done:
                self._log_day_end()
        self.trade_date = today
        self.books = {}
        self.signals_today = []
        self._scanned = False
        self._eod_done = False
        self._daily_cache = {}
        self._bar_cursor = {}
        self._live_pending = {}
        self.day_pnl = 0.0
        self.day_trades = 0
        self.day_wins = 0
        log(f"[NEW_DAY] {today} UTC — scan after {SCAN_DELAY_SEC:.0f}s from open")

    async def _poll_once(self) -> None:
        if not self.trade_date:
            return
        now = time.time()
        day_start = _day_start_epoch(self.trade_date)
        elapsed = now - day_start
        day_end = day_start + 86_400.0

        if not self._scanned and elapsed >= SCAN_DELAY_SEC:
            await self._scan_and_arm()

        if self.books:
            await self._manage()

        if not self._eod_done and now >= day_end - EXIT_BEFORE_MIDNIGHT_SEC:
            await self._flatten_all("EOD")
            self._eod_done = True
            self._log_day_end()

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

            def one(sym: str):
                try:
                    return sym, fetch_daily_range(sym, start, end, FAPI)
                except Exception:
                    return sym, []

            with ThreadPoolExecutor(max_workers=12) as ex:
                for fut in as_completed([ex.submit(one, s) for s in syms]):
                    sym, bars = fut.result()
                    if bars:
                        out[sym] = bars
            return out

        self._daily_cache = await loop.run_in_executor(None, _load)
        log(f"[DAILY] loaded {len(self._daily_cache)} symbols")

    async def _scan_and_arm(self) -> None:
        try:
            if not self._daily_cache:
                await self._load_daily_cache()
            cands = scan_signals_for_trade_day(
                self.trade_date,
                self._daily_cache,
                prev_thr=PREV_THR,
                quiet_max=QUIET_MAX,
            )
        except Exception as e:
            log(f"[SCAN_ERR] {e}")
            return

        self._scanned = True
        self.signals_today = cands
        log(
            f"[SCAN] quiet3+prev≥{PREV_THR}% signals={len(cands)} "
            f"→ arm ladder {list(LEVELS)} TP{TP_PCT}"
        )

        loop = asyncio.get_running_loop()
        for s in cands:
            append_csv(
                WATCH_CSV,
                {
                    "ts": utc_now(),
                    "trade_date": self.trade_date,
                    "sym": s.sym,
                    "signal_date": s.signal_date,
                    "move_pct": round(s.move_pct, 4),
                    "side": s.side,
                    "quiet_d1": round(s.quiet_d1, 4),
                    "quiet_d2": round(s.quiet_d2, 4),
                    "quiet_d3": round(s.quiet_d3, 4),
                },
                [
                    "ts", "trade_date", "sym", "signal_date", "move_pct", "side",
                    "quiet_d1", "quiet_d2", "quiet_d3",
                ],
            )
            try:
                bars = await loop.run_in_executor(
                    None, lambda sym=s.sym: bars_5m_day(sym, self.trade_date, FAPI)
                )
            except Exception:
                bars = []
            day_open = bars[0].o if bars and bars[0].o > 0 else 0.0
            if day_open <= 0:
                try:
                    day_open = await loop.run_in_executor(
                        None, lambda sym=s.sym: mark_price(sym, FAPI)
                    )
                except Exception:
                    log(f"[ARM_SKIP] {s.sym} no day_open")
                    continue
            st = make_day_state(
                s, day_open, levels=LEVELS, max_per=MAX_PER_LEVEL, tp_pct=TP_PCT
            )
            self.books[s.sym] = st
            day_elapsed = time.time() - _day_start_epoch(self.trade_date)
            use_replay = REPLAY_TODAY and day_elapsed <= max(SCAN_DELAY_SEC + 300.0, 600.0)
            self._bar_cursor[s.sym] = 0 if use_replay else (len(bars) if bars else 0)
            log(
                f"[ARM] {s.sym} {s.side.upper()} open={day_open:.8g} "
                f"move={s.move_pct:+.1f}% cursor={self._bar_cursor[s.sym]} "
                f"replay={use_replay}"
            )

    async def _manage(self) -> None:
        if LIVE:
            await self._manage_live()
        else:
            await self._manage_dry()

    async def _manage_dry(self) -> None:
        loop = asyncio.get_running_loop()
        for sym, st in list(self.books.items()):
            try:
                bars = await loop.run_in_executor(
                    None, lambda s=sym: bars_5m_day(s, self.trade_date, FAPI)
                )
            except Exception as e:
                log(f"[BARS_ERR] {sym} {e}")
                continue
            if not bars:
                continue
            start_i = self._bar_cursor.get(sym, 0)
            end_i = len(bars)
            for i in range(start_i, end_i):
                b = bars[i]
                closed = apply_bar_dry(
                    st, i, b.o, b.h, b.l, notional=NOTIONAL, fee_rt=FEE_RT
                )
                for leg in closed:
                    self._record_close(leg)
            self._bar_cursor[sym] = end_i

    async def _manage_live(self) -> None:
        """Touch → next bar market entry; TP / EOD on open legs."""
        assert self.client is not None
        loop = asyncio.get_running_loop()
        for sym, st in list(self.books.items()):
            try:
                bars = await loop.run_in_executor(
                    None, lambda s=sym: bars_5m_day(s, self.trade_date, FAPI)
                )
            except Exception as e:
                log(f"[BARS_ERR] {sym} {e}")
                continue
            if not bars:
                continue
            start_i = self._bar_cursor.get(sym, 0)
            end_i = len(bars)
            # process completed bars only (drop forming last if mid-bar)
            # use all available; idempotent via cursor
            for i in range(start_i, end_i):
                b = bars[i]
                # manage TP on open legs
                for leg in list(active_open_legs(st)):
                    hit_tp = (leg.side == "long" and b.h >= leg.tp_px) or (
                        leg.side == "short" and b.l <= leg.tp_px
                    )
                    if hit_tp:
                        await self._live_close_leg(leg, leg.tp_px, "TP")

                # fill live pendings scheduled for this bar
                pend = self._live_pending.get(sym, {})
                for lv, touch_i in list(pend.items()):
                    if touch_i + 1 != i:
                        continue
                    del pend[lv]
                    if st.open_count(lv) >= st.max_per:
                        continue
                    if self._total_open_legs() >= MAX_OPEN_LEGS:
                        log(f"[SKIP_MAX_LEGS] {sym} L{lv}")
                        continue
                    await self._live_enter_leg(st, lv, b.o, i)

                # new touches
                for lv in st.levels:
                    if pend.get(lv) is not None:
                        continue
                    if st.open_count(lv) >= st.max_per:
                        continue
                    px = st.level_px(lv)
                    if st.armed.get(lv, True):
                        hit = (b.h >= px) if st.side == "long" else (b.l <= px)
                        if hit:
                            pend[lv] = i
                            st.armed[lv] = False
                            log(f"[TOUCH] {sym} L{lv:g} @{px:.8g} bar={i} → next-bar entry")
                    if not st.armed.get(lv, True) and pend.get(lv) is None:
                        left = (b.l < px) if st.side == "long" else (b.h > px)
                        if left and st.open_count(lv) < st.max_per:
                            st.armed[lv] = True
                self._live_pending[sym] = pend
            self._bar_cursor[sym] = end_i

    async def _live_enter_leg(
        self, st: SymbolDayState, lv: float, bar_open: float, bar_i: int
    ) -> None:
        assert self.client is not None
        loop = asyncio.get_running_loop()
        async with self._lock:
            if st.open_count(lv) >= st.max_per:
                return
            if self._total_open_legs() >= MAX_OPEN_LEGS:
                return
            try:
                await loop.run_in_executor(
                    None, self.client.set_max_leverage, st.sym, LEVERAGE_CAP
                )
                await loop.run_in_executor(
                    None,
                    self.client.market_order_notional,
                    st.sym,
                    entry_order_side(st.side),
                    NOTIONAL,
                )
                row = await loop.run_in_executor(
                    None, self.client.position_row, st.sym
                )
                entry = float((row or {}).get("entryPrice") or 0) or bar_open
                qty_total = await loop.run_in_executor(
                    None, self.client.position_qty, st.sym
                )
                # approximate this leg qty from notional
                qty = NOTIONAL / entry if entry > 0 else 0.0
                if qty_total <= 0:
                    log(f"[ENTER_FLAT] {st.sym} L{lv} qty=0")
                    return
            except Exception as e:
                log(f"[ENTER_ERR] {st.sym} L{lv}: {e}")
                return

            tp = (
                entry * (1.0 + TP_PCT / 100.0)
                if st.side == "long"
                else entry * (1.0 - TP_PCT / 100.0)
            )
            leg = LadderLeg(
                id=st.next_leg_id(lv),
                sym=st.sym,
                side=st.side,
                level=lv,
                signal_date=st.signal_date,
                trade_date=st.trade_date,
                move_pct=st.move_pct,
                day_open=st.day_open,
                entry=entry,
                tp_px=tp,
                qty=qty,
                fill_bar=bar_i,
            )
            st.open_legs.append(leg)
            log(
                f"[FILL] {st.sym} {st.side.upper()} L{lv:g} @ {entry:.8g} "
                f"TP={tp:.8g} qty≈{qty:.6g} move={st.move_pct:+.1f}%"
            )

    async def _live_close_leg(self, leg: LadderLeg, exit_px: float, reason: str) -> None:
        assert self.client is not None
        loop = asyncio.get_running_loop()
        async with self._lock:
            if not leg.open:
                return
            for attempt in range(FLATTEN_RETRIES):
                try:
                    await loop.run_in_executor(
                        None, self.client.cancel_all_open_orders, leg.sym
                    )
                    qty = leg.qty
                    if qty <= 0:
                        # fallback: close NOTIONAL share
                        px = await loop.run_in_executor(
                            None, mark_price, leg.sym, FAPI
                        )
                        qty = NOTIONAL / px if px > 0 else 0.0
                    pos_qty = await loop.run_in_executor(
                        None, self.client.position_qty, leg.sym
                    )
                    qty = min(qty, pos_qty) if pos_qty > 0 else qty
                    if qty > 0:
                        await loop.run_in_executor(
                            None,
                            self.client.market_close_qty,
                            leg.sym,
                            close_order_side(leg.side),
                            qty,
                        )
                    break
                except Exception as e:
                    log(f"[CLOSE_ERR] {leg.sym} {reason} try{attempt+1}: {e}")
                    await asyncio.sleep(1.0)
            try:
                mark = await loop.run_in_executor(None, mark_price, leg.sym, FAPI)
                if mark > 0:
                    exit_px = mark
            except Exception:
                pass
            from orb30_engine import pnl_usd

            leg.exit_px = exit_px
            leg.reason = reason
            leg.open = False
            leg.pnl = pnl_usd(leg.side, leg.entry, leg.exit_px, NOTIONAL, FEE_RT)
            self._record_close(leg)

    def _record_close(self, leg: LadderLeg) -> None:
        self.day_trades += 1
        self.day_pnl += leg.pnl
        if leg.pnl > 0:
            self.day_wins += 1
        append_csv(
            TRADES_CSV,
            {
                "ts": utc_now(),
                "trade_date": leg.trade_date,
                "sym": leg.sym,
                "side": leg.side,
                "level": leg.level,
                "signal_date": leg.signal_date,
                "move_pct": round(leg.move_pct, 4),
                "day_open": leg.day_open,
                "entry": leg.entry,
                "exit": leg.exit_px,
                "tp_px": leg.tp_px,
                "reason": leg.reason,
                "pnl_usd": round(leg.pnl, 6),
                "leg_id": leg.id,
                "mode": MODE,
            },
            [
                "ts", "trade_date", "sym", "side", "level", "signal_date", "move_pct",
                "day_open", "entry", "exit", "tp_px", "reason", "pnl_usd", "leg_id", "mode",
            ],
        )
        log(
            f"[CLOSE] {leg.sym} {leg.side.upper()} L{leg.level:g} "
            f"{leg.reason} entry={leg.entry:.8g} exit={leg.exit_px:.8g} "
            f"pnl=${leg.pnl:+.3f}"
        )

    async def _flatten_all(self, reason: str) -> None:
        loop = asyncio.get_running_loop()
        for sym, st in list(self.books.items()):
            opens = active_open_legs(st)
            if not opens:
                continue
            try:
                px = await loop.run_in_executor(None, mark_price, sym, FAPI)
            except Exception:
                px = st.day_open
            if px <= 0:
                px = st.day_open
            if LIVE and self.client:
                for leg in list(opens):
                    await self._live_close_leg(leg, px, reason)
                # hard flatten leftover net
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
                                    close_order_side(st.side),
                                    qty,
                                )
                            break
                        except Exception as e:
                            log(f"[FLATTEN_ERR] {sym} try{attempt+1}: {e}")
                            await asyncio.sleep(1.0)
            else:
                closed = close_eod_legs(
                    st, px, notional=NOTIONAL, fee_rt=FEE_RT, reason=reason
                )
                for leg in closed:
                    self._record_close(leg)
            log(f"[FLATTEN] {sym} via {reason}")

    def _log_day_end(self) -> None:
        n = self.day_trades
        tot = self.day_pnl
        wins = self.day_wins
        wr = 100.0 * wins / n if n else 0.0
        open_n = self._total_open_legs()
        append_csv(
            DAILY_CSV,
            {
                "date": self.trade_date,
                "strategy": STRATEGY_NAME,
                "trades": n,
                "wins": wins,
                "wr": round(wr, 2),
                "pnl": round(tot, 4),
                "open_left": open_n,
                "signals": len(self.signals_today),
                "mode": MODE,
            },
            [
                "date", "strategy", "trades", "wins", "wr", "pnl",
                "open_left", "signals", "mode",
            ],
        )
        log(
            f"========== DAY_END {self.trade_date} {STRATEGY_NAME} ==========\n"
            f"  trades={n} wins={wins} WR={wr:.0f}% pnl=${tot:+.2f} "
            f"open_left={open_n} signals={len(self.signals_today)}\n"
            f"============================================================="
        )


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Quiet3 ladder TP30 dry/live (replaces WinBE hedge)"
    )
    ap.add_argument(
        "--once",
        action="store_true",
        help="One poll cycle then exit",
    )
    args = ap.parse_args()
    asyncio.run(LadderTP30Bot().run(once=args.once))


if __name__ == "__main__":
    main()
