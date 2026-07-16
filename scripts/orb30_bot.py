#!/usr/bin/env python3
"""ORB 30m breakout on top-mover next-days — dry paper + optional live.

Day (05:30 IST = 00:00 UTC):
  1. Scan yesterday top5 gain/loss (7d-clean) → today's watchlist
  2. Build 30m ORB (05:30–06:00 IST)
  3. Breakout long/short, TP 10% / SL 10%, max 3 trades/symbol/day

Live paper-parity:
  - Paper DECIDES on 5m bar H/L touch (not candle close). Entry PRICE is always ORB hi/lo.
  - Bar wick touched ORB → market fill (same as paper). Mark-only beyond ORB → limit @ ORB.
  - Re-entries after TP/SL when next bar touches ORB again (like paper stacks).
  - Periodic paper sync + open-order audit every 10m (cancel/replace wrong limits).
  - TP/SL brackets from ORB entry; exchange algos placed after fill.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
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
    ORB_5M_BARS,
    Bracket,
    MoverSignal,
    bars_5m_day,
    bracket_prices,
    day_start_ms_now,
    inside_orb,
    latest_5m_bar,
    mark_price,
    orb_from_5m,
    pnl_pct,
    pnl_usd,
    replay_orb_state,
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
MAX_OPEN = _env_int("ORB30_MAX_OPEN_POSITIONS", 15)
# false = paper sim_orb parity (re-entry on ORB touch without needing inside-first)
FRESH_BREAKOUT = _env_bool("ORB30_FRESH_BREAKOUT", False)
# Live: market only when |mark-ORB|/ORB <= this %; else limit at ORB (no chase).
MAX_ENTRY_SLIP_PCT = _env_float("ORB30_MAX_ENTRY_SLIP_PCT", 0.35)
# Place exchange STOP/TP algos after fill (bot poll is backup).
EXCHANGE_BRACKETS = _env_bool("ORB30_EXCHANGE_BRACKETS", True)
WORKING_TYPE = _env("ORB30_WORKING_TYPE", "MARK_PRICE")
# Live only enters a side if today's 5m paper replay agrees (blocks BANK-style opposite).
PAPER_GATE = _env_bool("ORB30_PAPER_GATE", True)
# Re-sync paper path + audit resting limits (cancel/replace if wrong).
ORDER_AUDIT_SEC = _env_float("ORB30_ORDER_AUDIT_SEC", 600.0)
LOOKBACK = _env_int("ORB30_LOOKBACK_DAYS", 7)
POLL_SEC = _env_float("ORB30_POLL_SEC", 3.0)
SCAN_DELAY_SEC = _env_float("ORB30_SCAN_DELAY_SEC", 120.0)
FEE_RT = _env_float("ORB30_FEE_RT", 0.0008)
LEVERAGE_CAP = _env_int("ORB30_LEVERAGE_CAP", 20)
FLATTEN_RETRIES = _env_int("ORB30_FLATTEN_RETRIES", 4)
MAX_DAILY_LOSS = _env_float("ORB30_MAX_DAILY_LOSS_USDT", 10.0)

LOG_FILE = OUT_DIR / ("orb30_live.log" if LIVE else "orb30_dry.log")
TRADES_CSV = OUT_DIR / ("orb30_live_trades.csv" if LIVE else "orb30_dry_trades.csv")
DAILY_CSV = OUT_DIR / ("orb30_live_daily.csv" if LIVE else "orb30_dry_daily.csv")
WATCH_FILE = OUT_DIR / "orb30_active_watch.json"

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
    catchup_done: bool = False
    prev_inside_orb: bool = True
    trades_today: int = 0
    pos: Position | None = None
    # Live resting entry at ORB (paper fills at exact hi/lo).
    pending_side: str = ""
    pending_order_id: int = 0
    pending_orb_entry: float = 0.0
    last_entry_bar_ts: int = 0  # one entry per 5m bar (paper parity)


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
        self.day_pnl = 0.0
        self.day_trades = 0
        self.day_wins = 0
        self._lock = asyncio.Lock()
        self._scanned = False
        self._last_audit_ts = 0.0

    def init_live(self) -> None:
        if not LIVE:
            return
        key, sec = binance_api_key(), binance_api_secret()
        if not key or not sec:
            raise RuntimeError("BINANCE_API_KEY / BINANCE_API_SECRET missing")
        self.client = BinanceFuturesClient(key, sec, FAPI)
        self.client.warm_cache()

    def _load_persisted_watch(self) -> tuple[str, list[str]]:
        if not WATCH_FILE.is_file():
            return "", []
        try:
            data = json.loads(WATCH_FILE.read_text())
            if not isinstance(data, dict):
                return "", []
            trade_date = str(data.get("trade_date") or "")
            syms = data.get("symbols") or []
            if not isinstance(syms, list):
                return trade_date, []
            return trade_date, [str(s).upper() for s in syms if s]
        except Exception:
            return "", []

    def _persist_watch(self) -> None:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        payload = {
            "trade_date": self.trade_date,
            "symbols": sorted(self.watch.keys()),
            "updated": utc_now(),
        }
        WATCH_FILE.write_text(json.dumps(payload, indent=2))

    async def run(self) -> None:
        self.init_live()
        log(
            f"start | ${NOTIONAL} tp={TP_PCT}% sl={SL_PCT}% "
            f"max_trades/sym={MAX_TRADES_SYM} max_open={MAX_OPEN} "
            f"fresh_breakout={FRESH_BREAKOUT} max_slip={MAX_ENTRY_SLIP_PCT}% "
            f"exchange_brackets={EXCHANGE_BRACKETS} paper_gate={PAPER_GATE} "
            f"order_audit={ORDER_AUDIT_SEC}s orb=30m"
        )
        if LIVE:
            prev_date, prev_syms = self._load_persisted_watch()
            today = utc_today()
            if prev_syms and prev_date and prev_date != today:
                log(f"[STARTUP] flattening stale watch from {prev_date}: {prev_syms}")
                await self._flatten_symbol_list(prev_syms, "STARTUP_STALE_DAY")
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
            _, prev_syms = self._load_persisted_watch()
            await self._flatten_symbol_list(prev_syms, "DAY_ROLLOVER_RESIDUAL")
            self._log_day_end()
        self.trade_date = today
        self.day_start_ms = day_start_ms_now()
        self.watch = {}
        self._scanned = False
        self.session_pnl = 0.0
        self.day_pnl = 0.0
        self.day_trades = 0
        self.day_wins = 0
        log(f"[NEW_DAY] {today} (05:30 IST open)")

    def _log_day_end(self) -> None:
        if not self.trade_date:
            return
        log(
            f"[DAY_END] {self.trade_date} | trades={self.day_trades} "
            f"wins={self.day_wins} losses={self.day_trades - self.day_wins} "
            f"pnl=${self.day_pnl:+.4f}"
        )
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        new = not DAILY_CSV.is_file()
        with DAILY_CSV.open("a", newline="") as f:
            w = csv.DictWriter(
                f,
                fieldnames=["trade_date", "trades", "wins", "losses", "pnl_usd"],
            )
            if new:
                w.writeheader()
            w.writerow({
                "trade_date": self.trade_date,
                "trades": self.day_trades,
                "wins": self.day_wins,
                "losses": self.day_trades - self.day_wins,
                "pnl_usd": round(self.day_pnl, 4),
            })

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
        if LIVE and self._all_orb_locked():
            now = time.time()
            if self._last_audit_ts == 0 or now - self._last_audit_ts >= ORDER_AUDIT_SEC:
                self._last_audit_ts = now
                await self._audit_paper_and_orders()

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
        self._persist_watch()
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
                await self._catchup_symbol(st)
            except Exception as e:
                log(f"[ORB_ERR] {st.sym} {e}")

    async def _catchup_symbol(self, st: SymState) -> None:
        """Sync to paper path: closed trade count + open side (avoid BANK-style miss).

        If 5m replay says paper still holds LONG/SHORT at ORB and we are flat,
        enter that side immediately (market near ORB / else limit) so we never
        take the opposite breakout after missing the first one.
        """
        if st.catchup_done:
            return
        loop = asyncio.get_running_loop()
        try:
            bars = await loop.run_in_executor(
                None, bars_5m_day, st.sym, self.trade_date, FAPI
            )
            replay = replay_orb_state(
                bars,
                ORB_5M_BARS,
                TP_PCT,
                SL_PCT,
                MAX_TRADES_SYM,
                close_open_at_end=False,
            )
            st.trades_today = replay.trades_done
            try:
                px = await loop.run_in_executor(None, mark_price, st.sym, FAPI)
            except Exception:
                px = bars[-1].c if bars else st.day_open
            st.prev_inside_orb = inside_orb(px, st.orb_high, st.orb_low)
            st.catchup_done = True
            log(
                f"[CATCHUP] {st.sym} replay trades={st.trades_today}/{MAX_TRADES_SYM} "
                f"open={replay.open_side or 'flat'}@{replay.open_entry or 0:.8g} "
                f"px={px:.8g} inside_orb={st.prev_inside_orb}"
            )
            await self._adopt_exchange_pos(st)
            # Paper still in a trade we missed → join that side, don't wait for opposite.
            if (
                st.pos is None
                and st.pending_order_id <= 0
                and replay.open_side
                and st.trades_today < MAX_TRADES_SYM
                and self._open_count() < MAX_OPEN
            ):
                await self._catchup_enter_paper_open(
                    st, replay.open_side, replay.open_entry, force_market=True
                )
        except Exception as e:
            log(f"[CATCHUP_ERR] {st.sym} {e}")
            st.catchup_done = True

    async def _catchup_enter_paper_open(
        self,
        st: SymState,
        side: str,
        orb_entry: float,
        *,
        force_market: bool = False,
    ) -> None:
        """Enter the side paper is already holding (missed breakout / restart recovery)."""
        if orb_entry <= 0:
            orb_entry = st.orb_high if side == "long" else st.orb_low
        if orb_entry <= 0:
            return
        loop = asyncio.get_running_loop()
        try:
            px = await loop.run_in_executor(None, mark_price, st.sym, FAPI)
        except Exception:
            px = orb_entry
        slip_pct = abs(px - orb_entry) / orb_entry * 100.0 if orb_entry > 0 else 999.0
        log(
            f"[CATCHUP_SYNC] {st.sym} paper still {side.upper()} @{orb_entry:.8g} "
            f"mark={px:.8g} slip={slip_pct:.2f}% — joining paper path"
        )
        bar_touch = force_market
        if not bar_touch:
            try:
                bar = await loop.run_in_executor(None, latest_5m_bar, st.sym, FAPI)
                if bar:
                    bar_touch = (side == "long" and bar.h >= st.orb_high) or (
                        side == "short" and bar.l <= st.orb_low
                    )
            except Exception:
                pass
        await self._execute_entry(
            st, side, orb_entry, px, bar_touch or force_market, tag="CATCHUP"
        )

    async def _adopt_exchange_pos(self, st: SymState) -> None:
        """Same-day restart: attach open Binance position to state (don't flatten)."""
        if not LIVE or self.client is None or st.pos is not None:
            return
        loop = asyncio.get_running_loop()
        try:
            row = await loop.run_in_executor(None, self.client.position_row, st.sym)
        except Exception:
            return
        if not row:
            return
        amt = float(row.get("positionAmt") or 0)
        if abs(amt) <= 0:
            return
        side = "long" if amt > 0 else "short"
        qty = abs(amt)
        fill = float(row.get("entryPrice") or 0) or (
            st.orb_high if side == "long" else st.orb_low
        )
        orb_entry = st.orb_high if side == "long" else st.orb_low
        if orb_entry <= 0:
            orb_entry = fill
        br = bracket_prices(side, orb_entry, TP_PCT, SL_PCT)
        st.pos = Position(st.sym, side, orb_entry, br.tp_px, br.sl_px, qty, st.bucket)
        await self._place_exchange_brackets(st)
        log(
            f"[ADOPT] {st.sym} {side.upper()} qty={qty} orb_entry={orb_entry:.8g} "
            f"fill={fill:.8g} tp={br.tp_px:.8g} sl={br.sl_px:.8g}"
        )

    def _open_count(self) -> int:
        n = 0
        for st in self.watch.values():
            if st.pos is not None or st.pending_order_id > 0:
                n += 1
        return n

    def _detect_signal(
        self, st: SymState, px: float, bar_h: float, bar_l: float
    ) -> tuple[str, float, bool]:
        """Return (side, orb_entry, bar_wick_touched). LONG-first like paper."""
        allow = (not FRESH_BREAKOUT) or st.prev_inside_orb
        if not allow:
            return "", 0.0, False
        if bar_h >= st.orb_high:
            return "long", st.orb_high, True
        if bar_l <= st.orb_low:
            return "short", st.orb_low, True
        if px >= st.orb_high:
            return "long", st.orb_high, False
        if px <= st.orb_low:
            return "short", st.orb_low, False
        return "", 0.0, False

    async def _paper_want_now(
        self, st: SymState, replay
    ) -> tuple[str, float]:
        """Side + ORB price paper wants right now (open pos or fresh bar touch)."""
        if replay.open_side:
            entry = replay.open_entry
            if entry <= 0:
                entry = st.orb_high if replay.open_side == "long" else st.orb_low
            return replay.open_side, entry
        if replay.trades_done >= MAX_TRADES_SYM:
            return "", 0.0
        loop = asyncio.get_running_loop()
        try:
            bar = await loop.run_in_executor(None, latest_5m_bar, st.sym, FAPI)
        except Exception:
            bar = None
        if not bar:
            return "", 0.0
        if bar.h >= st.orb_high:
            return "long", st.orb_high
        if bar.l <= st.orb_low:
            return "short", st.orb_low
        return "", 0.0

    async def _execute_entry(
        self,
        st: SymState,
        side: str,
        entry: float,
        px: float,
        bar_touch: bool,
        tag: str = "",
    ) -> bool:
        """Place live/dry entry — bar wick touch always markets (paper fill-at-touch)."""
        if entry <= 0 or not side:
            return False
        br = bracket_prices(side, entry, TP_PCT, SL_PCT)
        slip_pct = abs(px - entry) / entry * 100.0 if entry > 0 else 999.0
        suffix = f" {tag}" if tag else ""

        if not LIVE:
            st.pos = Position(st.sym, side, entry, br.tp_px, br.sl_px, bucket=st.bucket)
            log(
                f"[ENTRY] {st.sym} {side.upper()} @ {entry:.8g} "
                f"tp={br.tp_px:.8g} sl={br.sl_px:.8g} {st.bucket}{suffix} "
                f"trades={st.trades_today + 1}/{MAX_TRADES_SYM}"
            )
            return True

        if bar_touch or slip_pct <= MAX_ENTRY_SLIP_PCT:
            return await self._live_enter_market(
                st, side, entry, br, px, bar_touch=bar_touch, tag=tag
            )
        return await self._live_place_orb_limit(
            st, side, entry, br, slip_pct, px, tag=tag
        )

    async def _audit_paper_and_orders(self) -> None:
        """Every ORDER_AUDIT_SEC: align with paper path; fix stale/wrong limits."""
        if not LIVE or self.client is None:
            return
        log("[AUDIT] paper sync + open-order check")
        loop = asyncio.get_running_loop()
        for st in self.watch.values():
            if not st.orb_locked or not st.catchup_done:
                continue
            try:
                replay = await self._paper_replay_now(st)
            except Exception as e:
                log(f"[AUDIT_ERR] {st.sym} replay {e}")
                continue
            st.trades_today = replay.trades_done
            want_side, want_entry = await self._paper_want_now(st, replay)

            # Orphan / wrong resting limits on exchange
            try:
                open_orders = await loop.run_in_executor(
                    None,
                    lambda s=st.sym: self.client._signed_get(  # type: ignore[union-attr]
                        "/fapi/v1/openOrders", {"symbol": s}
                    ),
                )
            except Exception:
                open_orders = []
            for od in open_orders or []:
                if str(od.get("type") or "").upper() != "LIMIT":
                    continue
                if str(od.get("reduceOnly", "")).lower() == "true":
                    continue
                oid = int(od.get("orderId") or 0)
                oside = "long" if od.get("side") == "BUY" else "short"
                oprice = float(od.get("price") or 0)
                bad = False
                reason = ""
                if not want_side:
                    bad, reason = True, "paper_flat_done"
                elif oside != want_side:
                    bad, reason = True, f"wrong_side want={want_side}"
                elif want_entry > 0 and oprice > 0:
                    if abs(oprice - want_entry) / want_entry > 0.002:
                        bad, reason = True, "wrong_orb_price"
                if bad and oid > 0:
                    if oid == st.pending_order_id:
                        await self._cancel_pending(st, f"audit_{reason}")
                    else:
                        try:
                            await loop.run_in_executor(
                                None, self.client.cancel_order, st.sym, oid
                            )
                            log(f"[AUDIT_CANCEL] {st.sym} oid={oid} {reason}")
                        except Exception as e:
                            log(f"[AUDIT_CANCEL_ERR] {st.sym} oid={oid} {e}")

            # Paper wants position we don't have
            has_live = False
            try:
                qty = await loop.run_in_executor(
                    None, self.client.position_qty, st.sym
                )
                has_live = qty > 0
            except Exception:
                pass
            if has_live and st.pos is None:
                await self._adopt_exchange_pos(st)

            if st.pos is None and st.pending_order_id <= 0 and want_side:
                if replay.open_side:
                    await self._catchup_enter_paper_open(
                        st, want_side, want_entry, force_market=True
                    )
                elif replay.trades_done < MAX_TRADES_SYM:
                    try:
                        px = await loop.run_in_executor(
                            None, mark_price, st.sym, FAPI
                        )
                        bar = await loop.run_in_executor(
                            None, latest_5m_bar, st.sym, FAPI
                        )
                    except Exception:
                        continue
                    bar_h = bar.h if bar else px
                    bar_l = bar.l if bar else px
                    side, entry, bar_touch = self._detect_signal(st, px, bar_h, bar_l)
                    if side and side == want_side:
                        if bar and bar.ts == st.last_entry_bar_ts:
                            continue
                        if await self._paper_allows_side(st, side):
                            await self._execute_entry(
                                st, side, entry, px, bar_touch, tag="AUDIT_REENTRY"
                            )

            # Stale limit: paper still wants same side but bar already touched → market
            if (
                st.pending_order_id > 0
                and want_side
                and st.pending_side == want_side
            ):
                try:
                    bar = await loop.run_in_executor(
                        None, latest_5m_bar, st.sym, FAPI
                    )
                except Exception:
                    bar = None
                if bar:
                    touched = (
                        want_side == "long" and bar.h >= st.orb_high
                    ) or (want_side == "short" and bar.l <= st.orb_low)
                    if touched:
                        await self._cancel_pending(st, "audit_bar_touch_market")
                        try:
                            px = await loop.run_in_executor(
                                None, mark_price, st.sym, FAPI
                            )
                        except Exception:
                            px = want_entry
                        if await self._paper_allows_side(st, want_side):
                            await self._execute_entry(
                                st,
                                want_side,
                                want_entry,
                                px,
                                bar_touch=True,
                                tag="AUDIT_TOUCH",
                            )

    async def _trade_loop(self) -> None:
        for st in self.watch.values():
            if st.pos:
                await self._monitor(st)
            elif st.pending_order_id > 0:
                await self._check_pending_entry(st)
            elif st.trades_today < MAX_TRADES_SYM and self._open_count() < MAX_OPEN:
                await self._try_entry(st)

    async def _paper_replay_now(self, st: SymState):
        """Today's 5m paper path so far (open position left open)."""
        loop = asyncio.get_running_loop()
        bars = await loop.run_in_executor(
            None, bars_5m_day, st.sym, self.trade_date, FAPI
        )
        return replay_orb_state(
            bars,
            ORB_5M_BARS,
            TP_PCT,
            SL_PCT,
            MAX_TRADES_SYM,
            close_open_at_end=False,
        )

    async def _paper_allows_side(self, st: SymState, side: str) -> bool:
        """Safe gate: live may enter only if paper is on (or would open) that side."""
        if not PAPER_GATE or not LIVE:
            return True
        try:
            replay = await self._paper_replay_now(st)
        except Exception as e:
            log(f"[PAPER_GATE_ERR] {st.sym} {e} — blocking entry")
            return False
        st.trades_today = max(st.trades_today, replay.trades_done)
        if replay.trades_done >= MAX_TRADES_SYM and not replay.open_side:
            log(f"[PAPER_GATE] {st.sym} paper done {replay.trades_done}/{MAX_TRADES_SYM}")
            return False
        if replay.open_side:
            ok = replay.open_side == side
            if not ok:
                log(
                    f"[PAPER_GATE] {st.sym} block {side.upper()} — "
                    f"paper still {replay.open_side.upper()} @{replay.open_entry:.8g}"
                )
            return ok
        # Paper flat: only allow the side paper would take next (LONG-first on last bar).
        # open_side empty + our side matches a fresh breakout the replay would open
        # if we only got here because bar/mark touched — require replay would open same.
        # Re-check last bar against ORB with long-first:
        loop = asyncio.get_running_loop()
        try:
            bar = await loop.run_in_executor(None, latest_5m_bar, st.sym, FAPI)
        except Exception:
            bar = None
        want = ""
        if bar:
            if bar.h >= st.orb_high:
                want = "long"
            elif bar.l <= st.orb_low:
                want = "short"
        if not want:
            # mark-only path already set side; paper flat with no bar touch → deny
            log(f"[PAPER_GATE] {st.sym} block {side.upper()} — paper flat, no bar touch")
            return False
        if want != side:
            log(
                f"[PAPER_GATE] {st.sym} block {side.upper()} — paper next would be {want.upper()}"
            )
            return False
        return True

    async def _try_entry(self, st: SymState) -> None:
        if not st.catchup_done:
            return
        loop = asyncio.get_running_loop()
        try:
            px = await loop.run_in_executor(None, mark_price, st.sym, FAPI)
        except Exception:
            return
        bar = None
        bar_h = bar_l = px
        try:
            bar = await loop.run_in_executor(None, latest_5m_bar, st.sym, FAPI)
            if bar:
                bar_h, bar_l = bar.h, bar.l
        except Exception:
            pass
        inside = inside_orb(px, st.orb_high, st.orb_low)
        side, entry, bar_touch = self._detect_signal(st, px, bar_h, bar_l)
        st.prev_inside_orb = inside
        if not side:
            return
        if bar and bar.ts == st.last_entry_bar_ts:
            return
        if not await self._paper_allows_side(st, side):
            return
        await self._execute_entry(st, side, entry, px, bar_touch)

    async def _live_enter_market(
        self,
        st: SymState,
        side: str,
        orb_entry: float,
        br: Bracket,
        mark: float,
        *,
        bar_touch: bool = False,
        tag: str = "",
    ) -> bool:
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
                fill_slip = abs(fill_px - orb_entry) / orb_entry * 100.0
                # Bar wick touch = paper fill-at-ORB; allow wider slip (not chase).
                if not bar_touch and fill_slip > MAX_ENTRY_SLIP_PCT * 2:
                    log(
                        f"[CHASE_ABORT] {sym} fill={fill_px:.8g} orb={orb_entry:.8g} "
                        f"slip={fill_slip:.2f}% — flattening"
                    )
                    close = close_order_side(side)
                    try:
                        await loop.run_in_executor(
                            None, self.client.market_close_qty, sym, close, qty
                        )
                    except Exception as e:
                        log(f"[CHASE_ABORT_ERR] {sym} {e}")
                    return False
                st.pos = Position(
                    sym, side, orb_entry, br.tp_px, br.sl_px, qty, st.bucket
                )
                try:
                    bar = await loop.run_in_executor(
                        None, latest_5m_bar, sym, FAPI
                    )
                    if bar:
                        st.last_entry_bar_ts = bar.ts
                except Exception:
                    pass
                await self._place_exchange_brackets(st)
                extra = f" {tag}" if tag else ""
                touch_note = " bar_touch" if bar_touch else ""
                log(
                    f"[ENTRY] {sym} {side.upper()} @ {orb_entry:.8g} "
                    f"(fill={fill_px:.8g} mark={mark:.8g}{touch_note}) "
                    f"tp={br.tp_px:.8g} sl={br.sl_px:.8g} {st.bucket}{extra} "
                    f"trades={st.trades_today + 1}/{MAX_TRADES_SYM}"
                )
                return True
            except Exception as e:
                log(f"[ENTRY_ERR] {sym} {e}")
                return False

    async def _live_place_orb_limit(
        self,
        st: SymState,
        side: str,
        orb_entry: float,
        br: Bracket,
        slip_pct: float,
        mark: float,
        *,
        tag: str = "",
    ) -> bool:
        """Resting limit at ORB hi/lo — matches paper fill-at-level on retest."""
        assert self.client is not None
        async with self._lock:
            if self._open_count() >= MAX_OPEN:
                return False
            if st.pending_order_id > 0:
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
                    self.client.limit_order_notional,
                    sym,
                    entry_order_side(side),
                    NOTIONAL,
                    orb_entry,
                )
                oid = int(resp.get("orderId") or 0)
                if oid <= 0:
                    log(f"[LIMIT_ERR] {sym} no orderId {resp}")
                    return False
                st.pending_side = side
                st.pending_order_id = oid
                st.pending_orb_entry = orb_entry
                extra = f" {tag}" if tag else ""
                log(
                    f"[LIMIT] {sym} {side.upper()} @ {orb_entry:.8g} "
                    f"oid={oid} mark={mark:.8g} slip={slip_pct:.2f}% "
                    f"(waiting retest — paper parity){extra} "
                    f"tp={br.tp_px:.8g} sl={br.sl_px:.8g}"
                )
                return True
            except Exception as e:
                log(f"[LIMIT_ERR] {sym} {e}")
                return False

    async def _check_pending_entry(self, st: SymState) -> None:
        if not LIVE or st.pending_order_id <= 0 or self.client is None:
            return
        loop = asyncio.get_running_loop()
        sym = st.sym
        oid = st.pending_order_id
        try:
            od = await loop.run_in_executor(None, self.client.query_order, sym, oid)
        except Exception as e:
            log(f"[PENDING_ERR] {sym} oid={oid} {e}")
            return
        status = str(od.get("status") or "").upper()
        if status in ("NEW", "PARTIALLY_FILLED"):
            # Cancel if paper no longer wants this side (BANK-safe).
            if PAPER_GATE and st.pending_side:
                if not await self._paper_allows_side(st, st.pending_side):
                    await self._cancel_pending(st, "paper_gate")
                    return
            # Cancel if signal flipped.
            try:
                px = await loop.run_in_executor(None, mark_price, sym, FAPI)
            except Exception:
                return
            want = ""
            if px >= st.orb_high:
                want = "long"
            elif px <= st.orb_low:
                want = "short"
            if want and want != st.pending_side:
                await self._cancel_pending(st, f"side_flip_to_{want}")
            return
        if status == "FILLED":
            fill_px, qty = parse_fill(od)
            row = await loop.run_in_executor(None, self.client.position_row, sym)
            if row:
                pq = abs(float(row.get("positionAmt") or 0))
                pe = float(row.get("entryPrice") or 0)
                if pq > 0:
                    qty = pq
                if pe > 0:
                    fill_px = pe
            side = st.pending_side
            orb_entry = st.pending_orb_entry
            st.pending_side = ""
            st.pending_order_id = 0
            st.pending_orb_entry = 0.0
            if qty <= 0 or not side or orb_entry <= 0:
                log(f"[PENDING_FILLED_EMPTY] {sym}")
                return
            br = bracket_prices(side, orb_entry, TP_PCT, SL_PCT)
            st.pos = Position(sym, side, orb_entry, br.tp_px, br.sl_px, qty, st.bucket)
            try:
                bar = await loop.run_in_executor(None, latest_5m_bar, sym, FAPI)
                if bar:
                    st.last_entry_bar_ts = bar.ts
            except Exception:
                pass
            await self._place_exchange_brackets(st)
            log(
                f"[ENTRY] {sym} {side.upper()} @ {orb_entry:.8g} "
                f"(limit_fill={fill_px:.8g}) "
                f"tp={br.tp_px:.8g} sl={br.sl_px:.8g} {st.bucket} "
                f"trades={st.trades_today + 1}/{MAX_TRADES_SYM}"
            )
            return
        # CANCELED / REJECTED / EXPIRED
        log(f"[LIMIT_DONE] {sym} oid={oid} status={status}")
        st.pending_side = ""
        st.pending_order_id = 0
        st.pending_orb_entry = 0.0

    async def _cancel_pending(self, st: SymState, reason: str) -> None:
        if not LIVE or self.client is None or st.pending_order_id <= 0:
            st.pending_side = ""
            st.pending_order_id = 0
            st.pending_orb_entry = 0.0
            return
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None, self.client.cancel_order, st.sym, st.pending_order_id
            )
            log(f"[LIMIT_CANCEL] {st.sym} oid={st.pending_order_id} {reason}")
        except Exception as e:
            log(f"[LIMIT_CANCEL_ERR] {st.sym} {e}")
        st.pending_side = ""
        st.pending_order_id = 0
        st.pending_orb_entry = 0.0

    async def _place_exchange_brackets(self, st: SymState) -> None:
        if not LIVE or not EXCHANGE_BRACKETS or not st.pos or self.client is None:
            return
        pos = st.pos
        loop = asyncio.get_running_loop()
        close = close_order_side(pos.side)

        def _place() -> None:
            assert self.client is not None
            self.client.cancel_all_algo_orders(pos.sym)
            tp = pos.tp_px
            sl = self.client.safe_stop_price(pos.sym, pos.side, pos.sl_px, WORKING_TYPE)
            self.client.take_profit_market_reduce(
                pos.sym, close, tp, pos.qty, WORKING_TYPE
            )
            self.client.stop_market_reduce(pos.sym, close, sl, pos.qty, WORKING_TYPE)

        try:
            await loop.run_in_executor(None, _place)
            log(
                f"[BRACKETS] {pos.sym} tp={pos.tp_px:.8g} sl={pos.sl_px:.8g} "
                f"qty={pos.qty:.8g}"
            )
        except Exception as e:
            log(f"[BRACKETS_ERR] {pos.sym} {e}")

    async def _cancel_exchange_brackets(self, sym: str) -> None:
        if not LIVE or self.client is None:
            return
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(None, self.client.cancel_all_algo_orders, sym)
        except Exception:
            pass
        try:
            await loop.run_in_executor(None, self.client.cancel_all_open_orders, sym)
        except Exception:
            pass

    async def _monitor(self, st: SymState) -> None:
        pos = st.pos
        if not pos:
            return
        loop = asyncio.get_running_loop()

        # Exchange algo may have already flat-closed.
        if LIVE and self.client is not None:
            try:
                qty = await loop.run_in_executor(None, self.client.position_qty, pos.sym)
            except Exception:
                qty = -1.0
            if qty == 0:
                try:
                    px = await loop.run_in_executor(None, mark_price, pos.sym, FAPI)
                except Exception:
                    px = pos.entry
                if pos.side == "long":
                    if px >= pos.tp_px * 0.998:
                        reason = "TP"
                    elif px <= pos.sl_px * 1.002:
                        reason = "SL"
                    else:
                        reason = "EXCHANGE_FLAT"
                else:
                    if px <= pos.tp_px * 1.002:
                        reason = "TP"
                    elif px >= pos.sl_px * 0.998:
                        reason = "SL"
                    else:
                        reason = "EXCHANGE_FLAT"
                await self._exit(st, reason, px, already_flat=True)
                return

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

    async def _exit(
        self,
        st: SymState,
        reason: str,
        mark: float,
        already_flat: bool = False,
    ) -> None:
        pos = st.pos
        if not pos:
            return
        exit_px = mark
        if LIVE:
            assert self.client is not None
            loop = asyncio.get_running_loop()
            await self._cancel_exchange_brackets(pos.sym)
            if not already_flat:
                close = close_order_side(pos.side)
                for _ in range(FLATTEN_RETRIES):
                    qty = await loop.run_in_executor(
                        None, self.client.position_qty, pos.sym
                    )
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
            try:
                exit_px = await loop.run_in_executor(
                    None, self.client.mark_price, pos.sym
                )
            except Exception:
                exit_px = mark

        pnl = pnl_usd(pos.side, pos.entry, exit_px, NOTIONAL, FEE_RT)
        self.session_pnl += pnl
        self.day_pnl += pnl
        self.day_trades += 1
        if pnl > 0:
            self.day_wins += 1
        st.trades_today += 1
        st.pos = None
        # After exit, require a return inside ORB before next stack when far —
        # prev_inside_orb=False blocks fresh mode; with fresh=false we still
        # rely on slip/limit so chase cannot market-fill deep.
        try:
            px_now = exit_px
            st.prev_inside_orb = inside_orb(px_now, st.orb_high, st.orb_low)
        except Exception:
            st.prev_inside_orb = False
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
        # Paper re-enters on next ORB touch in same bar or following bars.
        if st.trades_today < MAX_TRADES_SYM:
            await self._try_entry(st)

    async def _force_close_symbol(self, sym: str, reason: str) -> None:
        if not LIVE or self.client is None:
            return
        loop = asyncio.get_running_loop()
        await self._cancel_exchange_brackets(sym)
        for _ in range(FLATTEN_RETRIES):
            try:
                row = await loop.run_in_executor(None, self.client.position_row, sym)
            except Exception as e:
                log(f"[FORCE_CLOSE_ERR] {sym} {e}")
                break
            if not row:
                break
            amt = float(row.get("positionAmt") or 0)
            if abs(amt) <= 0:
                break
            side = "SELL" if amt > 0 else "BUY"
            qty = abs(amt)
            try:
                await loop.run_in_executor(
                    None, self.client.market_close_qty, sym, side, qty
                )
                log(f"[FORCE_CLOSE] {sym} {reason} qty={qty}")
            except Exception as e:
                log(f"[FORCE_CLOSE_ERR] {sym} {e}")
            self.client.invalidate_position_cache()
            await asyncio.sleep(0.35)

    async def _flatten_symbol_list(self, syms: list[str], reason: str) -> None:
        for sym in syms:
            await self._force_close_symbol(sym, reason)

    async def _flatten_all(self, reason: str) -> None:
        for st in list(self.watch.values()):
            if st.pending_order_id > 0:
                await self._cancel_pending(st, reason)
            if st.pos:
                loop = asyncio.get_running_loop()
                try:
                    px = await loop.run_in_executor(None, mark_price, st.pos.sym, FAPI)
                except Exception:
                    px = st.pos.entry
                await self._exit(st, reason, px)
            elif LIVE:
                await self._force_close_symbol(st.sym, reason)


async def main() -> None:
    bot = Orb30Bot()
    try:
        await bot.run()
    finally:
        if bot.trade_date and bot.day_trades > 0:
            bot._log_day_end()


if __name__ == "__main__":
    argparse.ArgumentParser(description="ORB 30m top-mover bot (dry/live)").parse_args()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("[STOP] interrupted")
