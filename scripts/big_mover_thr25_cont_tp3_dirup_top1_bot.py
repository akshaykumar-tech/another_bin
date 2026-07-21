#!/usr/bin/env python3
"""
Big-mover thr25 CONT + dir=up + TP3, dry (backtest/paper) + optional live.

Strategy definition:
  - thr = 25% above day open
  - direction = up only => first non-ambiguous threshold touch must be UP
  - CONT => UP touch => LONG
  - Entry: LONG @ OPEN*(1+thr) (LIMIT trade-through parity)
  - Exit: TP3% (if price touches TP level) else flatten at EOD
  - top1 selection:
      - In dry/backtest: choose by top1-mode (move_abs or first_fill)
      - In live: uses *first-fill* because "move_abs top1/day" isn't knowable before day ends.

Run:
  Dry/backtest:
    python3 scripts/big_mover_thr25_cont_tp3_dirup_top1_bot.py --backtest 2026-06-01 2026-07-18

  Live (paper):
    BM25_LIVE_ENABLED=true + BINANCE_API_KEY/BINANCE_API_SECRET
    python3 scripts/big_mover_thr25_cont_tp3_dirup_top1_bot.py
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

from binance_futures import BinanceFuturesClient  # noqa: E402
from live_config_lib import binance_api_key, binance_api_secret  # noqa: E402
from orb30_engine import (
    DAY_MS,
    day_ms,
    fetch_daily_range,
    latest_5m_bar,
    list_syms,
    through,
    utc_today,
)  # noqa: E402

from big_mover_thr25_cont_tp3_dirup_top1_engine import backtest_range  # noqa: E402


def load_dotenv(path: str = ".env") -> None:
    """Lightweight dotenv loader (same pattern as other bots)."""
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

# ---- Env knobs (prefix BM25_ = BigMover thr25) ----
BM_LIVE = _env_bool("BM25_LIVE_ENABLED", False)
BM_FAPI = _env("BM25_FAPI", "https://fapi.binance.com").rstrip("/")
BM_OUT_DIR = Path(_env("BM25_OUT_DIR", str(ROOT / "data/aws/bm_thr25_cont_tp3_dirup_top1")))
BM_NOTIONAL = _env_float("BM25_NOTIONAL_USDT", 6.0)
BM_THR_PCT = _env_float("BM25_THR_PCT", 25.0)
BM_TP_PCT = _env_float("BM25_TP_PCT", 3.0)
BM_UNIVERSE_LIMIT = _env_int("BM25_UNIVERSE_LIMIT", 50)  # keep live order-count manageable
BM_POLL_SEC = _env_float("BM25_POLL_SEC", 10.0)
BM_SCAN_DELAY_SEC = _env_float("BM25_SCAN_DELAY_SEC", 0.0)
BM_FEE_RT = _env_float("BM25_FEE_RT", 0.0008)
BM_LEVERAGE_CAP = _env_int("BM25_LEVERAGE_CAP", 20)
BM_FLATTEN_RETRIES = _env_int("BM25_FLATTEN_RETRIES", 4)

# Logger / CSVs
LOG_FILE = BM_OUT_DIR / ("bm_thr25_cont_tp3_dirup_top1_live.log" if BM_LIVE else "bm_thr25_cont_tp3_dirup_top1_dry.log")
TRADES_CSV = BM_OUT_DIR / ("bm_thr25_cont_tp3_dirup_top1_live_trades.csv" if BM_LIVE else "bm_thr25_cont_tp3_dirup_top1_dry_trades.csv")
DAILY_CSV = BM_OUT_DIR / ("bm_thr25_cont_tp3_dirup_top1_live_daily.csv" if BM_LIVE else "bm_thr25_cont_tp3_dirup_top1_dry_daily.csv")

MODE = "LIVE" if BM_LIVE else "DRY"


@dataclass
class Position:
    sym: str
    side: str  # long
    entry_date: str
    entry: float
    qty: float
    tp_placed: bool = False


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def log(msg: str) -> None:
    line = f"{utc_now()} [{MODE}] {msg}"
    print(line, flush=True)
    BM_OUT_DIR.mkdir(parents=True, exist_ok=True)
    with LOG_FILE.open("a") as f:
        f.write(line + "\n")


def append_csv(path: Path, row: dict, fieldnames: list[str]) -> None:
    BM_OUT_DIR.mkdir(parents=True, exist_ok=True)
    new = not path.is_file()
    with path.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if new:
            w.writeheader()
        w.writerow(row)


def _day_start_epoch(d: str) -> float:
    return day_ms(d) / 1000.0


class BigMoverThr25ContTp3DirUpTop1Bot:
    def __init__(self) -> None:
        self.client: BinanceFuturesClient | None = None
        self.trade_date = ""
        self.pending: dict[str, float] = {}  # sym -> entry_level_px
        self.day_open: dict[str, float] = {}  # sym -> open_px
        self.position: Position | None = None
        self.session_pnl = 0.0
        self.day_pnl = 0.0
        self.day_trades = 0
        self.day_wins = 0
        self._placed_orders = False
        self._lock = asyncio.Lock()

    def init_live(self) -> None:
        if not BM_LIVE:
            return
        key, sec = binance_api_key(), binance_api_secret()
        if not key or not sec:
            raise RuntimeError("BINANCE_API_KEY / BINANCE_API_SECRET missing")
        self.client = BinanceFuturesClient(key, sec, BM_FAPI)
        self.client.warm_cache()

    async def run(self) -> None:
        self.init_live()
        log(f"start | thr={BM_THR_PCT}% cont dir=up tp={BM_TP_PCT}% top1=first_fill in LIVE | NOTIONAL=${BM_NOTIONAL}")
        while True:
            if self.session_pnl <= -(15.0):
                log(f"[STOP] session loss ${self.session_pnl:.2f}")
                await asyncio.sleep(120)
                continue
            await self._tick_day()
            await self._poll_once()
            await asyncio.sleep(BM_POLL_SEC)

    async def _tick_day(self) -> None:
        today = utc_today()
        if today == self.trade_date:
            return
        if self.trade_date:
            log(f"[ROLLOVER] {self.trade_date} → {today} flatten EOD")
            await self._exit_all("EOD")
            self._log_day_end()

        self.trade_date = today
        self.pending = {}
        self.day_open = {}
        self.position = None
        self.session_pnl = 0.0
        self.day_pnl = 0.0
        self.day_trades = 0
        self.day_wins = 0
        self._placed_orders = False
        log(f"[NEW_DAY] {today} UTC — scan after {BM_SCAN_DELAY_SEC:.0f}s")

    async def _load_daily_opens_and_place_orders(self) -> None:
        assert self.trade_date
        syms = list_syms()
        if BM_UNIVERSE_LIMIT and BM_UNIVERSE_LIMIT > 0:
            syms = syms[: BM_UNIVERSE_LIMIT]

        # Load day open prices (1d candle open) for each symbol.
        loop = asyncio.get_running_loop()

        def load_one(sym: str) -> tuple[str, float] | None:
            bars = fetch_daily_range(sym, self.trade_date, self.trade_date, BM_FAPI)
            if not bars:
                return None
            b = bars[-1]
            if b.o <= 0:
                return None
            return sym, float(b.o)

        pairs: list[tuple[str, float]] = []
        tasks = [loop.run_in_executor(None, load_one, sym) for sym in syms]
        for fut in asyncio.as_completed(tasks):
            try:
                r = await fut
                if r:
                    pairs.append(r)
            except Exception:
                pass

        self.pending = {}
        self.day_open = {}
        for sym, o in pairs:
            lvl = o * (1.0 + BM_THR_PCT / 100.0)
            if self.client:
                lvl = float(self.client.round_price(sym, lvl))
            self.pending[sym] = lvl
            self.day_open[sym] = o

        if not pairs:
            log("[NO_UNIVERSE] no daily opens loaded")
            return

        if self.client and not BM_LIVE:
            raise RuntimeError("client must not exist in DRY mode")

        if BM_LIVE:
            assert self.client is not None
            # Place GTC LIMIT orders for all pending symbols. First fill wins.
            # Universe limit must stay small to avoid order-count/rate issues.
            log(f"[ORDERS] placing {len(self.pending)} LIMIT BUY orders @ OPEN+thr")
            for sym, lvl in list(self.pending.items()):
                try:
                    # set leverage best-effort
                    try:
                        self.client.set_max_leverage(sym, BM_LEVERAGE_CAP)
                    except Exception:
                        pass
                    self.client.limit_order_notional(sym, "BUY", BM_NOTIONAL, lvl)
                except Exception as e:
                    log(f"[ORDER_FAIL] {sym}: {e}")
                    self.pending.pop(sym, None)

        else:
            log(f"[PAPER] pending {len(self.pending)} virtual orders (no Binance orders)")

        self._placed_orders = True

    async def _poll_once(self) -> None:
        if not self.trade_date:
            return
        if not self._placed_orders:
            if time.time() - _day_start_epoch(self.trade_date) >= BM_SCAN_DELAY_SEC:
                async with self._lock:
                    if not self._placed_orders:
                        await self._load_daily_opens_and_place_orders()
            return

        now_ms = int(time.time() * 1000)
        day_end_ms = day_ms(self.trade_date) + DAY_MS - 60_000
        if now_ms >= day_end_ms and self.position is not None:
            await self._exit_all("EOD_GUARD")
            self._log_day_end()
            return

        # Entry fill detection
        if self.position is None:
            if BM_LIVE:
                await self._scan_live_entry()
            else:
                await self._scan_paper_entry()
        else:
            if BM_LIVE:
                await self._scan_live_tp_exit()
            else:
                await self._scan_paper_tp_exit()

    async def _scan_live_entry(self) -> None:
        assert self.client is not None
        syms = self.client.open_position_symbols()
        if not syms:
            return

        # First fill wins. If more than one position opened, close extras immediately.
        chosen = sorted(syms)[0]
        extras = [s for s in syms if s != chosen]
        for s in extras:
            try:
                qty = await asyncio.get_running_loop().run_in_executor(None, self.client.position_qty, s)
                if qty > 0:
                    await asyncio.get_running_loop().run_in_executor(None, self.client.market_close_qty, s, "SELL", qty)
            except Exception:
                pass

        # Cancel all pending open orders (best effort)
        for s in list(self.pending.keys()):
            try:
                self.client.cancel_all_open_orders(s)
            except Exception:
                pass
        self.pending = {}

        # Capture entry from Binance position row.
        try:
            qty = self.client.position_qty(chosen)
            row = self.client.position_row(chosen) or {}
            pe = float(row.get("entryPrice") or 0)
            if pe <= 0:
                pe = self.client.mark_price(chosen)
        except Exception as e:
            log(f"[ENTRY_CAPTURE_FAIL] {chosen}: {e}")
            return

        self.position = Position(
            sym=chosen,
            side="long",
            entry_date=self.trade_date,
            entry=pe,
            qty=qty,
            tp_placed=False,
        )
        log(f"[FILL] {chosen} LONG @ {pe:.8g} qty={qty:.6g}")

        # Place TP reduce-only algo
        tp_px = pe * (1.0 + BM_TP_PCT / 100.0)
        tp_px = float(self.client.round_price(chosen, tp_px))
        try:
            close_side = "SELL"  # close long
            self.client.take_profit_market_reduce(chosen, close_side, tp_px, qty)
            self.position.tp_placed = True
            log(f"[TP_PLACED] {chosen} TP@{tp_px:.8g}")
        except Exception as e:
            log(f"[TP_PLACE_FAIL] {chosen}: {e}")

    async def _scan_live_tp_exit(self) -> None:
        assert self.client is not None
        assert self.position is not None
        open_syms = self.client.open_position_symbols()
        if self.position.sym not in open_syms:
            # TP filled (or manually closed)
            # Exit price = take from position close is not available easily, so mark_price is used.
            try:
                exit_px = self.client.mark_price(self.position.sym)
            except Exception:
                exit_px = self.position.entry
            await self._exit_one(self.position.sym, "TP", exit_px)
            return

    async def _scan_paper_entry(self) -> None:
        # Check virtual orders by polling latest forming 5m candle for each pending symbol.
        # First non-ambiguous UP touch wins.
        if not self.pending:
            return
        # Fetch in a small parallel batch for speed.
        syms = list(self.pending.keys())
        loop = asyncio.get_running_loop()

        def fetch_check(sym: str) -> tuple[str, float] | None:
            lvl = self.pending.get(sym, 0.0)
            o = self.day_open.get(sym, 0.0)
            if lvl <= 0 or o <= 0:
                return None
            dn_lvl = o * (1.0 - BM_THR_PCT / 100.0)
            b = latest_5m_bar(sym, BM_FAPI)
            if not b:
                return None
            # ambiguous bar: touched both sides
            if through(b, lvl) and through(b, dn_lvl):
                return None
            # direction up: hit UP but not DOWN
            if through(b, lvl) and b.l > dn_lvl:
                return sym, lvl
            return None

        tasks = [loop.run_in_executor(None, fetch_check, sym) for sym in syms]
        done = [await t for t in asyncio.as_completed(tasks)]
        filled = [r for r in done if r]
        if not filled:
            return

        # If multiple triggers in same poll, pick the highest UP move (approx by entry level same anyway)
        sym, entry = filled[0]
        self.pending = {}
        self.position = Position(
            sym=sym,
            side="long",
            entry_date=self.trade_date,
            entry=entry,
            qty=0.0,
            tp_placed=True,  # paper; we just check TP touch
        )
        log(f"[PAPER_FILL] {sym} LONG @ {entry:.8g}")

    async def _scan_paper_tp_exit(self) -> None:
        assert self.position is not None
        b = latest_5m_bar(self.position.sym, BM_FAPI)
        if not b:
            return
        tp_px = self.position.entry * (1.0 + BM_TP_PCT / 100.0)
        if through(b, tp_px):
            await self._exit_one(self.position.sym, "TP", tp_px)

    async def _exit_all(self, reason: str) -> None:
        if self.position is None:
            return
        await self._exit_one(self.position.sym, reason, self.position.entry)
        self.position = None

    async def _exit_one(self, sym: str, reason: str, exit_px_guess: float) -> None:
        pos = self.position
        if not pos or pos.sym != sym:
            return

        loop = asyncio.get_running_loop()
        exit_px = exit_px_guess
        if BM_LIVE and self.client:
            try:
                # best-effort exact-ish exit: mark price
                exit_px = self.client.mark_price(sym)
            except Exception:
                pass
            try:
                # cancel TP algo to prevent double-close (best effort)
                try:
                    self.client.cancel_tp_algo_orders(sym)
                except Exception:
                    pass
                qty = await loop.run_in_executor(None, self.client.position_qty, sym)
                if qty > 0:
                    await loop.run_in_executor(
                        None,
                        self.client.market_close_qty,
                        sym,
                        "SELL",  # close long
                        qty,
                    )
            except Exception as e:
                log(f"[CLOSE_ERR] {sym}: {e}")
        else:
            # paper: if exiting at EOD, use last close of day.
            if reason.startswith("EOD"):
                try:
                    from orb30_engine import bars_5m_day

                    bars = bars_5m_day(sym, self.trade_date, BM_FAPI)
                    if bars:
                        exit_px = bars[-1].c
                except Exception:
                    pass

        usd = BM_NOTIONAL * ((exit_px - pos.entry) / pos.entry) - BM_NOTIONAL * BM_FEE_RT
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
                "side": pos.side,
                "entry": pos.entry,
                "exit": exit_px,
                "pnl_usd": round(usd, 6),
                "reason": reason,
                "mode": MODE,
            },
            ["ts", "trade_date", "sym", "side", "entry", "exit", "pnl_usd", "reason", "mode"],
        )
        log(f"[EXIT] {sym} {pos.side} entry={pos.entry:.8g} exit={exit_px:.8g} pnl=${usd:+.3f} {reason}")

        # Clear any remaining pending orders (best effort)
        if BM_LIVE and self.client:
            for s in list(self.pending.keys()):
                try:
                    self.client.cancel_all_open_orders(s)
                except Exception:
                    pass
        self.pending = {}

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
        log(f"[DAY_END] {self.trade_date} trades={self.day_trades} WR={wr:.0f}% pnl=${self.day_pnl:+.2f}")


def run_backtest(start: str, end: str, top1_mode: str) -> None:
    trades = backtest_range(
        start,
        end,
        thr_pct=BM_THR_PCT,
        tp_pct=BM_TP_PCT,
        notional=BM_NOTIONAL,
        fee_rt=BM_FEE_RT,
        top1_mode=top1_mode,  # type: ignore[arg-type]
        universe_limit=BM_UNIVERSE_LIMIT,
    )
    if not trades:
        print("No trades")
        return

    tot = sum(t.pnl for t in trades)
    wr = 100.0 * sum(1 for t in trades if t.pnl > 0) / len(trades)
    print(f"Backtest {start}→{end} N={len(trades)} total=${tot:+.2f} WR={wr:.1f}%")

    BM_OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = BM_OUT_DIR / f"bm_thr25_cont_tp3_dirup_top1_backtest_{top1_mode}_{start}_to_{end}.csv"
    with out.open("w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "trade_date",
                "sym",
                "entry",
                "exit",
                "entry_bar_i",
                "move_abs_pct",
                "reason",
                "pnl",
            ],
        )
        w.writeheader()
        for t in trades:
            w.writerow(
                {
                    "trade_date": t.trade_date,
                    "sym": t.sym,
                    "entry": t.entry,
                    "exit": t.exit,
                    "entry_bar_i": t.entry_bar_i,
                    "move_abs_pct": t.move_abs_pct,
                    "reason": t.reason,
                    "pnl": round(t.pnl, 6),
                }
            )
    print(f"wrote {out}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--backtest", nargs=2, metavar=("START", "END"))
    ap.add_argument("--top1-mode", default="move_abs", choices=["move_abs", "first_fill"])
    args = ap.parse_args()

    if args.backtest:
        start, end = args.backtest
        run_backtest(start, end, args.top1_mode)
        return

    bot = BigMoverThr25ContTp3DirUpTop1Bot()
    asyncio.run(bot.run())


if __name__ == "__main__":
    main()

