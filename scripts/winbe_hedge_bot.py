#!/usr/bin/env python3
"""WinBE hedge dry-run — TP3 + TP5 + TP8 together.

Prev-day |c2c| ≥ 20% → D+1 open LONG+SHORT ($6/leg each variant).
First TP → other arm BE @ entry (next 5m bar). Else EOD.

Dry only for now (no live orders). Day-end prints separate stats per TP.

Run:
  python3 scripts/winbe_hedge_bot.py
  python3 scripts/winbe_hedge_bot.py --once   # single scan+manage cycle then exit
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

from orb30_engine import (
    BAR_MS,
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
    DEFAULT_NOTIONAL,
    DEFAULT_THR,
    DEFAULT_TPS,
    HedgePos,
    Signal,
    apply_bar,
    close_eod,
    make_books_for_signal,
    scan_signals_for_trade_day,
    variant_name,
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

FAPI = _env("WINBE_FAPI", "https://fapi.binance.com").rstrip("/")
OUT_DIR = Path(_env("WINBE_OUT_DIR", str(ROOT / "data/aws/winbe_hedge")))
# Dry-only for multi-day test; live later after picking one TP.
LIVE = False
NOTIONAL = _env_float("WINBE_NOTIONAL_USDT", DEFAULT_NOTIONAL)
THR = _env_float("WINBE_THR", DEFAULT_THR)
FEE_RT = _env_float("WINBE_FEE_RT", DEFAULT_FEE_RT)
MAX_OPEN = _env_int("WINBE_MAX_OPEN_PER_VARIANT", 40)
POLL_SEC = _env_float("WINBE_POLL_SEC", 30.0)
SCAN_DELAY_SEC = _env_float("WINBE_SCAN_DELAY_SEC", 90.0)
EXIT_BEFORE_MIDNIGHT_SEC = _env_float("WINBE_EXIT_BEFORE_MIDNIGHT_SEC", 120.0)
DAILY_LOOKBACK = _env_int("WINBE_DAILY_LOOKBACK_DAYS", 10)
UNIVERSE_LIMIT = _env_int("WINBE_UNIVERSE_LIMIT", 0)
# false = mid-day start pe day-open + purani 5m bars replay mat karo;
# entry = live mark, manage sirf aage ke bars.
REPLAY_TODAY = _env_bool("WINBE_REPLAY_TODAY", False)

_tps_raw = _env("WINBE_TPS", "3,5,8")
TPS: tuple[float, ...] = tuple(
    float(x.strip()) for x in _tps_raw.split(",") if x.strip()
) or DEFAULT_TPS
VARIANTS = [variant_name(t) for t in TPS]

LOG_FILE = OUT_DIR / "winbe_dry.log"
TRADES_CSV = OUT_DIR / "winbe_dry_trades.csv"
DAILY_CSV = OUT_DIR / "winbe_dry_daily.csv"
WATCH_CSV = OUT_DIR / "winbe_dry_watch.csv"
STATE_FILE = OUT_DIR / "winbe_dry_state.json"
MODE = "dry"


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


class WinBEDryBot:
    def __init__(self) -> None:
        self.trade_date = ""
        # key = f"{variant}:{sym}"
        self.positions: dict[str, HedgePos] = {}
        self.entered_today: set[str] = set()  # variant:sym
        self._scanned = False
        self._eod_done = False
        self._daily_cache: dict[str, list[DayBar]] = {}
        # per-variant day stats (closed hedges today)
        self.day_pnl: dict[str, float] = {v: 0.0 for v in VARIANTS}
        self.day_trades: dict[str, int] = {v: 0 for v in VARIANTS}
        self.day_wins: dict[str, int] = {v: 0 for v in VARIANTS}
        self._bar_cursor: dict[str, int] = {}  # sym -> next bar index to process
        self._lock = asyncio.Lock()

    def _save_state(self) -> None:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        payload = {
            "trade_date": self.trade_date,
            "positions": [asdict(p) for p in self.positions.values()],
            "entered_today": sorted(self.entered_today),
            "scanned": self._scanned,
            "eod_done": self._eod_done,
            "day_pnl": self.day_pnl,
            "day_trades": self.day_trades,
            "day_wins": self.day_wins,
            "bar_cursor": self._bar_cursor,
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
                self.positions[row["variant"] + ":" + row["sym"]] = HedgePos(**{
                    k: row[k]
                    for k in HedgePos.__dataclass_fields__
                    if k in row
                })
            except Exception:
                continue
        self.entered_today = set(payload.get("entered_today") or [])
        self._scanned = bool(payload.get("scanned"))
        self._eod_done = bool(payload.get("eod_done"))
        self.day_pnl = {v: float((payload.get("day_pnl") or {}).get(v, 0)) for v in VARIANTS}
        self.day_trades = {v: int((payload.get("day_trades") or {}).get(v, 0)) for v in VARIANTS}
        self.day_wins = {v: int((payload.get("day_wins") or {}).get(v, 0)) for v in VARIANTS}
        self._bar_cursor = dict(payload.get("bar_cursor") or {})
        log(
            f"[STATE] loaded open={len(self.positions)} "
            f"trade_date={self.trade_date or '-'}"
        )

    async def run(self, once: bool = False) -> None:
        self._load_state()
        log(
            f"start DRY | thr≥{THR}% | TPs={list(TPS)} | ${NOTIONAL}/leg "
            f"(${NOTIONAL*2}/hedge/variant) | max_open/var={MAX_OPEN} | "
            f"poll={POLL_SEC}s scan_delay={SCAN_DELAY_SEC}s | "
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
            # EOD already flattened+logged near midnight; only catch stragglers.
            if self.positions:
                await self._flatten_all("ROLLOVER")
            if not self._eod_done:
                self._log_day_end()
        self.trade_date = today
        self.entered_today = set()
        self._scanned = False
        self._eod_done = False
        self._daily_cache = {}
        self._bar_cursor = {}
        self.day_pnl = {v: 0.0 for v in VARIANTS}
        self.day_trades = {v: 0 for v in VARIANTS}
        self.day_wins = {v: 0 for v in VARIANTS}
        log(f"[NEW_DAY] {today} UTC — scan after {SCAN_DELAY_SEC:.0f}s from open")

    async def _poll_once(self) -> None:
        if not self.trade_date:
            return
        now = time.time()
        day_start = _day_start_epoch(self.trade_date)
        elapsed = now - day_start
        day_end = day_start + 86_400.0

        if not self._scanned and elapsed >= SCAN_DELAY_SEC:
            await self._scan_and_enter()

        if self.positions:
            await self._manage_open()

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

    async def _scan_and_enter(self) -> None:
        try:
            if not self._daily_cache:
                await self._load_daily_cache()
            cands = scan_signals_for_trade_day(
                self.trade_date, self._daily_cache, thr=THR
            )
        except Exception as e:
            log(f"[SCAN_ERR] {e}")
            return

        self._scanned = True
        log(f"[SCAN] thr≥{THR}% signals={len(cands)} → open 3 variants each")

        for s in cands:
            append_csv(
                WATCH_CSV,
                {
                    "ts": utc_now(),
                    "trade_date": self.trade_date,
                    "sym": s.sym,
                    "signal_date": s.signal_date,
                    "move_pct": round(s.move_pct, 4),
                },
                ["ts", "trade_date", "sym", "signal_date", "move_pct"],
            )
            await self._enter_all_variants(s)

    async def _enter_all_variants(self, s: Signal) -> None:
        loop = asyncio.get_running_loop()
        async with self._lock:
            try:
                bars = await loop.run_in_executor(
                    None, lambda: bars_5m_day(s.sym, self.trade_date, FAPI)
                )
            except Exception:
                bars = []

            # Near UTC open + REPLAY: use day open (backtest-like).
            # Mid-day / default: live mark, no history catch-up.
            day_elapsed = time.time() - _day_start_epoch(self.trade_date)
            use_day_open = REPLAY_TODAY and day_elapsed <= max(SCAN_DELAY_SEC + 300.0, 600.0)
            entry = 0.0
            entry_mode = "mark"
            if use_day_open and bars and bars[0].o > 0:
                entry = bars[0].o
                entry_mode = "day_open"
            else:
                try:
                    entry = await loop.run_in_executor(
                        None, lambda: mark_price(s.sym, FAPI)
                    )
                    entry_mode = "mark"
                except Exception as e:
                    log(f"[ENTRY_SKIP] {s.sym} no price: {e}")
                    return
            if entry <= 0:
                log(f"[ENTRY_SKIP] {s.sym} bad entry")
                return

            books = make_books_for_signal(s, entry, TPS)
            opened = 0
            for pos in books:
                k = pos.key()
                if k in self.positions or k in self.entered_today:
                    continue
                n_var = sum(1 for p in self.positions.values() if p.variant == pos.variant)
                if n_var >= MAX_OPEN:
                    log(f"[SKIP_MAX] {pos.variant} {s.sym} open={n_var}")
                    continue
                self.positions[k] = pos
                self.entered_today.add(k)
                opened += 1
                log(
                    f"[OPEN] {pos.variant} {s.sym} L+S @ {entry:.8g} "
                    f"mode={entry_mode} move={s.move_pct:+.1f}% (sig={s.signal_date})"
                )
            if opened:
                # Replay off: skip already-finished bars (sirf aage ka live).
                # Replay on + day open: start from 0 to match backtest.
                if use_day_open:
                    self._bar_cursor[s.sym] = 0
                else:
                    self._bar_cursor[s.sym] = len(bars) if bars else 0
                    log(
                        f"[NO_REPLAY] {s.sym} skip {self._bar_cursor[s.sym]} past bars; "
                        f"manage from next 5m only"
                    )

    async def _manage_open(self) -> None:
        if not self.positions:
            return
        loop = asyncio.get_running_loop()
        syms = sorted({p.sym for p in self.positions.values() if not p.closed})
        for sym in syms:
            try:
                bars = await loop.run_in_executor(
                    None, lambda s=sym: bars_5m_day(s, self.trade_date, FAPI)
                )
            except Exception as e:
                log(f"[BARS_ERR] {sym} {e}")
                continue
            if not bars:
                continue
            # Only fully closed bars except allow last forming for dry (use completed)
            # Use all bars so far; apply_bar is idempotent via cursor
            start_i = self._bar_cursor.get(sym, 0)
            # Leave the current forming bar out if still open day — use len-1 when mid-day
            # For simplicity process all available bars; re-processing avoided by cursor
            end_i = len(bars)
            for i in range(start_i, end_i):
                b = bars[i]
                # apply to every open variant on this sym
                for pos in list(self.positions.values()):
                    if pos.sym != sym or pos.closed:
                        continue
                    done = apply_bar(
                        pos, i, b.h, b.l, notional=NOTIONAL, fee_rt=FEE_RT
                    )
                    if done:
                        self._record_close(pos)
            self._bar_cursor[sym] = end_i

    def _record_close(self, pos: HedgePos) -> None:
        k = pos.key()
        v = pos.variant
        self.day_trades[v] = self.day_trades.get(v, 0) + 1
        self.day_pnl[v] = self.day_pnl.get(v, 0.0) + pos.pnl
        if pos.pnl > 0:
            self.day_wins[v] = self.day_wins.get(v, 0) + 1
        path = f"{pos.long_reason}+{pos.short_reason}"
        append_csv(
            TRADES_CSV,
            {
                "ts": utc_now(),
                "trade_date": pos.trade_date,
                "variant": pos.variant,
                "sym": pos.sym,
                "signal_date": pos.signal_date,
                "move_pct": round(pos.move_pct, 4),
                "entry": pos.entry,
                "long_exit": pos.long_exit,
                "long_reason": pos.long_reason,
                "short_exit": pos.short_exit,
                "short_reason": pos.short_reason,
                "path": path,
                "pnl_usd": round(pos.pnl, 6),
                "mode": MODE,
            },
            [
                "ts", "trade_date", "variant", "sym", "signal_date", "move_pct",
                "entry", "long_exit", "long_reason", "short_exit", "short_reason",
                "path", "pnl_usd", "mode",
            ],
        )
        log(
            f"[CLOSE] {pos.variant} {pos.sym} path={path} "
            f"pnl=${pos.pnl:+.3f} (L {pos.long_reason}@{pos.long_exit:.8g} "
            f"S {pos.short_reason}@{pos.short_exit:.8g})"
        )
        if k in self.positions:
            del self.positions[k]

    async def _flatten_all(self, reason: str) -> None:
        if not self.positions:
            return
        loop = asyncio.get_running_loop()
        for pos in list(self.positions.values()):
            if pos.closed:
                continue
            try:
                px = await loop.run_in_executor(
                    None, lambda s=pos.sym: mark_price(s, FAPI)
                )
            except Exception:
                px = pos.entry
            if px <= 0:
                px = pos.entry
            close_eod(pos, px, notional=NOTIONAL, fee_rt=FEE_RT)
            # override reason tag if still EOD label is fine; note flatten reason in log
            self._record_close(pos)
            log(f"[FLATTEN] {pos.variant} {pos.sym} via {reason}")

    def _log_day_end(self) -> None:
        log(f"========== DAY_END {self.trade_date} (separate per TP) ==========")
        for v in VARIANTS:
            n = self.day_trades.get(v, 0)
            tot = self.day_pnl.get(v, 0.0)
            wins = self.day_wins.get(v, 0)
            wr = 100.0 * wins / n if n else 0.0
            open_n = sum(1 for p in self.positions.values() if p.variant == v)
            append_csv(
                DAILY_CSV,
                {
                    "date": self.trade_date,
                    "variant": v,
                    "trades": n,
                    "wins": wins,
                    "wr": round(wr, 2),
                    "pnl": round(tot, 4),
                    "open_left": open_n,
                    "mode": MODE,
                },
                ["date", "variant", "trades", "wins", "wr", "pnl", "open_left", "mode"],
            )
            log(
                f"  [{v}] trades={n} wins={wins} WR={wr:.0f}% "
                f"pnl=${tot:+.2f} open_left={open_n}"
            )
        log("===============================================================")


def main() -> None:
    ap = argparse.ArgumentParser(description="WinBE hedge dry-run (TP3+TP5+TP8)")
    ap.add_argument(
        "--once",
        action="store_true",
        help="One poll cycle then exit (scan if due, manage, save state)",
    )
    args = ap.parse_args()
    asyncio.run(WinBEDryBot().run(once=args.once))


if __name__ == "__main__":
    main()
