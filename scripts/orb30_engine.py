"""ORB 30m breakout on top-mover next-day symbols — shared engine."""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

FAPI_DEFAULT = "https://fapi.binance.com"
DAY_MS = 86_400_000
ORB_MS = 30 * 60 * 1000


@dataclass
class DayBar:
    date: str
    o: float
    h: float
    l: float
    c: float


@dataclass
class MoverSignal:
    sym: str
    signal_date: str
    trade_date: str
    bucket: str
    signal_pct: float


@dataclass
class Bracket:
    tp_px: float
    sl_px: float


def req_json(url: str):
    req = urllib.request.Request(url, headers={"User-Agent": "orb30"})
    for attempt in range(8):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code in (418, 429) and attempt < 7:
                time.sleep(min(2 ** (attempt + 1), 20))
                continue
            raise
        except Exception:
            time.sleep(min(2**attempt, 8))
    raise RuntimeError(url)


def utc_date(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def utc_today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def day_ms(d: str) -> int:
    y, m, day = map(int, d.split("-"))
    return int(datetime(y, m, day, tzinfo=timezone.utc).timestamp() * 1000)


def next_utc_midnight_ms() -> int:
    now = datetime.now(timezone.utc)
    nxt = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return int(nxt.timestamp() * 1000)


def day_start_ms_now() -> int:
    now = datetime.now(timezone.utc)
    sod = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(sod.timestamp() * 1000)


def list_syms(fapi: str = FAPI_DEFAULT) -> list[str]:
    info = req_json(f"{fapi}/fapi/v1/exchangeInfo")
    out: list[str] = []
    for s in info["symbols"]:
        if s.get("contractType") != "PERPETUAL":
            continue
        if s.get("quoteAsset") != "USDT":
            continue
        if s.get("status") != "TRADING":
            continue
        if s.get("underlyingType") not in (None, "COIN"):
            continue
        sym = s["symbol"]
        if sym.isascii():
            out.append(sym)
    return sorted(out)


def fetch_daily(sym: str, limit: int, fapi: str = FAPI_DEFAULT) -> list[DayBar]:
    url = f"{fapi}/fapi/v1/klines?symbol={sym}&interval=1d&limit={limit}"
    rows = req_json(url)
    return [
        DayBar(utc_date(int(k[0])), float(k[1]), float(k[2]), float(k[3]), float(k[4]))
        for k in rows
    ]


def build_chg_table(
    sym_bars: dict[str, dict[str, DayBar]],
    dates: list[str],
) -> dict[str, dict[str, float]]:
    chg: dict[str, dict[str, float]] = {d: {} for d in dates}
    for sym, bars in sym_bars.items():
        for i, d in enumerate(dates):
            if i == 0:
                continue
            prev = dates[i - 1]
            b, p = bars.get(d), bars.get(prev)
            if not b or not p or p.c <= 0:
                continue
            chg[d][sym] = (b.c - p.c) / p.c * 100.0
    return chg


def top5_sets(chg_day: dict[str, float]) -> tuple[set[str], set[str]]:
    ranked = sorted(chg_day.items(), key=lambda x: x[1], reverse=True)
    return {s for s, _ in ranked[:5]}, {s for s, _ in ranked[-5:]}


def date_range(start: str, end: str) -> list[str]:
    d0 = datetime.strptime(start, "%Y-%m-%d").date()
    d1 = datetime.strptime(end, "%Y-%m-%d").date()
    out: list[str] = []
    cur = d0
    while cur <= d1:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def scan_signals_for_day(
    signal_date: str,
    trade_date: str,
    *,
    lookback: int = 7,
    fapi: str = FAPI_DEFAULT,
    workers: int = 8,
) -> list[MoverSignal]:
    """Top5 gain/loss on signal_date, first time in lookback days → trade on trade_date."""
    warmup_start = (
        datetime.strptime(signal_date, "%Y-%m-%d").date() - timedelta(days=lookback + 2)
    ).isoformat()
    all_dates = date_range(warmup_start, signal_date)
    limit = len(all_dates) + 3
    syms = list_syms(fapi)

    sym_bars: dict[str, dict[str, DayBar]] = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fetch_daily, s, limit, fapi): s for s in syms}
        for fut in as_completed(futs):
            sym = futs[fut]
            try:
                sym_bars[sym] = {b.date: b for b in fut.result()}
            except Exception:
                pass

    chg = build_chg_table(sym_bars, all_dates)
    day_top = {d: top5_sets(chg[d]) for d in all_dates if chg.get(d)}
    if signal_date not in chg:
        return []

    gainers, losers = day_top.get(signal_date, (set(), set()))
    lb_dates = [
        (datetime.strptime(signal_date, "%Y-%m-%d").date() - timedelta(days=i)).isoformat()
        for i in range(1, lookback + 1)
    ]

    out: list[MoverSignal] = []
    for sym, pct in sorted(chg[signal_date].items(), key=lambda x: x[1], reverse=True):
        bucket = ""
        if sym in gainers:
            bucket = "TOP5_GAIN"
        elif sym in losers:
            bucket = "TOP5_LOSS"
        else:
            continue
        seen = False
        for lb in lb_dates:
            tops = day_top.get(lb)
            if not tops:
                continue
            g, l = tops
            if sym in g or sym in l:
                seen = True
                break
        if seen:
            continue
        out.append(MoverSignal(sym, signal_date, trade_date, bucket, pct))
    return out


def scan_yesterday_signals(
    *,
    lookback: int = 7,
    fapi: str = FAPI_DEFAULT,
    workers: int = 8,
) -> list[MoverSignal]:
    today = utc_today()
    yday = (datetime.strptime(today, "%Y-%m-%d").date() - timedelta(days=1)).isoformat()
    return scan_signals_for_day(yday, today, lookback=lookback, fapi=fapi, workers=workers)


def bracket_prices(side: str, entry: float, tp_pct: float, sl_pct: float) -> Bracket:
    if side == "long":
        return Bracket(entry * (1 + tp_pct / 100), entry * (1 - sl_pct / 100))
    return Bracket(entry * (1 - tp_pct / 100), entry * (1 + sl_pct / 100))


def pnl_pct(side: str, entry: float, exit_px: float) -> float:
    if side == "long":
        return (exit_px - entry) / entry * 100.0
    return (entry - exit_px) / entry * 100.0


def pnl_usd(side: str, entry: float, exit_px: float, notional: float, fee_rt: float) -> float:
    g = pnl_pct(side, entry, exit_px)
    return notional * g / 100.0 - notional * fee_rt


def orb_from_5m(sym: str, trade_date: str, fapi: str = FAPI_DEFAULT) -> tuple[float, float, float] | None:
    """Return (day_open, orb_high, orb_low) from first 6×5m candles."""
    start = day_ms(trade_date)
    end = start + ORB_MS - 1
    url = (
        f"{fapi}/fapi/v1/klines?symbol={sym}&interval=5m"
        f"&startTime={start}&endTime={end}&limit=10"
    )
    rows = req_json(url)
    if len(rows) < 6:
        return None
    orb = rows[:6]
    day_open = float(orb[0][1])
    hi = max(float(k[2]) for k in orb)
    lo = min(float(k[3]) for k in orb)
    return day_open, hi, lo


def mark_price(sym: str, fapi: str = FAPI_DEFAULT) -> float:
    j = req_json(f"{fapi}/fapi/v1/premiumIndex?symbol={sym}")
    return float(j["markPrice"])
