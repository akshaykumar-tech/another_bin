"""5x acceleration signal engine — shared by dry paper + live."""
from __future__ import annotations

import json
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta

FAPI = "https://fapi.binance.com"
IST = timezone(timedelta(hours=5, minutes=30))
DAY_MS = 86_400_000


@dataclass
class Bar:
    ts: int
    o: float
    h: float
    l: float
    c: float


@dataclass
class Signal:
    sym: str
    signal_side: str  # 5x continuation side (long/short)
    mult: float
    base_pct: float
    prev_pct: float
    entry_open: float = 0.0


def req_json(url: str):
    req = urllib.request.Request(url, headers={"User-Agent": "accel5x"})
    for a in range(6):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read())
        except Exception:
            time.sleep(min(2 ** a, 8))
    raise RuntimeError(url)


def ist_date(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).astimezone(IST).strftime("%Y-%m-%d")


def day_ms(d: str) -> int:
    y, m, day = map(int, d.split("-"))
    return int(datetime(y, m, day, tzinfo=timezone.utc).timestamp() * 1000)


def today_ist() -> str:
    return datetime.now(timezone.utc).astimezone(IST).strftime("%Y-%m-%d")


def entry_px(px: float, side: str, slip_bps: float = 1.0) -> float:
    slip = px * (slip_bps / 10000.0)
    return px - slip if side == "short" else px + slip


def exit_px(px: float, side: str, slip_bps: float = 1.0) -> float:
    slip = px * (slip_bps / 10000.0)
    return px + slip if side == "short" else px - slip


def pnl_usd(side: str, ent: float, ex: float, notional: float, fee_rt: float) -> float:
    g = (ex - ent) / ent * 100 if side == "long" else (ent - ex) / ent * 100
    return notional * g / 100 - notional * fee_rt


def mirror_side(side: str) -> str:
    return "short" if side == "long" else "long"


def exec_side(signal_side: str, btc_prev_green: bool | None, *, live_mode: bool) -> str:
    """Dry/backtest: same as signal. Live: mirror when BTC prev red; doji -> same."""
    if not live_mode or btc_prev_green is None:
        return signal_side
    if btc_prev_green:
        return signal_side
    return mirror_side(signal_side)


def list_all_syms(fapi: str = FAPI) -> list[str]:
    info = req_json(f"{fapi}/fapi/v1/exchangeInfo")
    return [
        s["symbol"] for s in info["symbols"]
        if s.get("contractType") == "PERPETUAL"
        and s.get("quoteAsset") == "USDT"
        and s.get("status") == "TRADING"
        and s["symbol"].isascii()
    ]


def fetch_daily(sym: str, limit: int = 5, fapi: str = FAPI) -> list[Bar]:
    url = f"{fapi}/fapi/v1/klines?symbol={sym}&interval=1d&limit={limit}"
    batch = req_json(url)
    return [Bar(int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4])) for k in batch]


def daily_map(bars: list[Bar]) -> dict[str, Bar]:
    return {ist_date(b.ts): b for b in bars}


def close_chg(db: dict[str, Bar], cal: list[str], i: int) -> float | None:
    if i < 1:
        return None
    b, p = db.get(cal[i]), db.get(cal[i - 1])
    if not b or not p or p.c <= 0:
        return None
    return (b.c - p.c) / p.c * 100


def btc_prev_green(btc_db: dict[str, Bar], entry_day: str, cal: list[str]) -> bool | None:
    i = cal.index(entry_day)
    if i < 1:
        return None
    b = btc_db.get(cal[i - 1])
    if not b:
        return None
    if b.c > b.o:
        return True
    if b.c < b.o:
        return False
    return None


def btc_prev_body_pct(btc_db: dict[str, Bar], entry_day: str, cal: list[str]) -> float | None:
    i = cal.index(entry_day)
    if i < 1:
        return None
    b = btc_db.get(cal[i - 1])
    if not b or b.o <= 0:
        return None
    return (b.c - b.o) / b.o * 100


def pick_signals(
    all_daily: dict[str, dict[str, Bar]],
    cal: list[str],
    entry_day: str,
    *,
    min_mult: float = 5.0,
    min_base_pct: float = 0.3,
    max_trades: int = 30,
) -> list[Signal]:
    i = cal.index(entry_day)
    if i < 2:
        return []
    d_prev1, d_prev2 = cal[i - 1], cal[i - 2]
    cands: list[Signal] = []
    for sym, db in all_daily.items():
        if entry_day not in db:
            continue
        c1 = close_chg(db, cal, i - 1)
        c2 = close_chg(db, cal, i - 2)
        if c1 is None or c2 is None or c1 == 0 or c2 == 0:
            continue
        if (c1 > 0) != (c2 > 0):
            continue
        if abs(c2) < min_base_pct:
            continue
        mult = abs(c1) / abs(c2)
        if mult < min_mult:
            continue
        side = "long" if c1 > 0 else "short"
        ent_o = db[entry_day].o if entry_day in db else 0.0
        cands.append(Signal(sym, side, mult, c2, c1, ent_o))
    cands.sort(key=lambda x: x.mult, reverse=True)
    return cands[:max_trades]


def build_cal(entry_day: str, back_days: int = 5) -> list[str]:
    d = datetime.strptime(entry_day, "%Y-%m-%d")
    start = d - timedelta(days=back_days)
    cal = []
    cur = start
    while cur <= d:
        cal.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return cal


def scan_universe(
    syms: list[str],
    entry_day: str,
    *,
    min_mult: float = 5.0,
    min_base_pct: float = 0.3,
    max_trades: int = 30,
    fapi: str = FAPI,
    sleep: float = 0.006,
) -> tuple[list[Signal], bool | None, float | None]:
    cal = build_cal(entry_day)
    all_daily: dict[str, dict[str, Bar]] = {}
    for sym in syms:
        all_daily[sym] = daily_map(fetch_daily(sym, fapi=fapi))
        time.sleep(sleep)
    btc_db = daily_map(fetch_daily("BTCUSDT", fapi=fapi))
    sigs = pick_signals(
        all_daily, cal, entry_day,
        min_mult=min_mult, min_base_pct=min_base_pct, max_trades=max_trades,
    )
    bg = btc_prev_green(btc_db, entry_day, cal)
    bp = btc_prev_body_pct(btc_db, entry_day, cal)
    return sigs, bg, bp


def next_utc_midnight_ms() -> int:
    now = datetime.now(timezone.utc)
    nxt = (now + timedelta(days=1)).replace(hour=0, minute=0, second=5, microsecond=0)
    if now.hour == 0 and now.minute == 0 and now.second < 10:
        return int(now.timestamp() * 1000) + 5000
    return int(nxt.timestamp() * 1000)
