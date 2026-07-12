"""Triple-combo overnight engine — top-mover watchlist, $10 hold 1 day.

Legs (mutually exclusive per symbol-day, matches behaviour backtest):
  1. Entry day GREEN (close > open) → SHORT next day
  2. TOP5_GAIN + entry RED + close vs signal close <= gain_fade_pct → SHORT
  3. TOP5_LOSS + entry RED + close vs signal close <= loss_bounce_pct → LONG

Signal day = watchlist signal_date; entry day = completed UTC daily candle.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from orb30_engine import DayBar, MoverSignal, fetch_daily_range, scan_signals_for_day

FAPI_DEFAULT = "https://fapi.binance.com"


@dataclass
class OvernightSignal:
    sym: str
    signal_date: str
    entry_date: str
    bucket: str
    side: str
    leg: str
    signal_close: float
    entry_open: float
    entry_close: float
    entry_chg_pct: float
    entry_candle: str


def entry_candle(bar: DayBar) -> str:
    if bar.o <= 0:
        return "?"
    return "GREEN" if bar.c > bar.o else "RED"


def close_chg_pct(entry: DayBar, signal_close: float) -> float | None:
    if signal_close <= 0:
        return None
    return (entry.c - signal_close) / signal_close * 100.0


def classify_overnight(
    bucket: str,
    entry_bar: DayBar,
    signal_close: float,
    *,
    gain_fade_pct: float = -15.0,
    loss_bounce_pct: float = -5.0,
) -> tuple[str, str] | None:
    """Return (side, leg) or None. Same rules as orb30_3day_behaviour backtest."""
    if entry_bar.o <= 0 or signal_close <= 0:
        return None
    ent_chg = close_chg_pct(entry_bar, signal_close)
    if ent_chg is None:
        return None

    if entry_bar.c > entry_bar.o:
        return "short", "LEG1_GREEN_SHORT"

    if bucket == "TOP5_GAIN" and ent_chg <= gain_fade_pct:
        return "short", "LEG2_GAIN_FADE"
    if bucket == "TOP5_LOSS" and ent_chg <= loss_bounce_pct:
        return "long", "LEG3_LOSS_BOUNCE"
    return None


def signal_day_for(entry_date: str) -> str:
    return (
        datetime.strptime(entry_date, "%Y-%m-%d").date() - timedelta(days=1)
    ).isoformat()


def build_overnight_signals(
    entry_date: str,
    watchlist: list[MoverSignal],
    *,
    gain_fade_pct: float = -15.0,
    loss_bounce_pct: float = -5.0,
    fapi: str = FAPI_DEFAULT,
) -> list[OvernightSignal]:
    """Evaluate completed entry_date daily candles for watchlist symbols."""
    sig_date = signal_day_for(entry_date)
    out: list[OvernightSignal] = []
    for item in watchlist:
        if item.trade_date != entry_date:
            continue
        try:
            bars = fetch_daily_range(item.sym, sig_date, entry_date, fapi)
        except Exception:
            continue
        by_date = {b.date: b for b in bars}
        sig_bar = by_date.get(sig_date)
        ent_bar = by_date.get(entry_date)
        if not sig_bar or not ent_bar:
            continue
        row = classify_overnight(
            item.bucket,
            ent_bar,
            sig_bar.c,
            gain_fade_pct=gain_fade_pct,
            loss_bounce_pct=loss_bounce_pct,
        )
        if not row:
            continue
        side, leg = row
        ent_chg = close_chg_pct(ent_bar, sig_bar.c)
        out.append(
            OvernightSignal(
                sym=item.sym,
                signal_date=sig_date,
                entry_date=entry_date,
                bucket=item.bucket,
                side=side,
                leg=leg,
                signal_close=sig_bar.c,
                entry_open=ent_bar.o,
                entry_close=ent_bar.c,
                entry_chg_pct=ent_chg if ent_chg is not None else 0.0,
                entry_candle=entry_candle(ent_bar),
            )
        )
    return out


def scan_watchlist_for_day(
    trade_date: str,
    *,
    lookback: int = 7,
    fapi: str = FAPI_DEFAULT,
) -> list[MoverSignal]:
    sig_date = signal_day_for(trade_date)
    return scan_signals_for_day(sig_date, trade_date, lookback=lookback, fapi=fapi)


def fetch_day_close(sym: str, day: str, fapi: str = FAPI_DEFAULT) -> float | None:
    try:
        bars = fetch_daily_range(sym, day, day, fapi)
    except Exception:
        return None
    for b in bars:
        if b.date == day and b.c > 0:
            return b.c
    return None


def pnl_pct(side: str, entry: float, exit_px: float) -> float:
    if entry <= 0:
        return 0.0
    if side == "long":
        return (exit_px - entry) / entry * 100.0
    return (entry - exit_px) / entry * 100.0


def pnl_usd(side: str, entry: float, exit_px: float, notional: float, fee_rt: float) -> float:
    return notional * pnl_pct(side, entry, exit_px) / 100.0 - notional * fee_rt
