#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from urllib.request import Request, urlopen

VISION = "https://data.binance.vision/data/futures/um/daily/klines"
IST = timezone(timedelta(hours=5, minutes=30))


@dataclass
class Bar:
    ts: int
    o: float
    h: float
    l: float
    c: float
    v: float


@dataclass
class Trade:
    side: int
    entry_ts: int
    entry: float
    sl: float
    tp: float
    exit_ts: int | None = None
    exit_px: float | None = None
    reason: str = "open"


def fetch_day(symbol: str, day) -> list[Bar]:
    url = f"{VISION}/{symbol}/5m/{symbol}-5m-{day.isoformat()}.zip"
    req = Request(url, headers={"User-Agent": "ifvg-parity-backtest"})
    with urlopen(req, timeout=60) as r:
        data = r.read()
    out: list[Bar] = []
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        name = z.namelist()[0]
        for rec in csv.reader(io.TextIOWrapper(z.open(name), newline="")):
            if not rec or rec[0] == "open_time":
                continue
            out.append(Bar(int(rec[0]), float(rec[1]), float(rec[2]), float(rec[3]), float(rec[4]), float(rec[5])))
    return out


def fetch_range(symbol: str, start_ms: int, end_ms: int) -> list[Bar]:
    d0 = datetime.fromtimestamp(start_ms / 1000, timezone.utc).date()
    d1 = datetime.fromtimestamp((end_ms - 1) / 1000, timezone.utc).date()
    rows: list[Bar] = []
    d = d0
    while d <= d1:
        rows.extend(fetch_day(symbol, d))
        d += timedelta(days=1)
    rows.sort(key=lambda b: b.ts)
    return [b for b in rows if start_ms <= b.ts < end_ms]


def atr(bars: list[Bar], i: int, n: int) -> float | None:
    if i < n:
        return None
    tr = []
    for j in range(i - n + 1, i + 1):
        prev = bars[j - 1].c
        b = bars[j]
        tr.append(max(b.h - b.l, abs(b.h - prev), abs(b.l - prev)))
    return sum(tr) / len(tr)


def round_to_tick(px: float, tick: float) -> float:
    return round(px / tick) * tick


def quality_pass(gap_atr: float, body_ratio: float, range_atr: float) -> bool:
    # Pine "Balanced" mode defaults.
    return gap_atr >= 0.25 and body_ratio >= 0.50 and range_atr >= 0.60


def line_price(top: float, bot: float, direction: int) -> float:
    # Pine default: Broken Boundary.
    return top if direction == 1 else bot


def net_pnl(side: int, entry: float, exit_px: float, notional: float, fee_rt: float) -> float:
    move = ((exit_px - entry) / entry * 100) if side == 1 else ((entry - exit_px) / entry * 100)
    return notional * move / 100 - notional * fee_rt


def run_day(
    bars: list[Bar],
    scan_start: int,
    scan_end: int,
    *,
    atr_len: int,
    sl_atr_mult: float,
    tp_rr: float,
    max_hidden_fvg: int,
    max_fvg_age: int,
    mintick: float,
    slippage_bps: float,
) -> tuple[list[Trade], int, int, int]:
    raw = []
    trades: list[Trade] = []
    active: Trade | None = None
    signals = 0
    blocked = 0
    filtered = 0

    for i, b in enumerate(bars):
        if i < 3:
            continue

        if active is not None and b.ts > active.entry_ts:
            hit_sl = b.l <= active.sl if active.side == 1 else b.h >= active.sl
            hit_tp = b.h >= active.tp if active.side == 1 else b.l <= active.tp
            # Pine logic: SL priority if both touched.
            if hit_sl:
                active.exit_ts = b.ts
                active.exit_px = active.sl
                active.reason = "sl"
                trades.append(active)
                active = None
            elif hit_tp:
                active.exit_ts = b.ts
                active.exit_px = active.tp
                active.reason = "tp"
                trades.append(active)
                active = None

        if b.ts >= scan_end:
            break

        for r in raw:
            r["age"] += 1
        raw = [r for r in raw if r["age"] <= max_fvg_age]

        a = atr(bars, i, atr_len)
        safe_atr = a if (a and a > 0) else mintick
        c_range = max(b.h - b.l, mintick)
        body_ratio = abs(b.c - b.o) / c_range
        range_atr = c_range / safe_atr

        if b.l > bars[i - 2].h:
            raw.append(
                {
                    "top": b.l,
                    "bot": bars[i - 2].h,
                    "dir": 1,
                    "age": 0,
                    "gap_atr": (b.l - bars[i - 2].h) / safe_atr,
                    "body_ratio": body_ratio,
                    "range_atr": range_atr,
                }
            )
        if b.h < bars[i - 2].l:
            raw.append(
                {
                    "top": bars[i - 2].l,
                    "bot": b.h,
                    "dir": -1,
                    "age": 0,
                    "gap_atr": (bars[i - 2].l - b.h) / safe_atr,
                    "body_ratio": body_ratio,
                    "range_atr": range_atr,
                }
            )
        if len(raw) > max_hidden_fvg:
            raw = raw[-max_hidden_fvg:]

        buf = safe_atr * 0.05
        new_sig = None
        for idx in range(len(raw) - 1, -1, -1):
            r = raw[idx]
            bull_inv = r["dir"] == -1 and b.c > r["top"] + buf
            bear_inv = r["dir"] == 1 and b.c < r["bot"] - buf
            if bull_inv or bear_inv:
                if quality_pass(r["gap_atr"], r["body_ratio"], r["range_atr"]):
                    new_sig = (r["top"], r["bot"], 1 if bull_inv else -1, safe_atr)
                else:
                    filtered += 1
                raw.pop(idx)
                break

        if not (scan_start <= b.ts < scan_end):
            continue

        if new_sig:
            signals += 1
            if active is not None:
                blocked += 1
                continue
            top, bot, direction, safe_atr = new_sig
            entry = line_price(top, bot, direction)
            # near-live parity: approximate adverse slippage at entry
            slip = entry * (slippage_bps / 10000.0)
            entry = entry + slip if direction == 1 else entry - slip
            entry = round_to_tick(entry, mintick)
            risk = safe_atr * sl_atr_mult
            sl = entry - risk if direction == 1 else entry + risk
            tp = entry + risk * tp_rr if direction == 1 else entry - risk * tp_rr
            active = Trade(
                side=direction,
                entry_ts=b.ts,
                entry=round_to_tick(entry, mintick),
                sl=round_to_tick(sl, mintick),
                tp=round_to_tick(tp, mintick),
            )

    if active is not None:
        mark = next((x.c for x in reversed(bars) if x.ts < scan_end), bars[-1].c)
        active.exit_ts = scan_end
        active.exit_px = mark
        active.reason = "open"
        trades.append(active)

    return trades, signals, blocked, filtered


def run_range(args) -> None:
    symbol = "BTCUSDT"
    start = datetime.fromisoformat(args.start)
    end = datetime.fromisoformat(args.end)
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    warmup_ms = start_ms - args.warmup_days * 24 * 3600 * 1000
    bars = fetch_range(symbol, warmup_ms, end_ms + 24 * 3600 * 1000)
    trades, signals, blocked, filtered = run_day(
        bars,
        start_ms,
        end_ms,
        atr_len=args.atr_len,
        sl_atr_mult=args.sl_atr_mult,
        tp_rr=args.tp_rr,
        max_hidden_fvg=args.max_hidden_fvg,
        max_fvg_age=args.max_fvg_age,
        mintick=args.mintick,
        slippage_bps=args.slippage_bps,
    )
    tp = sl = open_n = 0
    real = unrl = 0.0
    for t in trades:
        net = net_pnl(t.side, t.entry, t.exit_px, args.notional, args.fee_rt)
        if t.reason == "open":
            open_n += 1
            unrl += net
        else:
            tp += t.reason == "tp"
            sl += t.reason == "sl"
            real += net
    wr = (tp / (tp + sl) * 100) if (tp + sl) else 0.0
    print(
        f"{start.astimezone(IST):%Y-%m-%d} | signals={signals} blocked={blocked} filtered={filtered} | "
        f"ent={len(trades)} tp={tp} sl={sl} open={open_n} wr={wr:.1f}% | "
        f"real=${real:+.2f} unrl=${unrl:+.2f} comb=${real+unrl:+.2f}"
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="ISO datetime with timezone")
    ap.add_argument("--end", required=True, help="ISO datetime with timezone")
    ap.add_argument("--atr-len", type=int, default=14)
    ap.add_argument("--sl-atr-mult", type=float, default=1.5)
    ap.add_argument("--tp-rr", type=float, default=3.0)
    ap.add_argument("--max-hidden-fvg", type=int, default=120)
    ap.add_argument("--max-fvg-age", type=int, default=60)
    ap.add_argument("--warmup-days", type=int, default=3)
    ap.add_argument("--mintick", type=float, default=0.1)
    ap.add_argument("--notional", type=float, default=100.0)
    ap.add_argument("--fee-rt", type=float, default=0.0008)
    ap.add_argument("--slippage-bps", type=float, default=1.0)
    args = ap.parse_args()
    run_range(args)


if __name__ == "__main__":
    main()
