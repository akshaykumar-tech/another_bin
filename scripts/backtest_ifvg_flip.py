#!/usr/bin/env python3
"""IFVG flip backtest: hold long from ifvg+ until next ifvg-, short from ifvg- until next ifvg+.

No SL/TP — exit only on opposite IFVG signal or day-end mark. Uses same IFVG
detection as backtest_ifvg_btc_parity.py (Balanced quality filter).
"""
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

DEFAULT_SYMBOLS = (
    "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT,"
    "DOGEUSDT,ADAUSDT,AVAXUSDT,LINKUSDT,SUIUSDT"
)


@dataclass
class Bar:
    ts: int
    o: float
    h: float
    l: float
    c: float
    v: float


@dataclass
class Signal:
    ts: int
    side: int  # 1 ifvg+, -1 ifvg-
    entry: float


@dataclass
class Position:
    side: int
    entry_ts: int
    entry: float


def fetch_day(symbol: str, day, interval: str) -> list[Bar]:
    url = f"{VISION}/{symbol}/{interval}/{symbol}-{interval}-{day.isoformat()}.zip"
    req = Request(url, headers={"User-Agent": "ifvg-flip-backtest"})
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


def fetch_range(symbol: str, start_ms: int, end_ms: int, interval: str) -> list[Bar]:
    d0 = datetime.fromtimestamp(start_ms / 1000, timezone.utc).date()
    d1 = datetime.fromtimestamp((end_ms - 1) / 1000, timezone.utc).date()
    rows: list[Bar] = []
    d = d0
    while d <= d1:
        try:
            rows.extend(fetch_day(symbol, d, interval))
        except Exception:
            pass
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
    if tick <= 0:
        return px
    return round(px / tick) * tick


def quality_pass(gap_atr: float, body_ratio: float, range_atr: float, q) -> bool:
    return gap_atr >= q[0] and body_ratio >= q[1] and range_atr >= q[2]


def net_pnl(side: int, entry: float, exit_px: float, notional: float, fee_rt: float) -> float:
    move = ((exit_px - entry) / entry * 100) if side == 1 else ((entry - exit_px) / entry * 100)
    return notional * move / 100 - notional * fee_rt


def collect_signals(
    bars: list[Bar],
    scan_start: int,
    scan_end: int,
    *,
    atr_len: int,
    max_hidden_fvg: int,
    max_fvg_age: int,
    mintick: float,
    slippage_bps: float,
    q,
    inv_buf_atr: float,
) -> list[Signal]:
    raw: list[dict] = []
    out: list[Signal] = []

    for i, b in enumerate(bars):
        if i < 3:
            continue

        for r in raw:
            r["age"] += 1
        raw = [r for r in raw if r["age"] <= max_fvg_age]

        a = atr(bars, i, atr_len)
        safe_atr = a if (a and a > 0) else mintick
        c_range = max(b.h - b.l, mintick)
        body_ratio = abs(b.c - b.o) / c_range
        range_atr = c_range / safe_atr

        if b.l > bars[i - 2].h:
            raw.append({
                "top": b.l, "bot": bars[i - 2].h, "dir": 1, "age": 0,
                "gap_atr": (b.l - bars[i - 2].h) / safe_atr,
                "body_ratio": body_ratio, "range_atr": range_atr,
            })
        if b.h < bars[i - 2].l:
            raw.append({
                "top": bars[i - 2].l, "bot": b.h, "dir": -1, "age": 0,
                "gap_atr": (bars[i - 2].l - b.h) / safe_atr,
                "body_ratio": body_ratio, "range_atr": range_atr,
            })
        if len(raw) > max_hidden_fvg:
            raw = raw[-max_hidden_fvg:]

        buf = safe_atr * inv_buf_atr
        for idx in range(len(raw) - 1, -1, -1):
            r = raw[idx]
            bull_inv = r["dir"] == -1 and b.c > r["top"] + buf
            bear_inv = r["dir"] == 1 and b.c < r["bot"] - buf
            if bull_inv or bear_inv:
                if quality_pass(r["gap_atr"], r["body_ratio"], r["range_atr"], q):
                    if scan_start <= b.ts < scan_end:
                        direction = 1 if bull_inv else -1
                        entry = r["top"] if direction == 1 else r["bot"]
                        slip = entry * (slippage_bps / 10000.0)
                        entry = entry + slip if direction == 1 else entry - slip
                        entry = round_to_tick(entry, mintick)
                        out.append(Signal(b.ts, direction, entry))
                raw.pop(idx)
                break
    return out


def run_flip_day(
    bars: list[Bar],
    signals: list[Signal],
    scan_end: int,
    notional: float,
    fee_rt: float,
) -> tuple[float, float, int, int, int, int]:
    """Returns real_pnl, unrl_pnl, flips, long_legs, short_legs, open_side."""
    if not signals:
        mark = next((b.c for b in reversed(bars) if b.ts < scan_end), bars[-1].c if bars else 0.0)
        return 0.0, 0.0, 0, 0, 0, 0

    pos: Position | None = None
    real = 0.0
    flips = 0
    long_legs = 0
    short_legs = 0

    for sig in signals:
        if pos is None:
            pos = Position(sig.side, sig.ts, sig.entry)
            if sig.side == 1:
                long_legs += 1
            else:
                short_legs += 1
            continue

        if sig.side == pos.side:
            continue  # same direction — hold until opposite signal

        real += net_pnl(pos.side, pos.entry, sig.entry, notional, fee_rt)
        flips += 1
        pos = Position(sig.side, sig.ts, sig.entry)
        if sig.side == 1:
            long_legs += 1
        else:
            short_legs += 1

    unrl = 0.0
    open_side = 0
    if pos is not None:
        mark = next((b.c for b in reversed(bars) if b.ts < scan_end), bars[-1].c)
        unrl = net_pnl(pos.side, pos.entry, mark, notional, fee_rt)
        open_side = pos.side

    return real, unrl, flips, long_legs, short_legs, open_side


def ist_day_bounds(day_str: str) -> tuple[int, int]:
    """IST calendar day midnight boundaries as ms."""
    d = datetime.fromisoformat(day_str).replace(tzinfo=IST)
    start = int(d.timestamp() * 1000)
    end = int((d + timedelta(days=1)).timestamp() * 1000)
    return start, end


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", default="2026-06-28,2026-06-29,2026-06-30")
    ap.add_argument("--symbols", default=DEFAULT_SYMBOLS)
    ap.add_argument("--interval", default="5m")
    ap.add_argument("--warmup-days", type=int, default=3)
    ap.add_argument("--atr-len", type=int, default=14)
    ap.add_argument("--max-hidden-fvg", type=int, default=120)
    ap.add_argument("--max-fvg-age", type=int, default=60)
    ap.add_argument("--mintick", type=float, default=0.0001)
    ap.add_argument("--notional", type=float, default=100.0)
    ap.add_argument("--fee-rt", type=float, default=0.0008)
    ap.add_argument("--slippage-bps", type=float, default=1.0)
    ap.add_argument("--q-gap-atr", type=float, default=0.25)
    ap.add_argument("--q-body", type=float, default=0.50)
    ap.add_argument("--q-range", type=float, default=0.60)
    ap.add_argument("--inv-buf-atr", type=float, default=0.05)
    args = ap.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    days = [d.strip() for d in args.days.split(",") if d.strip()]
    q = (args.q_gap_atr, args.q_body, args.q_range)

    print("IFVG flip backtest | ifvg+ LONG until ifvg- | ifvg- SHORT until ifvg+ | no SL/TP")
    print(f"symbols={len(symbols)} notional=${args.notional} fee_rt={args.fee_rt}")
    print(f"days (IST): {', '.join(days)}")
    print()

    combo_real = combo_unrl = 0.0
    combo_flips = 0

    for day in days:
        scan_start, scan_end = ist_day_bounds(day)
        warmup_ms = scan_start - args.warmup_days * 24 * 3600 * 1000
        day_real = day_unrl = 0.0
        day_flips = 0
        day_sigs = 0
        per_sym: list[tuple[str, float, float, int]] = []

        for sym in symbols:
            bars = fetch_range(sym, warmup_ms, scan_end, args.interval)
            sigs = collect_signals(
                bars, scan_start, scan_end,
                atr_len=args.atr_len,
                max_hidden_fvg=args.max_hidden_fvg,
                max_fvg_age=args.max_fvg_age,
                mintick=args.mintick,
                slippage_bps=args.slippage_bps,
                q=q,
                inv_buf_atr=args.inv_buf_atr,
            )
            real, unrl, flips, _, _, _ = run_flip_day(bars, sigs, scan_end, args.notional, args.fee_rt)
            day_real += real
            day_unrl += unrl
            day_flips += flips
            day_sigs += len(sigs)
            per_sym.append((sym, real, unrl, len(sigs)))

        combo_real += day_real
        combo_unrl += day_unrl
        combo_flips += day_flips
        comb = day_real + day_unrl
        print(
            f"{day} | signals={day_sigs} flips={day_flips} | "
            f"real=${day_real:+.2f} unrl=${day_unrl:+.2f} comb=${comb:+.2f}"
        )
        for sym, r, u, n in sorted(per_sym, key=lambda x: -(x[1] + x[2])):
            if n == 0 and abs(r) < 1e-9 and abs(u) < 1e-9:
                continue
            print(f"  {sym:<12} sigs={n:>3} real=${r:+.2f} unrl=${u:+.2f} comb=${r+u:+.2f}")
        print()

    print(
        f"COMBO 3-day | flips={combo_flips} | "
        f"real=${combo_real:+.2f} unrl=${combo_unrl:+.2f} comb=${combo_real + combo_unrl:+.2f}"
    )


if __name__ == "__main__":
    main()
