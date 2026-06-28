#!/usr/bin/env python3
"""ICT LuxAlgo 5m backtest: OB retest + liq grabs, situation-based SL/TP, 30h scan + hold."""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from liquidity_grabs_lib import Bar, GrabState, on_bar_confirmed  # noqa: E402

FAPI = "https://fapi.binance.com"
SPOT = "https://data-api.binance.vision"
NOTIONAL = 6.0
FEE_RT = 0.0008
BAR_MS = 300_000
OB_LENGTH = 10
OB_RETEST_BARS = 20
OB_MAX_ZONE_PCT = 4.0
OB_MAX_RISK_PCT = 8.0
GRAB_MIN_SIZE = 1
RR = 1.5
TP_PCT: float | None = None  # fixed TP % when set; else RR-based
IST = timezone(timedelta(hours=5, minutes=30))


@dataclass
class TypeStats:
    signals: int = 0
    skipped: int = 0
    entries: int = 0
    tp: int = 0
    sl: int = 0
    timeout: int = 0
    wins: int = 0
    real: float = 0.0


@dataclass
class OBZone:
    top: float
    btm: float
    formed_i: int
    kind: str
    broken: bool = False
    traded: bool = False


@dataclass
class Pos:
    sym: str
    key: str
    side: str
    entry: float
    entry_ts: int
    sl: float
    tp: float
    bars_held: int = 0


@dataclass
class OpenSnap:
    sym: str
    key: str
    side: str
    entry: float


def _get(url: str):
    req = urllib.request.Request(url, headers={"User-Agent": "ict-30h-bt"})
    for attempt in range(6):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                data = json.loads(r.read())
                if isinstance(data, dict) and data.get("code") == -1003:
                    return None
                return data
        except urllib.error.HTTPError as e:
            if e.code in (418, 429, 400):
                time.sleep(0.4 * (2**attempt))
                continue
            return None
        except Exception:
            time.sleep(0.4 * (2**attempt))
    return None


def fetch_klines(sym: str, start_ms: int, end_ms: int) -> list[Bar]:
    rows: list[Bar] = []
    cur = start_ms
    while cur < end_ms:
        batch = None
        for base, path in [(FAPI, "/fapi/v1/klines"), (SPOT, "/api/v3/klines")]:
            url = f"{base}{path}?symbol={sym}&interval=5m&startTime={cur}&endTime={end_ms}&limit=1500"
            batch = _get(url)
            if batch and isinstance(batch, list):
                break
        if not batch:
            break
        for k in batch:
            rows.append(Bar(int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])))
        nxt = int(batch[-1][0]) + 1
        if nxt <= cur:
            break
        cur = nxt
        if len(batch) < 1500:
            break
        time.sleep(0.02)
    seen: set[int] = set()
    out: list[Bar] = []
    for b in rows:
        if b.ts not in seen:
            seen.add(b.ts)
            out.append(b)
    return sorted(out, key=lambda x: x.ts)


def list_symbols(n: int) -> list[str]:
    batch = _get(f"{FAPI}/fapi/v1/exchangeInfo")
    if not batch or not isinstance(batch, dict):
        batch = _get(f"{SPOT}/api/v3/exchangeInfo")
    if not batch:
        return []
    out: list[str] = []
    for s in batch["symbols"]:
        if s.get("contractType") and s.get("contractType") != "PERPETUAL":
            continue
        if s.get("quoteAsset") != "USDT" or s.get("status") != "TRADING":
            continue
        if not s["symbol"].isascii():
            continue
        out.append(s["symbol"])
    return out[:n]


def pnl(side: str, entry: float, exit_px: float) -> float:
    g = (exit_px - entry) / entry * 100 if side == "long" else (entry - exit_px) / entry * 100
    return NOTIONAL * g / 100 - NOTIONAL * FEE_RT


def unrealized(side: str, entry: float, mark: float) -> float:
    if entry <= 0 or mark <= 0:
        return 0.0
    g = (mark - entry) / entry * 100 if side == "long" else (entry - mark) / entry * 100
    return NOTIONAL * g / 100


def try_exit(side: str, b: Bar, sl: float, tp: float, bars_held: int, max_hold: int) -> tuple[str, float] | None:
    if side == "long":
        if b.l <= sl:
            return "sl", sl
        if b.h >= tp:
            return "tp", tp
    else:
        if b.h >= sl:
            return "sl", sl
        if b.l <= tp:
            return "tp", tp
    if bars_held >= max_hold:
        return "timeout", b.c
    return None


def record_exit(st: TypeStats, reason: str, net: float) -> None:
    if reason == "tp":
        st.tp += 1
    elif reason == "sl":
        st.sl += 1
    else:
        st.timeout += 1
    st.real += net
    if net > 0:
        st.wins += 1


def ob_zone_pct(ob: OBZone) -> float:
    mid = (ob.top + ob.btm) / 2
    if mid <= 0:
        return 999.0
    return (ob.top - ob.btm) / mid * 100


def tp_price(side: str, entry: float) -> float:
    if TP_PCT is not None:
        if side == "long":
            return entry * (1 + TP_PCT / 100)
        return entry * (1 - TP_PCT / 100)
    raise ValueError("tp_price called without TP_PCT in RR mode")


def ob_levels(side: str, entry: float, ob: OBZone) -> tuple[float, float] | None:
    if ob_zone_pct(ob) > OB_MAX_ZONE_PCT:
        return None
    buf = 0.0005
    if side == "long":
        sl = ob.btm * (1 - buf)
        risk = entry - sl
        if risk <= 0 or risk / entry * 100 > OB_MAX_RISK_PCT:
            return None
        tp = tp_price(side, entry) if TP_PCT is not None else entry + RR * risk
    else:
        sl = ob.top * (1 + buf)
        risk = sl - entry
        if risk <= 0 or risk / entry * 100 > OB_MAX_RISK_PCT:
            return None
        tp = tp_price(side, entry) if TP_PCT is not None else entry - RR * risk
    return sl, tp


def grab_levels(g) -> tuple[float, float] | None:
    entry, sl = g.entry_px, g.sl_px
    side = g.side
    if side == "short":
        risk = sl - entry
        if risk <= 0 or risk / entry * 100 > OB_MAX_RISK_PCT:
            return None
        tp = tp_price(side, entry) if TP_PCT is not None else entry - RR * risk
    else:
        risk = entry - sl
        if risk <= 0 or risk / entry * 100 > OB_MAX_RISK_PCT:
            return None
        tp = tp_price(side, entry) if TP_PCT is not None else entry + RR * risk
    return sl, tp


def swing_update(bars: list[Bar], i: int, length: int, st: dict) -> None:
    if i < length:
        return
    upper = max(b.h for b in bars[i - length + 1 : i + 1])
    lower = min(b.l for b in bars[i - length + 1 : i + 1])
    hi_len, lo_len = bars[i - length].h, bars[i - length].l
    prev = st["os"]
    if hi_len > upper:
        st["os"] = 0
    elif lo_len < lower:
        st["os"] = 1
    if st["os"] == 0 and prev != 0:
        st["top_y"] = hi_len
        st["top_x"] = i - length
        st["top_crossed"] = False
    if st["os"] == 1 and prev != 1:
        st["btm_y"] = lo_len
        st["btm_x"] = i - length
        st["btm_crossed"] = False


def form_ob(bars: list[Bar], i: int, st: dict, obs: list[OBZone]) -> None:
    b = bars[i]
    if st.get("top_y") and not st.get("top_crossed") and b.c > st["top_y"]:
        st["top_crossed"] = True
        tx = st["top_x"]
        minima = min(bars[tx].l, bars[tx].c, bars[tx].o)
        maxima = max(bars[tx].h, bars[tx].c, bars[tx].o)
        for j in range(tx, i):
            mn = min(bars[j].l, bars[j].c, bars[j].o)
            mx = max(bars[j].h, bars[j].c, bars[j].o)
            if mn < minima:
                minima, maxima = mn, mx
            elif mn == minima:
                maxima = max(maxima, mx)
        obs.append(OBZone(maxima, minima, i, "ob_plus"))
    if st.get("btm_y") and not st.get("btm_crossed") and b.c < st["btm_y"]:
        st["btm_crossed"] = True
        tx = st["btm_x"]
        minima = min(bars[tx].l, bars[tx].c, bars[tx].o)
        maxima = max(bars[tx].h, bars[tx].c, bars[tx].o)
        for j in range(tx, i):
            mn = min(bars[j].l, bars[j].c, bars[j].o)
            mx = max(bars[j].h, bars[j].c, bars[j].o)
            if mx > maxima:
                maxima, minima = mx, mn
            elif mx == maxima:
                minima = min(minima, mn)
        obs.append(OBZone(maxima, minima, i, "ob_minus"))


def backtest_symbol(
    sym: str,
    bars: list[Bar],
    scan_start: int,
    scan_end: int,
    sim_end: int,
    max_hold: int,
) -> tuple[dict[str, TypeStats], list[OpenSnap], float] | None:
    if len(bars) < 200:
        return None

    stats: dict[str, TypeStats] = defaultdict(TypeStats)
    swing_st = {"os": 0, "top_y": None, "top_x": 0, "top_crossed": False, "btm_y": None, "btm_x": 0, "btm_crossed": False}
    obs: list[OBZone] = []
    grab_st = GrabState()
    open_pos: Pos | None = None
    open_snap: list[OpenSnap] = []
    scan_mark = 0.0
    snap_done = False

    for i, b in enumerate(bars):
        if b.ts >= sim_end:
            break

        if not snap_done and b.ts >= scan_end:
            snap_done = True
            if i > 0:
                scan_mark = bars[i - 1].c
            else:
                scan_mark = b.c
            if open_pos and scan_start <= open_pos.entry_ts < scan_end:
                open_snap.append(OpenSnap(sym, open_pos.key, open_pos.side, open_pos.entry))

        # exits (through sim_end for hold resolution)
        if open_pos and b.ts > open_pos.entry_ts:
            open_pos.bars_held += 1
            hit = try_exit(open_pos.side, b, open_pos.sl, open_pos.tp, open_pos.bars_held, max_hold)
            if hit and open_pos.entry_ts >= scan_start:
                record_exit(stats[open_pos.key], hit[0], pnl(open_pos.side, open_pos.entry, hit[1]))
                open_pos = None

        in_scan = scan_start <= b.ts < scan_end
        if not in_scan or open_pos:
            if b.ts < scan_end:
                swing_update(bars, i, OB_LENGTH, swing_st)
                form_ob(bars, i, swing_st, obs)
            continue

        swing_update(bars, i, OB_LENGTH, swing_st)
        form_ob(bars, i, swing_st, obs)
        for ob in obs:
            if ob.broken or ob.traded:
                continue
            if ob.kind == "ob_plus" and min(b.c, b.o) < ob.btm:
                ob.broken = True
            if ob.kind == "ob_minus" and max(b.c, b.o) > ob.top:
                ob.broken = True

        # OB retest
        for ob in obs:
            if ob.traded or ob.broken or i <= ob.formed_i or i - ob.formed_i > OB_RETEST_BARS:
                continue
            if not (b.l <= ob.top and b.h >= ob.btm):
                continue
            key = ob.kind
            stats[key].signals += 1
            side = "long" if key == "ob_plus" else "short"
            lv = ob_levels(side, b.c, ob)
            if lv is None:
                stats[key].skipped += 1
                continue
            sl, tp = lv
            stats[key].entries += 1
            open_pos = Pos(sym, key, side, b.c, b.ts, sl, tp)
            ob.traded = True
            break

        if open_pos:
            continue

        if i < 60:
            continue
        g = on_bar_confirmed(grab_st, bars, i)
        if not g:
            continue
        key = g.grab_type
        stats[key].signals += 1
        if g.grab_size < GRAB_MIN_SIZE:
            stats[key].skipped += 1
            continue
        lv = grab_levels(g)
        if lv is None:
            stats[key].skipped += 1
            continue
        sl, tp = lv
        stats[key].entries += 1
        open_pos = Pos(sym, key, g.side, g.entry_px, b.ts, sl, tp)

    if scan_mark <= 0:
        for b in reversed(bars):
            if b.ts < scan_end:
                scan_mark = b.c
                break
    return stats, open_snap, scan_mark


def merge(dst: dict[str, TypeStats], src: dict[str, TypeStats]) -> None:
    for k, s in src.items():
        d = dst[k]
        d.signals += s.signals
        d.skipped += s.skipped
        d.entries += s.entries
        d.tp += s.tp
        d.sl += s.sl
        d.timeout += s.timeout
        d.wins += s.wins
        d.real += s.real


def format_block(stats: dict[str, TypeStats], opens: list[OpenSnap], marks: dict[str, float]) -> list[str]:
    unrl_by: dict[str, float] = defaultdict(float)
    open_by: dict[str, int] = defaultdict(int)
    for t in opens:
        open_by[t.key] += 1
        unrl_by[t.key] += unrealized(t.side, t.entry, marks.get(t.sym, t.entry))

    ex = sum(s.tp + s.sl + s.timeout for s in stats.values())
    wr = sum(s.wins for s in stats.values()) / ex * 100 if ex else 0.0
    real = sum(s.real for s in stats.values())
    unrl = sum(unrl_by.values())

    lines = [
        f"[stats] ict signals={sum(s.signals for s in stats.values())} "
        f"entries={sum(s.entries for s in stats.values())} exits={ex} wr={wr:.1f}% "
        f"real=${real:+.2f} tp={sum(s.tp for s in stats.values())} "
        f"sl={sum(s.sl for s in stats.values())} timeout={sum(s.timeout for s in stats.values())} "
        f"open={len(opens)} skipped={sum(s.skipped for s in stats.values())}",
        "",
    ]
    for key in sorted(set(stats) | set(open_by) | set(unrl_by)):
        s = stats[key]
        lines.append(
            f"[stats_by_type]  {key}: sig={s.signals} skip={s.skipped} ent={s.entries} "
            f"tp={s.tp} sl={s.sl} to={s.timeout} open={open_by.get(key, 0)} "
            f"real=${s.real:+.2f} unrl=${unrl_by.get(key, 0.0):+.2f}"
        )
    lines.append(f"# combined real+unrl=${real + unrl:+.2f}")
    lines.append("")
    return lines


def process(sym: str, scan_start: int, scan_end: int, sim_end: int, warmup: int, max_hold: int):
    bars = fetch_klines(sym, warmup, sim_end)
    r = backtest_symbol(sym, bars, scan_start, scan_end, sim_end, max_hold)
    if r is None:
        return sym, None
    return sym, r


def main() -> None:
    global TP_PCT
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-06-26T00:00:00+00:00")
    ap.add_argument("--hours", type=float, default=30.0)
    ap.add_argument("--timeout-hours", type=float, default=24.0)
    ap.add_argument("--symbols", type=int, default=300)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--tp", type=float, default=None, help="Fixed TP %% (e.g. 0.5); omit for RR-based TP")
    ap.add_argument("--out", type=Path, default=Path("data/aws/sr_chartprime/ict_26jun_30h_300sym.txt"))
    args = ap.parse_args()
    TP_PCT = args.tp

    max_hold = max(1, int(args.timeout_hours * 3600 * 1000 / BAR_MS))
    start = datetime.fromisoformat(args.start)
    end = start + timedelta(hours=args.hours)
    scan_start = int(start.timestamp() * 1000)
    scan_end = int(end.timestamp() * 1000)
    sim_end = scan_end + int(args.timeout_hours * 3600 * 1000)
    warmup = scan_start - 5 * 24 * BAR_MS

    symbols = list_symbols(args.symbols)
    tp_desc = f"TP={args.tp}% fixed" if args.tp is not None else f"TP={RR}R"
    print(
        f"ICT 30h backtest | {len(symbols)} sym | scan {args.hours}h hold {args.timeout_hours}h | "
        f"SL=situational (OB zone + grab wick) {tp_desc} max risk {OB_MAX_RISK_PCT}%"
    )
    print(f"scan UTC: {start:%Y-%m-%d %H:%M} → {end:%Y-%m-%d %H:%M}")
    print(f"scan IST: {start.astimezone(IST):%Y-%m-%d %H:%M} → {end.astimezone(IST):%Y-%m-%d %H:%M}\n")

    all_stats: dict[str, TypeStats] = defaultdict(TypeStats)
    all_open: list[OpenSnap] = []
    marks: dict[str, float] = {}
    ok = 0

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {
            ex.submit(process, sym, scan_start, scan_end, sim_end, warmup, max_hold): sym for sym in symbols
        }
        for n, fut in enumerate(as_completed(futs), 1):
            sym = futs[fut]
            try:
                res = fut.result()
            except Exception as e:
                print(f"  ERR {sym}: {e}")
                continue
            if not res:
                continue
            _, pack = res
            if pack is None:
                continue
            st, op, mark = pack
            merge(all_stats, st)
            all_open.extend(op)
            marks[sym] = mark
            ok += 1
            if n % 25 == 0:
                print(f"  {n}/{len(symbols)} done...", flush=True)

    header = [
        f"ICT LuxAlgo 5m | scan={args.hours}h hold={args.timeout_hours}h | snapshot @ scan end",
        f"symbols={ok} notional=${NOTIONAL} max_hold={max_hold}bars",
        f"exits: OB/grab SL=situational | {tp_desc} | filters zone<{OB_MAX_ZONE_PCT}% risk<{OB_MAX_RISK_PCT}%",
        f"scan_utc: {start:%Y-%m-%d %H:%M} → {end:%Y-%m-%d %H:%M}",
        f"scan_ist: {start.astimezone(IST):%Y-%m-%d %H:%M} → {end.astimezone(IST):%Y-%m-%d %H:%M}",
        "",
    ]
    body = format_block(all_stats, all_open, marks)
    text = "\n".join(header + body)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(text, encoding="utf-8")
    print(f"\nSymbols OK: {ok}/{len(symbols)}")
    print(text)
    print(f"written: {args.out}")


if __name__ == "__main__":
    main()
