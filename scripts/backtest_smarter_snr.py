#!/usr/bin/env python3
"""Backtest Smarter SnR (trading1.log) — multi-config, 25 Jun scan."""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from smarter_snr_lib import Bar, SNRConfig, scan_signals  # noqa: E402

FAPI = "https://fapi.binance.com"
SPOT = "https://data-api.binance.vision"
NOTIONAL = 6.0
BANKROLL = 100.0
FEE_RT = 0.0008
BAR_MS = 300_000
IST = timezone(timedelta(hours=5, minutes=30))

SYMS20 = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "SUIUSDT",
    "LTCUSDT", "TRXUSDT", "DOTUSDT", "NEARUSDT", "APTUSDT",
    "ARBUSDT", "OPUSDT", "INJUSDT", "ATOMUSDT", "UNIUSDT",
]


@dataclass
class TradeCfg:
    name: str
    sd: SNRConfig
    tp_pct: float = 1.5
    sl_pct: float = 8.0
    max_hold: int = 288


TRADE_CONFIGS = [
    TradeCfg("snr_cross_tp15_sl8", SNRConfig(signals="snr_cross")),
]


@dataclass
class TypeStats:
    sig: int = 0
    ent: int = 0
    tp: int = 0
    sl: int = 0
    to: int = 0
    real: float = 0.0
    unrl: float = 0.0
    wins: int = 0


def _get(url: str):
    req = urllib.request.Request(url, headers={"User-Agent": "smarter-snr-bt"})
    for _ in range(5):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                d = json.loads(r.read())
                if isinstance(d, dict) and d.get("code") == -1003:
                    return None
                return d
        except (urllib.error.HTTPError, Exception):
            time.sleep(0.4)
    return None


def fetch_klines(sym: str, start_ms: int, end_ms: int) -> list[Bar]:
    rows: list[Bar] = []
    cur = start_ms
    while cur < end_ms:
        batch = None
        for base, path in [(FAPI, "/fapi/v1/klines"), (SPOT, "/api/v3/klines")]:
            batch = _get(f"{base}{path}?symbol={sym}&interval=5m&startTime={cur}&endTime={end_ms}&limit=1500")
            if batch and isinstance(batch, list):
                break
        if not batch:
            break
        for k in batch:
            rows.append(Bar(int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])))
        nxt = int(batch[-1][0]) + BAR_MS
        if nxt <= cur:
            break
        cur = nxt
        if len(batch) < 1500:
            break
        time.sleep(0.02)
    return rows


def list_symbols(n: int) -> list[str]:
    batch = _get(f"{FAPI}/fapi/v1/exchangeInfo") or _get(f"{SPOT}/api/v3/exchangeInfo")
    if not batch:
        return SYMS20[:n]
    out: list[str] = []
    for s in batch["symbols"]:
        if s.get("contractType") and s.get("contractType") != "PERPETUAL":
            continue
        if s.get("quoteAsset") != "USDT" or s.get("status") != "TRADING":
            continue
        if s["symbol"].isascii():
            out.append(s["symbol"])
    return out[:n]


def pnl(side: str, entry: float, px: float) -> float:
    g = (px - entry) / entry * 100 if side == "long" else (entry - px) / entry * 100
    return NOTIONAL * g / 100 - NOTIONAL * FEE_RT


def unrealized(side: str, entry: float, mark: float) -> float:
    g = (mark - entry) / entry * 100 if side == "long" else (entry - mark) / entry * 100
    return NOTIONAL * g / 100


def try_exit(side: str, b: Bar, sl: float, tp: float, held: int, max_hold: int):
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
    if held >= max_hold:
        return "timeout", b.c
    return None


def run_one(bars: list[Bar], tc: TradeCfg, scan_start: int, scan_end: int, sim_end: int) -> dict[str, TypeStats]:
    stats: dict[str, TypeStats] = defaultdict(TypeStats)
    sigs = scan_signals(bars, tc.sd)
    sig_by_i: dict[int, list] = defaultdict(list)
    for s in sigs:
        sig_by_i[s.bar_i].append(s)

    pos = None
    open_snap: list[tuple[str, str, float]] = []
    scan_mark = 0.0
    snap = False

    for i, b in enumerate(bars):
        if b.ts >= sim_end:
            break
        if not snap and b.ts >= scan_end:
            snap = True
            scan_mark = bars[i - 1].c if i else b.c
            if pos and scan_start <= pos["ts"] < scan_end:
                open_snap.append((pos["key"], pos["side"], pos["entry"]))

        if pos and b.ts > pos["ts"]:
            pos["bars"] += 1
            hit = try_exit(pos["side"], b, pos["sl"], pos["tp"], pos["bars"], tc.max_hold)
            if hit and pos["ts"] >= scan_start:
                st = stats[pos["key"]]
                r, px = hit
                st.tp += r == "tp"
                st.sl += r == "sl"
                st.to += r == "timeout"
                net = pnl(pos["side"], pos["entry"], px)
                st.real += net
                if net > 0:
                    st.wins += 1
                pos = None

        if scan_start <= b.ts < scan_end and not pos and i in sig_by_i:
            for sig in sig_by_i[i]:
                st = stats[sig.sig_type]
                st.sig += 1
                entry = sig.entry
                if sig.side == "long":
                    sl = entry * (1 - tc.sl_pct / 100)
                    tp = entry * (1 + tc.tp_pct / 100)
                else:
                    sl = entry * (1 + tc.sl_pct / 100)
                    tp = entry * (1 - tc.tp_pct / 100)
                st.ent += 1
                pos = {"key": sig.sig_type, "side": sig.side, "entry": entry, "ts": b.ts, "sl": sl, "tp": tp, "bars": 0}
                break

    if scan_mark <= 0:
        for b in reversed(bars):
            if b.ts < scan_end:
                scan_mark = b.c
                break
    for key, side, entry in open_snap:
        stats[key].unrl += unrealized(side, entry, scan_mark)
    return stats


def run_one_dry_parity(bars: list[Bar], tc: TradeCfg, scan_start: int, scan_end: int) -> dict[str, TypeStats]:
    """
    Match dry paper behavior:
    - walk-forward on rolling BAR_HISTORY_MAX bars
    - signals only on latest closed bar
    - first signal only
    - snapshot exits/open exactly at scan_end
    """
    history_max = 400
    warmup_bars = 80
    stats: dict[str, TypeStats] = defaultdict(TypeStats)
    pos = None
    window: list[Bar] = []
    open_snap: list[tuple[str, str, float]] = []
    scan_mark = 0.0

    for b in bars:
        if b.ts >= scan_end and scan_mark <= 0:
            scan_mark = window[-1].c if window else b.c
            if pos and scan_start <= pos["ts"] < scan_end:
                open_snap.append((pos["key"], pos["side"], pos["entry"]))

        window.append(b)
        if len(window) > history_max:
            window = window[-history_max:]

        if pos and b.ts > pos["ts"]:
            pos["bars"] += 1
            hit = try_exit(pos["side"], b, pos["sl"], pos["tp"], pos["bars"], tc.max_hold)
            if hit and pos["ts"] >= scan_start and b.ts <= scan_end:
                st = stats[pos["key"]]
                r, px = hit
                st.tp += r == "tp"
                st.sl += r == "sl"
                st.to += r == "timeout"
                net = pnl(pos["side"], pos["entry"], px)
                st.real += net
                if net > 0:
                    st.wins += 1
                pos = None

        if scan_start <= b.ts < scan_end and not pos and len(window) >= warmup_bars:
            latest_i = len(window) - 1
            sigs = [s for s in scan_signals(window, tc.sd) if s.bar_i == latest_i]
            if sigs:
                sig = sigs[0]
                st = stats[sig.sig_type]
                st.sig += 1
                entry = sig.entry
                if sig.side == "long":
                    sl = entry * (1 - tc.sl_pct / 100)
                    tp = entry * (1 + tc.tp_pct / 100)
                else:
                    sl = entry * (1 + tc.sl_pct / 100)
                    tp = entry * (1 - tc.tp_pct / 100)
                st.ent += 1
                pos = {"key": sig.sig_type, "side": sig.side, "entry": entry, "ts": b.ts, "sl": sl, "tp": tp, "bars": 0}

        if b.ts >= scan_end:
            break

    if scan_mark <= 0:
        for b in reversed(bars):
            if b.ts < scan_end:
                scan_mark = b.c
                break
    if pos and scan_start <= pos["ts"] < scan_end:
        open_snap.append((pos["key"], pos["side"], pos["entry"]))
    for key, side, entry in open_snap:
        stats[key].unrl += unrealized(side, entry, scan_mark)
    return stats


def merge(dst: dict[str, TypeStats], src: dict[str, TypeStats]) -> None:
    for k, s in src.items():
        d = dst[k]
        for f in ("sig", "ent", "tp", "sl", "to", "wins"):
            setattr(d, f, getattr(d, f) + getattr(s, f))
        d.real += s.real
        d.unrl += s.unrl


def summarize(st: dict[str, TypeStats]) -> tuple[int, float, float, float, float]:
    ent = sum(x.ent for x in st.values())
    ex = sum(x.tp + x.sl + x.to for x in st.values())
    wr = sum(x.wins for x in st.values()) / ex * 100 if ex else 0.0
    real = sum(x.real for x in st.values())
    unrl = sum(x.unrl for x in st.values())
    return ent, wr, real, unrl, real + unrl


def process(sym: str, tcs: list[TradeCfg], scan_start: int, scan_end: int, sim_end: int, warmup: int, mode: str):
    end_ms = sim_end if mode == "classic" else scan_end + BAR_MS
    bars = fetch_klines(sym, warmup, end_ms)
    if len(bars) < 200:
        return sym, None
    if mode == "dry-parity":
        return sym, {tc.name: run_one_dry_parity(bars, tc, scan_start, scan_end) for tc in tcs}
    return sym, {tc.name: run_one(bars, tc, scan_start, scan_end, sim_end) for tc in tcs}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-06-25T00:00:00+00:00")
    ap.add_argument("--hours", type=float, default=30.0)
    ap.add_argument("--timeout-hours", type=float, default=24.0)
    ap.add_argument("--symbols", type=int, default=20)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--mode", choices=["classic", "dry-parity"], default="classic")
    ap.add_argument("--out", type=Path, default=Path("data/aws/sr_chartprime/smarter_snr_snr_cross.txt"))
    args = ap.parse_args()

    syms = SYMS20[: args.symbols] if args.symbols <= len(SYMS20) else list_symbols(args.symbols)
    if args.mode == "dry-parity":
        syms = sorted(syms)
    start = datetime.fromisoformat(args.start)
    end = start + timedelta(hours=args.hours)
    scan_start = int(start.timestamp() * 1000)
    scan_end = int(end.timestamp() * 1000)
    sim_end = scan_end + int(args.timeout_hours * 3600 * 1000)
    warmup = scan_start - 5 * 24 * BAR_MS
    max_hold = max(1, int(args.timeout_hours * 3600 * 1000 / BAR_MS))
    for tc in TRADE_CONFIGS:
        tc.max_hold = max_hold

    merged = {tc.name: defaultdict(TypeStats) for tc in TRADE_CONFIGS}
    ok = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(process, s, TRADE_CONFIGS, scan_start, scan_end, sim_end, warmup, args.mode): s for s in syms}
        for fut in as_completed(futs):
            sym, res = fut.result()
            if not res:
                continue
            ok += 1
            for name, st in res.items():
                merge(merged[name], st)

    cfg_name = TRADE_CONFIGS[0].name
    st = merged[cfg_name]
    ent, wr, real, unrl, comb = summarize(st)
    pp = real / BANKROLL * 100
    cp = comb / BANKROLL * 100

    lines = [
        "Smarter SnR — snr_cross_tp15_sl8 signal breakdown",
        f"mode={args.mode}",
        f"symbols={ok}/{len(syms)} | notional=${NOTIONAL} bankroll=${BANKROLL}",
        f"TP=1.5% SL=8% | scan={args.hours}h hold={args.timeout_hours}h",
        f"scan_utc: {start:%Y-%m-%d %H:%M} → {end:%Y-%m-%d %H:%M}",
        f"scan_ist: {start.astimezone(IST):%Y-%m-%d %H:%M} → {end.astimezone(IST):%Y-%m-%d %H:%M}",
        "",
        f"TOTAL  ent={ent}  tp={sum(x.tp for x in st.values())}  sl={sum(x.sl for x in st.values())}  "
        f"to={sum(x.to for x in st.values())}  wr={wr:.1f}%  "
        f"real=${real:+.2f} ({pp:+.1f}%)  unrl=${unrl:+.2f}  comb=${comb:+.2f} ({cp:+.1f}%)",
        "",
        f"{'signal':<10} {'ent':>5} {'tp':>5} {'sl':>5} {'to':>5} {'WR%':>6} {'real':>8} {'PnL%':>7} {'unrl':>8} {'comb':>8}",
        "-" * 78,
    ]

    rows = []
    for key in sorted(st):
        s = st[key]
        if s.ent == 0:
            continue
        ex = s.tp + s.sl + s.to
        swr = s.wins / ex * 100 if ex else 0.0
        scomb = s.real + s.unrl
        spp = s.real / BANKROLL * 100
        rows.append((scomb, key, s, swr, scomb, spp))

    rows.sort(key=lambda x: -x[0])
    for _, key, s, swr, scomb, spp in rows:
        lines.append(
            f"{key:<10} {s.ent:>5} {s.tp:>5} {s.sl:>5} {s.to:>5} {swr:>5.1f}% "
            f"{s.real:>+8.2f} {spp:>+6.1f}% {s.unrl:>+8.2f} {scomb:>+8.2f}"
        )

    lines.extend([
        "-" * 78,
        f"{'TOTAL':<10} {ent:>5} {sum(x.tp for x in st.values()):>5} {sum(x.sl for x in st.values()):>5} "
        f"{sum(x.to for x in st.values()):>5} {wr:>5.1f}% "
        f"{real:>+8.2f} {pp:>+6.1f}% {unrl:>+8.2f} {comb:>+8.2f}",
        "",
        "Signals: s_co=support cross up LONG | s_cu=support cross down SHORT",
        "         r_co=resistance cross up LONG | r_cu=resistance cross down SHORT",
    ])

    text = "\n".join(lines) + "\n"
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(text)
    print(text)
    print(f"written: {args.out}")


if __name__ == "__main__":
    main()
