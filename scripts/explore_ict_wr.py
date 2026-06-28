#!/usr/bin/env python3
"""Explore ICT filter/exit 'jugaad' configs targeting high WR (no lookahead)."""

from __future__ import annotations

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

from backtest_ict_30h import (  # noqa: E402
    BAR_MS,
    FEE_RT,
    GRAB_MIN_SIZE,
    NOTIONAL,
    OB_LENGTH,
    OB_MAX_RISK_PCT,
    OB_MAX_ZONE_PCT,
    OB_RETEST_BARS,
    OBZone,
    OpenSnap,
    Pos,
    TypeStats,
    fetch_klines,
    form_ob,
    list_symbols,
    merge,
    pnl,
    record_exit,
    swing_update,
    try_exit,
    unrealized,
)
from liquidity_grabs_lib import Bar, GrabState, on_bar_confirmed  # noqa: E402

FAPI = "https://fapi.binance.com"
SPOT = "https://data-api.binance.vision"


@dataclass
class Cfg:
    name: str
    tp_pct: float = 0.5
    max_risk_pct: float = 8.0
    min_grab: int = 1
    confirm: bool = False  # OB: close in trade dir; grab: body in dir
    trend: bool = False  # OB only: trade with 20-bar SMA
    be_trigger_pct: float | None = None  # move SL to entry after this MFE
    max_hold_bars: int = 288
    ob_only: bool = False
    grab_only: bool = False
    zone_max_pct: float = 4.0
    retest_bars: int = 20


CONFIGS = [
    Cfg("baseline_tp05", tp_pct=0.5),
    Cfg("tp01", tp_pct=0.1),
    Cfg("tp015", tp_pct=0.15),
    Cfg("tp02", tp_pct=0.2),
    Cfg("tp01_be008", tp_pct=0.1, be_trigger_pct=0.08),
    Cfg("tp015_be01", tp_pct=0.15, be_trigger_pct=0.10),
    Cfg("tp02_be012", tp_pct=0.2, be_trigger_pct=0.12),
    Cfg("confirm_tp015", tp_pct=0.15, confirm=True),
    Cfg("trend_tp02", tp_pct=0.2, trend=True),
    Cfg("tight3_tp015", tp_pct=0.15, max_risk_pct=3.0),
    Cfg("grab3_tp01", tp_pct=0.1, min_grab=3, grab_only=True),
    Cfg("ob_confirm_trend_tp015", tp_pct=0.15, confirm=True, trend=True, ob_only=True),
    Cfg("stack_v1", tp_pct=0.12, be_trigger_pct=0.08, confirm=True, max_risk_pct=3.0, min_grab=2, max_hold_bars=48),
    Cfg("stack_v2", tp_pct=0.08, be_trigger_pct=0.05, confirm=True, trend=True, max_risk_pct=2.0, min_grab=3, zone_max_pct=2.5, retest_bars=12, max_hold_bars=24),
    Cfg("micro_tp005", tp_pct=0.05, be_trigger_pct=0.04, max_hold_bars=36),
    Cfg("micro_tp003", tp_pct=0.03, be_trigger_pct=0.025, max_hold_bars=24),
]


def sma20(bars: list[Bar], i: int) -> float | None:
    if i < 19:
        return None
    return sum(b.c for b in bars[i - 19 : i + 1]) / 20


def levels(side: str, entry: float, sl: float, tp_pct: float) -> tuple[float, float]:
    if side == "long":
        return sl, entry * (1 + tp_pct / 100)
    return sl, entry * (1 - tp_pct / 100)


def ob_sl(side: str, entry: float, ob: OBZone, max_risk: float, zone_max: float) -> float | None:
    mid = (ob.top + ob.btm) / 2
    if mid <= 0 or (ob.top - ob.btm) / mid * 100 > zone_max:
        return None
    buf = 0.0005
    if side == "long":
        sl = ob.btm * (1 - buf)
        if entry - sl <= 0 or (entry - sl) / entry * 100 > max_risk:
            return None
    else:
        sl = ob.top * (1 + buf)
        if sl - entry <= 0 or (sl - entry) / entry * 100 > max_risk:
            return None
    return sl


def grab_sl(g, max_risk: float) -> float | None:
    entry, sl = g.entry_px, g.sl_px
    if g.side == "short":
        if sl - entry <= 0 or (sl - entry) / entry * 100 > max_risk:
            return None
    else:
        if entry - sl <= 0 or (entry - sl) / entry * 100 > max_risk:
            return None
    return sl


def try_exit_be(side: str, b: Bar, sl: float, tp: float, entry: float, bars_held: int, max_hold: int, be_armed: bool) -> tuple[str, float] | None:
    eff_sl = sl
    if be_armed:
        if side == "long":
            eff_sl = max(sl, entry)
        else:
            eff_sl = min(sl, entry)
    return try_exit(side, b, eff_sl, tp, bars_held, max_hold)


def mfe_hit(side: str, entry: float, hi: float, lo: float, trigger_pct: float) -> bool:
    if side == "long":
        return (hi - entry) / entry * 100 >= trigger_pct
    return (entry - lo) / entry * 100 >= trigger_pct


def run_cfg(sym: str, bars: list[Bar], cfg: Cfg, scan_start: int, scan_end: int, sim_end: int) -> tuple[dict[str, TypeStats], int, int]:
    stats: dict[str, TypeStats] = defaultdict(TypeStats)
    swing_st = {"os": 0, "top_y": None, "top_x": 0, "top_crossed": False, "btm_y": None, "btm_x": 0, "btm_crossed": False}
    obs: list[OBZone] = []
    grab_st = GrabState()
    open_pos: Pos | None = None
    be_armed = False

    for i, b in enumerate(bars):
        if b.ts >= sim_end:
            break

        if open_pos and b.ts > open_pos.entry_ts:
            if cfg.be_trigger_pct and not be_armed:
                if mfe_hit(open_pos.side, open_pos.entry, b.h, b.l, cfg.be_trigger_pct):
                    be_armed = True
            open_pos.bars_held += 1
            hit = try_exit_be(open_pos.side, b, open_pos.sl, open_pos.tp, open_pos.entry, open_pos.bars_held, cfg.max_hold_bars, be_armed)
            if hit and open_pos.entry_ts >= scan_start:
                record_exit(stats[open_pos.key], hit[0], pnl(open_pos.side, open_pos.entry, hit[1]))
                open_pos, be_armed = None, False

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

        if not cfg.grab_only:
            for ob in obs:
                if ob.traded or ob.broken or i <= ob.formed_i or i - ob.formed_i > cfg.retest_bars:
                    continue
                if not (b.l <= ob.top and b.h >= ob.btm):
                    continue
                key = ob.kind
                side = "long" if key == "ob_plus" else "short"
                stats[key].signals += 1
                if cfg.confirm:
                    if side == "long" and b.c <= b.o:
                        stats[key].skipped += 1
                        continue
                    if side == "short" and b.c >= b.o:
                        stats[key].skipped += 1
                        continue
                if cfg.trend:
                    ma = sma20(bars, i)
                    if ma is None:
                        stats[key].skipped += 1
                        continue
                    if side == "long" and b.c < ma:
                        stats[key].skipped += 1
                        continue
                    if side == "short" and b.c > ma:
                        stats[key].skipped += 1
                        continue
                sl = ob_sl(side, b.c, ob, cfg.max_risk_pct, cfg.zone_max_pct)
                if sl is None:
                    stats[key].skipped += 1
                    continue
                _, tp = levels(side, b.c, sl, cfg.tp_pct)
                stats[key].entries += 1
                open_pos = Pos(sym, key, side, b.c, b.ts, sl, tp)
                be_armed = False
                ob.traded = True
                break

        if open_pos or cfg.ob_only:
            continue
        if i < 60:
            continue
        g = on_bar_confirmed(grab_st, bars, i)
        if not g:
            continue
        key = g.grab_type
        stats[key].signals += 1
        if g.grab_size < cfg.min_grab:
            stats[key].skipped += 1
            continue
        if cfg.confirm:
            if g.side == "long" and g.entry_px <= bars[i].o:
                stats[key].skipped += 1
                continue
            if g.side == "short" and g.entry_px >= bars[i].o:
                stats[key].skipped += 1
                continue
        sl = grab_sl(g, cfg.max_risk_pct)
        if sl is None:
            stats[key].skipped += 1
            continue
        _, tp = levels(g.side, g.entry_px, sl, cfg.tp_pct)
        stats[key].entries += 1
        open_pos = Pos(sym, key, g.side, g.entry_px, b.ts, sl, tp)
        be_armed = False

    wins = sum(s.wins for s in stats.values())
    ex = sum(s.tp + s.sl + s.timeout for s in stats.values())
    return stats, ex, wins


def summarize(all_cfg: dict[str, TypeStats]) -> tuple[int, int, float, float]:
    ex = sum(s.tp + s.sl + s.timeout for s in all_cfg.values())
    ent = sum(s.entries for s in all_cfg.values())
    wr = sum(s.wins for s in all_cfg.values()) / ex * 100 if ex else 0.0
    real = sum(s.real for s in all_cfg.values())
    return ent, ex, wr, real


def process(sym: str, cfgs: list[Cfg], scan_start: int, scan_end: int, sim_end: int, warmup: int):
    bars = fetch_klines(sym, warmup, sim_end)
    if len(bars) < 200:
        return sym, None
    out = {}
    for cfg in cfgs:
        st, ex, _ = run_cfg(sym, bars, cfg, scan_start, scan_end, sim_end)
        out[cfg.name] = st
    return sym, out


def main():
    scan_start = int(datetime.fromisoformat("2026-06-26T00:00:00+00:00").timestamp() * 1000)
    scan_end = scan_start + int(30 * 3600 * 1000)
    sim_end = scan_end + int(24 * 3600 * 1000)
    warmup = scan_start - 5 * 24 * BAR_MS
    symbols = list_symbols(300)

    merged: dict[str, dict[str, TypeStats]] = {c.name: defaultdict(TypeStats) for c in CONFIGS}
    ok = 0
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(process, s, CONFIGS, scan_start, scan_end, sim_end, warmup): s for s in symbols}
        for n, fut in enumerate(as_completed(futs), 1):
            sym, res = fut.result()
            if not res:
                continue
            ok += 1
            for cname, st in res.items():
                merge(merged[cname], st)
            if n % 25 == 0:
                print(f"  {n}/{len(symbols)}", flush=True)

    rows = []
    for cfg in CONFIGS:
        ent, ex, wr, real = summarize(merged[cfg.name])
        rows.append((wr, ent, ex, real, cfg.name, cfg))

    rows.sort(key=lambda x: (-x[0], -x[1]))
    lines = [
        f"ICT WR exploration | 26 Jun 30h | {ok} symbols | {len(CONFIGS)} configs",
        "goal: high WR without lookahead | BE=move SL to entry after MFE trigger",
        "",
        f"{'config':<28} {'ent':>6} {'exits':>6} {'WR%':>6} {'real$':>8}  notes",
        "-" * 78,
    ]
    for wr, ent, ex, real, name, cfg in rows:
        notes = f"tp={cfg.tp_pct}% risk<{cfg.max_risk_pct}% hold={cfg.max_hold_bars}"
        if cfg.be_trigger_pct:
            notes += f" be@{cfg.be_trigger_pct}%"
        if cfg.confirm:
            notes += " confirm"
        if cfg.trend:
            notes += " trend"
        if cfg.min_grab > 1:
            notes += f" grab>={cfg.min_grab}"
        flag = " ***" if wr >= 98 and ent >= 30 else (" **" if wr >= 90 and ent >= 50 else "")
        lines.append(f"{name:<28} {ent:>6} {ex:>6} {wr:>5.1f}% {real:>+8.2f}  {notes}{flag}")

    best = [r for r in rows if r[0] >= 98 and r[1] >= 20]
    lines.append("")
    if best:
        lines.append("configs with WR>=98% and ent>=20:")
        for r in best:
            lines.append(f"  {r[4]}: WR={r[0]:.1f}% ent={r[1]} real=${r[3]:+.2f}")
    else:
        lines.append("no config hit WR>=98% with ent>=20 on this day.")
        top = rows[0]
        lines.append(f"best: {top[4]} WR={top[0]:.1f}% ent={top[1]} real=${top[3]:+.2f}")

    text = "\n".join(lines) + "\n"
    out = ROOT / "data/aws/sr_chartprime/ict_wr_explore_26jun.txt"
    out.write_text(text)
    print(text)
    print(f"written: {out}")


if __name__ == "__main__":
    main()
