#!/usr/bin/env python3
"""
Per-symbol gate optimizer on 1s kline CSVs.

Reads config/whale-focused.yaml, grid-searches gates per symbol,
writes data/analysis/per_symbol_gates.yaml with optimized values.

  python3 scripts/per_symbol_optimize.py
  python3 scripts/per_symbol_optimize.py --apply   # patch whale-focused.yaml gates
"""
from __future__ import annotations

import argparse
import copy
import statistics as st
from pathlib import Path

import yaml

from focused_lib import Bar, SymbolState, gate_passes, in_cooldown, pnl_t1_bars

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG = ROOT / "config" / "whale-focused.yaml"
KDIR = ROOT / "data" / "klines" / "1s"
OUT = ROOT / "data" / "analysis" / "per_symbol_gates.yaml"
LEG_MIN_AMP = 6.0
LEG_GAP_MS = 120_000


def load_bars(sym: str) -> list[Bar]:
    path = KDIR / f"{sym}.csv"
    if not path.is_file():
        return []
    import csv

    bars = []
    with path.open() as f:
        for r in csv.DictReader(f):
            bars.append(
                Bar(
                    sec=int(r["timestamp_ms"]),
                    o=float(r["open"]),
                    h=float(r["high"]),
                    l=float(r["low"]),
                    c=float(r["close"]),
                    vol=float(r["volume"]),
                )
            )
    return bars


def find_legs(bars: list[Bar]) -> list[tuple[int, str, float]]:
    legs = []
    last = 0
    for i, b in enumerate(bars):
        if b.vol < 100 or b.amp_pct() < LEG_MIN_AMP:
            continue
        if last and b.sec - last < LEG_GAP_MS:
            continue
        legs.append((b.sec, b.direction(), b.amp_pct()))
        last = b.sec
    return legs


def sim_bars(bars: list[Bar], gate: dict, rearm_sec: int) -> dict:
    st_state = SymbolState()
    sigs = []
    for i, b in enumerate(bars):
        st_state.push(b)
        if in_cooldown(st_state, b.sec, rearm_sec):
            continue
        if gate_passes(gate, b, st_state):
            sigs.append((b.sec, b.direction(), i))
            st_state.last_signal_ms = b.sec
    pnls = []
    for _, d, idx in sigs:
        r = pnl_t1_bars(bars, idx, d)
        if r:
            pnls.append(r[0])
    legs = find_legs(bars)
    tp = sum(
        1
        for lt, ld, _ in legs
        if any(ld == d and 1000 <= lt - s <= 60_000 for s, d, _ in sigs)
    )
    fp = 0
    for s, d, _ in sigs:
        if not any(ld == d and 1000 <= lt - s <= 60_000 for lt, ld, _ in legs):
            fp += 1
    wins = sum(1 for p in pnls if p > 0.5)
    return {
        "n": len(sigs),
        "tp": tp,
        "fp": fp,
        "legs": len(legs),
        "wins": wins,
        "wr": 100 * wins / len(pnls) if pnls else 0,
        "avg": st.mean(pnls) if pnls else 0,
        "pnls": pnls,
    }


def grid_pct_burst(bars: list[Bar], legs: list) -> tuple[dict, dict] | None:
    best = None
    for amp in [0.7, 1.0, 1.5, 2.0, 2.5, 3.0]:
        for notional in [5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000]:
            for rearm in [180, 300, 600]:
                gate = {"type": "pct_burst", "amp_min_pct": amp, "notional_min_usdt": notional, "rearm_sec": rearm}
                ev = sim_bars(bars, gate, rearm)
                if ev["n"] < 2:
                    continue
                score = ev["wr"] * 0.5 + ev["avg"] * 5 + (ev["tp"] / max(ev["legs"], 1)) * 30 - (ev["fp"] / max(ev["tp"], 1)) * 2
                if ev["wr"] < 40 and ev["avg"] <= 0:
                    continue
                cand = (score, gate, ev)
                if best is None or cand[0] > best[0]:
                    best = cand
    if not best:
        return None
    _, gate, ev = best
    return gate, ev


def grid_vol_ramp(bars: list[Bar], legs: list) -> tuple[dict, dict] | None:
    best = None
    for vmin in [500_000, 1_000_000, 5_000_000, 10_000_000, 20_000_000]:
        for rmin in [0.5, 1.0, 2.0, 3.0]:
            for rearm in [180, 300, 600]:
                gate = {"type": "vol_ramp", "vol10_min": vmin, "ret60_min_pct": rmin, "rearm_sec": rearm}
                ev = sim_bars(bars, gate, rearm)
                if ev["n"] < 2:
                    continue
                score = ev["wr"] * 0.5 + ev["avg"] * 5 + (ev["tp"] / max(ev["legs"], 1)) * 30 - (ev["fp"] / max(ev["tp"], 1)) * 2
                if ev["wr"] < 40 and ev["avg"] <= 0:
                    continue
                cand = (score, gate, ev)
                if best is None or cand[0] > best[0]:
                    best = cand
    if not best:
        return None
    _, gate, ev = best
    return gate, ev


def default_gate_from_bars(bars: list[Bar]) -> dict | None:
    """Conservative per-symbol gate from observed spike volume."""
    spikes = [b.notional for b in bars if b.amp_pct() >= 1.0]
    if not spikes:
        spikes = [b.notional for b in bars if b.vol > 0]
    if not spikes:
        return None
    spikes.sort()
    p75 = spikes[int(len(spikes) * 0.75)]
    amp = 2.0
    if len(spikes) >= 5:
        amps = sorted(b.amp_pct() for b in bars if b.amp_pct() >= 0.5)
        if amps:
            amp = max(1.5, min(3.0, amps[int(len(amps) * 0.75)]))
    return {
        "type": "pct_burst",
        "amp_min_pct": round(amp, 2),
        "notional_min_usdt": max(int(p75), 5_000),
        "rearm_sec": 600,
    }


def pick_gate(sym: str, profile: str, bars: list[Bar]) -> dict:
    legs = find_legs(bars)
    if not bars:
        return {"type": "disabled", "reason": "no_kline_data"}

    candidates = []
    if profile == "cascade_pump":
        r = grid_vol_ramp(bars, legs)
        if r:
            candidates.append(r)
    r = grid_pct_burst(bars, legs)
    if r:
        candidates.append(r)

    if candidates:
        def rank(item):
            gate, ev = item
            leg_bonus = (ev["tp"] / max(ev["legs"], 1)) * 25 if ev["legs"] else 0
            return ev["wr"] * 0.4 + ev["avg"] * 4 + leg_bonus

        gate, ev = max(candidates, key=rank)
        return {
            **gate,
            "backtest": {
                "fires": ev["n"],
                "winrate_pct": round(ev["wr"], 1),
                "avg_pnl_pct": round(ev["avg"], 2),
                "leg_tp": ev["tp"],
                "legs_total": len(legs),
                "fp": ev["fp"],
            },
        }

    fallback = default_gate_from_bars(bars)
    if not fallback:
        return {"type": "disabled", "reason": "no_kline_data"}
    ev = sim_bars(bars, fallback, int(fallback["rearm_sec"]))
    return {
        **fallback,
        "backtest": {
            "fires": ev["n"],
            "winrate_pct": round(ev["wr"], 1),
            "avg_pnl_pct": round(ev["avg"], 2),
            "leg_tp": ev["tp"],
            "legs_total": len(legs),
            "fp": ev["fp"],
            "note": "data_driven_default",
        },
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=str(DEFAULT_CONFIG))
    ap.add_argument("--apply", action="store_true", help="write optimized gates into whale-focused.yaml")
    args = ap.parse_args()
    cfg_path = Path(args.config)
    cfg = yaml.safe_load(cfg_path.read_text())
    symbols_cfg = cfg.get("symbols") or {}

    results = {"generated_from": str(cfg_path), "symbols": {}}
    print(f"{'symbol':<16} {'profile':<14} {'gate':<12} {'fires':>5} {'WR%':>5} {'avg%':>7} {'legs':>5}")
    print("-" * 72)

    for sym in sorted(symbols_cfg):
        sc = symbols_cfg[sym]
        profile = sc.get("profile", "burst_drift")
        bars = load_bars(sym)
        gate = pick_gate(sym, profile, bars)
        enabled = gate.get("type") != "disabled"
        results["symbols"][sym] = {
            "profile": profile,
            "enabled": enabled,
            "gate": {k: v for k, v in gate.items() if k != "backtest"},
            "backtest": gate.get("backtest"),
        }
        bt = gate.get("backtest") or {}
        gtype = gate.get("type", "?")
        print(
            f"{sym:<16} {profile:<14} {gtype:<12} {bt.get('fires', 0):>5} "
            f"{bt.get('winrate_pct', 0):>5.1f} {bt.get('avg_pnl_pct', 0):>+7.2f} "
            f"{bt.get('leg_tp', 0)}/{bt.get('legs_total', 0)}"
        )
        if gate.get("type") == "disabled":
            print(f"  -> disabled: {gate.get('reason', '?')}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(yaml.dump(results, default_flow_style=False, sort_keys=False))
    print(f"\nWrote {OUT}")

    if args.apply:
        patched = copy.deepcopy(cfg)
        for sym, res in results["symbols"].items():
            if sym not in patched["symbols"]:
                continue
            patched["symbols"][sym]["enabled"] = res.get("enabled", False)
            if res.get("gate"):
                patched["symbols"][sym]["gate"] = res["gate"]
        cfg_path.write_text(yaml.dump(patched, default_flow_style=False, sort_keys=False))
        print(f"Applied gates to {cfg_path}")


if __name__ == "__main__":
    main()
