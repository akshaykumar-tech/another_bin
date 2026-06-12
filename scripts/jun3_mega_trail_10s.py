#!/usr/bin/env python3
"""10s price path after entry for Jun 3 mega_tp / trail trades."""
import json
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

BASE = "https://fapi.binance.com/fapi/v1/aggTrades"

# Jun 3 mega/trail — from journal transcript (deduped)
TRADES = [
    {
        "symbol": "HIGHUSDT",
        "side": "SELL",
        "exit": "mega_tp",
        "entry_utc": "2026-06-03T08:19:41Z",
        "signal_entry": 0.121639,
        "live_entry": 0.119600,
        "exit_utc": "2026-06-03T08:21:43Z",
    },
    {
        "symbol": "PROMUSDT",
        "side": "BUY",
        "exit": "trail",
        "entry_utc": "2026-06-03T09:54:18Z",
        "signal_entry": 1.054527,
        "live_entry": 1.050000,
        "exit_utc": "2026-06-03T09:55:00Z",
    },
    {
        "symbol": "XNYUSDT",
        "side": "BUY",
        "exit": "trail",
        "entry_utc": "2026-06-03T10:53:47Z",
        "signal_entry": 0.006223,
        "live_entry": 0.006226,
        "exit_utc": "2026-06-03T10:57:15Z",
    },
    {
        "symbol": "NAORISUSDT",
        "side": "BUY",
        "exit": "trail",
        "entry_utc": "2026-06-03T13:00:17Z",
        "signal_entry": 0.033867,
        "live_entry": 0.034000,
        "exit_utc": "2026-06-03T13:00:30Z",
    },
]


def parse_utc(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def fetch_agg(symbol: str, start_ms: int, end_ms: int) -> list:
    rows = []
    cur = start_ms
    while cur < end_ms:
        q = urllib.parse.urlencode(
            {"symbol": symbol, "startTime": cur, "endTime": end_ms, "limit": 1000}
        )
        with urllib.request.urlopen(
            f"{BASE}?{q}", timeout=60
        ) as resp:
            batch = json.loads(resp.read())
        if not batch:
            break
        rows.extend(batch)
        cur = batch[-1]["T"] + 1
        time.sleep(0.06)
    out = []
    seen = set()
    for r in rows:
        if r["a"] in seen:
            continue
        seen.add(r["a"])
        out.append({"t": r["T"], "p": float(r["p"])})
    out.sort(key=lambda x: x["t"])
    return out


def move_pct(side: str, entry: float, px: float) -> float:
    if side == "BUY":
        return (px - entry) / entry * 100.0
    return (entry - px) / entry * 100.0


def price_at(ticks: list, t_ms: int) -> float:
    px = ticks[0]["p"]
    for x in ticks:
        if x["t"] <= t_ms:
            px = x["p"]
        else:
            break
    return px


def analyze(t: dict) -> dict:
    entry_dt = parse_utc(t["entry_utc"])
    entry_ms = int(entry_dt.timestamp() * 1000)
    ticks = fetch_agg(t["symbol"], entry_ms - 2000, entry_ms + 15_000)
    entry_px = t["live_entry"]
    side = t["side"]

    horizons_ms = [0, 100, 200, 300, 500, 750, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000]
    path = []
    fav_max = -1e9
    adv_max = 1e9
    for h in horizons_ms:
        px = price_at(ticks, entry_ms + h)
        m = move_pct(side, entry_px, px)
        fav_max = max(fav_max, m)
        adv_max = min(adv_max, m)
        path.append({"ms": h, "price": px, "move_pct": round(m, 4)})

    # per-second buckets 1..10
    seconds = []
    for sec in range(1, 11):
        t_end = entry_ms + sec * 1000
        slice_ticks = [x for x in ticks if entry_ms < x["t"] <= t_end]
        if not slice_ticks:
            seconds.append({"sec": sec, "move_pct": None})
            continue
        # min/max move in that second window (cumulative from entry)
        moves = [move_pct(side, entry_px, x["p"]) for x in slice_ticks]
        seconds.append(
            {
                "sec": sec,
                "end_price": slice_ticks[-1]["p"],
                "move_pct": round(moves[-1], 4),
                "fav_peak_in_sec": round(max(moves), 4),
                "adv_low_in_sec": round(min(moves), 4),
            }
        )

    return {
        "trade": t,
        "n_ticks": len(ticks),
        "path_ms": path,
        "by_second": seconds,
        "fav_max_10s": round(fav_max, 4),
        "adv_max_10s": round(adv_max, 4),
    }


def main():
    results = []
    for t in TRADES:
        print(f"Fetching {t['symbol']} {t['side']} {t['exit']} ...", flush=True)
        results.append(analyze(t))
        time.sleep(0.15)

    for r in results:
        t = r["trade"]
        print("\n" + "=" * 72)
        print(
            f"{t['symbol']} | Direction: **{t['side']}** | Exit: **{t['exit']}** | "
            f"Entry UTC: {t['entry_utc']}"
        )
        print(f"  signal_entry={t['signal_entry']}  live_entry={t['live_entry']}")
        print(f"  10s max favorable: {r['fav_max_10s']:+.4f}%  |  max adverse: {r['adv_max_10s']:+.4f}%")
        print("\n  Time after live entry → move % (favorable + / adverse - for position):")
        for p in r["path_ms"]:
            tag = "fav" if p["move_pct"] >= 0 else "adv"
            print(f"    +{p['ms']:5d}ms: {p['move_pct']:+7.4f}%  @ {p['price']:.8f}  ({tag})")
        print("\n  Per second (price at end of each second from entry):")
        for s in r["by_second"]:
            if s["move_pct"] is None:
                print(f"    t={s['sec']}s: no ticks")
            else:
                print(
                    f"    t={s['sec']:2d}s: {s['move_pct']:+7.4f}%  "
                    f"(peak in sec {s['fav_peak_in_sec']:+.4f}%, low {s['adv_low_in_sec']:+.4f}%)  "
                    f"px={s['end_price']:.8f}"
                )

    out = "/home/deeporion/Desktop/binance/crypto_announcements_go/scripts/jun3_mega_trail_10s.json"
    with open(out, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nWrote {out}")


if __name__ == "__main__":
    main()
