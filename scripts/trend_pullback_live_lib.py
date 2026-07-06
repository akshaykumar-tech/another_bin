"""Live sync-mirror: opposite side, exits at original signal SL/TP prices."""
from __future__ import annotations

import asyncio
import traceback
from typing import Callable

from binance_futures import BinanceFuturesClient, parse_fill
from live_config_lib import (
    binance_api_key,
    binance_api_secret,
    live_algo_working_type,
    live_margin_buffer,
    live_max_open,
    live_min_leverage,
    live_notional_usdt,
    live_reconcile_sec,
)
from trend_pullback_engine import Signal, mirror_side


def mirror_brackets(signal: Signal) -> tuple[str, str, float, float]:
    """(mirror_side, close_order_side, take_profit_trigger, stop_loss_trigger)."""
    ms = mirror_side(signal.signal_side)
    if ms == "short":
        return ms, "BUY", signal.sl, signal.tp
    return ms, "SELL", signal.sl, signal.tp


class TrendPullbackMirrorLive:
    def __init__(self, fapi: str, log: Callable[[str], None], *, enabled: bool | None = None) -> None:
        self.log = log
        self.fapi = fapi.rstrip("/")
        self.enabled = False if enabled is None else enabled
        self.notional = live_notional_usdt(6.0)
        self.max_open = live_max_open(5)
        self.min_leverage = live_min_leverage(10)
        self.margin_buffer = live_margin_buffer(1.05)
        self.reconcile_sec = live_reconcile_sec(60)
        self.working_type = live_algo_working_type("CONTRACT_PRICE")
        self.binance: BinanceFuturesClient | None = None
        self.slots: dict[str, dict] = {}
        self.stats = {"entries": 0, "exits": 0, "skips": 0}
        self._lock = asyncio.Lock()

    def configured(self) -> bool:
        return self.binance is not None and self.binance.configured()

    def init_client(self) -> None:
        if not self.enabled:
            return
        key, sec = binance_api_key(), binance_api_secret()
        if not key or not sec:
            return
        self.binance = BinanceFuturesClient(key, sec, self.fapi)
        self.binance.warm_cache()

    async def bootstrap(self) -> None:
        if not self.enabled or self.binance is None:
            return
        loop = asyncio.get_running_loop()
        removed = await loop.run_in_executor(None, self.binance.cancel_orphan_algo_orders)
        if removed:
            self.log(f"[LIVE_BOOT] cancelled orphan algos: {removed}")
        for sym in self.binance.open_position_symbols():
            row = self.binance.position_row(sym)
            if not row:
                continue
            amt = float(row.get("positionAmt") or 0)
            if amt == 0:
                continue
            self.slots[sym] = {"side": "long" if amt > 0 else "short", "qty": abs(amt)}
        self.log(
            f"[LIVE_BOOT] mirror slots={len(self.slots)} notional=${self.notional} "
            f"max_open={self.max_open}"
        )

    async def reconcile_loop(self) -> None:
        while True:
            await asyncio.sleep(max(15, self.reconcile_sec))
            if not self.enabled or self.binance is None:
                continue
            loop = asyncio.get_running_loop()
            try:
                removed = await loop.run_in_executor(None, self.binance.cancel_orphan_algo_orders)
                if removed:
                    self.log(f"[LIVE_RECONCILE] orphan algos cancelled: {removed}")
                open_syms = set(self.binance.open_position_symbols())
                for sym in list(self.slots.keys()):
                    if sym not in open_syms:
                        self.slots.pop(sym, None)
                        self.log(f"[LIVE_RECONCILE] {sym} flat — slot cleared")
            except Exception as e:
                self.log(f"[LIVE_RECONCILE] err {e}")

    async def enter_mirror(self, sig: Signal) -> None:
        if not self.enabled or self.binance is None:
            return
        sym = sig.sym.upper()
        ms, close_side, tp_trig, sl_trig = mirror_brackets(sig)

        def _place():
            if sym in self.binance.open_position_symbols():
                raise RuntimeError("position already open")
            if len(self.binance.open_position_symbols()) >= self.max_open:
                raise RuntimeError(f"max_open={self.max_open}")
            if not self.binance.symbol_tradable(sym):
                raise RuntimeError("not tradable")
            if self.binance.max_leverage(sym) < self.min_leverage:
                raise RuntimeError(f"lev<{self.min_leverage}x")

            lev = self.binance.set_max_leverage(sym)
            margin_need = (self.notional / lev) * self.margin_buffer
            bal = self.binance.available_usdt()
            if bal < margin_need:
                raise RuntimeError(f"margin need ${margin_need:.2f} avail ${bal:.2f}")

            entry_side = "BUY" if ms == "long" else "SELL"
            resp = self.binance.market_order_notional(sym, entry_side, self.notional)
            entry, qty = parse_fill(resp)
            if qty <= 0:
                qty = self.binance.position_qty(sym)
            row = self.binance.position_row(sym)
            if row:
                ex = float(row.get("entryPrice") or 0)
                if ex > 0:
                    entry = ex

            self.binance.cancel_all_algo_orders(sym)
            self.binance.take_profit_market_reduce(sym, close_side, tp_trig, qty, self.working_type)
            self.binance.stop_market_reduce(sym, close_side, sl_trig, qty, self.working_type)
            return lev, entry, qty, tp_trig, sl_trig, bal

        try:
            async with self._lock:
                if sym in self.slots:
                    self.stats["skips"] += 1
                    return
                lev, entry, qty, tp_t, sl_t, bal = await asyncio.get_running_loop().run_in_executor(
                    None, _place
                )
                self.slots[sym] = {"side": ms, "qty": qty, "signal_side": sig.signal_side}
                self.stats["entries"] += 1
                self.log(
                    f"[LIVE_MIRROR] {sym} sig={sig.signal_side.upper()} → {ms.upper()} "
                    f"fill={entry:.8f} TP@{tp_t:.8f} STOP@{sl_t:.8f} lev={lev}x bal=${bal:.2f}"
                )
        except Exception as e:
            self.stats["skips"] += 1
            self.log(f"[LIVE_MIRROR_FAIL] {sym} {e}\n{traceback.format_exc()}")

    def schedule_mirror(self, sig: Signal) -> None:
        try:
            asyncio.get_running_loop().create_task(self.enter_mirror(sig))
        except RuntimeError:
            pass
