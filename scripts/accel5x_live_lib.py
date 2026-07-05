"""5x accel live trader — market entry/exit only, no SL/TP."""
from __future__ import annotations

import asyncio
import traceback
from typing import Callable

from binance_futures import BinanceFuturesClient, parse_fill
from live_config_lib import (
    binance_api_key,
    binance_api_secret,
    live_margin_buffer,
    live_max_open,
    live_min_leverage,
    live_notional_usdt,
    live_reconcile_sec,
)


def entry_order_side(dry_side: str) -> str:
    return "BUY" if dry_side == "long" else "SELL"


def close_order_side(dry_side: str) -> str:
    return "SELL" if dry_side == "long" else "BUY"


class Accel5xLiveTrader:
    def __init__(self, fapi: str, log: Callable[[str], None], *, enabled: bool | None = None) -> None:
        self.log = log
        self.fapi = fapi.rstrip("/")
        self.enabled = False if enabled is None else enabled
        self.notional = live_notional_usdt(6.0)
        self.max_open = live_max_open(30)
        self.min_leverage = live_min_leverage(10)
        self.margin_buffer = live_margin_buffer(1.05)
        self.reconcile_sec = live_reconcile_sec(60)
        self.binance: BinanceFuturesClient | None = None
        self.slots: dict[str, dict] = {}
        self.stats = {"entries": 0, "exits": 0, "skips": 0}
        self._entry_lock = asyncio.Lock()

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

    def _reconcile_slots_sync(self) -> list[str]:
        if self.binance is None:
            return []
        open_syms = set(self.binance.open_position_symbols())
        removed = []
        for sym in list(self.slots.keys()):
            if sym not in open_syms:
                self.slots.pop(sym, None)
                removed.append(sym)
        return removed

    async def bootstrap(self) -> None:
        if not self.enabled or self.binance is None:
            return
        loop = asyncio.get_running_loop()
        n_pos = len(self.binance.open_position_symbols())
        for sym in self.binance.open_position_symbols():
            row = self.binance.position_row(sym)
            if not row:
                continue
            amt = float(row.get("positionAmt") or 0)
            if amt == 0:
                continue
            self.slots[sym] = {
                "side": "long" if amt > 0 else "short",
                "qty": abs(amt),
                "entry_price": float(row.get("entryPrice") or 0),
            }
        self.log(
            f"[LIVE_BOOT] positions={n_pos} slots={len(self.slots)} "
            f"notional=${self.notional} max_open={self.max_open} (no SL/TP)"
        )

    async def reconcile(self) -> None:
        if not self.enabled or self.binance is None:
            return
        loop = asyncio.get_running_loop()
        for sym in await loop.run_in_executor(None, self._reconcile_slots_sync):
            self.log(f"[LIVE_RECONCILE] {sym} flat — slot cleared")

    async def reconcile_loop(self) -> None:
        while True:
            await asyncio.sleep(max(15, self.reconcile_sec))
            await self.reconcile()

    async def entry(self, symbol: str, side: str, ref_px: float, *, tag: str = "") -> None:
        if not self.enabled or self.binance is None:
            return
        sym = symbol.upper()
        tag_s = f" {tag}" if tag else ""
        try:
            async with self._entry_lock:
                if sym in self.slots:
                    self.stats["skips"] += 1
                    self.log(f"[LIVE_SKIP]{tag_s} {sym} already open")
                    return
                open_syms = set(self.binance.open_position_symbols())
                if sym in open_syms:
                    self.stats["skips"] += 1
                    self.log(f"[LIVE_SKIP]{tag_s} {sym} exchange position open")
                    return
                if len(open_syms) >= self.max_open:
                    self.stats["skips"] += 1
                    self.log(f"[LIVE_SKIP]{tag_s} max_open={self.max_open}")
                    return
                if not self.binance.symbol_tradable(sym):
                    self.stats["skips"] += 1
                    return
                if self.binance.max_leverage(sym) < self.min_leverage:
                    self.stats["skips"] += 1
                    self.log(f"[LIVE_SKIP]{tag_s} {sym} lev<{self.min_leverage}x")
                    return

                order_side = entry_order_side(side)

                def _place():
                    lev = self.binance.set_max_leverage(sym)
                    margin_needed = (self.notional / lev) * self.margin_buffer
                    bal = self.binance.available_usdt()
                    if bal < margin_needed:
                        raise RuntimeError(
                            f"margin need ${margin_needed:.2f} @ {lev}x, avail ${bal:.2f}"
                        )
                    resp = self.binance.market_order_notional(sym, order_side, self.notional)
                    entry, qty = parse_fill(resp)
                    if qty <= 0:
                        qty = self.binance.position_qty(sym)
                    if entry <= 0:
                        entry = ref_px
                    row = self.binance.position_row(sym)
                    if row:
                        ex = float(row.get("entryPrice") or 0)
                        if ex > 0:
                            entry = ex
                    return lev, entry, qty, margin_needed, bal

                lev, entry, qty, margin_needed, bal = await asyncio.get_running_loop().run_in_executor(
                    None, _place
                )
                self.slots[sym] = {"side": side, "qty": qty, "entry_price": entry}
                self.stats["entries"] += 1
                self.log(
                    f"[LIVE_ENTRY]{tag_s} {sym} {side.upper()} fill={entry:.8f} qty={qty:.8f} "
                    f"notional=${self.notional} lev={lev}x margin~=${margin_needed:.2f} bal=${bal:.2f}"
                )
        except Exception as e:
            self.stats["skips"] += 1
            self.log(f"[LIVE_ENTRY_FAIL]{tag_s} {sym} {e}\n{traceback.format_exc()}")

    async def exit(self, symbol: str, side: str, *, reason: str = "day_close", tag: str = "") -> None:
        if not self.enabled or self.binance is None:
            return
        sym = symbol.upper()
        tag_s = f" {tag}" if tag else ""
        close_side = close_order_side(side)

        def _close():
            pos_qty = self.binance.position_qty(sym)
            if pos_qty <= 0:
                return 0.0, True
            slot = self.slots.get(sym, {})
            qty = min(pos_qty, float(slot.get("qty") or pos_qty))
            resp = self.binance.market_close_qty(sym, close_side, qty)
            exit_px, _ = parse_fill(resp)
            if exit_px <= 0:
                exit_px = self.binance.mark_price(sym)
            if self.binance.position_qty(sym) > 0:
                self.binance.market_close_qty(sym, close_side, self.binance.position_qty(sym))
            return exit_px, False

        try:
            async with self._entry_lock:
                exit_px, already_flat = await asyncio.get_running_loop().run_in_executor(None, _close)
                self.slots.pop(sym, None)
                self.stats["exits"] += 1
                if already_flat:
                    self.log(f"[LIVE_EXIT]{tag_s} {sym} {reason} already_flat")
                else:
                    self.log(f"[LIVE_EXIT]{tag_s} {sym} {reason} exit={exit_px:.8f} {side.upper()}")
        except Exception as e:
            self.log(f"[LIVE_EXIT_FAIL]{tag_s} {sym} {reason} {e}")

    async def exit_all(self, reason: str = "day_close") -> None:
        if not self.enabled or self.binance is None:
            return
        for sym in list(self.slots.keys()):
            side = self.slots[sym].get("side", "long")
            await self.exit(sym, side, reason=reason)

    def schedule_entry(self, symbol: str, side: str, ref_px: float, *, tag: str = "") -> None:
        try:
            asyncio.get_running_loop().create_task(self.entry(symbol, side, ref_px, tag=tag))
        except RuntimeError:
            pass
