"""Binance live orders for IFVG 1h first-1min: SL=entry, TP=hour candle close."""

from __future__ import annotations

import asyncio
import os
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
from sr_live_lib import close_order_side, close_order_side_from_live, entry_order_side, order_to_live_side


def _env_bool(name: str, default: bool) -> bool:
    v = os.environ.get(name, "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "on")


class IfvgH1LiveTrader:
    """Market entry + stop at fill price; exit at hour close (no fixed TP algo)."""

    def __init__(
        self,
        fapi: str,
        log: Callable[[str], None],
        *,
        enabled: bool | None = None,
    ) -> None:
        self.log = log
        self.fapi = fapi.rstrip("/")
        self.enabled = _env_bool("IFVG_H1_LIVE_ENABLED", False) if enabled is None else enabled
        self.notional = live_notional_usdt(6.0)
        self.max_open = live_max_open(40)
        self.min_leverage = live_min_leverage(10)
        self.margin_buffer = live_margin_buffer(1.05)
        self.reconcile_sec = live_reconcile_sec(60)
        self.algo_working_type = live_algo_working_type("CONTRACT_PRICE")
        self.binance: BinanceFuturesClient | None = None
        self.slots: dict[str, dict] = {}
        self.stats = {"entries": 0, "exits": 0, "skips": 0}
        self._entry_lock = asyncio.Lock()

    def configured(self) -> bool:
        return self.binance is not None and self.binance.configured()

    def init_client(self) -> None:
        if not self.enabled:
            return
        api_key = binance_api_key()
        api_secret = binance_api_secret()
        if not api_key or not api_secret:
            return
        self.binance = BinanceFuturesClient(api_key, api_secret, self.fapi)
        self.binance.warm_cache()

    def _place_entry_stop(self, sym: str, live_side: str, entry: float, qty: float) -> tuple[float, dict]:
        sl_px = self.binance.round_price(sym, entry)
        close_side = close_order_side_from_live(live_side)
        sl_resp = self.binance.stop_market_reduce(
            sym, close_side, sl_px, qty, working_type=self.algo_working_type
        )
        return sl_px, sl_resp

    def _cancel_and_close(self, sym: str, close_side: str, qty: float) -> float:
        try:
            self.binance.cancel_all_algo_orders(sym)
        except Exception:
            pass
        pos_qty = self.binance.position_qty(sym)
        if pos_qty <= 0:
            return 0.0
        qty = min(qty, pos_qty)
        resp = self.binance.market_close_qty(sym, close_side, qty)
        exit_px, _ = parse_fill(resp)
        if self.binance.position_qty(sym) > 0:
            rem = self.binance.position_qty(sym)
            resp2 = self.binance.market_close_qty(sym, close_side, rem)
            px2, _ = parse_fill(resp2)
            if px2 > 0:
                exit_px = px2
        if exit_px <= 0:
            exit_px = self.binance.mark_price(sym)
        return exit_px

    def _reconcile_slots_sync(self) -> list[str]:
        if self.binance is None:
            return []
        open_syms = set(self.binance.open_position_symbols())
        removed: list[str] = []
        for sym in list(self.slots.keys()):
            if sym not in open_syms:
                try:
                    self.binance.cancel_all_algo_orders(sym)
                except Exception:
                    pass
                self.slots.pop(sym, None)
                removed.append(sym)
        return removed

    async def bootstrap(self) -> None:
        if not self.enabled or self.binance is None:
            return
        loop = asyncio.get_running_loop()
        n_pos = len(self.binance.open_position_symbols())
        self.log(
            f"[IFVG_H1_LIVE_BOOT] exchange_positions={n_pos} max_open={self.max_open} "
            f"notional=${self.notional:.2f} SL=entry TP=hour_close (no new entries on startup)"
        )

    async def reconcile(self) -> None:
        if not self.enabled or self.binance is None:
            return
        loop = asyncio.get_running_loop()
        removed = await loop.run_in_executor(None, self._reconcile_slots_sync)
        for sym in removed:
            self.log(f"[IFVG_H1_LIVE_RECONCILE] {sym} flat on exchange — slot cleared")

    async def reconcile_loop(self) -> None:
        while True:
            await asyncio.sleep(max(15, self.reconcile_sec))
            await self.reconcile()

    async def entry(
        self,
        symbol: str,
        dry_side: str,
        ref_px: float,
        hour_start_ms: int,
    ) -> None:
        if not self.enabled or self.binance is None:
            return
        sym = symbol.upper()
        try:
            async with self._entry_lock:
                if sym in self.slots:
                    self.stats["skips"] += 1
                    self.log(f"[IFVG_H1_LIVE_SKIP] {sym} already in live_slots")
                    return
                open_syms = set(self.binance.open_position_symbols())
                if sym in open_syms:
                    self.stats["skips"] += 1
                    self.log(f"[IFVG_H1_LIVE_SKIP] {sym} exchange position already open")
                    return
                if len(open_syms) >= self.max_open:
                    self.stats["skips"] += 1
                    self.log(
                        f"[IFVG_H1_LIVE_SKIP] {sym} max_open={self.max_open} (exchange={len(open_syms)})"
                    )
                    return
                if not self.binance.symbol_tradable(sym):
                    self.stats["skips"] += 1
                    self.log(f"[IFVG_H1_LIVE_SKIP] {sym} not tradable")
                    return
                if self.binance.max_leverage(sym) < self.min_leverage:
                    self.stats["skips"] += 1
                    self.log(f"[IFVG_H1_LIVE_SKIP] {sym} lev<{self.min_leverage}x")
                    return

                order_side = entry_order_side(dry_side)

                def _place():
                    lev = self.binance.set_max_leverage(sym)
                    margin_needed = (self.notional / lev) * self.margin_buffer
                    bal = self.binance.available_usdt()
                    if bal < margin_needed:
                        raise RuntimeError(
                            f"insufficient margin: need ${margin_needed:.2f} "
                            f"(notional=${self.notional:.2f} @ {lev}x), available=${bal:.2f}"
                        )
                    resp = self.binance.market_order_notional(sym, order_side, self.notional)
                    entry, qty = parse_fill(resp)
                    if qty <= 0:
                        qty = self.binance.position_qty(sym)
                    if entry <= 0:
                        entry = ref_px
                    row = self.binance.position_row(sym)
                    if row:
                        ex_entry = float(row.get("entryPrice") or 0)
                        if ex_entry > 0:
                            entry = ex_entry
                    live_side = order_to_live_side(order_side)
                    sl_px, sl_resp = self._place_entry_stop(sym, live_side, entry, qty)
                    return lev, entry, qty, margin_needed, bal, live_side, sl_px, sl_resp

                lev, entry, qty, margin_needed, bal, live_side, sl_px, sl_resp = await asyncio.get_running_loop().run_in_executor(
                    None, _place
                )
                self.slots[sym] = {
                    "dry_side": dry_side,
                    "qty": qty,
                    "entry_price": entry,
                    "leverage": lev,
                    "live_side": live_side,
                    "sl_px": sl_px,
                    "hour_start_ms": hour_start_ms,
                }
                self.stats["entries"] += 1
                self.log(
                    f"[IFVG_H1_LIVE_ENTRY] {sym} dry={dry_side.upper()} binance={order_side} "
                    f"fill={entry:.8f} qty={qty:.8f} notional=${self.notional:.2f} lev={lev}x "
                    f"margin~=${margin_needed:.2f} bal=${bal:.2f} SL@entry={sl_px:.8f} "
                    f"hour_start={hour_start_ms} sl_algo={sl_resp.get('algoId', sl_resp.get('clientAlgoId', '?'))}"
                )
        except Exception as e:
            self.stats["skips"] += 1
            self.log(f"[IFVG_H1_LIVE_ENTRY_FAIL] {sym} {e}\n{traceback.format_exc()}")

    async def exit(self, symbol: str, dry_side: str, reason: str) -> None:
        if not self.enabled or self.binance is None:
            return
        sym = symbol.upper()
        slot = self.slots.get(sym)
        close_side = close_order_side(dry_side)

        def _close():
            pos_qty = self.binance.position_qty(sym)
            if pos_qty <= 0:
                try:
                    self.binance.cancel_all_algo_orders(sym)
                except Exception:
                    pass
                return 0.0, True
            slot_qty = float(slot["qty"]) if slot else pos_qty
            exit_px = self._cancel_and_close(sym, close_side, slot_qty)
            return exit_px, False

        try:
            async with self._entry_lock:
                exit_px, already_flat = await asyncio.get_running_loop().run_in_executor(None, _close)
                self.slots.pop(sym, None)
                self.stats["exits"] += 1
                if already_flat:
                    self.log(f"[IFVG_H1_LIVE_EXIT] {sym} reason={reason} already_flat")
                else:
                    self.log(
                        f"[IFVG_H1_LIVE_EXIT] {sym} reason={reason} side={close_side} exit={exit_px:.8f} "
                        f"dry={dry_side.upper()}"
                    )
        except Exception as e:
            self.log(f"[IFVG_H1_LIVE_EXIT_FAIL] {sym} reason={reason} {e}")
            try:
                flat = await asyncio.get_running_loop().run_in_executor(
                    None, lambda: self.binance.position_qty(sym) <= 0
                )
                if flat:
                    self.slots.pop(sym, None)
                    self.stats["exits"] += 1
            except Exception:
                pass

    def schedule_entry(self, symbol: str, dry_side: str, ref_px: float, hour_start_ms: int) -> None:
        try:
            asyncio.get_running_loop().create_task(
                self.entry(symbol, dry_side, ref_px, hour_start_ms)
            )
        except RuntimeError:
            pass

    def schedule_exit(self, symbol: str, dry_side: str, reason: str) -> None:
        try:
            asyncio.get_running_loop().create_task(self.exit(symbol, dry_side, reason))
        except RuntimeError:
            pass
