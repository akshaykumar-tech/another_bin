"""Shared Binance futures live bracket orders (SR_GODMODE_LIVE_* env)."""

from __future__ import annotations

import asyncio
import os
from typing import Callable

from binance_futures import BinanceFuturesClient, parse_fill


def _env(name: str, default: str) -> str:
    return os.environ.get(name, "").strip() or default


def _env_int(name: str, default: int) -> int:
    return int(_env(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(_env(name, str(default)))


def _env_bool(name: str, default: bool) -> bool:
    v = _env(name, "true" if default else "false").lower()
    return v in ("1", "true", "yes", "on")


def _env_alt(primary: str, fallback: str, default: str) -> str:
    v = os.environ.get(primary, "").strip()
    if v:
        return v
    v2 = os.environ.get(fallback, "").strip()
    return v2 or default


def entry_order_side(dry_side: str) -> str:
    return "BUY" if dry_side == "long" else "SELL"


def close_order_side(dry_side: str) -> str:
    return "SELL" if dry_side == "long" else "BUY"


def order_to_live_side(order_side: str) -> str:
    return "long" if order_side == "BUY" else "short"


def close_order_side_from_live(live_side: str) -> str:
    return "SELL" if live_side == "long" else "BUY"


class SrLiveTrader:
    """Market entry + limit SL/TP algos from fill price (same as godmode fakeout_res live)."""

    def __init__(
        self,
        fapi: str,
        log: Callable[[str], None],
        *,
        enabled: bool | None = None,
    ) -> None:
        self.log = log
        self.fapi = fapi.rstrip("/")
        self.enabled = _env_bool("SR_GODMODE_LIVE_ENABLED", False) if enabled is None else enabled
        self.notional = _env_float("SR_GODMODE_LIVE_NOTIONAL_USDT", 6.0)
        self.sl_pct = _env_float("SR_GODMODE_LIVE_SL_PCT", 8.0)
        self.tp_pct = _env_float("SR_GODMODE_LIVE_TP_PCT", 1.5)
        self.max_open = _env_int("SR_GODMODE_LIVE_MAX_OPEN", 25)
        self.min_leverage = _env_int("SR_GODMODE_LIVE_MIN_LEVERAGE", 50)
        self.margin_buffer = _env_float("SR_GODMODE_LIVE_MARGIN_BUFFER", 1.05)
        self.reconcile_sec = _env_int("SR_GODMODE_LIVE_RECONCILE_SEC", 60)
        self.algo_working_type = _env("SR_GODMODE_LIVE_ALGO_WORKING_TYPE", "CONTRACT_PRICE").upper()
        self.binance: BinanceFuturesClient | None = None
        self.slots: dict[str, dict] = {}
        self.stats = {"entries": 0, "exits": 0, "skips": 0}
        self._lock: asyncio.Lock | None = None

    def configured(self) -> bool:
        return self.binance is not None and self.binance.configured()

    def init_client(self) -> None:
        if not self.enabled:
            return
        api_key = _env_alt("SR_GODMODE_BINANCE_API_KEY", "BINANCE_API_KEY", "")
        api_secret = _env_alt("SR_GODMODE_BINANCE_API_SECRET", "BINANCE_API_SECRET", "")
        if not api_key or not api_secret:
            return
        self.binance = BinanceFuturesClient(api_key, api_secret, self.fapi)
        self.binance.warm_cache()

    def _lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def sl_tp(self, entry: float, live_side: str) -> tuple[float, float]:
        if live_side == "long":
            return entry * (1 - self.sl_pct / 100), entry * (1 + self.tp_pct / 100)
        return entry * (1 + self.sl_pct / 100), entry * (1 - self.tp_pct / 100)

    def sl_tp_rounded(self, sym: str, entry: float, live_side: str) -> tuple[float, float]:
        sl_px, tp_px = self.sl_tp(entry, live_side)
        if self.binance is None:
            return sl_px, tp_px
        return self.binance.round_price(sym, sl_px), self.binance.round_price(sym, tp_px)

    def _place_bracket(self, sym: str, live_side: str, entry: float, qty: float) -> tuple[float, float, dict, dict]:
        sl_px, tp_px = self.sl_tp_rounded(sym, entry, live_side)
        close_side = close_order_side_from_live(live_side)
        sl_resp = self.binance.stop_limit_reduce(
            sym, close_side, sl_px, sl_px, qty, working_type=self.algo_working_type
        )
        tp_resp = self.binance.take_profit_limit_reduce(
            sym, close_side, tp_px, tp_px, qty, working_type=self.algo_working_type
        )
        return sl_px, tp_px, sl_resp, tp_resp

    def _limit_close_at(self, sym: str, close_side: str, qty: float, limit_px: float) -> dict:
        return self.binance.limit_close_qty(sym, close_side, qty, limit_px)

    def _tp_hit(self, live_side: str, px: float, tp_px: float) -> bool:
        if live_side == "long":
            return px >= tp_px
        return px <= tp_px

    def _cancel_orphan_algos_sync(self) -> list[str]:
        if self.binance is None:
            return []
        try:
            return self.binance.cancel_orphan_algo_orders()
        except Exception:
            return []

    def _reconcile_slots_sync(self) -> list[tuple[str, bool]]:
        if self.binance is None:
            return []
        open_syms = set(self.binance.open_position_symbols())
        removed: list[tuple[str, bool]] = []
        for sym in list(self.slots.keys()):
            if sym not in open_syms:
                cancelled = False
                try:
                    self.binance.cancel_all_algo_orders(sym)
                    cancelled = True
                except Exception:
                    pass
                self.slots.pop(sym, None)
                removed.append((sym, cancelled))
        return removed

    def _seed_slots_sync(self) -> int:
        if self.binance is None:
            return 0
        seeded = 0
        for sym in self.binance.open_position_symbols():
            if sym in self.slots:
                continue
            row = self.binance.position_row(sym)
            if not row:
                continue
            amt = float(row.get("positionAmt") or 0)
            qty = abs(amt)
            if qty <= 0:
                continue
            live_side = "long" if amt > 0 else "short"
            entry = float(row.get("entryPrice") or 0)
            sl_px, tp_px = self.sl_tp_rounded(sym, entry, live_side) if entry > 0 else (0.0, 0.0)
            self.slots[sym] = {
                "dry_side": "unknown",
                "qty": qty,
                "entry_price": entry,
                "leverage": int(float(row.get("leverage") or 0)),
                "live_side": live_side,
                "sl_px": sl_px,
                "tp_px": tp_px,
            }
            seeded += 1
        return seeded

    def _refresh_tp_orders_sync(self) -> tuple[list[tuple], list[tuple], list[tuple]]:
        if self.binance is None:
            return [], [], []
        closed: list[tuple] = []
        updated: list[tuple] = []
        failed: list[tuple] = []
        for sym in self.binance.open_position_symbols():
            try:
                row = self.binance.position_row(sym)
                if not row:
                    continue
                amt = float(row.get("positionAmt") or 0)
                qty = abs(amt)
                if qty <= 0:
                    continue
                live_side = "long" if amt > 0 else "short"
                slot = self.slots.get(sym, {})
                entry = float(slot.get("entry_price") or 0)
                if entry <= 0:
                    entry = float(row.get("entryPrice") or 0)
                if entry <= 0:
                    failed.append((sym, "no entry price"))
                    continue
                sl_px, tp_px = self.sl_tp_rounded(sym, entry, live_side)
                close_side = close_order_side_from_live(live_side)
                px = self.binance.last_price(sym)
                old_tp = float(slot.get("tp_px") or 0)
                if self._tp_hit(live_side, px, tp_px):
                    try:
                        self.binance.cancel_all_algo_orders(sym)
                    except Exception:
                        pass
                    self._limit_close_at(sym, close_side, qty, tp_px)
                    if self.binance.position_qty(sym) > 0:
                        self._limit_close_at(sym, close_side, self.binance.position_qty(sym), tp_px)
                    self.slots.pop(sym, None)
                    closed.append((sym, px, tp_px))
                    continue
                try:
                    self.binance.cancel_all_algo_orders(sym)
                except Exception:
                    pass
                sl_px, tp_px, sl_resp, tp_resp = self._place_bracket(sym, live_side, entry, qty)
                self.slots[sym] = {
                    **slot,
                    "qty": qty,
                    "entry_price": entry,
                    "live_side": live_side,
                    "sl_px": sl_px,
                    "tp_px": tp_px,
                }
                algo_id = tp_resp.get("algoId", tp_resp.get("clientAlgoId", "?"))
                updated.append((sym, old_tp, tp_px, algo_id))
            except Exception as e:
                failed.append((sym, str(e)))
        return closed, updated, failed

    async def bootstrap(self, label: str = "LIVE") -> None:
        if not self.enabled or self.binance is None:
            return
        loop = asyncio.get_running_loop()
        seeded = await loop.run_in_executor(None, self._seed_slots_sync)
        closed, updated, failed = await loop.run_in_executor(None, self._refresh_tp_orders_sync)
        n_pos = len(self.binance.open_position_symbols())
        self.log(
            f"[{label}_BOOT] exchange_positions={n_pos} seeded_slots={seeded} "
            f"tp_pct={self.tp_pct}% sl_pct={self.sl_pct}% max_open={self.max_open} "
            f"(no new entry orders on startup)"
        )
        for sym, px, tp_px in closed:
            self.stats["exits"] += 1
            self.log(f"[{label}_TP_REFRESH] {sym} closed @ {px:.8f} (TP {tp_px:.8f} already hit)")
        for sym, old_tp, new_tp, algo_id in updated:
            old_s = f"{old_tp:.8f}" if old_tp > 0 else "none"
            self.log(f"[{label}_TP_REFRESH] {sym} TP {old_s} -> {new_tp:.8f} ({self.tp_pct}%) tp_algo={algo_id}")
        for sym, err in failed:
            self.log(f"[{label}_TP_REFRESH_FAIL] {sym} {err}")

    async def reconcile(self, label: str = "LIVE") -> None:
        if not self.enabled or self.binance is None:
            return
        loop = asyncio.get_running_loop()
        removed = await loop.run_in_executor(None, self._reconcile_slots_sync)
        for sym, cancelled in removed:
            extra = " algos_cancelled" if cancelled else ""
            self.log(f"[{label}_RECONCILE] {sym} flat on exchange — slot cleared{extra}")
        for sym in await loop.run_in_executor(None, self._cancel_orphan_algos_sync):
            self.log(f"[{label}_RECONCILE] {sym} orphan SL/TP algos cancelled (no position)")

    async def reconcile_loop(self, label: str = "LIVE") -> None:
        while True:
            await asyncio.sleep(max(15, self.reconcile_sec))
            await self.reconcile(label)

    async def entry(self, symbol: str, dry_side: str, ref_px: float, *, label: str = "LIVE", tag: str = "") -> None:
        if not self.enabled or self.binance is None:
            return
        sym = symbol.upper()
        tag_s = f" {tag}" if tag else ""
        try:
            async with self._lock():
                if sym in self.slots:
                    self.stats["skips"] += 1
                    self.log(f"[{label}_SKIP]{tag_s} {sym} already in live_slots")
                    return
                open_syms = set(self.binance.open_position_symbols())
                if sym in open_syms:
                    self.stats["skips"] += 1
                    self.log(f"[{label}_SKIP]{tag_s} {sym} exchange position already open")
                    return
                n_open = len(open_syms)
                if n_open >= self.max_open:
                    self.stats["skips"] += 1
                    self.log(f"[{label}_SKIP]{tag_s} {sym} max_open={self.max_open} (exchange={n_open})")
                    return
                if not self.binance.symbol_tradable(sym):
                    self.stats["skips"] += 1
                    self.log(f"[{label}_SKIP]{tag_s} {sym} not tradable")
                    return
                if self.binance.max_leverage(sym) < self.min_leverage:
                    self.stats["skips"] += 1
                    self.log(f"[{label}_SKIP]{tag_s} {sym} lev<{self.min_leverage}x")
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
                    sl_px, tp_px, sl_resp, tp_resp = self._place_bracket(sym, live_side, entry, qty)
                    return lev, entry, qty, margin_needed, bal, live_side, sl_px, tp_px, sl_resp, tp_resp

                (
                    lev, entry, qty, margin_needed, bal,
                    live_side, sl_px, tp_px, sl_resp, tp_resp,
                ) = await asyncio.get_running_loop().run_in_executor(None, _place)
                self.slots[sym] = {
                    "dry_side": dry_side,
                    "qty": qty,
                    "entry_price": entry,
                    "leverage": lev,
                    "live_side": live_side,
                    "sl_px": sl_px,
                    "tp_px": tp_px,
                }
                self.stats["entries"] += 1
                self.log(
                    f"[{label}_ENTRY]{tag_s} {sym} dry={dry_side.upper()} binance={order_side} "
                    f"fill={entry:.8f} qty={qty:.8f} notional=${self.notional:.2f} lev={lev}x "
                    f"margin~=${margin_needed:.2f} bal=${bal:.2f} ref={ref_px:.8f} "
                    f"SL={sl_px:.8f} ({self.sl_pct}%) TP={tp_px:.8f} ({self.tp_pct}%) limit_from_entry "
                    f"sl_algo={sl_resp.get('algoId', sl_resp.get('clientAlgoId', '?'))} "
                    f"tp_algo={tp_resp.get('algoId', tp_resp.get('clientAlgoId', '?'))}"
                )
        except Exception as e:
            self.stats["skips"] += 1
            self.log(f"[{label}_ENTRY_FAIL]{tag_s} {sym} {e}")

    async def exit(self, symbol: str, dry_side: str, reason: str, *, label: str = "LIVE", tag: str = "") -> None:
        if not self.enabled or self.binance is None:
            return
        sym = symbol.upper()
        slot = self.slots.get(sym)
        close_side = close_order_side(dry_side)
        tag_s = f" {tag}" if tag else ""

        def _close():
            try:
                self.binance.cancel_all_algo_orders(sym)
            except Exception:
                pass
            pos_qty = self.binance.position_qty(sym)
            if pos_qty <= 0:
                return 0.0, True
            slot_qty = float(slot["qty"]) if slot else pos_qty
            qty = min(pos_qty, slot_qty)
            limit_px = 0.0
            if slot:
                if reason == "tp":
                    limit_px = float(slot.get("tp_px") or 0)
                elif reason == "sl":
                    limit_px = float(slot.get("sl_px") or 0)
            if limit_px > 0:
                resp = self._limit_close_at(sym, close_side, qty, limit_px)
            else:
                resp = self.binance.market_close_qty(sym, close_side, qty)
            exit_px, _ = parse_fill(resp)
            if exit_px <= 0:
                exit_px = limit_px if limit_px > 0 else self.binance.mark_price(sym)
            if self.binance.position_qty(sym) > 0:
                rem = self.binance.position_qty(sym)
                if limit_px > 0:
                    self._limit_close_at(sym, close_side, rem, limit_px)
                else:
                    self.binance.market_close_qty(sym, close_side, rem)
            return exit_px, False

        try:
            async with self._lock():
                exit_px, already_flat = await asyncio.get_running_loop().run_in_executor(None, _close)
                self.slots.pop(sym, None)
                self.stats["exits"] += 1
                if already_flat:
                    self.log(f"[{label}_EXIT]{tag_s} {sym} reason={reason} already_flat")
                else:
                    self.log(
                        f"[{label}_EXIT]{tag_s} {sym} reason={reason} side={close_side} "
                        f"exit={exit_px:.8f} dry={dry_side.upper()}"
                    )
        except Exception as e:
            self.log(f"[{label}_EXIT_FAIL]{tag_s} {sym} reason={reason} {e}")
            try:
                flat = await asyncio.get_running_loop().run_in_executor(
                    None, lambda: self.binance.position_qty(sym) <= 0
                )
                if flat:
                    self.slots.pop(sym, None)
                    self.stats["exits"] += 1
                    self.log(f"[{label}_EXIT]{tag_s} {sym} reason={reason} flat_after_fail")
            except Exception:
                pass

    def schedule_entry(self, symbol: str, dry_side: str, ref_px: float, *, tag: str = "") -> None:
        try:
            asyncio.get_running_loop().create_task(
                self.entry(symbol, dry_side, ref_px, label="SNR_LIVE", tag=tag)
            )
        except RuntimeError:
            pass

    def schedule_exit(self, symbol: str, dry_side: str, reason: str, *, tag: str = "") -> None:
        try:
            asyncio.get_running_loop().create_task(
                self.exit(symbol, dry_side, reason, label="SNR_LIVE", tag=tag)
            )
        except RuntimeError:
            pass
