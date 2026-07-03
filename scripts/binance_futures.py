"""Minimal Binance USDT-M futures client for live paper bots."""
from __future__ import annotations

import hashlib
import hmac
import json
import math
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


@dataclass
class LotRules:
    step_size: float = 0.0
    min_qty: float = 0.0
    min_notional: float = 5.0
    tick_size: float = 0.0


class BinanceFuturesClient:
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        base: str = "https://fapi.binance.com",
    ) -> None:
        self.api_key = (api_key or "").strip()
        self.api_secret = (api_secret or "").strip()
        self.base = base.rstrip("/")
        self.tradable: dict[str, bool] = {}
        self.lot_rules: dict[str, LotRules] = {}
        self.max_leverage_map: dict[str, int] = {}
        self._pos_lock = threading.Lock()
        self._pos_cache: list[dict[str, Any]] | None = None
        self._pos_cache_ts: float = 0.0
        self._pos_cache_ttl: float = 3.0

    def configured(self) -> bool:
        return bool(self.api_key and self.api_secret)

    @staticmethod
    def _is_rate_limit_error(exc: BaseException) -> bool:
        msg = str(exc)
        return (
            "429" in msg
            or "-1003" in msg
            or "Too many requests" in msg
            or "Too Many Requests" in msg
        )

    def _http(
        self,
        method: str,
        path: str,
        params: dict[str, str],
        signed: bool = False,
    ) -> Any:
        last_err: BaseException | None = None
        for attempt in range(4):
            if attempt > 0:
                time.sleep(min(8.0, 0.5 * (2 ** (attempt - 1))))
            try:
                return self._http_once(method, path, params, signed)
            except RuntimeError as e:
                last_err = e
                if self._is_rate_limit_error(e) and attempt < 3:
                    continue
                raise
            except urllib.error.HTTPError as e:
                last_err = e
                if e.code == 429 and attempt < 3:
                    continue
                body = e.read().decode(errors="replace")
                try:
                    err = json.loads(body)
                    if isinstance(err, dict) and err.get("msg"):
                        raise RuntimeError(
                            f"binance {err.get('code')}: {err.get('msg')}"
                        ) from e
                except json.JSONDecodeError:
                    pass
                raise RuntimeError(f"HTTP {e.code}: {body[:200]}") from e
        if last_err is not None:
            raise last_err
        raise RuntimeError("binance request failed")

    def _http_once(
        self,
        method: str,
        path: str,
        params: dict[str, str],
        signed: bool = False,
    ) -> Any:
        if signed:
            params = dict(params)
            params["timestamp"] = str(int(time.time() * 1000))
            params["recvWindow"] = "5000"
            query = urllib.parse.urlencode(sorted(params.items()))
            sig = hmac.new(
                self.api_secret.encode(),
                query.encode(),
                hashlib.sha256,
            ).hexdigest()
            headers = {"X-MBX-APIKEY": self.api_key}
            if method == "GET":
                url = f"{self.base}{path}?{query}&signature={sig}"
                req = urllib.request.Request(url, method=method, headers=headers)
            else:
                body = f"{query}&signature={sig}".encode()
                headers["Content-Type"] = "application/x-www-form-urlencoded"
                req = urllib.request.Request(
                    f"{self.base}{path}",
                    data=body,
                    method=method,
                    headers=headers,
                )
        else:
            url = f"{self.base}{path}"
            if params:
                url += "?" + urllib.parse.urlencode(params)
            req = urllib.request.Request(url, method=method)
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read()
        data = json.loads(raw)
        if isinstance(data, dict) and self._is_binance_error(data):
            raise RuntimeError(f"binance {data.get('code')}: {data.get('msg')}")
        return data

    def invalidate_position_cache(self) -> None:
        with self._pos_lock:
            self._pos_cache = None

    def _all_position_risk(self, force: bool = False) -> list[dict[str, Any]]:
        with self._pos_lock:
            now = time.time()
            if (
                not force
                and self._pos_cache is not None
                and (now - self._pos_cache_ts) < self._pos_cache_ttl
            ):
                return self._pos_cache
        rows = self._signed_get("/fapi/v2/positionRisk")
        if not isinstance(rows, list):
            rows = []
        with self._pos_lock:
            self._pos_cache = rows
            self._pos_cache_ts = time.time()
        return rows

    @staticmethod
    def _is_binance_error(data: dict[str, Any]) -> bool:
        code = data.get("code")
        if code is None:
            return False
        try:
            c = int(code)
        except (TypeError, ValueError):
            return True
        # Binance returns code=200 on some successful cancels (e.g. algoOpenOrders).
        return c < 0 or c >= 400

    def _signed_post(self, path: str, params: dict[str, str]) -> dict[str, Any]:
        out = self._http("POST", path, params, signed=True)
        return out  # type: ignore[return-value]

    def _signed_get(self, path: str, params: dict[str, str] | None = None) -> Any:
        return self._http("GET", path, params or {}, signed=True)

    def _signed_delete(self, path: str, params: dict[str, str]) -> Any:
        return self._http("DELETE", path, params, signed=True)

    def warm_cache(self) -> None:
        info = self._http("GET", "/fapi/v1/exchangeInfo", {})
        for s in info.get("symbols", []):
            sym = s.get("symbol", "").upper()
            if s.get("status") != "TRADING":
                continue
            self.tradable[sym] = True
            rules = LotRules()
            for f in s.get("filters", []):
                ft = f.get("filterType")
                if ft == "LOT_SIZE":
                    rules.step_size = float(f.get("stepSize") or 0)
                    rules.min_qty = float(f.get("minQty") or 0)
                elif ft == "MIN_NOTIONAL":
                    n = float(f.get("notional") or f.get("minNotional") or 0)
                    if n > 0:
                        rules.min_notional = n
                elif ft == "PRICE_FILTER":
                    rules.tick_size = float(f.get("tickSize") or 0)
            self.lot_rules[sym] = rules
        if self.configured():
            try:
                rows = self._signed_get("/fapi/v1/leverageBracket")
                for row in rows:
                    sym = row.get("symbol", "").upper()
                    max_lev = 1
                    for b in row.get("brackets", []):
                        lev = int(b.get("initialLeverage") or 0)
                        if lev > max_lev:
                            max_lev = lev
                    if max_lev > 0:
                        self.max_leverage_map[sym] = max_lev
            except Exception:
                pass

    def symbol_tradable(self, symbol: str) -> bool:
        return self.tradable.get(symbol.upper(), False)

    def max_leverage(self, symbol: str) -> int:
        return self.max_leverage_map.get(symbol.upper(), 125)

    def set_max_leverage(self, symbol: str, cap: int | None = None) -> int:
        """Set highest leverage Binance accepts (bracket max may exceed account limit)."""
        sym = symbol.upper()
        target = self.max_leverage(sym)
        if cap is not None and cap > 0:
            target = min(target, cap)
        candidates: list[int] = []
        for lev in (target, 75, 50, 25, 20, 10, 5, 3, 2, 1):
            if lev > 0 and lev <= target and lev not in candidates:
                candidates.append(lev)
        last_err: Exception | None = None
        for lev in candidates:
            try:
                self._signed_post(
                    "/fapi/v1/leverage",
                    {"symbol": sym, "leverage": str(lev)},
                )
                return lev
            except Exception as e:
                last_err = e
        raise RuntimeError(f"could not set leverage for {sym}: {last_err}")

    @staticmethod
    def _floor_step(qty: float, step: float) -> float:
        if step <= 0:
            return qty
        return math.floor(qty / step) * step

    @staticmethod
    def _price_str(price: float, tick: float) -> str:
        if tick > 0:
            decimals = max(0, -int(math.floor(math.log10(tick))))
            s = f"{price:.{decimals}f}".rstrip("0").rstrip(".")
            return s or "0"
        if price >= 1000:
            return f"{price:.2f}"
        if price >= 1:
            return f"{price:.4f}"
        return f"{price:.8f}".rstrip("0").rstrip(".") or "0"

    @staticmethod
    def _qty_str(qty: float, step: float) -> str:
        if step >= 1:
            return str(int(qty))
        decimals = max(0, -int(math.floor(math.log10(step)))) if step > 0 else 8
        s = f"{qty:.{decimals}f}".rstrip("0").rstrip(".")
        return s or "0"

    def _format_qty(self, symbol: str, qty: float, price: float | None = None) -> float:
        sym = symbol.upper()
        rules = self.lot_rules.get(sym, LotRules())
        q = self._floor_step(qty, rules.step_size)
        if rules.min_qty > 0 and q < rules.min_qty:
            q = rules.min_qty
        if q <= 0:
            raise ValueError("quantity rounds to zero")
        px = price if price and price > 0 else self.mark_price(sym)
        if rules.min_notional > 0 and q * px < rules.min_notional:
            q = self._floor_step((rules.min_notional * 1.02) / px, rules.step_size)
            if rules.min_qty > 0 and q < rules.min_qty:
                q = rules.min_qty
        return q

    def available_usdt(self) -> float:
        rows = self._signed_get("/fapi/v2/balance")
        for row in rows:
            if row.get("asset") == "USDT":
                return float(row.get("availableBalance") or 0)
        return 0.0

    def position_qty(self, symbol: str) -> float:
        row = self.position_row(symbol)
        if not row:
            return 0.0
        return abs(float(row.get("positionAmt") or 0))

    def position_row(self, symbol: str) -> dict[str, Any] | None:
        sym = symbol.upper()
        for row in self._all_position_risk():
            if row.get("symbol") == sym:
                return row
        return None

    def open_position_symbols(self) -> list[str]:
        out: list[str] = []
        for row in self._all_position_risk():
            if abs(float(row.get("positionAmt") or 0)) > 0:
                sym = str(row.get("symbol") or "").upper()
                if sym:
                    out.append(sym)
        return out

    def cancel_algo_order(self, algo_id: int) -> None:
        self._signed_delete("/fapi/v1/algoOrder", {"algoId": str(algo_id)})

    def cancel_all_algo_orders(self, symbol: str) -> None:
        self._signed_delete(
            "/fapi/v1/algoOpenOrders",
            {"symbol": symbol.upper()},
        )

    def open_algo_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        params: dict[str, str] = {}
        if symbol:
            params["symbol"] = symbol.upper()
        data = self._signed_get("/fapi/v1/openAlgoOrders", params)
        if isinstance(data, list):
            return [r for r in data if isinstance(r, dict)]
        if isinstance(data, dict):
            rows = data.get("orders") or data.get("data") or []
            return [r for r in rows if isinstance(r, dict)]
        return []

    def cancel_tp_algo_orders(self, symbol: str) -> int:
        cancelled = 0
        for row in self.open_algo_orders(symbol):
            ot = str(row.get("orderType") or row.get("type") or "").upper()
            if ot not in ("TAKE_PROFIT_MARKET", "TAKE_PROFIT", "TAKE_PROFIT_LIMIT"):
                continue
            algo_id = row.get("algoId")
            if algo_id is None:
                continue
            try:
                self.cancel_algo_order(int(algo_id))
                cancelled += 1
            except Exception:
                pass
        return cancelled

    def open_algo_order_symbols(self) -> list[str]:
        data = self._signed_get("/fapi/v1/openAlgoOrders", {})
        if isinstance(data, list):
            rows = data
        elif isinstance(data, dict):
            rows = data.get("orders") or data.get("data") or []
        else:
            rows = []
        syms: set[str] = set()
        for row in rows:
            if not isinstance(row, dict):
                continue
            sym = str(row.get("symbol") or "").upper()
            if sym:
                syms.add(sym)
        return sorted(syms)

    def cancel_orphan_algo_orders(self) -> list[str]:
        """Cancel SL/TP algos on symbols with no open position (e.g. manual close)."""
        open_pos = set(self.open_position_symbols())
        cancelled: list[str] = []
        for sym in self.open_algo_order_symbols():
            if sym in open_pos:
                continue
            try:
                self.cancel_all_algo_orders(sym)
                cancelled.append(sym)
            except Exception:
                pass
        return cancelled

    def has_reduce_stop_algo(self, symbol: str) -> bool:
        for row in self.open_algo_orders(symbol):
            ot = str(row.get("orderType") or row.get("type") or "").upper()
            if "STOP" in ot:
                return True
        return False

    def cancel_order(self, symbol: str, order_id: int) -> None:
        self._signed_delete(
            "/fapi/v1/order",
            {"symbol": symbol.upper(), "orderId": str(order_id)},
        )

    def cancel_all_open_orders(self, symbol: str) -> None:
        self._signed_delete(
            "/fapi/v1/allOpenOrders",
            {"symbol": symbol.upper()},
        )

    def _algo_reduce_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        trigger_price: float,
        qty: float,
        working_type: str = "MARK_PRICE",
        limit_price: float | None = None,
    ) -> dict[str, Any]:
        sym = symbol.upper()
        rules = self.lot_rules.get(sym, LotRules())
        q = self._format_qty(sym, qty)
        params: dict[str, str] = {
            "algoType": "CONDITIONAL",
            "symbol": sym,
            "side": side.upper(),
            "type": order_type,
            "triggerPrice": self._price_str(trigger_price, rules.tick_size),
            "quantity": self._qty_str(q, rules.step_size),
            "reduceOnly": "true",
            "workingType": working_type,
            "newOrderRespType": "RESULT",
        }
        if limit_price is not None:
            params["price"] = self._price_str(limit_price, rules.tick_size)
        return self._signed_post("/fapi/v1/algoOrder", params)

    def stop_market_reduce(
        self,
        symbol: str,
        side: str,
        stop_price: float,
        qty: float,
        working_type: str = "MARK_PRICE",
    ) -> dict[str, Any]:
        """Place STOP_MARKET via Binance Algo Order API (required since 2025-12-09)."""
        return self._algo_reduce_order(
            symbol, side, "STOP_MARKET", stop_price, qty, working_type
        )

    def take_profit_market_reduce(
        self,
        symbol: str,
        side: str,
        trigger_price: float,
        qty: float,
        working_type: str = "MARK_PRICE",
    ) -> dict[str, Any]:
        """Place TAKE_PROFIT_MARKET reduce-only close."""
        return self._algo_reduce_order(
            symbol, side, "TAKE_PROFIT_MARKET", trigger_price, qty, working_type
        )

    def take_profit_limit_reduce(
        self,
        symbol: str,
        side: str,
        trigger_price: float,
        limit_price: float,
        qty: float,
        working_type: str = "MARK_PRICE",
    ) -> dict[str, Any]:
        """Take-profit limit: trigger and fill both at limit_price (no market slippage)."""
        return self._algo_reduce_order(
            symbol,
            side,
            "TAKE_PROFIT",
            trigger_price,
            qty,
            working_type,
            limit_price=limit_price,
        )

    def stop_limit_reduce(
        self,
        symbol: str,
        side: str,
        trigger_price: float,
        limit_price: float,
        qty: float,
        working_type: str = "MARK_PRICE",
    ) -> dict[str, Any]:
        """Stop-loss limit: trigger and fill both at limit_price."""
        return self._algo_reduce_order(
            symbol,
            side,
            "STOP",
            trigger_price,
            qty,
            working_type,
            limit_price=limit_price,
        )

    def round_price(self, symbol: str, price: float) -> float:
        """Snap price to symbol tick size (Binance PRICE_FILTER)."""
        sym = symbol.upper()
        rules = self.lot_rules.get(sym, LotRules())
        s = self._price_str(price, rules.tick_size)
        return float(s) if s else price

    def trigger_reference_price(self, symbol: str, working_type: str = "MARK_PRICE") -> float:
        """Price used to validate conditional stops (matches algo workingType)."""
        if working_type.upper() == "CONTRACT_PRICE":
            return self.last_price(symbol)
        return self.mark_price(symbol)

    def breakeven_stop_price(
        self,
        symbol: str,
        live_side: str,
        entry: float,
        working_type: str = "MARK_PRICE",
    ) -> float:
        """SL at entry when valid; else nudge 1+ ticks so Binance won't reject (-2021)."""
        sym = symbol.upper()
        rules = self.lot_rules.get(sym, LotRules())
        tick = rules.tick_size if rules.tick_size > 0 else 1e-8
        ref = self.trigger_reference_price(sym, working_type)
        sl = self.round_price(sym, entry)

        if live_side == "long":
            # SELL STOP_MARKET: trigger must stay below reference price.
            n = 0
            while sl >= ref and n < 20:
                sl = self.round_price(sym, ref - tick * (n + 1))
                n += 1
        else:
            # BUY STOP_MARKET: trigger must stay above reference price.
            n = 0
            while sl <= ref and n < 20:
                sl = self.round_price(sym, ref + tick * (n + 1))
                n += 1
        return sl

    def last_price(self, symbol: str) -> float:
        data = self._http(
            "GET",
            "/fapi/v1/ticker/price",
            {"symbol": symbol.upper()},
        )
        px = float(data.get("price") or 0)
        if px <= 0:
            raise ValueError(f"invalid last price for {symbol}")
        return px

    def mark_price(self, symbol: str) -> float:
        data = self._http(
            "GET",
            "/fapi/v1/premiumIndex",
            {"symbol": symbol.upper()},
        )
        px = float(data.get("markPrice") or 0)
        if px <= 0:
            raise ValueError(f"invalid mark price for {symbol}")
        return px

    def market_order_notional(self, symbol: str, side: str, notional_usdt: float) -> dict[str, Any]:
        sym = symbol.upper()
        rules = self.lot_rules.get(sym, LotRules())
        min_n = rules.min_notional or 5.0
        if notional_usdt < min_n:
            raise ValueError(f"notional {notional_usdt:.2f} below min {min_n:.2f}")
        price = self.mark_price(sym)
        rules = self.lot_rules.get(sym, LotRules())
        qty = self._format_qty(sym, notional_usdt / price, price)
        out = self._signed_post(
            "/fapi/v1/order",
            {
                "symbol": sym,
                "side": side.upper(),
                "type": "MARKET",
                "quantity": self._qty_str(qty, rules.step_size),
                "newOrderRespType": "RESULT",
            },
        )
        self.invalidate_position_cache()
        return out

    def market_close_qty(self, symbol: str, side: str, qty: float) -> dict[str, Any]:
        sym = symbol.upper()
        rules = self.lot_rules.get(sym, LotRules())
        q = self._format_qty(sym, qty)
        out = self._signed_post(
            "/fapi/v1/order",
            {
                "symbol": sym,
                "side": side.upper(),
                "type": "MARKET",
                "quantity": self._qty_str(q, rules.step_size),
                "reduceOnly": "true",
                "newOrderRespType": "RESULT",
            },
        )
        self.invalidate_position_cache()
        return out

    def limit_order_notional(
        self,
        symbol: str,
        side: str,
        notional_usdt: float,
        limit_price: float,
    ) -> dict[str, Any]:
        sym = symbol.upper()
        rules = self.lot_rules.get(sym, LotRules())
        px = max(limit_price, rules.tick_size)
        qty = self._format_qty(sym, notional_usdt / px, px)
        return self._signed_post(
            "/fapi/v1/order",
            {
                "symbol": sym,
                "side": side.upper(),
                "type": "LIMIT",
                "timeInForce": "GTC",
                "price": self._price_str(px, rules.tick_size),
                "quantity": self._qty_str(qty, rules.step_size),
                "newOrderRespType": "RESULT",
            },
        )

    def limit_close_qty(
        self,
        symbol: str,
        side: str,
        qty: float,
        limit_price: float,
    ) -> dict[str, Any]:
        sym = symbol.upper()
        rules = self.lot_rules.get(sym, LotRules())
        q = self._format_qty(sym, qty, limit_price)
        out = self._signed_post(
            "/fapi/v1/order",
            {
                "symbol": sym,
                "side": side.upper(),
                "type": "LIMIT",
                "timeInForce": "GTC",
                "price": self._price_str(limit_price, rules.tick_size),
                "quantity": self._qty_str(q, rules.step_size),
                "reduceOnly": "true",
                "newOrderRespType": "RESULT",
            },
        )
        self.invalidate_position_cache()
        return out

    def query_order(self, symbol: str, order_id: int) -> dict[str, Any]:
        return self._signed_get(
            "/fapi/v1/order",
            {"symbol": symbol.upper(), "orderId": str(order_id)},
        )


def parse_fill(resp: dict[str, Any]) -> tuple[float, float]:
    entry = float(resp.get("avgPrice") or 0)
    qty = float(resp.get("executedQty") or resp.get("origQty") or 0)
    return entry, qty


def order_side_for_dir(trade_dir: str) -> str:
    return "BUY" if trade_dir == "high" else "SELL"


def close_side_for_dir(trade_dir: str) -> str:
    return "SELL" if trade_dir == "high" else "BUY"
