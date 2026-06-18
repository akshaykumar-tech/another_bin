"""Minimal Binance USDT-M futures client for focused live orders."""
from __future__ import annotations

import hashlib
import hmac
import json
import math
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

    def configured(self) -> bool:
        return bool(self.api_key and self.api_secret)

    def _http(
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
            body = f"{query}&signature={sig}".encode()
            req = urllib.request.Request(
                f"{self.base}{path}",
                data=body,
                method=method,
                headers={
                    "X-MBX-APIKEY": self.api_key,
                    "Content-Type": "application/x-www-form-urlencoded",
                },
            )
        else:
            url = f"{self.base}{path}"
            if params:
                url += "?" + urllib.parse.urlencode(params)
            req = urllib.request.Request(url, method=method)
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors="replace")
            try:
                err = json.loads(body)
                if isinstance(err, dict) and err.get("msg"):
                    raise RuntimeError(f"binance {err.get('code')}: {err.get('msg')}") from e
            except json.JSONDecodeError:
                pass
            raise RuntimeError(f"HTTP {e.code}: {body[:200]}") from e
        data = json.loads(raw)
        if isinstance(data, dict) and data.get("code"):
            raise RuntimeError(f"binance {data.get('code')}: {data.get('msg')}")
        return data

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
        sym = symbol.upper()
        rows = self._signed_get("/fapi/v2/positionRisk", {"symbol": sym})
        for row in rows:
            if row.get("symbol") == sym:
                return abs(float(row.get("positionAmt") or 0))
        return 0.0

    def cancel_algo_order(self, algo_id: int) -> None:
        self._signed_delete("/fapi/v1/algoOrder", {"algoId": str(algo_id)})

    def cancel_all_algo_orders(self, symbol: str) -> None:
        self._signed_delete(
            "/fapi/v1/algoOpenOrders",
            {"symbol": symbol.upper()},
        )

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
    ) -> dict[str, Any]:
        sym = symbol.upper()
        rules = self.lot_rules.get(sym, LotRules())
        q = self._format_qty(sym, qty)
        return self._signed_post(
            "/fapi/v1/algoOrder",
            {
                "algoType": "CONDITIONAL",
                "symbol": sym,
                "side": side.upper(),
                "type": order_type,
                "triggerPrice": self._price_str(trigger_price, rules.tick_size),
                "quantity": self._qty_str(q, rules.step_size),
                "reduceOnly": "true",
                "workingType": working_type,
                "newOrderRespType": "RESULT",
            },
        )

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
        return self._signed_post(
            "/fapi/v1/order",
            {
                "symbol": sym,
                "side": side.upper(),
                "type": "MARKET",
                "quantity": self._qty_str(qty, rules.step_size),
                "newOrderRespType": "RESULT",
            },
        )

    def market_close_qty(self, symbol: str, side: str, qty: float) -> dict[str, Any]:
        sym = symbol.upper()
        rules = self.lot_rules.get(sym, LotRules())
        q = self._format_qty(sym, qty)
        return self._signed_post(
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
        return self._signed_post(
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
