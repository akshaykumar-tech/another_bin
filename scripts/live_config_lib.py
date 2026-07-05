"""Global live-trading env — one set of LIVE_* keys for SNR / ACCEL5X live bots."""

from __future__ import annotations

import os


def _env(name: str, default: str) -> str:
    return os.environ.get(name, "").strip() or default


def _env_int(name: str, default: int) -> int:
    return int(_env(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(_env(name, str(default)))


def live_notional_usdt(default: float = 6.0) -> float:
    return _env_float("LIVE_NOTIONAL_USDT", default)


def live_max_open(default: int = 40) -> int:
    return _env_int("LIVE_MAX_OPEN", default)


def live_min_leverage(default: int = 10) -> int:
    return _env_int("LIVE_MIN_LEVERAGE", default)


def live_margin_buffer(default: float = 1.05) -> float:
    return _env_float("LIVE_MARGIN_BUFFER", default)


def live_reconcile_sec(default: int = 60) -> int:
    return _env_int("LIVE_RECONCILE_SEC", default)


def live_algo_working_type(default: str = "CONTRACT_PRICE") -> str:
    return _env("LIVE_ALGO_WORKING_TYPE", default).upper()


def live_sl_pct(default: float = 8.0) -> float:
    return _env_float("LIVE_SL_PCT", default)


def live_tp_pct(default: float = 1.5) -> float:
    return _env_float("LIVE_TP_PCT", default)


def binance_api_key() -> str:
    return _env("BINANCE_API_KEY", "")


def binance_api_secret() -> str:
    return _env("BINANCE_API_SECRET", "")
