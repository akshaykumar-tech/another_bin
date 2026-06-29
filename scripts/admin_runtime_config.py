"""Hot-reload JSON runtime overrides (admin panel can write e.g. data/admin/godmode_live.json)."""
from __future__ import annotations

import json
from pathlib import Path

VALID_LIVE_SYMBOL_SCOPES = frozenset({"all", "btc_independent"})


class AdminRuntimeConfig:
    def __init__(self, path: Path, defaults: dict | None = None) -> None:
        self.path = path
        self.defaults = defaults or {}
        self._data: dict = dict(self.defaults)
        self._mtime = 0.0

    def reload(self, force: bool = False) -> None:
        if not self.path.is_file():
            self._data = dict(self.defaults)
            self._mtime = 0.0
            return
        mtime = self.path.stat().st_mtime
        if not force and mtime == self._mtime:
            return
        self._mtime = mtime
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                self._data = {**self.defaults, **raw}
        except Exception:
            pass

    def live_symbol_scope(self, env_default: str) -> str:
        self.reload()
        v = str(self._data.get("live_symbol_scope", env_default)).strip().lower()
        if v in VALID_LIVE_SYMBOL_SCOPES:
            return v
        fb = env_default.strip().lower()
        return fb if fb in VALID_LIVE_SYMBOL_SCOPES else "all"
