"""Source registry – thin wrapper around source-loading logic in ProxyPoolManager."""
from __future__ import annotations

import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from proxy_pool_v3 import ProxyPoolManager

__all__ = ("SourceRegistry",)


class SourceRegistry:
    def __init__(self, manager: ProxyPoolManager):
        self._manager = manager

    def refresh_sources_locked(self, force: bool = False):
        """Refresh sources while holding the refresh lock; returns snapshot."""
        return self._manager._reload_sources_locked(force=force)

    @property
    def source_states(self):
        return self._manager._source_states

    @property
    def last_refresh(self) -> float:
        return self._manager._last_refresh

    def refresh_if_due(self):
        if not self._manager.managed:
            return self._manager.snapshot()
        with self._manager._refresh_lock:
            return self._manager._reload_sources_locked(force=False)

    def reload_sources(self, force: bool = False):
        if not self._manager.managed:
            return self._manager.snapshot()
        with self._manager._refresh_lock:
            return self._manager._reload_sources_locked(force=force)