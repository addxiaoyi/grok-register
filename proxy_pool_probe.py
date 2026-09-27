"""Probe scheduler for health-checking proxy nodes."""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, TYPE_CHECKING

from proxy_pool_v3 import ProxyPoolError, safe_proxy_error_text

if TYPE_CHECKING:
    from proxy_pool_v3 import ProxyPoolManager

__all__ = (
    "ProbeScheduler",
)


class ProbeScheduler:
    def __init__(self, manager: ProxyPoolManager):
        self._manager = manager
        self._events: Dict[str, threading.Event] = {}
        self._last_probe_all: float = 0.0
        self._probe_all_running: bool = False

    def schedule_failure_probe(
        self,
        node_id: str,
        penalize_on_failure: bool = False,
        suspected_error: Optional[str] = None,
        lease=None,
    ):
        with self._manager._lock:
            existing = self._events.get(node_id)
            if existing is not None and not existing.is_set():
                return
            event = threading.Event()
            self._events[node_id] = event
        def runner():
            try:
                result = self._manager.probe_node(node_id)
                if penalize_on_failure and result.get("status") != "healthy":
                    self._manager._apply_transport_failure(
                        node_id,
                        suspected_error or result.get("error") or "probe failed",
                        schedule_probe=False,
                        lease=lease,
                    )
            except Exception:
                if penalize_on_failure:
                    self._manager._apply_transport_failure(
                        node_id,
                        suspected_error or "probe failed",
                        schedule_probe=False,
                        lease=lease,
                    )
            finally:
                event.set()
                with self._manager._lock:
                    if self._events.get(node_id) is event:
                        self._events.pop(node_id, None)
        threading.Thread(target=runner, name="proxy-probe-%s" % node_id[:8], daemon=True).start()

    def probe_all(self, force: bool = False) -> List[Dict]:
        now = time.time()
        with self._manager._lock:
            if not force and self._manager.probe_interval > 0 and now - self._last_probe_all < self._manager.probe_interval:
                return []
            node_ids = [node.id for node in self._manager._nodes.values() if not node.retired]
            self._last_probe_all = now
        if not node_ids:
            return []
        results = []
        with ThreadPoolExecutor(max_workers=min(8, len(node_ids)), thread_name_prefix="proxy-probe") as executor:
            futures = {executor.submit(self._manager.probe_node, node_id): node_id for node_id in node_ids}
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as exc:
                    results.append({"id": futures[future], "status": "unhealthy", "error": safe_proxy_error_text(exc)})
        return results

    def schedule_periodic_probe_if_due(self):
        if not self._manager.managed or self._manager.probe_interval <= 0:
            return
        now = time.time()
        with self._manager._lock:
            if self._probe_all_running or now - self._last_probe_all < self._manager.probe_interval:
                return
            self._probe_all_running = True
            self._last_probe_all = now
        def runner():
            try:
                self._manager.probe_all(force=True)
            finally:
                with self._manager._lock:
                    self._probe_all_running = False
        threading.Thread(target=runner, name="proxy-probe-all", daemon=True).start()

    def cleanup_event(self, node_id: str):
        with self._manager._lock:
            self._events.pop(node_id, None)
