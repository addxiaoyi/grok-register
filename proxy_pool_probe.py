"""Probe scheduling for the proxy pool.

This module isolates the three timer concerns that were previously mixed
inside ProxyPoolManager: per-node failure probes (deduped by in-flight event),
batch probes (throttled by interval), and the background periodic guard.
"""
from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from proxy_pool_v3 import ProxyPoolManager

__all__ = ("ProbeScheduler",)


class ProbeScheduler:
    def __init__(self, manager: "ProxyPoolManager"):
        self._manager = manager

    def schedule_failure_probe(
        self,
        node_id: str,
        penalize_on_failure: bool = False,
        suspected_error: str | None = None,
        lease=None,
    ):
        """Schedule a single-node probe, deduping concurrent requests for the same node."""
        mgr = self._manager
        with mgr._lock:
            existing = mgr._probe_events.get(node_id)
            if existing is not None and not existing.is_set():
                return
            event = threading.Event()
            mgr._probe_events[node_id] = event

        def runner():
            try:
                result = mgr.probe_node(node_id)
                if penalize_on_failure and result.get("status") != "healthy":
                    mgr._apply_transport_failure(
                        node_id,
                        suspected_error or result.get("error") or "probe failed",
                        schedule_probe=False,
                        lease=lease,
                    )
            except Exception:
                if penalize_on_failure:
                    mgr._apply_transport_failure(
                        node_id,
                        suspected_error or "probe failed",
                        schedule_probe=False,
                        lease=lease,
                    )
            finally:
                event.set()
                with mgr._lock:
                    if mgr._probe_events.get(node_id) is event:
                        mgr._probe_events.pop(node_id, None)

        threading.Thread(target=runner, name="proxy-probe-%s" % node_id[:8], daemon=True).start()

    def probe_all(self, force: bool = False):
        """Batch-probe all non-retired nodes, throttled by probe_interval."""
        mgr = self._manager
        now = time.time()
        with mgr._lock:
            if not force and mgr.probe_interval > 0 and now - mgr._last_probe_all < mgr.probe_interval:
                return []
            node_ids = [node.id for node in mgr._nodes.values() if not node.retired]
            mgr._last_probe_all = now

        if not node_ids:
            return []

        results = []
        with ThreadPoolExecutor(max_workers=min(8, len(node_ids)), thread_name_prefix="proxy-probe") as executor:
            futures = {executor.submit(mgr.probe_node, node_id): node_id for node_id in node_ids}
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as exc:
                    from proxy_pool_v3 import safe_proxy_error_text
                    results.append({"id": futures[future], "status": "unhealthy", "error": safe_proxy_error_text(exc)})
        return results

    def schedule_periodic_probe_if_due(self):
        """Background guard that triggers probe_all when the interval expires."""
        mgr = self._manager
        if not mgr.managed or mgr.probe_interval <= 0:
            return
        now = time.time()
        with mgr._lock:
            if mgr._probe_all_running or now - mgr._last_probe_all < mgr.probe_interval:
                return
            mgr._probe_all_running = True
            mgr._last_probe_all = now

        def runner():
            try:
                self.probe_all(force=True)
            finally:
                with mgr._lock:
                    mgr._probe_all_running = False

        threading.Thread(target=runner, name="proxy-probe-all", daemon=True).start()
