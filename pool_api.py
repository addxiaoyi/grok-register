"""Public facade over the proxy pool.

Callers depend on this surface rather than on ProxyPoolManager directly, so the
manager's internals (node map, condition variable, probe timers) can keep moving
without breaking every consumer. The module-level helpers in proxy_pool_v3 are
now thin wrappers over the process-wide ProxyPool instance.
"""
from __future__ import annotations

import hashlib
import secrets
import threading

__all__ = ("ProxyPool",)

def _tls():
    """Resolve the shared thread-local from proxy_pool_v3 at call time to avoid a circular import."""
    import proxy_pool_v3
    return proxy_pool_v3._TLS


class ProxyPool:
    """Acquisition and reporting surface for one proxy pool.

    Holds the lease for the calling thread, mirroring the thread-local contract
    the registration flow already relies on: one outstanding lease per thread.
    """

    def __init__(self, manager):
        self._manager = manager

    @property
    def manager(self):
        return self._manager

    @property
    def managed(self) -> bool:
        return bool(self._manager.managed)

    def acquire(
        self,
        slot_index,
        attempt_index=1,
        worker_key=None,
        cancel_callback=None,
    ):
        """Lease a proxy for the calling thread.

        Affinity and session key are derived here rather than by callers so the
        selection stays stable across attempts for the same worker and slot.
        """
        if self.lease is not None:
            from proxy_pool_v3 import ProxyPoolError
            raise ProxyPoolError("当前线程已有未释放的代理租约")
        manager = self._manager
        if not manager.managed:
            return None
        worker = str(worker_key or threading.current_thread().name or "worker")
        slot = int(slot_index)
        attempt = int(attempt_index)
        affinity = "%s:slot:%s" % (worker, slot)
        session_seed = "%s:%s:%s:%s" % (worker, slot, attempt, secrets.token_hex(8))
        session_key = hashlib.sha256(session_seed.encode("utf-8")).hexdigest()[:16]
        lease = manager.acquire(
            affinity=affinity,
            worker_key=worker,
            slot_index=slot,
            attempt_index=attempt,
            session_key=session_key,
            cancel_callback=cancel_callback,
        )
        _tls().lease = lease
        return lease

    @property
    def lease(self):
        return getattr(_tls(), "lease", None)

    def release(self, success=False, transport_error=None):
        """Report the outcome of the held lease and hand the proxy back."""
        lease = self.lease
        if lease is None:
            return
        manager = self._manager
        try:
            if transport_error is not None:
                manager.report_transport_failure(lease, transport_error)
            elif success:
                manager.report_success(lease)
        finally:
            manager.release(lease)
            _tls().lease = None

    def report_transport_failure(self, error):
        lease = self.lease
        if lease is not None:
            self._manager.report_transport_failure(lease, error)

    def report_suspected_transport_failure(self, error):
        lease = self.lease
        if lease is not None:
            self._manager.report_suspected_transport_failure(lease, error)

    def status(self) -> dict:
        return self._manager.snapshot()

    # Alias kept so callers can read either name against the same surface.
    snapshot = status

    def reload(self, force: bool = False) -> dict:
        return self._manager.reload_sources(force=force)

    def probe(self, force: bool = False) -> list:
        return self._manager.probe_all(force=force)

    def shutdown(self):
        self._manager.shutdown()
