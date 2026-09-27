"""Proxy source bookkeeping and node-set reconciliation for the pool.

``SourceRegistry`` owns the per-source state machine (configured, last success,
last error, diagnostics) so the manager stops juggling two parallel dicts. The
diff in ``reconcile_nodes`` stays a pure function of (previous nodes, source
entries): it mutates node objects in place, but takes no locks, does no I/O, and
returns the next node mapping. Locking and persistence stay with the caller.
"""
from __future__ import annotations

import time
from typing import Callable, Dict, Iterable, Tuple

__all__ = ("SourceRegistry", "SourceState", "reconcile_nodes")

SOURCE_NAMES = ("file", "subscription")


class SourceState:
    """Last known outcome of one configured proxy source."""

    __slots__ = ("configured", "descriptors", "last_error", "last_success_at", "generation", "diagnostics")

    def __init__(self):
        self.configured = False
        self.descriptors: list = []
        self.last_error = ""
        self.last_success_at = 0.0
        self.generation = 0
        self.diagnostics: Dict = {}


class SourceRegistry:
    """Collects fresh descriptors from each source, tolerating partial failure.

    A source that fails to load keeps its last successful descriptors, so a
    subscription outage degrades to stale-but-usable rather than an empty pool.
    """

    def __init__(self, config: Dict, log):
        self._config = config
        self._log = log
        self._states = {name: SourceState() for name in SOURCE_NAMES}
        self.diagnostics: Dict = {}

    @property
    def states(self) -> Dict:
        return self._states

    def _config_key(self, name: str) -> str:
        return "proxy_pool_file" if name == "file" else "proxy_pool_subscription_url"

    def refresh_one(self, name: str, loader: Callable[[], object]) -> None:
        """Load one source, recording either its result or the failure that replaced it."""
        state = self._states[name]
        configured = bool(self._config.get(self._config_key(name)))
        state.configured = configured
        if not configured:
            state.descriptors = []
            state.last_error = ""
            state.diagnostics = {}
            return
        try:
            result = loader()
            if result is None:
                state.descriptors = []
                return
            state.descriptors = list(result.nodes)
            state.last_success_at = time.time()
            state.last_error = ""
            state.generation += 1
            state.diagnostics = result.as_dict()
            state.diagnostics.update(
                {"stale": False, "generation": state.generation, "last_success_at": state.last_success_at}
            )
            if result.skipped:
                self._log("[!] %s 跳过 %s 个无法解析的节点" % (name, result.skipped))
        except Exception as exc:
            from proxy_pool_v3 import safe_proxy_error_text

            state.last_error = safe_proxy_error_text(exc)
            if state.descriptors:
                state.diagnostics = dict(state.diagnostics)
                state.diagnostics.update(
                    {"stale": True, "error": state.last_error, "generation": state.generation}
                )
                self._log("[!] %s 刷新失败，继续使用最近一次成功节点: %s" % (name, state.last_error))
            else:
                state.diagnostics = {"stale": True, "error": state.last_error, "generation": state.generation}

    def collect(self, loaders: Dict[str, Callable[[], object]]) -> list:
        """Refresh every source and return the deduplicated (source, descriptor) pairs.

        Raises when nothing is usable at all, so the caller can surface the
        combined reason instead of handing back a pool that cannot serve.
        """
        values = []
        for name in SOURCE_NAMES:
            loader = loaders.get(name)
            if loader is None:
                continue
            self.refresh_one(name, loader)
            values.extend((name, item) for item in self._states[name].descriptors)

        unique, seen = [], set()
        for source, descriptor in values:
            if descriptor.node_id not in seen:
                seen.add(descriptor.node_id)
                unique.append((source, descriptor))
        self.diagnostics = {
            name: dict(state.diagnostics) for name, state in self._states.items() if state.configured
        }
        if not unique:
            from proxy_pool_v3 import ProxyPoolError

            errors = [state.last_error for state in self._states.values() if state.last_error]
            detail = "; ".join(errors) if errors else "未配置代理池文件或订阅"
            raise ProxyPoolError("代理池没有可用节点: %s" % detail)
        return unique


def reconcile_nodes(
    previous: Dict,
    entries: Iterable[Tuple[str, object]],
    make_node: Callable[[str, object], object],
    rotating_for: Callable[[object], bool],
    restore_state: Callable[[object], None],
) -> Dict:
    """Rebuild the node mapping from a fresh source listing.

    A node present in both generations is reused so accumulated health, cooldown
    and counters survive a refresh. A node that disappeared is only retired while
    it still has in-flight leases; dropping it outright would hand a live lease a
    node that no longer exists.
    """
    updated: Dict = {}
    for source, descriptor in entries:
        node_id = descriptor.node_id
        old = previous.get(node_id)
        if old is not None:
            old.source = source
            old.proxy_url = descriptor.canonical_uri
            old.descriptor = descriptor
            old.protocol = descriptor.protocol
            old.name = descriptor.name
            old.backend = descriptor.backend
            old.rotating = rotating_for(descriptor)
            old.retired = False
            updated[node_id] = old
        else:
            node = make_node(source, descriptor)
            restore_state(node)
            updated[node_id] = node

    for node_id, old in previous.items():
        if node_id not in updated and old.inflight > 0:
            old.retired = True
            updated[node_id] = old

    return updated
