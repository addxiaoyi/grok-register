"""Selection and failure-handling policies for the proxy pool scheduler."""
from __future__ import annotations

import hashlib
import time
from typing import Protocol, Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from proxy_pool_v3 import ProxyNode

__all__ = (
    "SelectorPolicy",
    "FailurePolicy",
    "DefaultSelectorPolicy",
    "DefaultFailurePolicy",
    "PROBE_TIER_HEALTHY",
    "PROBE_TIER_STALE",
    "PROBE_TIER_UNHEALTHY",
)

PROBE_TIER_HEALTHY = 0
PROBE_TIER_STALE = 1
PROBE_TIER_UNHEALTHY = 2


class SelectorPolicy(Protocol):
    """Decides which eligible node to pick for a given affinity."""

    def select(self, nodes: Sequence[ProxyNode], affinity: str) -> ProxyNode:
        """Pick one node from the candidate set."""
        ...


class FailurePolicy(Protocol):
    """Decides health/cooldown updates after a failure."""

    def apply_transport_failure(
        self,
        node: ProxyNode,
        is_rotating: bool,
        cooldown_cap: float = 600.0,
        base_cooldown: float = 30.0,
    ) -> None:
        """Update node health, failure counters, and cooldown window."""
        ...


class DefaultSelectorPolicy:
    """Original _select_locked logic extracted as a policy."""

    @staticmethod
    def probe_tier(node: ProxyNode, now: float, probe_interval: float = 180.0) -> int:
        freshness = max(60, (probe_interval * 2) if probe_interval > 0 else 300)
        if not node.last_probed_at or now - node.last_probed_at > freshness:
            return PROBE_TIER_STALE
        if node.probe_status == "healthy":
            return PROBE_TIER_HEALTHY
        if node.probe_status == "unhealthy":
            return PROBE_TIER_UNHEALTHY
        return PROBE_TIER_STALE

    def select(self, nodes: Sequence[ProxyNode], affinity: str) -> ProxyNode:
        now = time.time()
        # Assume caller passes probe_interval via node or global config;
        # for backward-compat we derive from first node's config if available.
        probe_interval = getattr(nodes[0], "probe_interval", 180.0) if nodes else 180.0

        best_tier = min(self.probe_tier(node, now, probe_interval) for node in nodes)
        pool = sorted(
            (node for node in nodes if self.probe_tier(node, now, probe_interval) == best_tier),
            key=lambda n: n.id,
        )
        digest = hashlib.sha256(str(affinity or "").encode("utf-8")).digest()
        selected = pool[int.from_bytes(digest[:8], "big") % len(pool)]
        if selected.rotating or selected.health >= 0.8 or len(pool) == 1:
            return selected
        return max(pool, key=lambda n: (n.health, -n.inflight, n.id))


class DefaultFailurePolicy:
    """Original _apply_transport_failure logic extracted as a policy."""

    def apply_transport_failure(
        self,
        node: ProxyNode,
        is_rotating: bool,
        cooldown_cap: float = 600.0,
        base_cooldown: float = 30.0,
    ) -> None:
        if is_rotating:
            node.exit_failures += 1
            node.last_error = "transport: rotating exit"
            return

        node.failure_count += 1
        node.health = max(0.05, node.health * 0.7)
        cooldown = min(cooldown_cap, base_cooldown * (2 ** min(max(node.failure_count - 1, 0), 4)))
        node.cooldown_until = time.time() + cooldown
        node.last_error = "transport"
