"""Node-set reconciliation for the proxy pool.

The diff here is deliberately a pure function of (previous nodes, source entries):
it mutates node objects in place, but it takes no locks, does no I/O, and returns
the next node mapping. Locking and persistence stay with the caller.
"""
from __future__ import annotations

from typing import Callable, Dict, Iterable, Tuple

__all__ = ("reconcile_nodes",)


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
