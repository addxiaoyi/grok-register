"""Network error taxonomy shared by the proxy pool and its callers.

Classification is pure text/attribute inspection with no I/O and no dependency on
pool state, so it stays importable from anywhere without a circular import.
"""
from __future__ import annotations

# Classification outcomes, ordered from "the node is unusable as configured" to
# "the failure is the business caller's fault, not the proxy's".
COMPATIBILITY = "compatibility"
CONFIGURATION = "configuration"
HARD_TRANSPORT = "hard_transport"
SUSPECTED_TRANSPORT = "suspected_transport"
APPLICATION = "application"

TRANSPORT_KINDS = (HARD_TRANSPORT, SUSPECTED_TRANSPORT)

# Markers are matched as substrings against a lowercased error text. Ordered from
# most specific to least: a proxy-credentials failure can also read as a generic
# connection error, and compatibility messages can read as unsupported-scheme
# connection errors.
_COMPATIBILITY_MARKERS = (
    "unknown url type", "unsupported proxy scheme", "http-compatible proxy endpoint",
    "does not support scheme", "代理协议不受", "proxy scheme is unsupported",
    "unsupported proxy protocol", "native-only",
)

_CONFIGURATION_MARKERS = (
    "proxy authentication", "proxy auth", "authentication failed", "authentication method rejected",
    "credentials rejected", "credential", "http_proxy_auth", "socks_auth", "407 proxy authentication",
)

_HARD_TRANSPORT_MARKERS = (
    "socks4 connect failed", "socks5 connect failed", "proxy connection failed", "proxy server refused",
    "tunnel connection failed", "could not connect to proxy", "failed to connect to proxy",
    "err_proxy_connection_failed", "err_tunnel_connection_failed", "connection refused",
    "no route to host", "network is unreachable", "upstream_connect", "http_connect", "socks_connect",
)

_SUSPECTED_TRANSPORT_MARKERS = (
    "tls connect error", "ssl", "handshake", "unexpected eof", "unexpected_eof",
    "connection reset", "connection aborted", "remote end closed", "broken pipe",
    "timed out", "timeout", "temporarily unavailable", "connect error", "failed to connect",
    "could not connect", "remote_reset", "https_proxy_tls", "local_dns", "remote_dns",
)

# Structured signals the protocol bridge attaches to diagnostics. These are exact
# kinds, not free text, so they win over any substring match.
_STRUCTURED_KINDS = {
    "socks_auth": CONFIGURATION,
    "http_proxy_auth": CONFIGURATION,
    "configuration": CONFIGURATION,
    "upstream_connect": HARD_TRANSPORT,
    "http_connect": HARD_TRANSPORT,
    "socks_connect": HARD_TRANSPORT,
    "https_proxy_tls": SUSPECTED_TRANSPORT,
    "remote_reset": SUSPECTED_TRANSPORT,
    "local_dns": SUSPECTED_TRANSPORT,
    "remote_dns": SUSPECTED_TRANSPORT,
    "bridge": SUSPECTED_TRANSPORT,
}


def classify_proxy_network_error(value):
    """Bucket a proxy/network failure into one of the five taxonomy outcomes.

    Accepts an exception, a diagnostic object exposing ``kind``, or raw text.
    """
    kind = getattr(value, "kind", "")
    if kind in _STRUCTURED_KINDS:
        return _STRUCTURED_KINDS[kind]

    text = str(value or "").lower()
    if not text:
        return APPLICATION
    for outcome, markers in (
        (COMPATIBILITY, _COMPATIBILITY_MARKERS),
        (CONFIGURATION, _CONFIGURATION_MARKERS),
        (HARD_TRANSPORT, _HARD_TRANSPORT_MARKERS),
        (SUSPECTED_TRANSPORT, _SUSPECTED_TRANSPORT_MARKERS),
    ):
        if any(marker in text for marker in markers):
            return outcome
    return APPLICATION


def is_transport_error_text(value):
    """True when the failure is transport-shaped rather than config or business."""
    return classify_proxy_network_error(value) in TRANSPORT_KINDS
