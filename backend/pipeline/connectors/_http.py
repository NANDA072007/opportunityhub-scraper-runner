"""
Connector HTTP Fetch Helper
============================
Shared, dependency-injectable HTTP fetch utilities for canonical connectors.

Connectors MUST only fetch raw payloads (OpportunityRaw) and MUST NOT contain
domain or branch-specific business logic. This helper centralises the common
HTTP concerns (timeouts, retries, rate limiting, user-agent rotation) so each
connector stays focused on parsing its source's payload.

The fetch layer is injectable (``httpx.Client`` / ``transport``) so unit tests
can run against mocked responses without network access.
"""

from __future__ import annotations

import ipaddress
import logging
import random
import socket
import time
from typing import Any, Callable, Dict, List, Optional, TypeVar

import httpx

logger = logging.getLogger(__name__)

# Default timeout for outbound connector requests.
DEFAULT_TIMEOUT: float = 20.0

# Pool of user agents rotated per request to reduce bot-detection drops.
_USER_AGENTS: List[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) "
    "Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:126.0) "
    "Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 "
    "Mobile/15E148 Safari/604.1",
]

# HTTP statuses that warrant an automatic retry.
_RETRYABLE_STATUSES = frozenset({408, 429, 500, 502, 503, 504})


def is_ssrf_safe_host(host: str) -> bool:
    """Validate that hostname resolves strictly to public, globally routable IPs.
    
    Rejects:
    - Loopback (127.0.0.0/8, ::1)
    - Private networks (10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16)
    - Cloud metadata (169.254.169.254, link-local)
    - Multicast, broadcast, reserved
    """
    if not host or not isinstance(host, str):
        return False
    cleaned_host = host.strip().lower()
    if cleaned_host in ("localhost", "metadata.google.internal") or cleaned_host.endswith(".local"):
        return False

    try:
        addrinfo = socket.getaddrinfo(cleaned_host, None)
        for _, _, _, _, sockaddr in addrinfo:
            ip_str = sockaddr[0]
            ip = ipaddress.ip_address(ip_str)
            if not ip.is_global or ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
                logger.error("SSRF defense blocked: host '%s' resolved to non-public IP '%s'", host, ip)
                return False
        return True
    except socket.gaierror as exc:
        logger.warning("DNS resolution failed for '%s': %s", host, exc)
        return False
    except Exception as exc:
        logger.error("Unexpected error verifying IP for '%s': %s", host, exc)
        return False


def make_ssrf_request_hook(enforce: bool = True) -> Callable[[httpx.Request], None]:
    """Build an httpx event hook to enforce SSRF validation on every outbound request and redirect."""
    def hook(request: httpx.Request) -> None:
        if not enforce:
            return
        host = request.url.host
        if not is_ssrf_safe_host(host):
            raise PermissionError(f"SSRF blocked: request to non-public/internal host '{host}' is prohibited.")
    return hook


def build_client(
    *,
    timeout: float = DEFAULT_TIMEOUT,
    transport: Optional[httpx.BaseTransport] = None,
    enforce_ssrf_safety: bool = True,
    max_redirects: int = 3,
) -> httpx.Client:
    """Build a configured httpx.Client with SSRF protections and bounded redirects.

    Args:
        timeout: Request timeout in seconds.
        transport: Optional mocked transport for unit tests.
        enforce_ssrf_safety: When True and transport is not mock, verifies host IP is public.
        max_redirects: Strict bound on redirect hops (default: 3).

    Returns:
        A configured, SSRF-safe httpx.Client.
    """
    event_hooks: Dict[str, List[Any]] = {}
    if transport is None and enforce_ssrf_safety:
        event_hooks["request"] = [make_ssrf_request_hook(enforce=True)]

    return httpx.Client(
        headers={
            "User-Agent": random.choice(_USER_AGENTS),
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "application/json;q=0.8,*/*;q=0.8"
            ),
            "Accept-Language": "en-IN,en;q=0.9,hi;q=0.8",
        },
        timeout=timeout,
        follow_redirects=True,
        max_redirects=max_redirects,
        transport=transport,
        event_hooks=event_hooks,
    )


class ConnectorHttpClient:
    """Thin wrapper around httpx with retries and light rate limiting.

    Accepts an optional injected ``httpx.Client`` (or transport) so callers
    can unit-test connectors offline via ``httpx.MockTransport``.
    """

    def __init__(
        self,
        *,
        client: Optional[httpx.Client] = None,
        transport: Optional[httpx.BaseTransport] = None,
        timeout: float = DEFAULT_TIMEOUT,
        max_retries: int = 3,
        backoff_base: float = 1.5,
        rate_limit_per_sec: Optional[float] = None,
    ) -> None:
        if client is None:
            client = build_client(timeout=timeout, transport=transport)
        self._client: httpx.Client = client
        self._max_retries = max(1, max_retries)
        self._backoff_base = backoff_base
        self._min_interval = 1.0 / rate_limit_per_sec if rate_limit_per_sec else 0.0
        self._last_request_ts = 0.0

    @property
    def client(self) -> httpx.Client:
        return self._client

    def _throttle(self) -> None:
        if self._min_interval <= 0:
            return
        now = time.monotonic()
        elapsed = now - self._last_request_ts
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_request_ts = time.monotonic()

    def get(self, url: str, params: Optional[Dict[str, Any]] = None) -> httpx.Response:
        """Perform a GET with retries on transient failures."""
        last_error: Optional[Exception] = None
        for attempt in range(1, self._max_retries + 1):
            self._throttle()
            try:
                response = self._client.get(url, params=params)
                if response.status_code in _RETRYABLE_STATUSES and attempt < self._max_retries:
                    self._sleep(attempt)
                    continue
                response.raise_for_status()
                return response
            except httpx.HTTPStatusError as exc:
                last_error = exc
                status = exc.response.status_code
                if status in _RETRYABLE_STATUSES and attempt < self._max_retries:
                    self._sleep(attempt)
                    continue
                raise
            except (
                httpx.ConnectError,
                httpx.TimeoutException,
            ) as exc:
                # TimeoutException covers Connect/Read/Write/Pool timeouts.
                # Catching only ReadTimeout/WriteTimeout meant a connect
                # timeout (geo-blocked or dead host) failed with zero retries.
                last_error = exc
                if attempt < self._max_retries:
                    self._sleep(attempt)
                    continue
                raise
        raise RuntimeError(f"All retries exhausted for {url}") from last_error

    def get_json(self, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """GET and decode a JSON response body."""
        return self.get(url, params=params).json()

    def get_text(self, url: str, params: Optional[Dict[str, Any]] = None) -> str:
        """GET and return the response body as text."""
        return self.get(url, params=params).text

    def _sleep(self, attempt: int) -> None:
        delay = self._backoff_base ** attempt + random.uniform(0, 0.5)
        logger.debug("Backing off %.2fs before retry %d.", delay, attempt)
        time.sleep(delay)

    def close(self) -> None:
        self._client.close()


_T = TypeVar("_T")


def parse_guarded(
    parser: Callable[[Any], Optional[_T]],
    raw_item: Any,
    *,
    context: str = "",
    logger_name: Optional[str] = None,
) -> Optional[_T]:
    """Parse a single raw item inside a per-item try/except.

    A malformed entry is logged and skipped (None) instead of aborting the
    entire connector run, so 99 good records survive one bad record.
    """
    if raw_item is None:
        return None
    try:
        return parser(raw_item)
    except Exception as exc:  # noqa: BLE001
        log = logger if logger_name is None else logging.getLogger(logger_name)
        log.warning(
            "Skipping malformed %s item: %s",
            context or "connector",
            exc,
        )
        return None


__all__ = [
    "ConnectorHttpClient",
    "build_client",
    "parse_guarded",
    "DEFAULT_TIMEOUT",
]
