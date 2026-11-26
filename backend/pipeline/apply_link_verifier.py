"""
ApplyLinkVerifier
=================
Production apply-link verification for opportunity listings.

Verifies that ``apply_url`` values are live and reachable before/after they
are published. This is the component that produces the ``is_404`` /
``is_application_closed`` signals consumed by ``ExpirationRules`` and powers
the "Verified apply links >99%" KPI.

Classification:
- VERIFIED   : HTTP 2xx/3xx from the listing host (link is live).
- DEAD       : HTTP 404/410 (listing removed).
- CLOSED     : Application closed (``?gh_src`` gone / explicit closed page).
- UNREACHABLE: Network/timeout errors (transient; not a verdict).
- INVALID    : Malformed URL (no scheme/host) - not attempted.

Verification is injectable (``httpx.Client`` / transport) so it is fully
testable offline via ``httpx.MockTransport``.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

import httpx

from backend.pipeline.connectors._http import ConnectorHttpClient

logger = logging.getLogger(__name__)

# HTTP statuses treated as "listing removed".
_DEAD_STATUSES = frozenset({404, 410})

# Statuses treated as "application closed" (soft 4xx on ATS boards).
_CLOSED_STATUSES = frozenset({405})

# Statuses treated as anti-bot or access challenge (transient; not closed).
_CHALLENGE_STATUSES = frozenset({403, 451})

# Signals that indicate an application-closed / empty listing page.
_CLOSED_MARKERS = (
    "application closed",
    "this job is no longer",
    "position has been filled",
    "we are no longer accepting",
    "opportunity is closed",
    "job expired",
    "no longer accepting applications",
)


class VerificationStatus(str, Enum):
    VERIFIED = "verified"
    DEAD = "dead"
    CLOSED = "closed"
    UNREACHABLE = "unreachable"
    INVALID = "invalid"


@dataclass(frozen=True)
class VerificationResult:
    """Outcome of verifying a single apply URL."""

    apply_url: str
    status: VerificationStatus
    http_status: Optional[int] = None
    reason: str = ""
    latency_ms: float = 0.0

    @property
    def is_live(self) -> bool:
        """True when the link is considered usable by end users."""
        return self.status is VerificationStatus.VERIFIED

    @property
    def is_verdict(self) -> bool:
        """True when the result is a definitive verdict (not transient)."""
        return self.status in (
            VerificationStatus.VERIFIED,
            VerificationStatus.DEAD,
            VerificationStatus.CLOSED,
            VerificationStatus.INVALID,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "apply_url": self.apply_url,
            "status": self.status.value,
            "http_status": self.http_status,
            "reason": self.reason,
            "latency_ms": self.latency_ms,
            "is_live": self.is_live,
            "is_verdict": self.is_verdict,
        }


class ApplyLinkVerifier:
    """Verifies apply-link liveness for batches of opportunities.

    Args:
        http: Injected HTTP client (offline-testable).
        verify_ssl: Whether to verify TLS certificates.
        max_results: Upper bound on URLs verified in a single batch.
    """

    def __init__(
        self,
        *,
        http: Optional[ConnectorHttpClient] = None,
        http_client: Optional[httpx.Client] = None,
        max_results: int = 1000,
        max_workers: Optional[int] = None,
    ) -> None:
        if http is not None:
            self._http = http
            self._max_workers = max_workers if max_workers is not None else 1
        elif http_client is not None:
            self._http = ConnectorHttpClient(client=http_client, max_retries=1)
            self._max_workers = max_workers if max_workers is not None else 1
        else:
            # Link liveness checks fail fast on dead/geo-blocked hosts and run
            # concurrently across independent domains to stay well under CI limits.
            self._http = ConnectorHttpClient(
                timeout=8.0,
                max_retries=1,
                rate_limit_per_sec=None,
            )
            self._max_workers = max_workers if max_workers is not None else 12
        self._max_results = max_results

    # ------------------------------------------------------------------
    # Batch API
    # ------------------------------------------------------------------

    def verify_batch(
        self,
        urls: List[str],
        *,
        check_content: bool = False,
    ) -> List[VerificationResult]:
        """Verify a batch of apply URLs.

        Args:
            urls: Apply URLs to verify (deduplicated internally).
            check_content: When True, additionally inspect body text for
                application-closed markers.

        Returns:
            A list of VerificationResult, one per unique URL (capped at
            ``max_results``).
        """
        unique_urls: List[str] = []
        seen: set = set()
        for url in urls:
            if not url or url in seen:
                continue
            seen.add(url)
            if len(unique_urls) >= self._max_results:
                logger.warning("ApplyLinkVerifier reached max_results=%d", self._max_results)
                break
            unique_urls.append(url)

        if not unique_urls:
            return []

        if self._max_workers <= 1 or len(unique_urls) == 1:
            return [self.verify_url(url, check_content=check_content) for url in unique_urls]

        worker_count = min(self._max_workers, len(unique_urls))
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            return list(
                executor.map(
                    lambda u: self.verify_url(u, check_content=check_content),
                    unique_urls,
                )
            )

    def verify_url(self, url: str, *, check_content: bool = False) -> VerificationResult:
        """Verify a single apply URL."""
        if not self._is_valid_url(url):
            return VerificationResult(
                apply_url=url,
                status=VerificationStatus.INVALID,
                reason="URL has no valid scheme or host.",
            )

        start = time.monotonic()
        try:
            response = self._http.get(url)
        except httpx.HTTPStatusError as exc:
            return self._classify_http_error(url, exc.response, _elapsed(start))
        except (httpx.ConnectError, httpx.TimeoutException) as exc:
            return VerificationResult(
                apply_url=url,
                status=VerificationStatus.UNREACHABLE,
                reason=f"Network error: {type(exc).__name__}",
                latency_ms=_elapsed(start),
            )
        except Exception as exc:
            return VerificationResult(
                apply_url=url,
                status=VerificationStatus.UNREACHABLE,
                reason=f"Unexpected error: {type(exc).__name__}: {exc}",
                latency_ms=_elapsed(start),
            )

        return self._classify_response(url, response, _elapsed(start), check_content)

    # ------------------------------------------------------------------
    # Classification
    # ------------------------------------------------------------------

    def _classify_response(
        self,
        url: str,
        response: httpx.Response,
        latency_ms: float,
        check_content: bool,
    ) -> VerificationResult:
        status = response.status_code

        if status in _DEAD_STATUSES:
            return VerificationResult(
                apply_url=url, status=VerificationStatus.DEAD,
                http_status=status, reason=f"HTTP {status}: listing removed.",
                latency_ms=latency_ms,
            )

        if status in _CHALLENGE_STATUSES:
            reason = (
                f"HTTP {status}: anti-bot challenge response."
                if status == 403
                else f"HTTP {status}: legal / geo-restriction challenge."
            )
            return VerificationResult(
                apply_url=url,
                status=VerificationStatus.UNREACHABLE,
                http_status=status,
                reason=reason,
                latency_ms=latency_ms,
            )

        if status in _CLOSED_STATUSES:
            return VerificationResult(
                apply_url=url, status=VerificationStatus.CLOSED,
                http_status=status, reason=f"HTTP {status}: application closed.",
                latency_ms=latency_ms,
            )

        if 200 <= status < 400:
            # Detect 301/302 redirects to generic search or portal home pages
            if response.history:
                final_path = response.url.path.rstrip("/").lower()
                if final_path in ("", "/jobs", "/search", "/explore", "/job-listings", "/careers"):
                    return VerificationResult(
                        apply_url=url,
                        status=VerificationStatus.CLOSED,
                        http_status=status,
                        reason=f"Redirected to generic portal page '{response.url.path}'.",
                        latency_ms=latency_ms,
                    )

            # Support PDF document verification (e.g. Govt/Research PDF notifications)
            content_type = response.headers.get("content-type", "").lower()
            if url.lower().endswith(".pdf") or "application/pdf" in content_type:
                if len(response.content) > 0 or int(response.headers.get("content-length", "1")) > 0:
                    return VerificationResult(
                        apply_url=url, status=VerificationStatus.VERIFIED,
                        http_status=status, reason=f"HTTP {status}: Valid PDF Document Link.",
                        latency_ms=latency_ms,
                    )

            if check_content:
                body = response.text.lower()
                if any(marker in body for marker in _CLOSED_MARKERS):
                    return VerificationResult(
                        apply_url=url, status=VerificationStatus.CLOSED,
                        http_status=status, reason="Application-closed markers detected.",
                        latency_ms=latency_ms,
                    )
            return VerificationResult(
                apply_url=url, status=VerificationStatus.VERIFIED,
                http_status=status, reason=f"HTTP {status}: live.",
                latency_ms=latency_ms,
            )

        return VerificationResult(
            apply_url=url, status=VerificationStatus.UNREACHABLE,
            http_status=status, reason=f"HTTP {status}: unexpected.",
            latency_ms=latency_ms,
        )

    @staticmethod
    def _classify_http_error(
        url: str,
        response: httpx.Response,
        latency_ms: float,
    ) -> VerificationResult:
        status = response.status_code
        if status in _DEAD_STATUSES:
            return VerificationResult(
                apply_url=url, status=VerificationStatus.DEAD,
                http_status=status, reason=f"HTTP {status}: listing removed.",
                latency_ms=latency_ms,
            )
        if status in _CHALLENGE_STATUSES:
            reason = (
                f"HTTP {status}: anti-bot challenge response."
                if status == 403
                else f"HTTP {status}: legal / geo-restriction challenge."
            )
            return VerificationResult(
                apply_url=url,
                status=VerificationStatus.UNREACHABLE,
                http_status=status,
                reason=reason,
                latency_ms=latency_ms,
            )
        if status in _CLOSED_STATUSES:
            return VerificationResult(
                apply_url=url, status=VerificationStatus.CLOSED,
                http_status=status, reason=f"HTTP {status}: application closed.",
                latency_ms=latency_ms,
            )
        if 300 <= status < 400:
            # Redirect without follow: link is still live.
            return VerificationResult(
                apply_url=url, status=VerificationStatus.VERIFIED,
                http_status=status, reason=f"HTTP {status}: redirect (live).",
                latency_ms=latency_ms,
            )
        return VerificationResult(
            apply_url=url, status=VerificationStatus.UNREACHABLE,
            http_status=status, reason=f"HTTP {status}: unexpected.",
            latency_ms=latency_ms,
        )

    @staticmethod
    def _is_valid_url(url: str) -> bool:
        try:
            parsed = httpx.URL(url)
        except Exception:
            return False
        return bool(parsed.scheme in ("http", "https") and parsed.host)


def _elapsed(start_monotonic: float) -> float:
    return round((time.monotonic() - start_monotonic) * 1000.0, 2)


__all__ = ["ApplyLinkVerifier", "VerificationResult", "VerificationStatus"]
