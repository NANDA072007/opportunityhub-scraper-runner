"""
DomainCircuitBreaker
====================
Circuit breaker for OpportunityHub source connectors and scraping domains.

Invariants:
1. Cold-start volume guard:
   Must process at least `min_sample` (15) items before tripping.
2. Rejection rate threshold:
   Trips when (rejections / total_processed) > rejection_threshold (0.40).
3. Tripping action:
   - Transitions state to OPEN.
   - Logs error with metrics breakdown.
   - Disables connector via ConnectorRegistry.disable(source_slug).
   - Halts further scraping for that domain/source.
4. Tracking across runs:
   Accumulates total_processed and rejections across multiple ingestion runs.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional

from backend.pipeline.connectors.connector_registry import ConnectorRegistry

logger = logging.getLogger("DomainCircuitBreaker")


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class DomainMetrics:
    """Run-time metrics for a tracked domain or source connector."""

    domain_or_slug: str
    total_processed: int = 0
    rejections: int = 0
    state: CircuitState = CircuitState.CLOSED
    tripped_at: Optional[str] = None
    trip_reason: Optional[str] = None
    last_updated_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    @property
    def rejection_rate(self) -> float:
        if self.total_processed == 0:
            return 0.0
        return self.rejections / self.total_processed

    def to_dict(self) -> Dict[str, Any]:
        return {
            "domain_or_slug": self.domain_or_slug,
            "total_processed": self.total_processed,
            "rejections": self.rejections,
            "rejection_rate": round(self.rejection_rate, 4),
            "state": self.state.value,
            "tripped_at": self.tripped_at,
            "trip_reason": self.trip_reason,
            "last_updated_at": self.last_updated_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> DomainMetrics:
        state_str = data.get("state", "closed")
        try:
            state = CircuitState(state_str)
        except ValueError:
            state = CircuitState.CLOSED
        return cls(
            domain_or_slug=data["domain_or_slug"],
            total_processed=data.get("total_processed", 0),
            rejections=data.get("rejections", 0),
            state=state,
            tripped_at=data.get("tripped_at"),
            trip_reason=data.get("trip_reason"),
            last_updated_at=data.get(
                "last_updated_at", datetime.now(timezone.utc).isoformat()
            ),
        )


class DomainCircuitBreaker:
    """Production Domain & Source Circuit Breaker."""

    DEFAULT_MIN_SAMPLE: int = 15
    DEFAULT_REJECTION_THRESHOLD: float = 0.40

    def __init__(
        self,
        min_sample: int = DEFAULT_MIN_SAMPLE,
        rejection_threshold: float = DEFAULT_REJECTION_THRESHOLD,
        state_path: Optional[str] = None,
    ) -> None:
        self.min_sample = min_sample
        self.rejection_threshold = rejection_threshold
        self.state_path = state_path
        self._lock = threading.Lock()
        self._domains: Dict[str, DomainMetrics] = {}
        if self.state_path and os.path.exists(self.state_path):
            self._load_state()

    def record_run(
        self,
        domain_or_slug: str,
        processed_count: int,
        rejected_count: int,
        source_slug: Optional[str] = None,
    ) -> bool:
        """Record batch outcomes and evaluate circuit breaker condition.

        Returns True if circuit is TRIPPED (OPEN), False if healthy (CLOSED).
        """
        slug = (source_slug or domain_or_slug).lower()
        key = domain_or_slug.lower()

        with self._lock:
            metrics = self._domains.setdefault(
                key, DomainMetrics(domain_or_slug=key)
            )

            # If already tripped, maintain state
            if metrics.state == CircuitState.OPEN:
                return True

            metrics.total_processed += processed_count
            metrics.rejections += rejected_count
            metrics.last_updated_at = datetime.now(timezone.utc).isoformat()

            # Cold-start volume requirement: total_domain_processed >= min_sample (default 15)
            if metrics.total_processed >= self.min_sample:
                rate = metrics.rejection_rate
                if rate > self.rejection_threshold:
                    metrics.state = CircuitState.OPEN
                    metrics.tripped_at = datetime.now(timezone.utc).isoformat()
                    metrics.trip_reason = (
                        f"Rejection rate {rate:.1%} ({metrics.rejections}/{metrics.total_processed}) "
                        f"exceeded threshold {self.rejection_threshold:.1%} "
                        f"(sample size {metrics.total_processed} >= {self.min_sample})"
                    )

                    # Action 1: Record Error Log
                    logger.error(
                        "Circuit breaker TRIPPED for source '%s': %s. Disabling connector and halting domain scraping.",
                        slug,
                        metrics.trip_reason,
                    )

                    # Action 2: Disable in ConnectorRegistry
                    try:
                        ConnectorRegistry.disable(slug)
                    except Exception as exc:
                        logger.error(
                            "Failed to disable connector '%s' in ConnectorRegistry: %s",
                            slug,
                            exc,
                        )

                    # Action 3: Save state if configured
                    if self.state_path:
                        self._save_state()

                    return True

            if self.state_path:
                self._save_state()

            return False

    def record(
        self,
        domain_or_slug: str,
        processed_count: int,
        rejected_count: int,
        source_slug: Optional[str] = None,
    ) -> bool:
        """Alias for record_run."""
        return self.record_run(
            domain_or_slug=domain_or_slug,
            processed_count=processed_count,
            rejected_count=rejected_count,
            source_slug=source_slug,
        )

    def is_tripped(self, domain_or_slug: str) -> bool:
        """Check whether the circuit breaker is currently TRIPPED (OPEN)."""
        key = domain_or_slug.lower()
        with self._lock:
            metrics = self._domains.get(key)
            if not metrics:
                return False
            return metrics.state == CircuitState.OPEN

    def reset(self, domain_or_slug: str) -> None:
        """Manually reset the circuit breaker for a domain or source."""
        key = domain_or_slug.lower()
        with self._lock:
            if key in self._domains:
                self._domains[key] = DomainMetrics(domain_or_slug=key)
            if self.state_path:
                self._save_state()
        logger.info("Circuit breaker manually reset for '%s'.", key)

    def get_metrics(self, domain_or_slug: str) -> Optional[Dict[str, Any]]:
        """Return serialized metrics dict for a domain/source, or None."""
        key = domain_or_slug.lower()
        with self._lock:
            metrics = self._domains.get(key)
            return metrics.to_dict() if metrics else None

    def _save_state(self) -> None:
        if not self.state_path:
            return
        try:
            temp_path = f"{self.state_path}.tmp"
            data = {k: m.to_dict() for k, m in self._domains.items()}
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            os.replace(temp_path, self.state_path)
        except Exception as e:
            logger.warning("Failed to save circuit breaker state to %s: %s", self.state_path, e)

    def _load_state(self) -> None:
        if not self.state_path or not os.path.exists(self.state_path):
            return
        try:
            with open(self.state_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            for k, v in data.items():
                self._domains[k] = DomainMetrics.from_dict(v)
        except Exception as e:
            logger.warning("Failed to load circuit breaker state from %s: %s", self.state_path, e)


__all__ = ["DomainCircuitBreaker", "CircuitState", "DomainMetrics"]
