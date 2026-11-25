"""
KPI Audit
=========
Compute OpportunityHub production KPIs and compare them against Phase B
acceptance targets.

Targets
-------
1. Duplicate rate            < 2%    (duplicates_merged / total_ingested)
2. Expired shown             = 0     (active rows whose deadline/lifecycle has passed)
3. Fresh opportunities       > 80%   (freshness_score > 80, i.e. posted within ~14 days)
4. Verified apply links      > 99%   (is_verified = true)
5. Feed latency              < 100ms (feed publish/read round-trip)

The computation is a pure function of an injected snapshot, so it is fully
unit-testable. The Supabase-backed data collector lives in
``backend/scripts/audit_kpis.py``.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List


@dataclass(frozen=True)
class KpiAuditSnapshot:
    """Inputs required to evaluate all KPIs.

    Attributes:
        total_active: Active opportunities in the feed.
        expired_visible: Active opportunities that should actually be expired.
        fresh_opportunities: Active opportunities with freshness_score > 80.
        verified_apply_links: Active opportunities with is_verified = true.
        total_ingested: Opportunities ingested in the current audit window.
        duplicates_merged: Opportunities merged/deduplicated in the window.
        feed_latency_ms: Measured feed publish/read latency in milliseconds.
    """

    total_active: int
    expired_visible: int
    fresh_opportunities: int
    verified_apply_links: int
    total_ingested: int
    duplicates_merged: int
    feed_latency_ms: float


@dataclass
class KpiResult:
    """Outcome of a single KPI evaluation."""

    kpi: str
    value: float
    target: str
    passed: bool
    detail: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class KpiAuditReport:
    """Aggregated KPI audit results."""

    results: List[KpiResult] = field(default_factory=list)

    @property
    def passed_count(self) -> int:
        return sum(1 for r in self.results if r.passed)

    @property
    def total_count(self) -> int:
        return len(self.results)

    @property
    def all_passed(self) -> bool:
        return bool(self.results) and self.passed_count == self.total_count

    def to_dict(self) -> Dict[str, Any]:
        return {
            "results": [r.to_dict() for r in self.results],
            "passed_count": self.passed_count,
            "total_count": self.total_count,
            "all_passed": self.all_passed,
        }


class KpiAudit:
    """Evaluate the Phase B KPI targets against a snapshot."""

    # Target definitions (value + human-readable comparison string).
    TARGETS = {
        "duplicate_rate": (0.02, "duplicates < 2%"),
        "expired_shown": (0, "expired shown = 0"),
        "fresh_opportunities": (0.80, "fresh (>80%) within 14 days"),
        "verified_apply_links": (0.99, "verified apply links >99%"),
        "feed_latency_ms": (100.0, "feed latency <100 ms"),
    }

    def evaluate(self, snapshot: KpiAuditSnapshot) -> KpiAuditReport:
        results = [
            self._duplicate_rate(snapshot),
            self._expired_shown(snapshot),
            self._fresh_rate(snapshot),
            self._verified_rate(snapshot),
            self._feed_latency(snapshot),
        ]
        return KpiAuditReport(results=results)

    # ------------------------------------------------------------------
    # Individual KPIs
    # ------------------------------------------------------------------

    @staticmethod
    def _duplicate_rate(snapshot: KpiAuditSnapshot) -> KpiResult:
        threshold, label = KpiAudit.TARGETS["duplicate_rate"]
        rate = snapshot.duplicates_merged / snapshot.total_ingested if snapshot.total_ingested else 0.0
        detail = f"duplicates_merged={snapshot.duplicates_merged}, total_ingested={snapshot.total_ingested}"
        return KpiResult(
            kpi="duplicate_rate",
            value=round(rate, 4),
            target=label,
            passed=rate < threshold,
            detail=detail,
        )

    @staticmethod
    def _expired_shown(snapshot: KpiAuditSnapshot) -> KpiResult:
        threshold, label = KpiAudit.TARGETS["expired_shown"]
        detail = f"expired_visible={snapshot.expired_visible} of total_active={snapshot.total_active}"
        return KpiResult(
            kpi="expired_shown",
            value=float(snapshot.expired_visible),
            target=label,
            passed=snapshot.expired_visible == threshold,
            detail=detail,
        )

    @staticmethod
    def _fresh_rate(snapshot: KpiAuditSnapshot) -> KpiResult:
        threshold, label = KpiAudit.TARGETS["fresh_opportunities"]
        rate = snapshot.fresh_opportunities / snapshot.total_active if snapshot.total_active else 0.0
        detail = f"fresh={snapshot.fresh_opportunities}, total_active={snapshot.total_active}"
        return KpiResult(
            kpi="fresh_opportunities",
            value=round(rate, 4),
            target=label,
            passed=rate > threshold,
            detail=detail,
        )

    @staticmethod
    def _verified_rate(snapshot: KpiAuditSnapshot) -> KpiResult:
        threshold, label = KpiAudit.TARGETS["verified_apply_links"]
        rate = snapshot.verified_apply_links / snapshot.total_active if snapshot.total_active else 0.0
        detail = f"verified={snapshot.verified_apply_links}, total_active={snapshot.total_active}"
        return KpiResult(
            kpi="verified_apply_links",
            value=round(rate, 4),
            target=label,
            passed=rate > threshold,
            detail=detail,
        )

    @staticmethod
    def _feed_latency(snapshot: KpiAuditSnapshot) -> KpiResult:
        threshold, label = KpiAudit.TARGETS["feed_latency_ms"]
        detail = f"feed_latency_ms={snapshot.feed_latency_ms}"
        return KpiResult(
            kpi="feed_latency_ms",
            value=round(snapshot.feed_latency_ms, 2),
            target=label,
            passed=snapshot.feed_latency_ms < threshold,
            detail=detail,
        )


__all__ = [
    "KpiAudit",
    "KpiAuditSnapshot",
    "KpiAuditReport",
    "KpiResult",
]
