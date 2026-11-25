"""
Concrete Pipeline Stages
========================
Default production implementations of the canonical pipeline stages defined
in ``pipeline_stage_registry.py``.

Each stage owns a single processing concern and delegates to the shared
pipeline components (``EntityNormalizer``, ``OpportunityValidator``,
``OpportunityDeduplicator``, ``QualityEvaluator``) plus persistence callables
injected by ``PipelineOrchestrator`` at construction time. Stages never touch
the database directly; all persistence goes through the injected callables so
the stages remain unit-testable and independent of Supabase.

Stages communicate exclusively through the shared ``context`` dict:

- ``fetch``       -> ``context["raw_items"]``, ``context["fetch_latency_ms"]``,
                     ``context["fetch_error"]`` (set only on failure)
- ``normalize``   -> ``context["normalized_list"]``, ``context["unknown_entities_batch"]``
- ``validate``    -> ``context["valid_items"]``, ``context["quarantined_count"]``,
                     ``context["retried_count"]``
- ``deduplicate`` -> ``context["unique_items"]``, ``context["duplicates_removed"]``
- ``quality``     -> ``context["scored_items"]``
- ``store``       -> ``context["stored_count"]``, ``context["new_count"]``
- ``publish``     -> ``context["new_feed_version"]``
"""

from typing import Any, Callable, Dict, List, Optional, Tuple

import time

from backend.pipeline.models import OpportunityRaw, OpportunityNormalized
from backend.pipeline.entity_normalizer import EntityNormalizer
from backend.pipeline.opportunity_validator import OpportunityValidator
from backend.pipeline.validation_engine import (
    ValidationEngine,
    Classification,
    VALID,
    FIXABLE,
    RETRYABLE,
    QUARANTINE,
)
from backend.pipeline.opportunity_deduplicator import OpportunityDeduplicator
from backend.pipeline.quality_evaluator import QualityEvaluator
from backend.pipeline.security.url_validator import is_trusted_ats_url, is_trusted_govt_url
from backend.pipeline.pipeline_stage_registry import (
    PipelineStage,
    PipelineStageRegistry,
)


class FetchStage(PipelineStage):
    """Stage 1: FETCH raw items from the source connector.

    The connector is never imported here; fetching goes through the injected
    ``fetch_fn`` callable so this stage stays unit-testable and independent of
    ``ConnectorRegistry`` / Supabase. Failures are captured into
    ``context["fetch_error"]`` and short-circuit later stages via the
    orchestrator.
    """

    name = "fetch"
    order = 50

    def __init__(
        self,
        fetch_fn: Optional[
            Callable[[str, Dict[str, Any], Optional[str]], List[OpportunityRaw]]
        ] = None,
    ) -> None:
        self._fetch_fn = fetch_fn

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        source_slug: str = context.get("source_slug", "")
        source_config: Dict[str, Any] = context.get("source_config") or {}
        query: Optional[str] = context.get("query")

        start_time = time.monotonic()
        try:
            if self._fetch_fn:
                raw_items = self._fetch_fn(source_slug, source_config, query)
            else:
                raw_items = []
            context["raw_items"] = raw_items
            context["fetch_latency_ms"] = (time.monotonic() - start_time) * 1000.0
            context.pop("fetch_error", None)
        except Exception as e:  # noqa: BLE001
            context["fetch_latency_ms"] = (time.monotonic() - start_time) * 1000.0
            context["raw_items"] = []
            context["fetch_error"] = str(e)
        return context


class NormalizeStage(PipelineStage):
    """Stage 2: NORMALIZE raw items into clean ``OpportunityNormalized`` objects.

    Per-item isolation: a malformed record is quarantined and skipped instead of
    aborting the whole batch, so one bad item never loses the other 99.
    """

    name = "normalize"
    order = 100

    def __init__(
        self,
        normalizer: Optional[EntityNormalizer] = None,
        save_unknown_fn: Optional[Callable[[List[Dict[str, Any]]], None]] = None,
        quarantine_fn: Optional[
            Callable[[OpportunityRaw, str], None]
        ] = None,
    ) -> None:
        self._normalizer = normalizer or EntityNormalizer()
        self._save_unknown_fn = save_unknown_fn
        self._quarantine_fn = quarantine_fn

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        raw_items: List[OpportunityRaw] = context.get("raw_items") or []
        source_reputation: int = context.get("source_reputation", 85)
        normalized_list: List[OpportunityNormalized] = []
        unknown_entities_batch: List[Dict[str, Any]] = []
        malformed_count = 0

        for raw in raw_items:
            try:
                norm_item, unknowns = self._normalizer.normalize(
                    raw, source_reputation=source_reputation
                )
            except Exception as exc:  # noqa: BLE001
                malformed_count += 1
                if self._quarantine_fn:
                    self._quarantine_fn(raw, f"Malformed payload: {exc}")
                continue
            normalized_list.append(norm_item)
            unknown_entities_batch.extend(unknowns)

        context["normalized_list"] = normalized_list
        context["unknown_entities_batch"] = unknown_entities_batch
        context["malformed_count"] = malformed_count
        if self._save_unknown_fn:
            self._save_unknown_fn(unknown_entities_batch)
        return context


class ValidateStage(PipelineStage):
    """Stage 3: VALIDATE items into valid / quarantine / retry buckets."""

    name = "validate"
    order = 200

    def __init__(
        self,
        validator: Optional[OpportunityValidator] = None,
        engine: Optional[ValidationEngine] = None,
        quarantine_fn: Optional[
            Callable[[OpportunityNormalized, str], None]
        ] = None,
        retry_fn: Optional[Callable[[OpportunityNormalized, str], None]] = None,
    ) -> None:
        self._validator = validator or OpportunityValidator()
        self._engine = engine or ValidationEngine()
        self._quarantine_fn = quarantine_fn
        self._retry_fn = retry_fn

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        normalized_list: List[OpportunityNormalized] = (
            context.get("normalized_list") or []
        )
        valid_items: List[OpportunityNormalized] = []
        quarantined_count = 0
        retried_count = 0
        repaired_count = 0

        for norm in normalized_list:
            # Authoritative bucket: one of VALID / FIXABLE / RETRYABLE / QUARANTINE.
            cls = self._engine.classify(norm)
            bucket = cls.bucket

            # FIXABLE -> auto-repair -> re-validate -> store (or quarantine).
            if bucket == FIXABLE:
                repaired, changes = self._validator.repair(norm)
                if repaired is not None:
                    is_valid2, status_code2, reason2 = self._validator.validate(repaired)
                    if is_valid2:
                        norm = repaired
                        norm = norm.copy_with(
                            extraction_confidence={
                                **norm.extraction_confidence,
                                'repairs': changes,
                            }
                        )
                        repaired_count += 1
                        lifecycle_state = self._validator.update_lifecycle_state(norm)
                        valid_items.append(norm.copy_with(lifecycle_state=lifecycle_state))
                        continue
                # Repair failed or re-validation still failed.
                bucket = QUARANTINE
                cls = Classification(bucket=QUARANTINE, reasons=["repair-failed"])

            if bucket == VALID:
                lifecycle_state = self._validator.update_lifecycle_state(norm)
                valid_items.append(norm.copy_with(lifecycle_state=lifecycle_state))
            elif bucket == QUARANTINE:
                if self._quarantine_fn:
                    self._quarantine_fn(norm, ", ".join(cls.reasons) or cls.bucket)
                quarantined_count += 1
            elif bucket == RETRYABLE:
                if self._retry_fn:
                    self._retry_fn(norm, ", ".join(cls.reasons) or cls.bucket)
                retried_count += 1

        context["valid_items"] = valid_items
        context["quarantined_count"] = quarantined_count
        context["retried_count"] = retried_count
        context["repaired_count"] = repaired_count
        return context


class DeduplicateStage(PipelineStage):
    """Stage 4: DEDUPLICATE valid items via two-stage fingerprint + fuzzy match."""

    name = "deduplicate"
    order = 300

    def __init__(
        self, deduplicator: Optional[OpportunityDeduplicator] = None
    ) -> None:
        self._deduplicator = deduplicator or OpportunityDeduplicator()

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        valid_items: List[OpportunityNormalized] = (
            context.get("valid_items") or []
        )
        unique_items, duplicates_removed = self._deduplicator.deduplicate_batch(
            valid_items
        )
        context["unique_items"] = unique_items
        context["duplicates_removed"] = duplicates_removed
        return context


class QualityStage(PipelineStage):
    """Stage 5: SCORE unique items with the explainable quality evaluator."""

    name = "quality"
    order = 400

    def __init__(self, evaluator: Optional[QualityEvaluator] = None) -> None:
        self._evaluator = evaluator or QualityEvaluator()

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        unique_items: List[OpportunityNormalized] = (
            context.get("unique_items") or []
        )
        scored_items = [self._evaluator.evaluate(item) for item in unique_items]
        context["scored_items"] = scored_items
        return context


class AutoReviewStage(PipelineStage):
    """Stage 5.5: AUTO-REVIEW scored items using Tier1/Tier2 thresholds.

    - Tier 1 (quality_score >= 80): Auto-Approved ('approved')
    - Tier 2 (quality_score >= 60): Pending Manual Review ('pending')
    - Below Tier 2 (quality_score < 60): Auto-Quarantined ('quarantined')

    Outputs summary: "Auto-Approved: X | Auto-Quarantined: Y | Manual Review: Z"
    """

    name = "auto_review"
    order = 500

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        scored_items: List[OpportunityNormalized] = (
            context.get("scored_items") or []
        )
        auto_approved = 0
        auto_quarantined = 0
        manual_review = 0
        reviewed_items: List[OpportunityNormalized] = []

        for item in scored_items:
            score = item.quality_score
            trust = item.trust_score if item.trust_score is not None else 85
            fraud = float(getattr(item, "fraud_score", 0.0) or 0.0)
            source_rep = item.source_reputation if item.source_reputation is not None else 50

            # Hard security gate: items with trust < 40 (scams) or fraud >= 0.50 are quarantined
            if trust < 40 or fraud >= 0.50:
                review_status = "quarantined"
                auto_quarantined += 1
            # Verified corporate ATS domain direct approval (anti-spoofed)
            elif is_trusted_ats_url(item.apply_url) and fraud <= 0.05 and trust >= 70:
                review_status = "approved"
                auto_approved += 1
            # Verified official government research domain direct approval
            elif (
                item.source_slug == "govt_research"
                and is_trusted_govt_url(item.apply_url)
                and fraud == 0.0
                and bool(item.is_verified)
            ):
                review_status = "approved"
                auto_approved += 1
            # Standard high-quality auto-approval
            elif score >= 80 and trust >= 75 and fraud <= 0.05:
                review_status = "approved"
                auto_approved += 1
            # Medium-quality listings (60 <= score <= 79) -> pending manual review
            elif score >= 60:
                review_status = "pending"
                manual_review += 1
            # Reputable sources with clean fraud/trust but lower detail (e.g. undisclosed salary on Naukri) -> pending
            elif trust >= 75 and fraud <= 0.05 and source_rep >= 70:
                review_status = "pending"
                manual_review += 1
            # Truly low-quality listings with low reputation / insecure links (< 60) -> quarantined
            else:
                review_status = "quarantined"
                auto_quarantined += 1

            updated_confidence = dict(item.extraction_confidence or {})
            # The DB trigger is the sole review authority. Python records only
            # a recommendation; writing review_status here would let Guard 2
            # preserve a stale Python decision over the DB classification.
            updated_confidence["review_recommendation"] = review_status

            reviewed_item = item.copy_with(
                extraction_confidence=updated_confidence
            )
            reviewed_items.append(reviewed_item)

        context["scored_items"] = reviewed_items
        context["auto_approved"] = auto_approved
        context["auto_quarantined"] = auto_quarantined
        context["manual_review"] = manual_review

        summary_msg = f"Auto-Approved: {auto_approved} | Auto-Quarantined: {auto_quarantined} | Manual Review: {manual_review}"
        import logging
        logging.getLogger("AutoReviewStage").info(summary_msg)
        print(summary_msg)

        return context


class StoreStage(PipelineStage):
    """Stage 6: STORE scored items via the injected persistence callable."""

    name = "store"
    order = 600

    def __init__(
        self,
        store_fn: Optional[
            Callable[[List[OpportunityNormalized]], Tuple[int, int]]
        ] = None,
    ) -> None:
        self._store_fn = store_fn

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        scored_items: List[OpportunityNormalized] = (
            context.get("scored_items") or []
        )
        if self._store_fn:
            stored_count, new_count = self._store_fn(scored_items)
        else:
            stored_count, new_count = len(scored_items), len(scored_items)
        context["stored_count"] = stored_count
        context["new_count"] = new_count
        return context


class PublishStage(PipelineStage):
    """Stage 7: PUBLISH feed version, generate delta updates, and invalidate cache."""

    name = "publish"
    order = 700

    def __init__(
        self, publish_fn: Optional[Callable[[], int]] = None
    ) -> None:
        self._publish_fn = publish_fn

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        if self._publish_fn:
            context["new_feed_version"] = self._publish_fn()
        else:
            from backend.pipeline.feed_publisher import FeedPublisher
            publisher = FeedPublisher()
            changed_items = context.get("unique_items") or context.get("valid_items") or []
            archived_ids = context.get("archived_ids") or []
            new_version, delta_summary = publisher.publish_delta(
                changed_items=changed_items,
                archived_ids=archived_ids
            )
            context["new_feed_version"] = new_version
            context["delta_summary"] = delta_summary
        return context


def register_concrete_stages(
    *,
    fetch_fn: Optional[
        Callable[[str, Dict[str, Any], Optional[str]], List[OpportunityRaw]]
    ] = None,
    normalizer: Optional[EntityNormalizer] = None,
    validator: Optional[OpportunityValidator] = None,
    deduplicator: Optional[OpportunityDeduplicator] = None,
    evaluator: Optional[QualityEvaluator] = None,
    save_unknown_fn: Optional[Callable[[List[Dict[str, Any]]], None]] = None,
    malformed_quarantine_fn: Optional[
        Callable[[OpportunityRaw, str], None]
    ] = None,
    quarantine_fn: Optional[Callable[[OpportunityNormalized, str], None]] = None,
    retry_fn: Optional[Callable[[OpportunityNormalized, str], None]] = None,
    store_fn: Optional[
        Callable[[List[OpportunityNormalized]], Tuple[int, int]]
    ] = None,
    publish_fn: Optional[Callable[[], int]] = None,
) -> List[str]:
    """Register the default concrete stages into the ``PipelineStageRegistry``.

    Replaces the no-op placeholders (if present) with the production stage
    implementations wired to the supplied collaborators.

    Returns:
        List of registered stage names.
    """
    stages: List[PipelineStage] = [
        FetchStage(fetch_fn),
        NormalizeStage(normalizer, save_unknown_fn, malformed_quarantine_fn),
        ValidateStage(validator=validator, quarantine_fn=quarantine_fn, retry_fn=retry_fn),
        DeduplicateStage(deduplicator),
        QualityStage(evaluator),
        AutoReviewStage(),
        StoreStage(store_fn),
        PublishStage(publish_fn),
    ]
    return [PipelineStageRegistry.register(stage) for stage in stages]


__all__ = [
    "FetchStage",
    "NormalizeStage",
    "ValidateStage",
    "DeduplicateStage",
    "QualityStage",
    "AutoReviewStage",
    "StoreStage",
    "PublishStage",
    "register_concrete_stages",
]
