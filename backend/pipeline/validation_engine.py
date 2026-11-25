"""
Validation Engine
=================
A rules-driven classifier that separates every opportunity into one of four
buckets, each with a distinct queue/fate:

BUCKET                 RULES (examples)                          FATE
-------------------    ------------------------------------    --------------------
VALID                  all mandatory fields present             store
FIXABLE                repairable (e.g. missing http scheme)   validate -> repair -> store
RETRYABLE              rate limit, timeout, transient 5xx      retry_queue (V1.2: retry later)
QUARANTINE             malformed, broken schema, missing       quarantine_queue (manual review)
                                                               mandatory field, invalid date

Rules are data (``RETRY_RULES`` / ``QUARANTINE_RULES``); adding a rule is a
one-line change. ``classify()`` evaluates every rule in priority order and
returns a single authoritative ``Classification`` so the pipeline stores the
item in exactly ONE queue.

This module composes ``OpportunityValidator`` for the core structural rules and
adds the transport/schema/date rules that the core validator does not cover.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from backend.pipeline.models import OpportunityNormalized

logger = logging.getLogger(__name__)

VALID = "VALID"
FIXABLE = "FIXABLE"
RETRYABLE = "RETRYABLE"
QUARANTINE = "QUARANTINE"

# Buckets ordered highest-priority first. An item matching a RETRYABLE and a
# QUARANTINE rule simultaneously is bucketed RETRYABLE (retry can resolve
# transient issues; quarantine is a permanent discard).
_BUCKET_PRIORITY: Tuple[str, ...] = (RETRYABLE, QUARANTINE, FIXABLE, VALID)


@dataclass(frozen=True)
class Rule:
    """A single validation rule: a bucket, a stable id, a predicate, a message."""

    bucket: str
    rule_id: str
    predicate: Callable[[OpportunityNormalized], bool]
    message: str


@dataclass
class Classification:
    """The outcome of evaluating the ruleset against one opportunity."""

    bucket: str = VALID
    reasons: List[str] = field(default_factory=list)
    matched_rule_ids: List[str] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return self.bucket == VALID

    @property
    def is_fixable(self) -> bool:
        return self.bucket == FIXABLE

    @property
    def is_retryable(self) -> bool:
        return self.bucket == RETRYABLE

    @property
    def is_quarantine(self) -> bool:
        return self.bucket == QUARANTINE

    def to_dict(self) -> Dict[str, Any]:
        return {
            "bucket": self.bucket,
            "reasons": list(self.reasons),
            "matched_rule_ids": list(self.matched_rule_ids),
        }


# ----------------------------------------------------------------------
# Rule predicate builders
# ----------------------------------------------------------------------

def _missing_field(attr: str, min_len: int = 1) -> Callable[[OpportunityNormalized], bool]:
    def _pred(opp: OpportunityNormalized) -> bool:
        value = getattr(opp, attr, None)
        return value is None or len(str(value).strip()) < min_len

    return _pred


def _payload_has_flag(*keys: str) -> Callable[[OpportunityNormalized], bool]:
    """Transient transport markers recorded into raw_payload by connectors."""

    def _pred(opp: OpportunityNormalized) -> bool:
        payload = opp.raw_payload or {}
        for key in keys:
            if key in payload and payload.get(key):
                return True
        flags = payload.get("flags") or payload.get("errors") or []
        if isinstance(flags, list) and any(str(f).lower() in keys for f in flags):
            return True
        return False

    return _pred


def _transient_http_status() -> Callable[[OpportunityNormalized], bool]:
    """Rate-limit / timeout / transient 5xx detected during the fetch."""

    def _pred(opp: OpportunityNormalized) -> bool:
        payload = opp.raw_payload or {}
        return payload.get("http_status") in (408, 429, 500, 502, 503, 504)

    return _pred


def _broken_schema() -> Callable[[OpportunityNormalized], bool]:
    def _pred(opp: OpportunityNormalized) -> bool:
        payload = opp.raw_payload or {}
        if payload.get("schema_error"):
            return True
        return bool(opp.raw_payload is None)

    return _pred


def _invalid_date() -> Callable[[OpportunityNormalized], bool]:
    """Deadline set but outside a sane relative window (broken/unstable date)."""

    def _pred(opp: OpportunityNormalized) -> bool:
        if opp.deadline is None:
            return False
        fine = datetime.now().year
        return not (fine - 1 <= opp.deadline.year <= fine + 5)

    return _pred


# ----------------------------------------------------------------------
# Ruleset
# ----------------------------------------------------------------------

RETRY_RULES: Tuple[Rule, ...] = (
    Rule(RETRYABLE, "retry.http_status",
         _transient_http_status(), "Transient HTTP status (rate limit / 5xx)."),
    Rule(RETRYABLE, "retry.transient_flag",
_payload_has_flag("rate_limited", "timeout", "connection_error",
                           "transient_error", "retry_after"),
         "Transient failure marker recorded by the connector."),
)

def _invalid_missing_apply_url() -> Callable[[OpportunityNormalized], bool]:
    def _pred(opp: OpportunityNormalized) -> bool:
        url = (opp.apply_url or "").strip()
        return not url

    return _pred


def _invalid_missing_description() -> Callable[[OpportunityNormalized], bool]:
    def _pred(opp: OpportunityNormalized) -> bool:
        return not opp.description or len(str(opp.description).strip()) < 20

    return _pred


def _invalid_apply_url_scheme() -> Callable[[OpportunityNormalized], bool]:
    def _pred(opp: OpportunityNormalized) -> bool:
        url = (opp.apply_url or "").strip()
        return bool(url) and not url.startswith("http") and "://" not in url

    return _pred


# ----------------------------------------------------------------------
# Ruleset
# ----------------------------------------------------------------------

RETRY_RULES: Tuple[Rule, ...] = (
    Rule(RETRYABLE, "retry.http_status",
         _transient_http_status(), "Transient HTTP status (rate limit / 5xx)."),
    Rule(RETRYABLE, "retry.transient_flag",
         _payload_has_flag("rate_limited", "timeout", "connection_error",
                           "transient_error", "retry_after"),
         "Transient failure marker recorded by the connector."),
)

QUARANTINE_RULES: Tuple[Rule, ...] = (
    Rule(QUARANTINE, "q.broken_schema",
         _broken_schema(), "Broken schema / malformed payload."),
    Rule(QUARANTINE, "q.missing_title",
         _missing_field("title", 3), "Missing or under-3-char title."),
    Rule(QUARANTINE, "q.missing_company",
         _missing_field("company", 2), "Missing or invalid company."),
    Rule(QUARANTINE, "q.missing_apply_url",
         _invalid_missing_apply_url(), "Missing apply URL."),
    Rule(QUARANTINE, "q.missing_description",
         _invalid_missing_description(), "Missing or truncated description."),
    Rule(QUARANTINE, "q.invalid_date",
         _invalid_date(), "Deadline outside a sane date window."),
)

FIXABLE_RULES: Tuple[Rule, ...] = (
    Rule(FIXABLE, "fix.apply_url_scheme",
         _invalid_apply_url_scheme(), "Apply URL lacks an http(s) scheme."),
)


class ValidationEngine:
    """Evaluates the ruleset and classifies an opportunity into ONE bucket."""

    def __init__(
        self,
        retry_rules: Sequence[Rule] = RETRY_RULES,
        quarantine_rules: Sequence[Rule] = QUARANTINE_RULES,
        fixable_rules: Sequence[Rule] = FIXABLE_RULES,
    ) -> None:
        self._retry_rules = list(retry_rules)
        self._quarantine_rules = list(quarantine_rules)
        self._fixable_rules = list(fixable_rules)

    def classify(self, opp: OpportunityNormalized) -> Classification:
        """Return the single highest-priority classification for ``opp``."""
        matched: List[Tuple[str, str, str]] = []  # (bucket, rule_id, message)

        for rule in [*self._retry_rules, *self._quarantine_rules]:
            try:
                hit = bool(rule.predicate(opp))
            except Exception as exc:  # noqa: BLE001
                logger.debug("Rule '%s' errored: %s", rule.rule_id, exc)
                hit = False
            if hit:
                matched.append((rule.bucket, rule.rule_id, rule.message))

        severe_hit = any(b == RETRYABLE or b == QUARANTINE for b, _, _ in matched)

        # Fixable rules only apply when nothing more severe matched.
        if not severe_hit:
            for rule in self._fixable_rules:
                try:
                    hit = bool(rule.predicate(opp))
                except Exception as exc:  # noqa: BLE001
                    hit = False
                if hit:
                    matched.append((rule.bucket, rule.rule_id, rule.message))

        if not matched:
            return Classification()

        # Highest priority bucket = lowest index in _BUCKET_PRIORITY.
        bucket = min((b for b, _, _ in matched), key=_BUCKET_PRIORITY.index)
        bucket_rank = _BUCKET_PRIORITY.index(bucket)

        reasons: List[str] = []
        rule_ids: List[str] = []
        for b, rule_id, message in matched:
            if _BUCKET_PRIORITY.index(b) <= bucket_rank:
                reasons.append(message)
                rule_ids.append(rule_id)

        return Classification(bucket=bucket, reasons=reasons, matched_rule_ids=rule_ids)


__all__ = [
    "ValidationEngine", "Classification", "Rule",
    "VALID", "FIXABLE", "RETRYABLE", "QUARANTINE",
    "RETRY_RULES", "QUARANTINE_RULES", "FIXABLE_RULES",
]