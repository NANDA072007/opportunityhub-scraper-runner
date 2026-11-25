"""
Opportunity Validator
====================
Validates OpportunityNormalized instances and classifies them into a richer
status taxonomy:

- VALID      -> Proceed through pipeline (store)
- FIXABLE    -> Repairable auto-fix, then store (validate -> repair -> validate)
- RETRYABLE  -> Transient error (debounced; V1.2: handled by the HTTP layer)
- QUARANTINE -> Needs manual review (structural/corrupt / unfixable)

Lifecycle State Engine:
- ACTIVE -> STALE -> CLOSING_SOON -> EXPIRED -> ARCHIVED
"""

from re import sub
from typing import List, Tuple, Optional
from datetime import datetime, timedelta
from backend.pipeline.models import OpportunityNormalized


class OpportunityValidator:
    """Production Validation, Repair & Lifecycle State Engine."""

    UNFIXABLE_CODES = ("VALID", "FIXABLE", "QUARANTINE", "RETRYABLE")

    @classmethod
    def validate(
        cls, opp: OpportunityNormalized
    ) -> Tuple[bool, str, Optional[str]]:
        """Validate structural completeness of a normalized opportunity.

        Returns:
            (is_valid, status_code, reason) where status_code is
            'VALID' | 'FIXABLE' | 'QUARANTINE' | 'RETRYABLE'.
        """
        reasons: list[str] = []

        # Unrepairable structural defects -> QUARANTINE (needs manual review).
        if not opp.title or len(opp.title.strip()) < 3:
            return False, 'QUARANTINE', "Title is missing or under 3 characters."
        if not opp.company or len(opp.company.strip()) < 2:
            return False, 'QUARANTINE', "Company name is missing or invalid."
        if not opp.description or len(opp.description.strip()) < 20:
            return False, 'QUARANTINE', "Description is missing or truncated (< 20 chars)."

        # Repairable -> FIXABLE (the repair() method fixes apply_url scheme).
        if not opp.apply_url:
            return False, 'FIXABLE', "Apply URL is missing (nothing to fix for a bare scheme)."
        if not opp.apply_url.startswith('http'):
            reasons.append("Apply URL lacks an http(s) scheme.")

        if reasons:
            return False, 'FIXABLE', "; ".join(reasons)
        return True, 'VALID', "Opportunity meets structural quality standards."

    @classmethod
    def repair(cls, opp: OpportunityNormalized) -> Tuple[Optional[OpportunityNormalized], list]:
        """Try to auto-repair a FIXABLE opportunity.

        Returns:
            (repaired, repairs) where ``repaired`` is None if no fix is possible
            and ``repairs`` lists what was changed.
        """
        changes: list = []
        fixes_attempted = False

        repaired = opp
        apply_url = opp.apply_url or ""
        if apply_url and not apply_url.startswith('http'):
            if "://" in apply_url:
                return None, []
            repaired = repaired.copy_with(apply_url=f"https://{apply_url}")
            changes.append(f"apply_url scheme -> https://{apply_url}")
            fixes_attempted = True

        # If the URL was empty, the record is not worth storing as-is.
        if not fixes_attempted and not opp.apply_url:
            return None, []

        return repaired, changes

    @staticmethod
    def update_lifecycle_state(opp: OpportunityNormalized) -> str:
        """Backward-compatible alias for :meth:`refresh_lifecycle_state`."""
        return OpportunityValidator.refresh_lifecycle_state(opp)

    @staticmethod
    def refresh_lifecycle_state(opp: OpportunityNormalized) -> str:
        """Bump the lifecycle state from the stored deadline/posted dates.

        Pure function: models are immutable, so this does NOT mutate the input.
        Callers must apply the returned state via ``opp.copy_with(...)``.
        """
        now = datetime.utcnow()
        if opp.deadline:
            if opp.deadline < now:
                return "EXPIRED"
            if opp.deadline <= (now + timedelta(days=3)):
                return "CLOSING_SOON"
            return "ACTIVE"
        if opp.posted_at and (now - opp.posted_at) > timedelta(days=30):
            return "STALE"
        return "ACTIVE"