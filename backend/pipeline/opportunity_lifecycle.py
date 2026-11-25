"""
Opportunity Lifecycle Engine
===========================
Manages state transitions, dynamic age/deadline evaluations, zero-hard-delete rules,
and state recovery mechanisms for normalized opportunities.

State Graph:
- ACTIVE: Freshly posted opportunity receiving traffic.
- STALE: No recent scraper updates (>30 days old) but deadline not passed.
- CLOSING_SOON: Deadline approaching within 3 days.
- EXPIRED: Deadline passed or source posting taken down.
- ARCHIVED: Inactive for extended period (>90 days) or archived manually. (Soft delete substitute).

Recovery:
- Any non-active state (STALE, CLOSING_SOON, EXPIRED, ARCHIVED) can transition back to ACTIVE
  when a fresh scraper run or source verification detects a valid, active job listing.
"""

from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Tuple, Optional, Set
from backend.pipeline.models import OpportunityNormalized, LIFECYCLE_STATES


class OpportunityLifecycleEngine:
    """Production Opportunity Lifecycle Engine enforcing no-hard-deletes and recovery."""

    VALID_STATES = frozenset(LIFECYCLE_STATES)

    # Allowed state transition graph
    TRANSITION_GRAPH: Dict[str, Set[str]] = {
        "ACTIVE": {"STALE", "CLOSING_SOON", "EXPIRED", "ARCHIVED"},
        "STALE": {"ACTIVE", "CLOSING_SOON", "EXPIRED", "ARCHIVED"},
        "CLOSING_SOON": {"ACTIVE", "EXPIRED", "ARCHIVED"},
        "EXPIRED": {"ACTIVE", "ARCHIVED"},
        "ARCHIVED": {"ACTIVE"},
    }

    @classmethod
    def evaluate_state(
        cls,
        opp: OpportunityNormalized,
        reference_time: Optional[datetime] = None
    ) -> str:
        """Evaluate appropriate lifecycle state based on deadline and age."""
        now = reference_time or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)

        # Respect explicit ARCHIVED state unless recovery is explicitly called
        if opp.lifecycle_state == "ARCHIVED":
            return "ARCHIVED"

        # 1. Deadline Check
        if opp.deadline is not None:
            deadline = opp.deadline
            if deadline.tzinfo is None:
                deadline = deadline.replace(tzinfo=timezone.utc)

            if now > deadline:
                return "EXPIRED"
            elif (deadline - now) <= timedelta(days=3):
                return "CLOSING_SOON"

        # 2. Age / Freshness Check
        posted = opp.posted_at or now
        if posted.tzinfo is None:
            posted = posted.replace(tzinfo=timezone.utc)

        age_days = (now - posted).days

        if age_days > 90:
            return "ARCHIVED"
        elif age_days > 30:
            return "STALE"

        return "ACTIVE"

    @classmethod
    def transition(
        cls,
        opp: OpportunityNormalized,
        target_state: str,
        reason: str = "Automatic rule evaluation",
        reference_time: Optional[datetime] = None
    ) -> OpportunityNormalized:
        """Transition opportunity to target_state with validation and transition history logging."""
        target = target_state.upper()
        if target not in cls.VALID_STATES:
            raise ValueError(f"Invalid target lifecycle state '{target}'. Must be one of {cls.VALID_STATES}")

        current = opp.lifecycle_state or "ACTIVE"

        if current == target:
            return opp

        allowed_targets = cls.TRANSITION_GRAPH.get(current, set())
        if target not in allowed_targets:
            raise ValueError(f"Invalid state transition from '{current}' to '{target}'.")

        now = reference_time or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)

        # Log transition entry
        entry = {
            "from_state": current,
            "to_state": target,
            "reason": reason,
            "timestamp": now.isoformat(),
        }

        prev_history = list((opp.score_vector or {}).get("lifecycle_history", []))
        prev_history.append(entry)

        updated_score_vector = {
            **(opp.score_vector or {}),
            "lifecycle_history": prev_history,
            "last_transition_reason": reason,
        }

        return opp.copy_with(
            lifecycle_state=target,
            score_vector=updated_score_vector,
        )

    @classmethod
    def process_auto_transitions(
        cls,
        opp: OpportunityNormalized,
        reference_time: Optional[datetime] = None
    ) -> OpportunityNormalized:
        """Automatically evaluate and transition opportunity to its computed state."""
        computed_state = cls.evaluate_state(opp, reference_time=reference_time)
        if computed_state != opp.lifecycle_state:
            return cls.transition(
                opp,
                target_state=computed_state,
                reason=f"Automatic transition to {computed_state}",
                reference_time=reference_time,
            )
        return opp

    @classmethod
    def recover(
        cls,
        opp: OpportunityNormalized,
        reason: str = "Source re-scraped fresh / reposted",
        reference_time: Optional[datetime] = None
    ) -> OpportunityNormalized:
        """Recover an inactive, stale, expired, or archived opportunity back to ACTIVE."""
        return cls.transition(
            opp,
            target_state="ACTIVE",
            reason=f"Recovery: {reason}",
            reference_time=reference_time,
        )

    @classmethod
    def soft_delete(
        cls,
        opp: OpportunityNormalized,
        reason: str = "Soft deleted by administrator",
        reference_time: Optional[datetime] = None
    ) -> OpportunityNormalized:
        """Perform soft delete by transitioning state to ARCHIVED (never hard delete)."""
        return cls.transition(
            opp,
            target_state="ARCHIVED",
            reason=f"Soft Delete: {reason}",
            reference_time=reference_time,
        )


__all__ = ["OpportunityLifecycleEngine"]
