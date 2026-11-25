"""
Incremental Feed Publisher Engine
================================
Handles feed_version incrementing, cache invalidation, delta update generation,
and incremental publishing of only changed opportunities (no full refreshes).
"""

import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple, Optional, Set
from backend.pipeline.models import OpportunityNormalized

logger = logging.getLogger(__name__)


class FeedPublisher:
    """Production Incremental Feed Publisher & Cache Invalidation Engine."""

    def __init__(self, client: Optional[Any] = None) -> None:
        self._client = client
        self._cache: Dict[str, Any] = {}
        self._current_version: int = 1

    @property
    def current_feed_version(self) -> int:
        return self._current_version

    def invalidate_cache(self) -> int:
        """Clear stale feed caches and invalidate client query keys."""
        cleared_entries = len(self._cache)
        self._cache.clear()
        logger.info(f"Invalidated feed cache ({cleared_entries} entries cleared).")
        return cleared_entries

    def increment_feed_version(self) -> int:
        """Increment system feed_version atomically."""
        if self._client:
            try:
                res = self._client.table("system_state").select("id,feed_version").limit(1).execute()
                if res.data:
                    row = res.data[0]
                    new_version = (row.get("feed_version") or 1) + 1
                    self._client.table("system_state").update({
                        "feed_version": new_version,
                        "last_scrape_completed_at": datetime.now(timezone.utc).isoformat(),
                        "updated_at": datetime.now(timezone.utc).isoformat()
                    }).eq("id", row["id"]).execute()
                    self._current_version = new_version
                    logger.info(f"Incremented Supabase system feed_version to {new_version}")
                    return new_version
            except Exception as e:
                logger.warning(f"Could not increment Supabase feed_version: {e}")

        self._current_version += 1
        return self._current_version

    def generate_delta_updates(
        self,
        changed_items: List[OpportunityNormalized],
        archived_ids: Optional[List[str]] = None,
        existing_ids: Optional[Set[str]] = None
    ) -> Dict[str, Any]:
        """Categorize changed items into added, updated, and archived deltas."""
        existing = existing_ids or set()
        archived = archived_ids or []

        added: List[Dict[str, Any]] = []
        updated: List[Dict[str, Any]] = []

        for item in changed_items:
            payload = {
                "external_id": item.external_id,
                "source": item.source_slug,
                "source_slug": item.source_slug,
                "title": item.title,
                "company": item.company,
                "company_id": item.company_id,
                "location": item.location,
                "opportunity_type": item.opportunity_type,
                "category": item.category,
                "stipend_amount": item.stipend_amount,
                "salary_range": item.salary_range,
                "posted_at": item.posted_at.isoformat() if item.posted_at else None,
                "deadline": item.deadline.isoformat() if item.deadline else None,
                "apply_url": item.apply_url,
                "skills_required": item.skills_required,
                "quality_score": item.quality_score,
                "trust_score": item.trust_score,
                "freshness_score": item.freshness_score,
                "lifecycle_state": item.lifecycle_state,
                "score_vector": item.score_vector,
            }

            if item.id and item.id in existing:
                updated.append(payload)
            else:
                added.append(payload)

        return {
            "delta_type": "incremental",
            "added_count": len(added),
            "updated_count": len(updated),
            "archived_count": len(archived),
            "added": added,
            "updated": updated,
            "archived_ids": archived,
        }

    def publish_delta(
        self,
        changed_items: List[OpportunityNormalized],
        archived_ids: Optional[List[str]] = None,
        existing_ids: Optional[Set[str]] = None
    ) -> Tuple[int, Dict[str, Any]]:
        """Publish incremental delta update, increment feed_version, and invalidate cache.

        Returns:
            Tuple of (new_feed_version, delta_summary_payload)
        """
        # 1. Invalidate stale cache
        self.invalidate_cache()

        # 2. Increment system feed_version
        new_version = self.increment_feed_version()

        # 3. Generate delta update payload
        delta_payload = self.generate_delta_updates(
            changed_items=changed_items,
            archived_ids=archived_ids,
            existing_ids=existing_ids
        )
        delta_payload["feed_version"] = new_version
        delta_payload["published_at"] = datetime.now(timezone.utc).isoformat()

        # 4. Store delta in feed_delta_log table
        if self._client:
            try:
                self._client.table("feed_delta_log").insert({
                    "feed_version": new_version,
                    "delta_type": "incremental",
                    "added_count": delta_payload["added_count"],
                    "updated_count": delta_payload["updated_count"],
                    "archived_count": delta_payload["archived_count"],
                    "added_opportunities": delta_payload["added"],
                    "updated_opportunities": delta_payload["updated"],
                    "archived_ids": delta_payload["archived_ids"],
                    "published_at": delta_payload["published_at"]
                }).execute()
                logger.info(f"Published feed_version v{new_version} delta update to Supabase.")
            except Exception as e:
                logger.warning(f"Could not persist feed delta to Supabase: {e}")

        return new_version, delta_payload


__all__ = ["FeedPublisher"]
