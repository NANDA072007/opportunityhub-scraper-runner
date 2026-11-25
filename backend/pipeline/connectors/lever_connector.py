"""
Lever ATS Connector
===================
Fetches corporate career opportunities from Lever ATS public posting
endpoints. Returns generic OpportunityRaw objects with ZERO branch/domain
hardcoding.

Board tokens are read from the source ``config`` (``{"boards": [...]}``) or
fall back to a curated default list of high-value public Lever boards.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx

from backend.pipeline.models import OpportunityRaw
from backend.pipeline.connectors.base_connector import BaseConnector, ConnectorFetchError
from backend.pipeline.connectors.capabilities import Capability
from backend.pipeline.connectors._http import ConnectorHttpClient, parse_guarded
from backend.pipeline.connectors._pay import mine_pay_text, mine_structured_pay

logger = logging.getLogger(__name__)


class LeverConnector(BaseConnector):
    """Lever ATS connector for corporate career pages."""

    priority = 900
    version = "1.0.0"
    owner = "platform"
    capabilities = {Capability.JOB, Capability.REMOTE, Capability.HYBRID, Capability.ONSITE}
    supports_full_sync = True
    supports_incremental_sync = False
    estimated_rate_limit = 2.0

    # Verified live on 2026-10-26. The previous default list included
    # Dropbox/Mozilla/Turo/Reddit/Eventbrite, all of which now return 404
    # (they moved off Lever), producing five guaranteed failures per run.
    # Add a board only after confirming its public endpoint returns postings.
    DEFAULT_BOARDS = [
        {"name": "Spotify", "token": "spotify"},
    ]

    def __init__(
        self,
        *,
        client: Optional[ConnectorHttpClient] = None,
        http_client: Optional[httpx.Client] = None,
    ) -> None:
        if client is not None:
            self._http = client
        else:
            self._http = ConnectorHttpClient(client=http_client, rate_limit_per_sec=2.0)

    @property
    def source_slug(self) -> str:
        return "lever"

    @property
    def display_name(self) -> str:
        return "Lever ATS"

    @property
    def default_source_reputation(self) -> int:
        return 100

    def fetch_raw_opportunities(
        self, config: Dict[str, Any], query: Optional[str] = None
    ) -> List[OpportunityRaw]:
        boards = config.get("boards") or self.DEFAULT_BOARDS
        results: List[OpportunityRaw] = []
        successful_boards = 0
        failed_boards = 0

        for board in boards:
            token = board["token"]
            try:
                items = self._fetch_board(board.get("name", token), token)
            except Exception as exc:
                logger.warning("Lever board '%s' failed: %s", token, exc)
                failed_boards += 1
                continue
            successful_boards += 1
            results.extend(items)

        if failed_boards and not successful_boards:
            raise ConnectorFetchError(
                f"Lever: all {failed_boards} configured boards failed "
                "(API blocked or unreachable)."
            )

        logger.info("LeverConnector fetched %d total items.", len(results))
        return results

    def _fetch_board(self, company_name: str, token: str) -> List[OpportunityRaw]:
        url = f"https://api.lever.co/v0/postings/{token}"
        params = {"mode": "json"}

        data = self._http.get_json(url, params=params)

        if not isinstance(data, list):
            return []

        items: List[OpportunityRaw] = []
        for posting in data:
            parsed = parse_guarded(
                lambda p: self._parse_posting(company_name, token, p),
                posting,
                context="lever",
            )
            if parsed is not None:
                items.append(parsed)
        return items

    @staticmethod
    def _parse_posting(company_name: str, token: str, posting: Dict[str, Any]) -> OpportunityRaw:
        posting_id = str(posting.get("id") or "")
        if not posting_id:
            return None
        title = (posting.get("text") or "").strip()
        if not title:
            return None

        apply_url = posting.get("hostedUrl") or f"https://jobs.lever.co/{token}/{posting_id}"
        categories = posting.get("categories") or {}
        location = categories.get("location", "Remote") if isinstance(categories, dict) else "Remote"
        department = categories.get("team") or categories.get("department") if isinstance(categories, dict) else None
        description = posting.get("descriptionPlain") or f"Career opportunity at {company_name} for {title}."

        # Private-pay honesty: Lever posts no dedicated salary field, but
        # many descriptions publish a compensation section. Mine it with a
        # pay-keyword anchor (bare figures never match); "Competitive
        # Package" only when nothing is published.
        raw_salary = mine_pay_text(description) or mine_structured_pay(
            {k: posting.get(k) for k in ("salary", "compensation", "pay")}
        )

        # Lever exposes the true posting time as epoch milliseconds.
        created_ms = posting.get("createdAt")
        raw_posted_at = None
        if isinstance(created_ms, (int, float)) and created_ms > 0:
            raw_posted_at = datetime.fromtimestamp(
                created_ms / 1000.0, tz=timezone.utc
            ).isoformat()

        return OpportunityRaw(
            source_slug="lever",
            external_id=f"lev_{token}_{posting_id}",
            title=title,
            company_name=company_name,
            apply_url=apply_url,
            raw_description=description,
            raw_location=location,
            raw_salary=raw_salary,
            raw_posted_at=raw_posted_at,
            opportunity_type="job",
            requisition_id=posting_id,
            department=str(department) if department else None,
            subcategory_raw="private/corporate",
            raw_payload={"board": token, "ats": "lever", "categories": categories, "raw_job": posting},
        )


__all__ = ["LeverConnector"]
