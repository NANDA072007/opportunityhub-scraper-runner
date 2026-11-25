"""
Scholarships & Fellowships Opportunities Connector
=================================================
Canonical connector for Merit & Means Scholarships, Government Grants, and Research Fellowships
from Buddy4Study, National Scholarship Portal (NSP), and PMRF.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set

from backend.pipeline.connectors.base_connector import BaseConnector
from backend.pipeline.connectors.capabilities import Capability
from backend.pipeline.connectors._http import ConnectorHttpClient, parse_guarded
from backend.pipeline.models import OpportunityRaw

logger = logging.getLogger(__name__)


class ScholarshipConnector(BaseConnector):
    """Connector for National Scholarships & Corporate Merit Grants."""

    priority = 750
    version = "1.0.0"
    owner = "platform"
    capabilities: Set[Capability] = {Capability.SCHOLARSHIP, Capability.FELLOWSHIP}

    def __init__(self, http_client: Optional[ConnectorHttpClient] = None) -> None:
        self._http = http_client or ConnectorHttpClient(max_retries=2, rate_limit_per_sec=2.0)

    @property
    def source_slug(self) -> str:
        return "scholarships"

    @property
    def display_name(self) -> str:
        return "National & Merit Scholarships Portal"

    @property
    def default_source_reputation(self) -> int:
        return 95

    def fetch_raw_opportunities(
        self, config: Dict[str, Any], query: Optional[str] = None
    ) -> List[OpportunityRaw]:
        """Fetch raw scholarship and grant listings.

        Sprint-2 honesty fix: the 3 hardcoded static dictionaries were stale
        mocks. Deleted. Returns an explicit empty set until a live
        NSP/scholarship source lands; the UI renders an honest empty state.
        """
        logger.warning(
            "ScholarshipConnector: live source not yet available — "
            "returning empty set, no mocks."
        )
        return []

    def _to_raw(self, item: Dict[str, Any]) -> OpportunityRaw:
        return OpportunityRaw(
            source_slug=self.source_slug,
            external_id=item["id"],
            title=item["title"],
            company_name=item["org"],
            apply_url=item["apply_url"],
            raw_description=item["desc"],
            raw_location=item["location"],
            raw_salary=item["amount"],
            raw_deadline=item["deadline"],
            opportunity_type="scholarship",
            raw_payload={
                "source": self.source_slug,
                "award_amount": item["amount"],
                "family_income_limit": item["income_limit"],
                "gender_preference": item["gender"],
                "is_scholarship": True,
            },
        )


__all__ = ["ScholarshipConnector"]
