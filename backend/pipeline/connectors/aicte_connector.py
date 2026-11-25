"""
AICTE Internship Portal Connector
==================================
Canonical connector for AICTE-approved engineering and public infrastructure internships
from internship.aicte-india.org.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set

from backend.pipeline.connectors.base_connector import BaseConnector
from backend.pipeline.connectors.capabilities import Capability
from backend.pipeline.connectors._http import ConnectorHttpClient, parse_guarded
from backend.pipeline.models import OpportunityRaw

logger = logging.getLogger(__name__)

_DEFAULT_AICTE_URL = "https://internship.aicte-india.org"


class AicteConnector(BaseConnector):
    """Connector for AICTE-approved internships (Government & Technical)."""

    priority = 750
    version = "1.0.0"
    owner = "platform"
    capabilities: Set[Capability] = {Capability.INTERNSHIP, Capability.GOVERNMENT}

    def __init__(self, http_client: Optional[ConnectorHttpClient] = None) -> None:
        self._http = http_client or ConnectorHttpClient(max_retries=2, rate_limit_per_sec=2.0)

    @property
    def source_slug(self) -> str:
        return "aicte"

    @property
    def display_name(self) -> str:
        return "AICTE Internship Portal"

    @property
    def default_source_reputation(self) -> int:
        return 80

    def fetch_raw_opportunities(
        self, config: Dict[str, Any], query: Optional[str] = None
    ) -> List[OpportunityRaw]:
        """Fetch raw AICTE internships.

        Sprint-2 honesty fix: the 4 hardcoded mock dictionaries were stale
        seeds masquerading as live data. They are deleted. Until the portal
        API-emulation spike lands (CAPTCHA/WAF-gated — never block releases
        on it), this returns an explicit empty set so the UI renders an
        honest "no internships currently open" state instead of 2024 mocks.
        """
        logger.warning(
            "AicteConnector: live portal scrape not yet available "
            "(anti-bot spike pending) — returning empty set, no mocks."
        )
        return []

    def _to_raw(self, item: Dict[str, Any]) -> OpportunityRaw:
        domain = item["domain"]
        return OpportunityRaw(
            source_slug="aicte",
            external_id=f"aicte_{domain}_2026",
            title=item["title"],
            company_name="AICTE Government Approved Enterprise",
            apply_url=f"{_DEFAULT_AICTE_URL}/internships/{domain}",
            raw_description=f"AICTE-approved official technical internship in {domain} for B.Tech/Diploma students.",
            raw_location=item["location"],
            raw_salary="Stipend Provided per AICTE Norms",
            raw_deadline=None,
            opportunity_type="internship",
            raw_payload={
                "source": "aicte",
                "domain": domain,
                "government_approved": True,
            },
        )


__all__ = ["AicteConnector"]
