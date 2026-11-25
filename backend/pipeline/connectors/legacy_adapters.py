"""
Legacy Adapter Layer
====================
Adapters that bridge the legacy ingestion systems (``backend/scrapers`` and
``backend/connectors``) into the canonical ``BaseConnector`` contract without
migrating them.

Purpose:
- Isolate all old architectures behind the new interface.
- Convert legacy outputs (``OpportunitySchema`` / raw dicts) into
  ``OpportunityRaw`` so the canonical pipeline can process them.
- Contain NO business logic.

These adapters require constructor injection (a wrapped scraper/connector) and
are therefore NOT auto-discovered; they are registered explicitly via
``ConnectorRegistry.register_factory`` / ``register``.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set

from backend.pipeline.connectors.base_connector import BaseConnector
from backend.pipeline.connectors.capabilities import Capability
from backend.pipeline.connectors._http import parse_guarded
from backend.pipeline.models import OpportunityRaw

logger = logging.getLogger(__name__)


class LegacyScraperAdapter(BaseConnector):
    """Wrap a legacy ``backend.scrapers.BaseScraper`` as a canonical connector.

    The wrapped scraper exposes ``SOURCE_NAME`` and ``scrape()`` returning a
    list of ``OpportunitySchema`` instances.
    """

    def __init__(self, scraper: Any) -> None:
        self._scraper = scraper

    @property
    def source_slug(self) -> str:
        return getattr(self._scraper, "SOURCE_NAME", "legacy_scraper")

    @property
    def display_name(self) -> str:
        return getattr(
            self._scraper,
            "SOURCE_NAME",
            "Legacy Scraper",
        ).replace("_", " ").title()

    @property
    def default_source_reputation(self) -> int:
        return 70

    @property
    def capabilities(self) -> Set[Capability]:
        return {Capability.JOB}

    def fetch_raw_opportunities(
        self, config: Dict[str, Any], query: Optional[str] = None
    ) -> List[OpportunityRaw]:
        items = self._scraper.scrape()
        results: List[OpportunityRaw] = []
        for item in items:
            parsed = parse_guarded(
                self._to_raw, item, context=self.source_slug
            )
            if parsed is not None:
                results.append(parsed)
        return results

    def _to_raw(self, item: Any) -> OpportunityRaw:
        external_id = getattr(item, "url_hash", None) or str(
            getattr(item, "apply_url", "")
        )
        salary = getattr(item, "salary", None)
        return OpportunityRaw(
            source_slug=self.source_slug,
            external_id=str(external_id),
            title=getattr(item, "title", ""),
            company_name=getattr(item, "company", ""),
            apply_url=getattr(item, "apply_url", ""),
            raw_description=getattr(item, "description", ""),
            raw_location=getattr(item, "location", None),
            raw_salary=str(salary) if salary is not None else None,
            raw_deadline=getattr(item, "deadline", None),
            opportunity_type="job",
            raw_payload={"source": self.source_slug, "category": getattr(item, "category", "")},
        )


class LegacyConnectorAdapter(BaseConnector):
    """Wrap a legacy ``backend.connectors.BaseSourceConnector`` as canonical.

    The wrapped connector exposes ``source_id``, ``name`` and
    ``fetch_opportunities(strategy)`` returning a list of raw dicts.
    """

    def __init__(self, connector: Any) -> None:
        self._connector = connector

    @property
    def source_slug(self) -> str:
        source_id = getattr(self._connector, "source_id", "legacy_connector")
        return str(source_id).replace("cs_", "")

    @property
    def display_name(self) -> str:
        return getattr(self._connector, "name", "Legacy Connector")

    @property
    def default_source_reputation(self) -> int:
        return 65

    @property
    def capabilities(self) -> Set[Capability]:
        supported = getattr(self._connector, "supported_types", None) or []
        caps = {Capability.JOB}
        type_map = {
            "job": Capability.JOB,
            "internship": Capability.INTERNSHIP,
            "competition": Capability.COMPETITION,
            "research": Capability.RESEARCH,
            "scholarship": Capability.SCHOLARSHIP,
            "fellowship": Capability.FELLOWSHIP,
            "government": Capability.GOVERNMENT,
            "freelance": Capability.FREELANCE,
        }
        for item in supported:
            capability = type_map.get(str(item).lower())
            if capability is not None:
                caps.add(capability)
        return caps

    def fetch_raw_opportunities(
        self, config: Dict[str, Any], query: Optional[str] = None
    ) -> List[OpportunityRaw]:
        strategy: Dict[str, Any] = dict(config or {})
        if query:
            strategy["search_query"] = query
        items = self._connector.fetch_opportunities(strategy) or []
        results: List[OpportunityRaw] = []
        for item in items:
            parsed = parse_guarded(
                self._to_raw, item, context=self.source_slug
            )
            if parsed is not None:
                results.append(parsed)
        return results

    def _to_raw(self, item: Dict[str, Any]) -> OpportunityRaw:
        return OpportunityRaw(
            source_slug=self.source_slug,
            external_id=str(item.get("apply_url") or item.get("title") or ""),
            title=str(item.get("title", "")),
            company_name=str(item.get("company", "")),
            apply_url=str(item.get("apply_url", "")),
            raw_description=str(item.get("description", "")),
            raw_location=item.get("location"),
            raw_salary=item.get("salary_range"),
            raw_deadline=item.get("deadline"),
            opportunity_type=str(item.get("opportunity_type", "job")),
            raw_payload=item,
        )


__all__ = [
    "LegacyScraperAdapter",
    "LegacyConnectorAdapter",
]
