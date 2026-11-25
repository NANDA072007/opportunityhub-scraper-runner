"""
Generic Careers ATS & Feed Connector
====================================
Priority 1 Reusable Adapter Connector for corporate career pages.
Converts structured JSON, RSS/Atom, and Schema.org JobPosting feeds
into generic OpportunityRaw objects with ZERO site-specific hacks.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional

import httpx

from backend.pipeline.models import OpportunityRaw
from backend.pipeline.connectors.base_connector import BaseConnector, ConnectorFetchError
from backend.pipeline.connectors.capabilities import Capability
from backend.pipeline.connectors._http import ConnectorHttpClient, parse_guarded
from backend.pipeline.connectors._pay import mine_pay_text, mine_structured_pay

# Untrusted feeds must never expand external entities (XXE/Billion Laughs).
# Fail closed: if defusedxml is unavailable the feed is treated as failed
# instead of silently falling back to the vulnerable stdlib parser.
try:
    import defusedxml.ElementTree as DET
    from defusedxml.common import DefusedXmlException
    _XML_PARSE_ERRORS = (DET.ParseError, DefusedXmlException)
except ImportError:  # pragma: no cover
    DET = None
    _XML_PARSE_ERRORS = (Exception,)

logger = logging.getLogger(__name__)


class GenericCareersConnector(BaseConnector):
    """Priority 1 Reusable Connector for corporate career pages and JSON/RSS feeds."""

    priority = 850
    version = "1.0.0"
    owner = "platform"
    capabilities = {Capability.JOB, Capability.INTERNSHIP, Capability.REMOTE, Capability.ONSITE}
    supports_full_sync = True
    supports_incremental_sync = False
    estimated_rate_limit = 2.0

    # Verified live on 2026-10-26. The former Automattic "JSON API" entry
    # pointed at an HTML marketing page (no JSON, no wp-json, no ATS link),
    # so it logged a decode error on every run. Removed rather than pointed
    # at a guessed endpoint; add a replacement only after verifying its shape.
    DEFAULT_FEEDS = [
        {
            "name": "Mozilla Careers Feed",
            "url": "https://careers.mozilla.org/feed/",
            "type": "rss",
            "company_name": "Mozilla",
        },
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
        return "generic_careers"

    @property
    def display_name(self) -> str:
        return "Generic Corporate Careers"

    @property
    def default_source_reputation(self) -> int:
        return 95

    def fetch_raw_opportunities(
        self, config: Dict[str, Any], query: Optional[str] = None
    ) -> List[OpportunityRaw]:
        feeds = config.get("feeds") or self.DEFAULT_FEEDS
        results: List[OpportunityRaw] = []
        successful_feeds = 0
        failed_feeds = 0

        for feed in feeds:
            feed_type = feed.get("type", "json_api")
            company_name = feed.get("company_name", "Corporate Employer")
            url = feed.get("url")
            name = feed.get("name") or url or "feed"

            if not url:
                continue

            try:
                if feed_type == "rss":
                    items = self._fetch_rss_feed(company_name, url)
                else:
                    items = self._fetch_json_feed(company_name, url)
            except Exception as exc:
                logger.warning("GenericCareers feed '%s' failed: %s", name, exc)
                failed_feeds += 1
                continue
            successful_feeds += 1
            results.extend(items)

        if failed_feeds and not successful_feeds:
            raise ConnectorFetchError(
                f"GenericCareers: all {failed_feeds} configured feeds failed "
                "(network error, blocked endpoint, or invalid payload)."
            )

        logger.info("GenericCareersConnector fetched %d total items across %d feeds.", len(results), len(feeds))
        return results

    def _fetch_json_feed(self, company_name: str, url: str) -> List[OpportunityRaw]:
        data = self._http.get_json(url)
        if not isinstance(data, (list, dict)):
            raise ConnectorFetchError(f"GenericCareers feed '{url}' returned {type(data).__name__}, expected JSON object/array.")

        jobs = data if isinstance(data, list) else data.get("jobs") or data.get("postings") or []
        if not isinstance(jobs, list):
            return []

        items: List[OpportunityRaw] = []
        for idx, job in enumerate(jobs):
            if isinstance(job, dict):
                parsed = parse_guarded(
                    lambda j: self._parse_json_job(company_name, url, idx, j),
                    job,
                    context="generic_careers_json",
                )
                if parsed is not None:
                    items.append(parsed)
        return items

    def _fetch_rss_feed(self, company_name: str, url: str) -> List[OpportunityRaw]:
        if DET is None:
            raise ConnectorFetchError(
                f"GenericCareers RSS feed '{url}' skipped: defusedxml missing."
            )
        text = self._http.get_text(url)
        try:
            root = DET.fromstring(text)
        except _XML_PARSE_ERRORS as exc:
            raise ConnectorFetchError(
                f"GenericCareers RSS feed '{url}' XML parse failed: {exc}"
            ) from exc

        channel = root.find("channel")
        items_xml = channel.findall("item") if channel is not None else root.findall(".//item")

        results: List[OpportunityRaw] = []
        for idx, item in enumerate(items_xml):
            parsed = parse_guarded(
                lambda elem: self._parse_rss_item(company_name, url, idx, elem),
                item,
                context="generic_careers_rss",
            )
            if parsed is not None:
                results.append(parsed)
        return results

    @staticmethod
    def _parse_json_job(company_name: str, feed_url: str, idx: int, job: Dict[str, Any]) -> Optional[OpportunityRaw]:
        job_id = str(job.get("id") or job.get("job_id") or f"gen_{idx}")
        title = str(job.get("title") or job.get("name") or job.get("position") or "").strip()
        if not title:
            return None

        apply_url = str(job.get("url") or job.get("link") or job.get("apply_url") or feed_url)
        description = str(job.get("description") or job.get("summary") or job.get("details") or f"Role: {title} at {company_name}")
        location = str(job.get("location") or job.get("city") or "Remote")

        # Private-pay honesty: read explicitly pay-named keys first, then mine
        # the description as a last resort. "Competitive Package" only when
        # the feed publishes nothing pay-shaped.
        raw_salary = mine_structured_pay(job) or mine_pay_text(description)

        return OpportunityRaw(
            source_slug="generic_careers",
            external_id=f"gen_{company_name.lower().replace(' ', '_')}_{job_id}",
            title=title,
            company_name=company_name,
            apply_url=apply_url,
            raw_description=description,
            raw_location=location,
            raw_salary=raw_salary,
            opportunity_type="job",
            raw_payload={"feed_url": feed_url, "ats": "generic_json", "raw_job": job},
        )

    @staticmethod
    def _parse_rss_item(company_name: str, feed_url: str, idx: int, item: ET.Element) -> Optional[OpportunityRaw]:
        title_elem = item.find("title")
        link_elem = item.find("link")
        desc_elem = item.find("description")
        guid_elem = item.find("guid")

        title = title_elem.text.strip() if title_elem is not None and title_elem.text else ""
        if not title:
            return None

        apply_url = link_elem.text.strip() if link_elem is not None and link_elem.text else feed_url
        guid = guid_elem.text.strip() if guid_elem is not None and guid_elem.text else f"rss_{idx}"
        description = desc_elem.text.strip() if desc_elem is not None and desc_elem.text else f"Role: {title} at {company_name}"
        posted_elem = item.find("pubDate")
        raw_posted_at = (
            posted_elem.text.strip()
            if posted_elem is not None and posted_elem.text
            else None
        )

        return OpportunityRaw(
            source_slug="generic_careers",
            external_id=f"gen_{company_name.lower().replace(' ', '_')}_{guid}",
            title=title,
            company_name=company_name,
            apply_url=apply_url,
            raw_description=description,
            raw_location="Remote",
            raw_salary=mine_pay_text(description),
            raw_posted_at=raw_posted_at,
            opportunity_type="job",
            raw_payload={"feed_url": feed_url, "ats": "generic_rss"},
        )


__all__ = ["GenericCareersConnector"]
