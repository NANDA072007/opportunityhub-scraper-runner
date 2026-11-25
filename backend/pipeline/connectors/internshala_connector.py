"""
Internshala Connector
=====================
Fetches student internships & jobs from Internshala's server-rendered listing
pages. Returns generic OpportunityRaw objects with ZERO branch/domain
hardcoding beyond the site's own structure.

Internshala renders ``div.individual_internship`` cards server-side; the raw
HTML is preserved in ``raw_payload`` for audit while lightweight parsing
extracts the fields needed for stable ``external_id`` / ``apply_url``
deduplication.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
from bs4 import BeautifulSoup

from backend.pipeline.models import OpportunityRaw
from backend.pipeline.connectors.base_connector import BaseConnector
from backend.pipeline.connectors.capabilities import Capability
from backend.pipeline.connectors._http import ConnectorHttpClient, parse_guarded

logger = logging.getLogger(__name__)

# Fallback stipend scan: currency symbol + digits mandatory, so apply-by
# dates ("Apply by 12 Dec") and durations ("6 Months") never match.
_STIPEND_FALLBACK_RE = re.compile(
    r"(?:₹|\$|€|£|INR|Rs\.?)\s?[\d,]+(?:\.\d+)?\s?"
    r"(?:k|K|lakh|LPA|L|lpa)?"
    r"(?:\s?(?:/|per|-)?\s?(?:month|mo|week|wk|hour|hr|day|year|yr|annum|LPA|lpa))?",
)


class InternshalaConnector(BaseConnector):
    """Internshala connector for student internships and jobs."""

    priority = 700
    version = "1.0.0"
    owner = "platform"
    capabilities = {Capability.INTERNSHIP, Capability.JOB, Capability.REMOTE}
    supports_full_sync = True
    supports_incremental_sync = False
    estimated_rate_limit = 1.0

    BASE_URL = "https://internshala.com"

    _LISTING_PATHS = [
        "/internships/internship-in-india",
        "/internships/work-from-home-internships",
        "/internships/computer-science-internship",
        "/internships/engineering-internship",
        "/internships/marketing-internship",
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
            self._http = ConnectorHttpClient(client=http_client, rate_limit_per_sec=1.0)

    @property
    def source_slug(self) -> str:
        return "internshala"

    @property
    def display_name(self) -> str:
        return "Internshala"

    @property
    def default_source_reputation(self) -> int:
        return 85

    def fetch_raw_opportunities(
        self, config: Dict[str, Any], query: Optional[str] = None
    ) -> List[OpportunityRaw]:
        paths = config.get("paths") or self._LISTING_PATHS
        results: List[OpportunityRaw] = []
        seen_urls = set()

        for path in paths:
            url = f"{self.BASE_URL}{path}"
            try:
                html = self._http.get_text(url)
            except Exception as exc:
                logger.warning("Internshala listing '%s' failed: %s", path, exc)
                continue
            results.extend(self._parse_listing_html(html, seen_urls))

        logger.info("InternshalaConnector fetched %d total items.", len(results))
        return results

    def _parse_listing_html(self, html: str, seen_urls: set) -> List[OpportunityRaw]:
        soup = BeautifulSoup(html, "html.parser")
        cards = soup.select("div.individual_internship, div.internship_meta")
        items: List[OpportunityRaw] = []

        for card in cards:
            parsed = parse_guarded(
                lambda c: self._parse_card(c, seen_urls),
                card,
                context="internshala",
            )
            if parsed is not None:
                items.append(parsed)
        return items

    @staticmethod
    def _parse_card(card, seen_urls: set) -> OpportunityRaw:
        title_elem = card.select_one(
            "a.job-title-href, "
            "a.view_detail_button, "
            "h3.heading_4_5 a, "
            "div.heading_4_5 a, "
            "a.internship_heading"
        )
        if title_elem is None:
            return None
        title = title_elem.get_text(strip=True)
        relative_url = title_elem.get("href")
        if not title or not relative_url:
            return None

        apply_url = relative_url if relative_url.startswith("http") else f"https://internshala.com{relative_url}"
        if apply_url in seen_urls:
            return None
        seen_urls.add(apply_url)

        company_elem = card.select_one(
            "p.company-name, h4.heading_6, p.company_name, a.link_display_like_text"
        )
        company = company_elem.get_text(strip=True) if company_elem else "Unknown Company"

        location_elem = card.select_one(
            ".locations a, span.location_link a, #location_names a"
        )
        location = location_elem.get_text(strip=True) if location_elem else "India"
        if "work from home" in location.lower():
            location = "Remote, India"

        stipend_elem = card.select_one("span.stipend, div.item_body span.desktop")
        stipend_text = stipend_elem.get_text(strip=True) if stipend_elem else ""
        if not stipend_text:
            # Fallback: currency-denominated figure anywhere in the card
            # (markup variants move the stipend node). Requires an explicit
            # currency symbol + digits so deadlines/headcounts never match.
            stipend_text = InternshalaConnector._scan_card_salary(card) or ""

        # Sprint-2: duration is a first-class student planning signal
        # (semester fit). Never parsed before — was silently dropped.
        duration_text = InternshalaConnector._parse_duration(card)

        # Sprint-2: PPO conversion + work-mode badges from detail-row-2.
        # The deprecated div.apply_by date parse is REMOVED (element retired
        # upstream in favor of relative freshness tags); freshness text is
        # kept as a posted_at hint only.
        ppo_offered, work_mode, freshness = InternshalaConnector._parse_badges(card)

        responsibilities = InternshalaConnector._parse_responsibilities(card)
        desc = f"{title} at {company}. Location: {location}."
        if responsibilities:
            desc = f"{desc} Responsibilities: {responsibilities[:800]}"

        return OpportunityRaw(
            source_slug="internshala",
            external_id=apply_url,
            title=title,
            company_name=company,
            apply_url=apply_url,
            raw_description=desc[:2000],
            raw_location=location,
            raw_salary=stipend_text or None,
            raw_deadline=None,
            opportunity_type="internship",
            ppo_offered=ppo_offered,
            duration=duration_text,
            subcategory_raw=f"internship/{work_mode}" if work_mode else "internship",
            raw_payload={
                "source": "internshala",
                "company": company,
                "duration": duration_text,
                "work_mode": work_mode,
                "freshness": freshness,
                "category": "student",
                "raw_salary": stipend_text or None,
                "responsibilities": responsibilities[:800] if responsibilities else None,
            },
        )

    @staticmethod
    def _scan_card_salary(card) -> Optional[str]:
        """Last-resort stipend text from anywhere in the card.

        Only fires when the primary selectors miss. The currency-symbol
        requirement keeps date/duration numbers out; `parse_salary`
        downstream remains the final arbiter (unparseable → undisclosed).
        """
        try:
            text = card.get_text(" ", strip=True)
        except Exception:
            return None
        match = _STIPEND_FALLBACK_RE.search(text or "")
        if not match:
            return None
        return match.group(0).strip()[:120] or None

    @staticmethod
    def _parse_duration(card) -> Optional[str]:
        """Extract internship duration (e.g. '6 Months', '4 Weeks')."""
        for selector in (
            "div.other_detail_item div.item_body",
            ".duration",
            "span.duration",
        ):
            try:
                for elem in card.select(selector):
                    text = elem.get_text(" ", strip=True)
                    if re.search(r"\d+\s*(week|month|day|year)", text, re.IGNORECASE):
                        return re.sub(r"\s+", " ", text).strip()[:64]
            except Exception:
                continue
        # Fallback: scan the whole card text for a duration line.
        try:
            text = card.get_text(" ", strip=True)
            match = re.search(
                r"duration\s*[:\-]?\s*(\d+\s*(?:week|month|day|year)s?)",
                text,
                re.IGNORECASE,
            )
            if match:
                return match.group(1).strip()[:64]
        except Exception:
            pass
        return None

    @staticmethod
    def _parse_badges(card) -> tuple:
        """Return (ppo_offered, work_mode, freshness) from badge rows.

        Never throws; unknown badge layouts yield (None, None, None).
        """
        ppo_offered: Optional[bool] = None
        work_mode: Optional[str] = None
        freshness: Optional[str] = None
        try:
            badge_text = " ".join(
                elem.get_text(" ", strip=True)
                for elem in card.select("div.detail-row-2, .badges, .status-container")
            ).lower()
        except Exception:
            return None, None, None
        if not badge_text.strip():
            return None, None, None
        if any(k in badge_text for k in ("job offer", "ppo", "pre-placement")):
            ppo_offered = True
        if "part time" in badge_text or "part-time" in badge_text:
            work_mode = "part-time"
        elif "full time" in badge_text or "full-time" in badge_text:
            work_mode = "full-time"
        for token in ("today", "yesterday", "day ago", "days ago", "week ago", "actively hiring"):
            if token in badge_text:
                freshness = token
                break
        return ppo_offered, work_mode, freshness

    @staticmethod
    def _parse_responsibilities(card) -> str:
        """Day-to-day responsibility bullets for the description."""
        try:
            bullets = [
                elem.get_text(" ", strip=True)
                for elem in card.select("div.other_detail_item_body li, div.other_detail_item_body")
            ]
            cleaned = [b for b in (re.sub(r"\s+", " ", b).strip() for b in bullets) if b]
            return " ".join(cleaned)[:1200]
        except Exception:
            return ""


__all__ = ["InternshalaConnector"]
