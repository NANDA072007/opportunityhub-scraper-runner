"""
Wellfound Connector
===================
Fetches startup & remote tech jobs from Wellfound (AngelList Talent).

Wellfound has no stable public API and enforces aggressive anti-bot
protections; this connector fetches server-rendered job listing pages and
parses structured cards when present, degrading gracefully (returning an empty
list) when the site blocks or changes markup. All raw HTML is preserved in
``raw_payload`` for audit.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional

import httpx
from bs4 import BeautifulSoup

from backend.pipeline.models import OpportunityRaw
from backend.pipeline.connectors.base_connector import BaseConnector, ConnectorFetchError
from backend.pipeline.connectors.capabilities import Capability
from backend.pipeline.connectors._http import ConnectorHttpClient

logger = logging.getLogger(__name__)

# Currency-denominated pay figure with optional magnitude/period hints.
# Deliberately strict: bare numbers (headcount, YOE) must never match.
_SALARY_HINT_RE = re.compile(
    r"(?:₹|\$|€|£|INR|USD|EUR)\s?[\d,]+(?:\.\d+)?\s?"
    r"(?:k|K|lakh|LPA|L|lpa)?"
    r"(?:\s?(?:/|per|a|-)?\s?(?:year|yr|month|mo|week|wk|hour|hr|day|annum|CTC|LPA|lpa))?",
)

# Wellfound cards render location as "Remote only • United States" or
# "Hybrid • Bengaluru, India". Work-mode marker first, optional place after.
_LOCATION_MARKER_RE = re.compile(
    r"^(?:remote(?:\s+only)?|hybrid|on-?site|in[- ]office)\b",
    re.IGNORECASE,
)

# Card segments that are employment type / recency, never a location.
_NON_LOCATION_SEGMENT_HINTS = (
    "full-time", "part-time", "contract", "internship", "temporary",
    "easy apply", "actively hiring", "ago", "yrs", "years",
)

# Location lines are split on these separators ("Remote only • United States").
_SEGMENT_SEPARATOR_RE = re.compile(r"[•·|]")
_HAS_LETTER_RE = re.compile(r"[A-Za-z]")

_COMPANY_HREF_RE = re.compile(r"^/company/([^/]+)/jobs/")
_COMPANY_PROFILE_RE = re.compile(r"^/company/([^/?#]+)/?$")


class WellfoundConnector(BaseConnector):
    """Wellfound connector for startup & remote tech jobs."""

    priority = 700
    version = "1.0.0"
    owner = "platform"
    capabilities = {Capability.JOB, Capability.REMOTE}
    supports_full_sync = True
    supports_incremental_sync = False
    estimated_rate_limit = 1.0

    BASE_URL = "https://wellfound.com"

    _ROLE_PATHS = [
        "/role/r/software-engineer",
        "/role/r/data-engineer",
        "/role/r/product-manager",
        "/role/r/frontend-engineer",
        "/role/r/backend-engineer",
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
        return "wellfound"

    @property
    def display_name(self) -> str:
        return "Wellfound"

    @property
    def default_source_reputation(self) -> int:
        return 80

    def fetch_raw_opportunities(
        self, config: Dict[str, Any], query: Optional[str] = None
    ) -> List[OpportunityRaw]:
        paths = config.get("paths") or self._ROLE_PATHS
        results: List[OpportunityRaw] = []
        seen_urls: set = set()
        successful_pages = 0
        failed_pages = 0

        for path in paths:
            url = f"{self.BASE_URL}{path}"
            try:
                html = self._http.get_text(url)
            except Exception as exc:
                logger.warning("Wellfound listing '%s' failed: %s", path, exc)
                failed_pages += 1
                continue
            successful_pages += 1
            results.extend(self._parse_listing_html(html, seen_urls))

        if failed_pages and not successful_pages:
            raise ConnectorFetchError(
                f"Wellfound: all {failed_pages} configured listing pages failed "
                "(site blocked or unreachable)."
            )

        logger.info("WellfoundConnector fetched %d total items.", len(results))
        return results

    def _parse_listing_html(self, html: str, seen_urls: set) -> List[OpportunityRaw]:
        soup = BeautifulSoup(html, "html.parser")
        items: List[OpportunityRaw] = []

        # Wellfound renders job cards as anchor/link elements with hrefs to /company/*/jobs/*.
        for link in soup.select("a[href*='/jobs/']"):
            try:
                href = link.get("href", "")
                if not href.startswith("/"):
                    continue
                apply_url = f"{self.BASE_URL}{href}"
                if apply_url in seen_urls:
                    continue
                seen_urls.add(apply_url)

                title = (link.get("title") or link.get_text(strip=True) or "").strip()
                if not title:
                    continue

                # The employer is not printed in the anchor text. Live
                # Wellfound title links are /jobs/<id> (no company in the
                # href), so read the card's startup header first and only
                # fall back to /company/<slug>/jobs/ hrefs.
                card_company, company_slug = self._find_card_company(link)
                company = card_company or self._company_from_href(href)
                if company and title.lower().endswith(f" at {company.lower()}"):
                    title = title[: -(len(company) + 4)].strip() or title

                # Compensation and location live in the card container, not
                # the anchor. parse_salary downstream is the arbiter —
                # unparseable text yields None (undisclosed), never a guess.
                raw_salary = self._find_card_salary(link)
                location = self._find_card_location(link)

                description_parts = [
                    f"Startup / tech role '{title}'",
                    f"at {company}" if company else "",
                    "listed on Wellfound.",
                ]
                if location:
                    description_parts.append(f"Location: {location}.")
                if raw_salary:
                    description_parts.append(f"Compensation: {raw_salary}.")
                description = " ".join(p for p in description_parts if p)

                items.append(OpportunityRaw(
                    source_slug="wellfound",
                    external_id=apply_url,
                    title=title,
                    company_name=company or "Confidential",
                    apply_url=apply_url,
                    raw_description=description,
                    raw_location=location,
                    raw_salary=raw_salary,
                    opportunity_type="job",
                    raw_payload={
                        "source": "wellfound",
                        "company_slug": company_slug,
                        "company_name": company,
                        "location_text": location,
                        "raw_html": str(link),
                    },
                ))
            except Exception as exc:
                logger.warning("Skipping malformed Wellfound listing: %s", exc)
                continue
        return items

    @staticmethod
    def _company_from_href(href: str) -> Optional[str]:
        """Derive an employer display name from /company/<slug>/jobs/<id>."""
        match = _COMPANY_HREF_RE.match(href or "")
        if not match:
            return None
        return WellfoundConnector._display_from_slug(match.group(1))

    @staticmethod
    def _display_from_slug(slug: str) -> Optional[str]:
        cleaned = (slug or "").strip()
        if not cleaned or cleaned.lower() in ("unknown", "confidential", "company"):
            return None
        name = " ".join(part.capitalize() for part in re.split(r"[-_]+", cleaned) if part)
        return name or None

    @classmethod
    def _find_card_company(cls, link) -> tuple[Optional[str], Optional[str]]:
        """Return (display_name, slug) from the card's startup header.

        Live Wellfound job links are /jobs/<id>-<slug>, so the employer is
        not in the href. It sits in the surrounding card:
        ``<div data-testid="startup-header"><a href="/company/virta-health">``
        ``<h2>Virta Health</h2></a></div>``. Climb ancestors until a node
        with exactly one company profile link (the card; one startup can
        post several roles, so job-link count is not a boundary), and read
        that anchor's visible name. Returns (None, None) rather than
        inventing an employer or stealing the next card's company.
        """
        node = link
        for _ in range(6):
            node = node.parent
            if node is None:
                return None, None

            profile_hrefs = {
                anchor.get("href", "")
                for anchor in node.select("a[href^='/company/']")
                if _COMPANY_PROFILE_RE.match(anchor.get("href", ""))
            }
            headers = node.select("[data-testid='startup-header']")
            if len(headers) > 1:
                # Climbed into the listing container; every further ancestor
                # would mix companies from different cards.
                return None, None

            scope = headers[0] if headers else node
            # A company profile link is only trustworthy inside a card: one
            # header, or exactly one job link and one company. Listing
            # containers mix many companies.
            if headers or (len(profile_hrefs) == 1 and len(node.select("a[href*='/jobs/']")) == 1):
                derived_fallback: Optional[tuple[str, str]] = None
                for anchor in scope.select("a[href^='/company/']"):
                    href = anchor.get("href", "")
                    match = _COMPANY_PROFILE_RE.match(href)
                    if not match:
                        continue
                    # Prefer visible company text; logo anchors are empty and
                    # would otherwise yield "Swara 1" from the URL slug.
                    name = anchor.get_text(" ", strip=True)
                    if name:
                        return name[:120], match.group(1)
                    if derived_fallback is None:
                        derived = cls._display_from_slug(match.group(1))
                        if derived:
                            derived_fallback = (derived, match.group(1))
                if derived_fallback:
                    return derived_fallback
            if headers:
                return None, None
        return None, None

    @staticmethod
    def _find_card_location(link) -> Optional[str]:
        """Extract the card's location line ("Remote only • United States").

        Scans the same bounded ancestor chain as salary extraction, but
        inspects individual TEXT NODES: card containers concatenate the job
        title with the location, so splitting the container text on bullets
        left the title glued to the work-mode marker and never matched.
        Returns None when no work-mode marker is present instead of the
        previous fabricated "Remote / US / Global" for every listing.
        """
        node = link
        for _ in range(3):
            node = node.parent
            if node is None:
                return None
            segments: List[str] = []
            for child in node.find_all(string=True):
                raw = str(child).strip()
                if not raw:
                    continue
                # The job title lives inside the link itself; "Remote Sensing
                # Engineer" must never be mistaken for a "Remote" location.
                if any(parent is link for parent in child.parents):
                    continue
                segments.extend(
                    part.strip()
                    for part in _SEGMENT_SEPARATOR_RE.split(raw)
                    if part.strip()
                )

            for idx, segment in enumerate(segments):
                if not _LOCATION_MARKER_RE.match(segment):
                    continue
                location = segment
                if idx + 1 < len(segments):
                    following = segments[idx + 1]
                    lowered = following.lower()
                    # Skip trailing artifacts ("6", "+", "2 weeks ago") that
                    # previously produced "Remote only • 6".
                    if (
                        _HAS_LETTER_RE.search(following)
                        and not any(hint in lowered for hint in _NON_LOCATION_SEGMENT_HINTS)
                    ):
                        location = f"{segment} • {following}"
                return location.strip(" •·|-,")[:120]
        return None

    @staticmethod
    def _find_card_salary(link) -> Optional[str]:
        """Best-effort salary text from the link's card container.

        Requires an explicit currency symbol + digits (optionally with
        k/thousand suffixes and period hints). Adjacent range figures
        ("$162k • $233k") are joined so the normalizer can keep the full
        range instead of storing only the lower bound. Returns None when
        nothing compensation-shaped is present — never guess from bare
        numbers (headcounts, years of experience) that would corrupt pay.
        """
        node = link
        for _ in range(3):
            node = node.parent
            if node is None:
                return None
            text = node.get_text(" ", strip=True)
            matches = list(_SALARY_HINT_RE.finditer(text))
            if not matches:
                continue
            first = matches[0]
            value = first.group(0).strip()
            last_end = first.end()
            for nxt in matches[1:]:
                gap = text[last_end:nxt.start()]
                if gap.strip(" •·|-–—\t"):
                    break
                value = f"{value} • {nxt.group(0).strip()}"
                last_end = nxt.end()
            return value[:120]
        return None


__all__ = ["WellfoundConnector"]
