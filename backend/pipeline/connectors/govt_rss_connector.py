"""
Government RSS Connector (Sprint-2)
===================================
Ingests statutory UPSC/SSC/PSU recruitment notices from verified RSS streams
(default: FreeJobAlert) to un-starve the Government tab (RAC alone yields
1-2 notices/month).

Security contract (reviewer-mandated):
- ALL HTML stripped from summaries via StructuralNormalizer (no raw markup,
  no external ad links reach the DB/UI — XSS-safe by construction).
- Vacancy counts extracted ONLY via strict numeric regex; non-matching rows
  ingest with vacancy_count=None rather than a guessed number.
- Rows enter with the default `pending` review_status and flow through the
  standard approval gate (0037) like every other source.
- Official-link extraction: extracts authoritative .gov.in/.nic.in/.ac.in links
  from post bodies with polite fetching, robots.txt compliance, caching, and
  fail-soft error handling. Items without an official link stay hidden.

Memory contract: feeds are parsed item-by-item from a bounded response body
(`max_bytes` guard + per-feed item cap) so a 10MB RSS dump cannot bloat the
orchestrator.
"""

from __future__ import annotations

import hashlib
import logging
import re
import urllib.robotparser
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

from bs4 import BeautifulSoup

# Red-team fix (XXE/Billion Laughs): stdlib ElementTree expands external
# entities from untrusted feeds. defusedxml is a drop-in replacement that
# blocks entity expansion. Fail CLOSED (skip the feed) if unavailable —
# never silently fall back to the vulnerable parser.
try:
    import defusedxml.ElementTree as DET
    from defusedxml.common import DefusedXmlException
    _XML_PARSE_ERRORS = (DET.ParseError, DefusedXmlException)
except ImportError:  # pragma: no cover
    DET = None
    _XML_PARSE_ERRORS = (Exception,)

from backend.pipeline.connectors._http import ConnectorHttpClient, parse_guarded
from backend.pipeline.connectors._pay import mine_pay_text
from backend.pipeline.connectors.base_connector import BaseConnector, ConnectorFetchError
from backend.pipeline.government_deadline import extract_application_deadline
from backend.pipeline.connectors.capabilities import Capability
from backend.pipeline.models import OpportunityRaw
from backend.pipeline.normalizers import StructuralNormalizer
from backend.pipeline.security.url_validator import is_trusted_govt_url

logger = logging.getLogger(__name__)

_DEFAULT_FEEDS = [
    {"name": "FreeJobAlert", "url": "https://www.freejobalert.com/feed/"},
]

_MAX_ITEMS_PER_FEED = 100
_MAX_FEED_BYTES = 5 * 1024 * 1024

# Query keys that are primary routing keys and must survive normalization.
_ROUTING_QUERY_KEYS = frozenset({
    "id", "job_id", "post_id", "advt", "ad no", "notice_id", "p", "page_id",
})

# Government feeds mix genuine recruitment notices with post-exam artifacts.
# Results, admit cards, answer keys, etc. are not opportunities: ingesting them
# pollutes the student feed with rows nobody can apply to. Match the TITLE only
# (bodies legitimately reference results/syllabus inside real recruitment posts).
_NON_OPPORTUNITY_TITLE_RE = re.compile(
    r"\b(?:"
    r"results?|answer\s+keys?|admit\s+cards?|hall\s+tickets?|call\s+letters?|"
    r"intimation\s+slips?|exam(?:ination)?\s+patterns?|syllabus|"
    r"cut[\s-]?off(?:\s+marks?)?|merit\s+lists?|score\s*cards?|"
    r"question\s+papers?|revaluation|re-?checking|seat\s+allotments?|"
    r"response\s+sheets?|objection\s+(?:link|window)|final\s+answers?|"
    r"marks?\s+memo|exam\s+date\s+sheets?|date\s+sheets?|"
    r"selection\s+lists?|waiting\s+lists?|joining\s+letters?"
    r")\b",
    re.IGNORECASE,
)

# Text that marks a link as an application destination (highest priority).
_APPLY_TEXT_KEYS = (
    "apply online", "apply here", "apply now", "online application",
    "application link", "apply link", "register online", "application form",
)
_NOTIFICATION_TEXT_KEYS = ("notification", "advertisement", "details", "notice")


def normalize_feed_url(raw_url: str) -> str:
    """Canonicalize a feed link before MD5 hashing."""
    url = (raw_url or "").strip()
    if not url:
        return ""
    try:
        parts = urlsplit(url)
    except ValueError:
        return url
    scheme = "https"
    host = parts.hostname.lower() if parts.hostname else ""
    if not host:
        return url
    port = parts.port
    netloc = host if port in (None, 443) else f"{host}:{port}"
    path = parts.path.rstrip("/") or "/"
    kept = sorted(
        (k, v)
        for k, v in parse_qsl(parts.query, keep_blank_values=False)
        if k.strip().lower() in _ROUTING_QUERY_KEYS
    )
    query = urlencode(kept)
    return urlunsplit((scheme, netloc, path, query, ""))


def extract_official_govt_link_from_html(html: str) -> Optional[str]:
    """Parse HTML and extract authoritative .gov.in, .nic.in, or .ac.in link."""
    if not html or not isinstance(html, str):
        return None
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return None

    candidates: List[tuple[int, str]] = []
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href or href.startswith("#") or href.startswith("javascript:"):
            continue

        # Strict security validation: only trusted official government domains
        if not is_trusted_govt_url(href):
            continue

        text = a.get_text(" ", strip=True).lower()
        parent = a.parent
        parent_text = parent.get_text(" ", strip=True).lower() if parent else ""
        # When several anchors share a container, the container text mixes
        # their labels: a notification PDF would inherit "Apply Online" from
        # its sibling link. Only use container text when the anchor is alone.
        sibling_anchors = parent.find_all("a", href=True) if parent else []
        context_text = parent_text if len(sibling_anchors) <= 1 else text
        combined_text = f"{text} {context_text}"

        # An application destination always outranks a notification PDF: the
        # old scoring gave every PDF +50, so aggregator "Click Here -> PDF"
        # notification links beat real "Apply Online" links and sent users to
        # a static advertisement instead of the application form.
        is_apply = any(k in combined_text for k in _APPLY_TEXT_KEYS)

        score = 10
        if href.lower().endswith(".pdf"):
            score += 50
        if is_apply:
            score += 40
        elif any(k in combined_text for k in _NOTIFICATION_TEXT_KEYS):
            score += 30
        elif any(k in combined_text for k in ["official website", "official site"]):
            score += 20

        candidates.append((1 if is_apply else 0, score, href))

    if not candidates:
        return None

    candidates.sort(key=lambda c: (c[0], c[1]), reverse=True)
    return candidates[0][2]


# Strict: number + vacancy noun. "500 Trainees", "64 Posts", "1,200 Vacancies".
_VACANCY_RE = re.compile(
    r"(\d[\d,]*)\s*(posts?|vacanc(?:y|ies)|openings?|trainees?|recruits?|appointments?)",
    re.IGNORECASE,
)

# Obvious ad/tracker hosts that must never become an apply_url.
_BLOCKED_LINK_HOSTS = ("doubleclick", "googlesyndication", "taboola", "outbrain")


class GovtRssConnector(BaseConnector):
    """RSS connector for statutory government recruitment notices."""

    priority = 800
    version = "1.2.0"
    owner = "platform"
    capabilities: Set[Capability] = {Capability.GOVERNMENT, Capability.JOB}
    supports_full_sync = True
    supports_incremental_sync = False
    estimated_rate_limit = 1.0

    def __init__(self, http_client: Optional[ConnectorHttpClient] = None) -> None:
        self._http = http_client or ConnectorHttpClient(max_retries=2, rate_limit_per_sec=1.0)
        self._article_cache: Dict[str, Optional[str]] = {}
        self._robots_cache: Dict[str, urllib.robotparser.RobotFileParser] = {}

    @property
    def source_slug(self) -> str:
        return "govt_rss"

    @property
    def display_name(self) -> str:
        return "Government RSS Notices"

    @property
    def default_source_reputation(self) -> int:
        return 90

    def _can_fetch_robots(self, url: str) -> bool:
        """Check if robots.txt allows fetching url. Fails soft (allows) on network error."""
        try:
            parts = urlsplit(url)
            if not parts.scheme or not parts.netloc:
                return False
            base_url = f"{parts.scheme}://{parts.netloc}"
            if base_url not in self._robots_cache:
                rp = urllib.robotparser.RobotFileParser()
                robots_url = f"{base_url}/robots.txt"
                try:
                    robots_txt = self._http.get_text(robots_url)
                    rp.parse(robots_txt.splitlines())
                except Exception as exc:
                    logger.debug("Robots fetch failed for '%s': %s (defaulting to allow)", base_url, exc)
                    rp.allow_all = True
                self._robots_cache[base_url] = rp
            return self._robots_cache[base_url].can_fetch("*", url)
        except Exception as exc:
            logger.debug("Robots check error for '%s': %s", url, exc)
            return True

    def _extract_official_link(self, article_url: str, summary_html: str) -> Optional[str]:
        """Extract authoritative official gov link from summary or article body with caching."""
        # 1. Fast check: RSS summary / content:encoded
        summary_link = extract_official_govt_link_from_html(summary_html)
        if summary_link:
            return summary_link

        if not article_url:
            return None

        # 2. In-memory cache check
        if article_url in self._article_cache:
            return self._article_cache[article_url]

        # 3. Robots.txt check
        if not self._can_fetch_robots(article_url):
            logger.info("GovtRss article '%s' disallowed by robots.txt", article_url)
            self._article_cache[article_url] = None
            return None

        # 4. Polite fetch with fail-soft guard
        try:
            html = self._http.get_text(article_url)
            official_link = extract_official_govt_link_from_html(html)
            self._article_cache[article_url] = official_link
            return official_link
        except Exception as exc:
            logger.debug("GovtRss polite article fetch failed for '%s': %s", article_url, exc)
            self._article_cache[article_url] = None
            return None

    def fetch_raw_opportunities(
        self, config: Dict[str, Any], query: Optional[str] = None
    ) -> List[OpportunityRaw]:
        feeds = config.get("feeds") or _DEFAULT_FEEDS
        max_items = int(config.get("max_items_per_feed") or _MAX_ITEMS_PER_FEED)
        results: List[OpportunityRaw] = []
        seen_ids: Set[str] = set()
        successful_feeds = 0
        failed_feeds = 0

        for feed in feeds:
            name = feed.get("name", "rss")
            url = feed.get("url", "")
            if not url:
                continue
            try:
                xml_text = self._http.get_text(url)
            except Exception as exc:
                logger.warning("GovtRss feed '%s' failed: %s", name, exc)
                failed_feeds += 1
                continue
            if len(xml_text.encode("utf-8", "ignore")) > _MAX_FEED_BYTES:
                logger.warning(
                    "GovtRss feed '%s' exceeded %d bytes — truncating parse.",
                    name, _MAX_FEED_BYTES,
                )
                xml_text = xml_text[:_MAX_FEED_BYTES]
            try:
                parsed = self._parse_feed(name, xml_text, max_items, seen_ids)
            except ConnectorFetchError as exc:
                logger.warning("GovtRss feed '%s' unusable: %s", name, exc)
                failed_feeds += 1
                continue
            successful_feeds += 1
            results.extend(parsed)

        if failed_feeds and not successful_feeds:
            raise ConnectorFetchError(
                f"GovtRss: all {failed_feeds} configured feeds failed "
                "(network error or unparseable XML)."
            )

        logger.info("GovtRssConnector fetched %d total items.", len(results))
        return results

    def _parse_feed(
        self, feed_name: str, xml_text: str, max_items: int, seen_ids: Set[str]
    ) -> List[OpportunityRaw]:
        if DET is None:  # fail closed: never parse untrusted XML vulnerably
            raise ConnectorFetchError(
                f"GovtRss feed '{feed_name}' skipped: defusedxml missing (see requirements)."
            )
        try:
            root = DET.fromstring(xml_text)
        except _XML_PARSE_ERRORS as exc:
            raise ConnectorFetchError(
                f"GovtRss feed '{feed_name}' XML parse failed: {exc}"
            ) from exc

        items: List[OpportunityRaw] = []
        for entry in root.iter("item"):
            if len(items) >= max_items:
                break
            parsed = parse_guarded(
                lambda e: self._parse_entry(feed_name, e, seen_ids),
                entry,
                context="govt_rss",
            )
            if parsed is not None:
                items.append(parsed)
        return items

    @staticmethod
    def _text(entry, tag: str) -> str:
        elem = entry.find(tag)
        if elem is None or elem.text is None:
            # namespaced variants (content:encoded etc.)
            for child in entry:
                if child.tag.endswith(tag) and child.text:
                    return child.text.strip()
            return ""
        return elem.text.strip()

    def _parse_entry(
        self, feed_name: str, entry, seen_ids: Set[str]
    ) -> Optional[OpportunityRaw]:
        title_raw = self._text(entry, "title")
        link = self._text(entry, "link")
        if not title_raw or not link:
            return None
        if any(host in link.lower() for host in _BLOCKED_LINK_HOSTS):
            return None
        if _NON_OPPORTUNITY_TITLE_RE.search(title_raw):
            logger.debug("GovtRss skipped non-opportunity notice: %s", title_raw[:120])
            return None

        # XSS-safe by construction: strip every tag, unescape entities.
        summary_raw = self._text(entry, "description") or self._text(entry, "encoded")
        clean_desc = StructuralNormalizer.strip_html(f"{title_raw}. {summary_raw}", max_chars=2000)
        if not clean_desc:
            return None

        canonical_link = normalize_feed_url(link)
        external_id = f"govtrss_{hashlib.md5(canonical_link.encode('utf-8')).hexdigest()[:24]}"
        if external_id in seen_ids:
            return None
        seen_ids.add(external_id)

        # Extract authoritative official link (.gov.in / .nic.in / .ac.in)
        official_link = self._extract_official_link(link, summary_raw)
        effective_apply_url = official_link or link

        vacancy_count = self._parse_vacancies(f"{title_raw} {summary_raw}")
        pub_date = self._text(entry, "pubDate")
        raw_salary = mine_pay_text(f"{title_raw}. {summary_raw}")
        extracted_deadline = extract_application_deadline(clean_desc, posted_at=pub_date)

        return OpportunityRaw(
            source_slug="govt_rss",
            external_id=external_id,
            title=re.sub(r"\s+", " ", StructuralNormalizer.clean_text(title_raw))[:300],
            company_name=feed_name,
            apply_url=effective_apply_url,
            raw_description=clean_desc,
            raw_location="India",
            raw_salary=raw_salary,
            raw_deadline=(
                extracted_deadline.deadline.isoformat()
                if extracted_deadline
                else None
            ),
            raw_posted_at=pub_date or None,
            opportunity_type="job",
            vacancy_count=vacancy_count,
            subcategory_raw="government/rss",
            raw_payload={
                "source": "govt_rss",
                "feed": feed_name,
                "category": "government",
                "published_at": pub_date or None,
                "canonical_link": canonical_link,
                "official_source_url": official_link,
                "aggregator_url": link,
            },
        )

    @staticmethod
    def _parse_vacancies(text: str) -> Optional[int]:
        """Strict numeric vacancy extraction; None when not announced."""
        match = _VACANCY_RE.search(text or "")
        if not match:
            return None
        try:
            value = int(match.group(1).replace(",", ""))
        except (TypeError, ValueError):
            return None
        if 0 < value <= 1000000:
            return value
        return None


__all__ = ["GovtRssConnector", "normalize_feed_url", "extract_official_govt_link_from_html"]
