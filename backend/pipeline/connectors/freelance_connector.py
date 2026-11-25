"""
Freelance & Remote Contract Gigs Connector
=========================================
Canonical multi-source connector for Freelance projects, Contract gigs, and Remote opportunities
from live public APIs:
- Remotive (remotive.com/api/remote-jobs)
- Jobicy (jobicy.com/api/v2/remote-jobs)
- RemoteOK (remoteok.com/api)
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, Set

from backend.pipeline.connectors.base_connector import BaseConnector, ConnectorFetchError
from backend.pipeline.connectors.capabilities import Capability
from backend.pipeline.connectors._http import ConnectorHttpClient, parse_guarded
from backend.pipeline.models import OpportunityRaw

logger = logging.getLogger(__name__)

_REMOTIVE_URL = "https://remotive.com/api/remote-jobs"
_JOBICY_URL = "https://jobicy.com/api/v2/remote-jobs?count=50"
_REMOTEOK_URL = "https://remoteok.com/api"

# Sprint-1: Jobicy pagination (count + page) to move past the hardcoded
# first-50 cap. Kept bounded per run; ConnectorHttpClient throttles at 2/s.
_JOBICY_PER_PAGE = 50
_JOBICY_MAX_PAGES = 10

# jobLevel → canonical experience_level mapping (no regex inference).
_JOB_LEVEL_MAP = {
    "intern": "entry",
    "internship": "entry",
    "entry-level": "entry",
    "entry level": "entry",
    "junior": "junior",
    "mid": "mid",
    "mid-level": "mid",
    "senior": "senior",
    "lead": "lead",
    "staff": "lead",
    "principal": "lead",
}

# Explicit worldwide claims. NOTE: "remote" is deliberately absent — a bare
# "Remote" describes work mode, not eligibility; "Remote, USA only" must
# resolve to ["US"], not worldwide (see generic stopwords below).
_GEO_ALLOWLIST_HINTS = ("worldwide", "global", "anywhere")

# Work-mode words carrying zero eligibility signal; dropped before coding.
_GEO_GENERIC_TOKENS = frozenset({
    "remote", "hybrid", "onsite", "on-site", "wfh", "work from home",
})


class FreelanceConnector(BaseConnector):
    """Multi-Source Connector for Live Freelance Gigs & Contract Projects."""

    priority = 700
    version = "1.2.0"
    owner = "platform"
    capabilities: Set[Capability] = {Capability.FREELANCE, Capability.REMOTE}

    def __init__(self, http_client: Optional[ConnectorHttpClient] = None) -> None:
        self._http = http_client or ConnectorHttpClient(max_retries=2, rate_limit_per_sec=2.0)

    @property
    def source_slug(self) -> str:
        return "freelance_gigs"

    @property
    def display_name(self) -> str:
        return "Freelance & Remote Gigs Portal"

    @property
    def default_source_reputation(self) -> int:
        return 85

    def fetch_raw_opportunities(
        self, config: Dict[str, Any], query: Optional[str] = None
    ) -> List[OpportunityRaw]:
        """Fetch live freelance & contract listings from multi-source endpoints."""
        results: List[OpportunityRaw] = []
        seen_ids: Set[str] = set()
        seen_urls: Set[str] = set()
        successful_sources = 0
        failed_sources = 0

        # 1. Fetch Remotive API
        remotive_url = config.get("remotive_url") or _REMOTIVE_URL
        try:
            remotive_payload = self._http.get_json(remotive_url)
            jobs = remotive_payload.get("jobs", []) if isinstance(remotive_payload, dict) else []
            for item in jobs:
                parsed = parse_guarded(lambda it: self._to_raw_remotive(it, seen_ids, seen_urls), item, context="remotive")
                if parsed is not None:
                    results.append(parsed)
            successful_sources += 1
            logger.info("FreelanceConnector fetched %d items from Remotive.", len(results))
        except Exception as exc:
            logger.warning("FreelanceConnector fetch from Remotive failed: %s", exc)
            failed_sources += 1

        # 2. Fetch Jobicy API (cursor pagination)
        jobicy_url = config.get("jobicy_url") or "https://jobicy.com/api/v2/remote-jobs"
        jobicy_count = 0
        jobicy_pages_ok = 0
        jobicy_per_page = max(
            1, min(int(config.get("jobicy_per_page") or _JOBICY_PER_PAGE), 50)
        )
        jobicy_max_pages = max(
            1, int(config.get("jobicy_max_pages") or _JOBICY_MAX_PAGES)
        )
        jobicy_seen_ids: Set[str] = set()
        cursor: Optional[str] = None
        try:
            for page_no in range(1, jobicy_max_pages + 1):
                params: Dict[str, Any] = {"count": jobicy_per_page}
                if cursor:
                    params["cursor"] = cursor
                jobicy_payload = self._http.get_json(jobicy_url, params=params)
                jobicy_pages_ok += 1
                jobs = (
                    jobicy_payload.get("jobs", [])
                    if isinstance(jobicy_payload, dict)
                    else []
                )
                for item in jobs:
                    item_id = str(item.get("id") or "").strip()
                    if item_id and item_id in jobicy_seen_ids:
                        continue
                    if item_id:
                        jobicy_seen_ids.add(item_id)
                    parsed = parse_guarded(lambda it: self._to_raw_jobicy(it, seen_ids, seen_urls), item, context="jobicy")
                    if parsed is not None:
                        results.append(parsed)
                        jobicy_count += 1

                next_cursor = (
                    jobicy_payload.get("nextCursor")
                    if isinstance(jobicy_payload, dict)
                    else None
                )
                has_more = bool(
                    jobicy_payload.get("hasMore")
                    if isinstance(jobicy_payload, dict)
                    else False
                )
                if not jobs or not has_more or not next_cursor or next_cursor == cursor:
                    break
                cursor = str(next_cursor)
                if page_no == jobicy_max_pages:
                    logger.error(
                        "Jobicy hit max_pages=%d while hasMore=true; upstream "
                        "inventory exceeds crawl budget.",
                        jobicy_max_pages,
                    )
            if jobicy_pages_ok:
                successful_sources += 1
            logger.info(
                "FreelanceConnector fetched %d items from Jobicy (%d page(s)).",
                jobicy_count,
                jobicy_pages_ok,
            )
        except Exception as exc:
            if jobicy_pages_ok:
                # Keep the pages already fetched; a later-page error is not a
                # total source failure.
                successful_sources += 1
                logger.warning(
                    "FreelanceConnector Jobicy pagination stopped after %d page(s): %s",
                    jobicy_pages_ok,
                    exc,
                )
            else:
                logger.warning("FreelanceConnector fetch from Jobicy failed: %s", exc)
                failed_sources += 1

        # 3. Fetch RemoteOK API
        remoteok_url = config.get("remoteok_url") or _REMOTEOK_URL
        remoteok_count = 0
        try:
            remoteok_payload = self._http.get_json(remoteok_url)
            jobs = remoteok_payload if isinstance(remoteok_payload, list) else []
            for item in jobs:
                if not isinstance(item, dict) or not item.get("id"):
                    continue
                parsed = parse_guarded(lambda it: self._to_raw_remoteok(it, seen_ids, seen_urls), item, context="remoteok")
                if parsed is not None:
                    results.append(parsed)
                    remoteok_count += 1
            successful_sources += 1
            logger.info("FreelanceConnector fetched %d items from RemoteOK.", remoteok_count)
        except Exception as exc:
            logger.warning("FreelanceConnector fetch from RemoteOK failed: %s", exc)
            failed_sources += 1

        if failed_sources and not successful_sources:
            raise ConnectorFetchError(
                f"FreelanceGigs: all {failed_sources} configured endpoints failed "
                "(Remotive/Jobicy/RemoteOK blocked or unreachable)."
            )

        logger.info("FreelanceConnector total live items fetched: %d", len(results))
        return results

    def _to_raw_remotive(self, item: Dict[str, Any], seen_ids: Set[str], seen_urls: Set[str]) -> Optional[OpportunityRaw]:
        job_id = item.get("id")
        title = (item.get("title") or "").strip()
        url = (item.get("url") or "").strip()

        if not job_id or not title or not url or url in seen_urls:
            return None

        job_type = (item.get("job_type") or "").lower().strip()
        category = (item.get("category") or "").lower().strip()
        tags = [str(t).lower().strip() for t in item.get("tags", [])] if isinstance(item.get("tags"), list) else []
        title_lower = title.lower()

        is_freelance = (
            job_type in ["freelance", "contract"]
            or "freelance" in title_lower
            or "contract" in title_lower
            or "gig" in title_lower
            or "freelance" in category
            or "contract" in category
            or any("freelance" in t or "contract" in t for t in tags)
        )
        if not is_freelance:
            return None

        external_id = f"remotive_freelance_{job_id}"
        if external_id in seen_ids:
            return None
        seen_ids.add(external_id)
        seen_urls.add(url)

        # Geo leak fix: Remotive publishes candidate_required_location as a
        # free-text country list ("USA, UK, India, ..."). Without
        # normalization every row defaulted to worldwide, so USA-only roles
        # rendered in Indian feeds. Lists containing India stay visible.
        location_raw = item.get("candidate_required_location") or "Remote"

        return OpportunityRaw(
            source_slug=self.source_slug,
            external_id=external_id,
            title=title,
            company_name=(item.get("company_name") or "Remote Client").strip(),
            apply_url=url,
            raw_description=item.get("description") or title,
            raw_location=location_raw,
            raw_salary=item.get("salary"),
            raw_deadline=None,
            raw_posted_at=item.get("publication_date") or None,
            opportunity_type="job",
            geo_eligibility=_normalize_job_geo(location_raw),
            subcategory_raw="freelance/contract",
            raw_payload={
                "source": self.source_slug,
                "job_type": item.get("job_type"),
                "category": "freelance",
                "is_freelance": True,
                "raw_salary": item.get("salary"),
                "remotive_id": job_id,
                "published_at": item.get("publication_date"),
                "is_mock": False,
            },
        )

    def _to_raw_jobicy(self, item: Dict[str, Any], seen_ids: Set[str], seen_urls: Set[str]) -> Optional[OpportunityRaw]:
        job_id = item.get("id")
        title = (item.get("jobTitle") or "").strip()
        url = (item.get("url") or "").strip()

        if not job_id or not title or not url or url in seen_urls:
            return None

        # Sprint-1: explicit jobType array wins over title regex. Title
        # keywords are only a fallback when the array is absent.
        raw_types = item.get("jobType", [])
        if isinstance(raw_types, str):
            raw_types = [raw_types]
        raw_types = [str(t).lower().strip() for t in raw_types if str(t).strip()]
        title_lower = title.lower()

        if raw_types:
            is_contract = any("contract" in t or "freelance" in t for t in raw_types)
        else:
            is_contract = "contract" in title_lower or "freelance" in title_lower
        if not is_contract:
            return None

        external_id = f"jobicy_freelance_{job_id}"
        if external_id in seen_ids:
            return None
        seen_ids.add(external_id)
        seen_urls.add(url)

        # Experience level: direct mapping, no regex inference.
        level_raw = str(item.get("jobLevel") or "").strip()
        mapped_level = _JOB_LEVEL_MAP.get(level_raw.lower(), "entry") if level_raw else "entry"

        # Geographic eligibility: restricted geos stay explicit so they never
        # collapse to NULL (unrestricted) downstream.
        geo_raw = item.get("jobGeo") or ""
        geo_eligibility = _normalize_job_geo(geo_raw)

        # Structured compensation: period-aware, no string munging.
        salary_min = _to_float_or_none(item.get("salaryMin") or item.get("annualSalaryMin"))
        salary_max = _to_float_or_none(item.get("salaryMax") or item.get("annualSalaryMax"))
        salary_currency = str(item.get("salaryCurrency") or "USD").strip().upper()
        salary_period = str(item.get("salaryPeriod") or "year").strip().lower()
        raw_salary = _format_structured_salary(salary_min, salary_max, salary_currency, salary_period)

        industry = str(item.get("jobIndustry") or "").strip()

        return OpportunityRaw(
            source_slug=self.source_slug,
            external_id=external_id,
            title=title,
            company_name=(item.get("companyName") or "Remote Client").strip(),
            apply_url=url,
            raw_description=item.get("jobDescription") or title,
            raw_location=geo_raw or "Remote",
            raw_salary=raw_salary,
            raw_deadline=None,
            raw_posted_at=item.get("pubDate") or None,
            opportunity_type="job",
            geo_eligibility=geo_eligibility,
            salary_period=salary_period,
            salary_min=salary_min,
            salary_max=salary_max,
            salary_currency=salary_currency,
            experience_level_raw=level_raw or None,
            subcategory_raw="freelance/contract",
            raw_payload={
                "source": self.source_slug,
                "job_type": "contract",
                "category": "freelance",
                "is_freelance": True,
                "jobicy_id": job_id,
                "published_at": item.get("pubDate"),
                "job_level": level_raw or None,
                "mapped_experience_level": mapped_level,
                "job_geo": geo_raw,
                "job_industry": industry or None,
                "salary_min": salary_min,
                "salary_max": salary_max,
                "salary_currency": salary_currency,
                "salary_period": salary_period,
                "is_mock": False,
            },
        )

    def _to_raw_remoteok(self, item: Dict[str, Any], seen_ids: Set[str], seen_urls: Set[str]) -> Optional[OpportunityRaw]:
        job_id = item.get("id")
        title = (item.get("position") or "").strip()
        url = (item.get("url") or item.get("apply_url") or "").strip()

        if not job_id or not title or not url or url in seen_urls:
            return None

        tags = [str(t).lower() for t in item.get("tags", [])] if isinstance(item.get("tags"), list) else []
        title_lower = title.lower()

        is_contract = (
            "contract" in tags
            or "freelance" in tags
            or "contract" in title_lower
            or "freelance" in title_lower
            or "gig" in title_lower
        )
        if not is_contract:
            return None

        external_id = f"remoteok_freelance_{job_id}"
        if external_id in seen_ids:
            return None
        seen_ids.add(external_id)
        seen_urls.add(url)

        location_raw = item.get("location") or "Remote"

        return OpportunityRaw(
            source_slug=self.source_slug,
            external_id=external_id,
            title=title,
            company_name=(item.get("company") or "Remote Client").strip(),
            apply_url=url,
            raw_description=item.get("description") or title,
            raw_location=location_raw,
            raw_salary=f"${item.get('salary_min', '')}-${item.get('salary_max', '')}".strip("$- ") or None,
            raw_deadline=None,
            raw_posted_at=item.get("date") or None,
            opportunity_type="job",
            required_skills=[str(t) for t in tags if str(t).strip()],
            geo_eligibility=_normalize_job_geo(location_raw),
            salary_min=_to_float_or_none(item.get("salary_min")),
            salary_max=_to_float_or_none(item.get("salary_max")),
            salary_currency="USD",
            salary_period="year",
            subcategory_raw="freelance/contract",
            raw_payload={
                "source": self.source_slug,
                "job_type": "contract",
                "category": "freelance",
                "is_freelance": True,
                "raw_salary": f"${item.get('salary_min', '')}-${item.get('salary_max', '')}".strip("$- ") or None,
                "salary_min": _to_float_or_none(item.get("salary_min")),
                "salary_max": _to_float_or_none(item.get("salary_max")),
                "salary_currency": "USD",
                "salary_period": "year",
                "remoteok_id": job_id,
                "published_at": item.get("date"),
                "is_mock": False,
            },
        )


def _to_float_or_none(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        num = float(str(value).replace(",", "").strip().split()[0])
        return num
    except (TypeError, ValueError, IndexError):
        return None


def _format_structured_salary(
    salary_min: Optional[float],
    salary_max: Optional[float],
    currency: str,
    period: str,
) -> Optional[str]:
    parts = [p for p in (salary_min, salary_max) if p is not None]
    if not parts:
        return None
    if len(parts) == 2:
        return f"{parts[0]:,.0f}-{parts[1]:,.0f} {currency}/{period}"
    return f"{parts[0]:,.0f} {currency}/{period}"


def _normalize_job_geo(geo_raw: Any) -> List[str]:
    """Normalize a jobGeo/location string to eligibility codes.

    Worldwide/remote-style values collapse to ["GLOBAL"]; restricted values
    stay explicit (e.g. ["US"]) so they never default to NULL/unrestricted.
    Empty input yields [] (adapter-unknown); the normalizer layer decides the
    DB NULL, keeping "unknown" distinct from "worldwide" at this stage.
    """
    text = str(geo_raw or "").strip()
    if not text:
        return []
    lowered = text.lower()
    if any(h in lowered for h in _GEO_ALLOWLIST_HINTS):
        return ["GLOBAL"]
    codes: List[str] = []
    for token in lowered.replace("/", ",").split(","):
        token = token.strip()
        if not token or token in _GEO_GENERIC_TOKENS:
            continue
        if "usa" in token or token in ("us", "us only", "u.s.", "united states", "united states only", "usa only", "us-only", "usa-only", "us-citizen-only", "us citizen only"):
            codes.append("US")
        elif token in ("uk", "u.k.", "united kingdom"):
            codes.append("UK")
        elif "canada" in token or token == "ca":
            codes.append("CA")
        elif "india" in token or token in ("in",):
            codes.append("IN")
        elif "europe" in token or token == "eu":
            codes.append("EU")
        else:
            codes.append(token.upper()[:32])
    seen: Set[str] = set()
    deduped = [c for c in codes if not (c in seen or seen.add(c))]
    return deduped or ["GLOBAL"]


__all__ = ["FreelanceConnector"]
