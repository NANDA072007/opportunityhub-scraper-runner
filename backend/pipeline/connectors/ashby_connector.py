"""
Ashby ATS Direct Connector (`jobs.ashbyhq.com`) for OpportunityHub.
Path: `backend/pipeline/connectors/ashby_connector.py`

Queries Ashby's public Job Posting API (`api.ashbyhq.com/posting-api/job-board/{slug}`)
with `includeCompensation=true` to capture fresh internships and engineering roles
48-72 hours before secondary job boards.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    import httpx
except ImportError:
    httpx = None  # type: ignore

try:
    import requests
except ImportError:
    requests = None  # type: ignore

from backend.pipeline.connectors.base_connector import BaseConnector
from backend.pipeline.connectors.capabilities import Capability
from backend.pipeline.models import OpportunityNormalized, OpportunityRaw

logger = logging.getLogger(__name__)

ASHBY_API_TEMPLATE = "https://api.ashbyhq.com/posting-api/job-board/{board_slug}?includeCompensation=true"

# High-signal engineering & AI companies hosted on Ashby that hire in India or Remote.
# Can be extended via environment variable `ASHBY_BOARD_SLUGS="slug1,slug2,..."`
DEFAULT_ASHBY_BOARDS: List[Tuple[str, str]] = [
    ("supabase", "Supabase"),
    ("posthog", "PostHog"),
    ("linear", "Linear"),
    ("notion", "Notion"),
    ("perplexity", "Perplexity AI"),
    ("elevenlabs", "ElevenLabs"),
    ("replit", "Replit"),
    ("langfuse", "Langfuse"),
    ("resend", "Resend"),
    ("cal", "Cal.com"),
    ("dub", "Dub"),
    ("deel", "Deel"),
    ("airwallex", "Airwallex"),
    ("ramp", "Ramp"),
    ("vanta", "Vanta"),
    ("openai", "OpenAI"),
    ("snowflake", "Snowflake"),
    ("zed", "Zed Industries"),
    ("raycast", "Raycast"),
    ("ashby", "Ashby"),
]

INDIA_GEO_REGEX = re.compile(
    r"\b(india|bengaluru|bangalore|hyderabad|mumbai|pune|chennai|delhi|ncr|"
    r"gurugram|gurgaon|noida|kolkata|ahmedabad|kochi|remote|anywhere|global|apac)\b",
    re.IGNORECASE,
)

US_EU_RESTRICTED_REMOTE_REGEX = re.compile(
    r"\b(us\s+only|usa\s+only|united\s+states\s+only|uk\s+only|eu\s+only|"
    r"must\s+reside\s+in\s+the\s+(?:us|united\s+states)|u\.s\.\s+citizens?\s+only)\b",
    re.IGNORECASE,
)

INTERN_TITLE_REGEX = re.compile(
    r"\b(intern|internship|trainee|apprentice|fellowship|new\s+grad|university\s+grad|"
    r"graduate\s+engineer|sde\s*[-–]?\s*1|entry\s+level|fresher)\b",
    re.IGNORECASE,
)

SENIOR_TITLE_REGEX = re.compile(
    r"\b(senior|staff|principal|lead|manager|director|vp|head\s+of|architect|sr\.?)\b",
    re.IGNORECASE,
)

EXECUTIVE_ROLE_REGEX = re.compile(
    r"\b(head\s+of|vp|vice\s+president|director|chief|c-level|cxo)\b",
    re.IGNORECASE,
)

RELEVANT_DEPT_REGEX = re.compile(
    r"\b(eng|software|product|design|data|ai|ml|machine\s+learning|research|"
    r"security|infra|devops|cloud|platform|mobile|frontend|backend|fullstack|"
    r"developer|technical|analytics|it)\b",
    re.IGNORECASE,
)


class AshbyConnector(BaseConnector):
    """Direct connector for Ashby public job boards."""

    priority: int = 850
    version: str = "2.0.0"
    owner: str = "platform"
    capabilities: Set[Capability] = {
        Capability.JOB,
        Capability.INTERNSHIP,
        Capability.REMOTE,
        Capability.HYBRID,
    }

    default_category: str = "private"

    def __init__(
        self,
        board_targets: Optional[List[Tuple[str, str]]] = None,
        india_or_remote_only: bool = True,
        tech_roles_only: bool = True,
        timeout: int = 15,
        session: Optional[Any] = None,
        client: Optional[Any] = None,
        http_client: Optional[Any] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs) if hasattr(super(), "__init__") else None
        self.india_or_remote_only = india_or_remote_only
        self.tech_roles_only = tech_roles_only
        self.timeout = timeout
        self.board_targets = board_targets or self._load_board_targets()

        active_client = session or client or http_client
        if active_client is not None:
            self.session = active_client
        elif httpx is not None:
            self.session = httpx.Client(
                timeout=self.timeout,
                follow_redirects=True,
                headers={
                    "Accept": "application/json",
                    "User-Agent": "OpportunityHub-ATS-Ingestor/2.0",
                },
            )
        elif requests is not None:
            self.session = requests.Session()
            self.session.headers.update(
                {
                    "Accept": "application/json",
                    "User-Agent": "OpportunityHub-ATS-Ingestor/2.0",
                }
            )
        else:
            raise RuntimeError("Neither httpx nor requests is installed.")

    @property
    def source_slug(self) -> str:
        return "ashby"

    @property
    def display_name(self) -> str:
        return "Ashby ATS"

    @property
    def default_source_reputation(self) -> int:
        return 92

    @staticmethod
    def _load_board_targets() -> List[Tuple[str, str]]:
        env_slugs = os.environ.get("ASHBY_BOARD_SLUGS", "").strip()
        if not env_slugs:
            return list(DEFAULT_ASHBY_BOARDS)
        custom: List[Tuple[str, str]] = []
        for raw_slug in env_slugs.split(","):
            slug = raw_slug.strip().lower()
            if slug:
                custom.append((slug, slug.replace("-", " ").title()))
        return custom or list(DEFAULT_ASHBY_BOARDS)

    # -------------------------------------------------------------------------
    # Public Entrypoints (compatible with BaseConnector & PipelineOrchestrator)
    # -------------------------------------------------------------------------
    def fetch_raw_opportunities(
        self, config: Dict[str, Any], query: Optional[str] = None
    ) -> List[OpportunityRaw]:
        """Fetch raw items for full ingestion pipeline orchestrator."""
        raw_items: List[OpportunityRaw] = []
        query_lower = query.lower().strip() if query else None

        boards = config.get("board_targets") or self.board_targets
        for board_slug, company_name in boards:
            jobs = self._fetch_board_jobs(board_slug)
            for job in jobs:
                if not job.get("isListed", True):
                    continue
                try:
                    opp_raw = self._normalize_job_to_raw(job, board_slug=board_slug, company_name=company_name)
                    if opp_raw is None:
                        continue
                    if query_lower and query_lower not in f"{opp_raw.title} {opp_raw.raw_description}".lower():
                        continue
                    raw_items.append(opp_raw)
                except Exception as exc:
                    logger.warning(f"[ashby:{board_slug}] Error extracting raw job {job.get('id')}: {exc}")

        logger.info(f"[ashby] Fetched {len(raw_items)} raw opportunities across {len(boards)} boards.")
        return raw_items

    def fetch_opportunities(self, query: Optional[str] = None) -> List[OpportunityNormalized]:
        """Primary standalone entrypoint returning normalized opportunities."""
        results: List[OpportunityNormalized] = []
        query_lower = query.lower().strip() if query else None

        for board_slug, company_name in self.board_targets:
            jobs = self._fetch_board_jobs(board_slug)
            for job in jobs:
                if not job.get("isListed", True):
                    continue
                try:
                    opp = self._normalize_job(job, board_slug=board_slug, company_name=company_name)
                    if opp is None:
                        continue
                    if query_lower and query_lower not in f"{opp.title} {opp.description}".lower():
                        continue
                    results.append(opp)
                except Exception as exc:
                    logger.warning(f"[ashby:{board_slug}] Error normalizing job {job.get('id')}: {exc}")

        logger.info(f"[ashby] Prepared {len(results)} high-signal opportunities across {len(self.board_targets)} boards.")
        return results

    # Aliases
    def fetch(self, query: Optional[str] = None) -> List[OpportunityNormalized]:
        return self.fetch_opportunities(query=query)

    def scrape(self, query: Optional[str] = None) -> List[OpportunityNormalized]:
        return self.fetch_opportunities(query=query)

    # -------------------------------------------------------------------------
    # Board HTTP Fetcher
    # -------------------------------------------------------------------------
    def _fetch_board_jobs(self, board_slug: str) -> List[Dict[str, Any]]:
        url = ASHBY_API_TEMPLATE.format(board_slug=board_slug)
        try:
            resp = self.session.get(url, timeout=self.timeout)
            if resp.status_code == 404:
                logger.debug(f"[ashby] Board '{board_slug}' not found (404).")
                return []
            if resp.status_code != 200:
                logger.warning(f"[ashby] HTTP {resp.status_code} for board '{board_slug}'")
                return []
            payload = resp.json()
            jobs = payload.get("jobs")
            return jobs if isinstance(jobs, list) else []
        except Exception as exc:
            logger.warning(f"[ashby] Failed fetching board '{board_slug}': {exc}")
            return []

    # -------------------------------------------------------------------------
    # Normalization & Geo/Student Filtering
    # -------------------------------------------------------------------------
    def _parse_job_components(
        self,
        job: Dict[str, Any],
        board_slug: str,
        company_name: str,
    ) -> Optional[Dict[str, Any]]:
        job_id = str(job.get("id") or "").strip()
        title = str(job.get("title") or "").strip()
        if not job_id or not title:
            return None

        department = str(job.get("department") or "").strip()
        team = str(job.get("team") or "").strip()

        # 1. Tech/Student Relevance Filter
        employment_type = str(job.get("employmentType") or "FullTime").strip()
        is_intern_or_grad = employment_type.lower() == "intern" or bool(INTERN_TITLE_REGEX.search(title))

        if self.tech_roles_only and not is_intern_or_grad:
            dept_title_blob = f"{title} {department} {team}"
            if not RELEVANT_DEPT_REGEX.search(dept_title_blob):
                return None

        # 2. Resolve Locations & Remote Eligibility
        primary_loc = str(job.get("location") or "").strip()
        secondary_locs = [
            str(s.get("location") or "").strip()
            for s in (job.get("secondaryLocations") or [])
            if isinstance(s, dict) and s.get("location")
        ]
        address_country = (
            (job.get("address") or {}).get("postalAddress", {}).get("addressCountry") or ""
        ).strip()

        all_locs = [loc for loc in [primary_loc, *secondary_locs, address_country] if loc]
        location_str = " | ".join(dict.fromkeys(all_locs)) if all_locs else "Remote"

        workplace_type = str(job.get("workplaceType") or "").strip()
        is_remote = bool(
            job.get("isRemote")
            or workplace_type.lower() == "remote"
            or "remote" in location_str.lower()
        )

        # 3. India & Global Remote Geo-Filter
        if self.india_or_remote_only:
            has_india_or_remote = bool(INDIA_GEO_REGEX.search(location_str)) or is_remote
            if not has_india_or_remote:
                return None
            if is_remote and US_EU_RESTRICTED_REMOTE_REGEX.search(location_str) and "india" not in location_str.lower():
                return None

        # 4. Category & Experience Level Classification
        if is_intern_or_grad:
            category = "student"
            experience_level = "entry"
            opp_type = "internship"
        elif employment_type.lower() in ("contract", "temporary"):
            category = "freelance"
            experience_level = "any"
            opp_type = "job"
        else:
            category = "private"
            opp_type = "job"
            if SENIOR_TITLE_REGEX.search(title):
                experience_level = "senior"
            else:
                experience_level = "mid"

        # 5. Extract Description & Compensation
        description_plain = str(job.get("descriptionPlain") or "").strip()
        if not description_plain:
            html_desc = str(job.get("descriptionHtml") or "")
            description_plain = re.sub(r"<[^>]+>", " ", html_desc).strip()

        if len(description_plain) < 100:
            return None

        salary_text = self._extract_compensation_text(job.get("compensation"))
        apply_url = str(
            job.get("applyUrl")
            or job.get("jobUrl")
            or f"https://jobs.ashbyhq.com/{board_slug}/{job_id}"
        ).strip()
        posted_at = self._parse_iso_datetime(job.get("publishedAt")) or datetime.now(timezone.utc)

        # 6. Assemble Seed Tags
        seed_tags = [t for t in [department, team, workplace_type, employment_type] if t]
        title_lower = title.lower()
        if is_intern_or_grad:
            seed_tags.append("Internship")
        if any(w in title_lower for w in ("software", "engineer", "developer", "architect")):
            seed_tags.extend(["Software Engineering", "Programming"])
        if any(w in title_lower for w in ("ai", "ml", "machine learning", "data")):
            seed_tags.extend(["AI/ML", "Data Science"])
        if any(w in title_lower for w in ("backend", "platform", "infra", "systems", "cloud")):
            seed_tags.extend(["Backend", "Cloud Computing"])
        if any(w in title_lower for w in ("frontend", "fullstack", "full stack", "web")):
            seed_tags.extend(["Web Development", "Full Stack"])
        
        # Deduplicate tags
        seed_tags = list(dict.fromkeys(seed_tags))

        enriched_raw = {
            **job,
            "board_slug": board_slug,
            "tags": seed_tags,
            "skills": seed_tags,
            "salary_text": salary_text,
            "subcategory": "internship" if is_intern_or_grad else "full_time",
        }

        return {
            "job_id": job_id,
            "external_id": f"ashby-{board_slug}-{job_id}",
            "title": title,
            "company_name": company_name,
            "department": department,
            "team": team,
            "location_str": location_str,
            "is_remote": is_remote,
            "category": category,
            "experience_level": experience_level,
            "opp_type": opp_type,
            "is_intern_or_grad": is_intern_or_grad,
            "description_plain": description_plain,
            "salary_text": salary_text,
            "apply_url": apply_url,
            "posted_at": posted_at,
            "workplace_type": workplace_type,
            "employment_type": employment_type,
            "seed_tags": seed_tags,
            "enriched_raw": enriched_raw,
        }

    def _normalize_job_to_raw(
        self,
        job: Dict[str, Any],
        board_slug: str,
        company_name: str,
    ) -> Optional[OpportunityRaw]:
        parsed = self._parse_job_components(job, board_slug, company_name)
        if not parsed:
            return None

        return OpportunityRaw(
            source_slug=self.source_slug,
            external_id=parsed["external_id"],
            title=parsed["title"],
            company_name=parsed["company_name"],
            apply_url=parsed["apply_url"],
            raw_description=parsed["description_plain"],
            raw_location=parsed["location_str"],
            raw_salary=parsed["salary_text"],
            raw_posted_at=parsed["posted_at"].isoformat() if parsed["posted_at"] else None,
            opportunity_type=parsed["opp_type"],
            department=parsed["department"] or None,
            subcategory_raw="internship" if parsed["is_intern_or_grad"] else "full_time",
            required_skills=parsed["seed_tags"],
            raw_payload=parsed["enriched_raw"],
        )

    def _normalize_job(
        self,
        job: Dict[str, Any],
        board_slug: str,
        company_name: str,
    ) -> Optional[OpportunityNormalized]:
        parsed = self._parse_job_components(job, board_slug, company_name)
        if not parsed:
            return None

        # Executive roles are out of scope for a student entry-level feed.
        if EXECUTIVE_ROLE_REGEX.search(parsed["title"] or ""):
            return None

        salary_text = (
            parsed["salary_text"]
            or "Competitive Startup Equity & Market Salary (Direct ATS)"
        )

        department = parsed["department"]
        team = parsed["team"]
        workplace_type = parsed["workplace_type"]
        employment_type = parsed["employment_type"]
        is_intern_or_grad = parsed["is_intern_or_grad"]

        return self._instantiate_normalized(
            id=None,
            external_id=parsed["external_id"],
            source_slug=self.source_slug,
            title=parsed["title"],
            company=parsed["company_name"],
            location=parsed["location_str"],
            description=parsed["description_plain"],
            requirements=f"Department: {department or 'Engineering'} | Team: {team or 'Core'} | Mode: {workplace_type or 'Flexible'}",
            apply_url=parsed["apply_url"],
            category=parsed["category"],
            experience_level=parsed["experience_level"],
            opportunity_type=parsed["opp_type"],
            salary=salary_text,
            salary_range=salary_text,
            tags=parsed["seed_tags"],
            skills_required=parsed["seed_tags"],
            is_remote=parsed["is_remote"],
            posted_at=parsed["posted_at"],
            deadline=None,
            raw_payload=parsed["enriched_raw"],
            source_reputation=92,
            trust_score=92,
            quality_score=80,
            metadata={
                "department": department,
                "team": team,
                "workplace_type": workplace_type,
                "employment_type": employment_type,
                "subcategory_raw": "internship" if is_intern_or_grad else "full_time",
                "is_direct_ats": True,
            },
        )

    @staticmethod
    def _extract_compensation_text(comp: Any) -> Optional[str]:
        if not isinstance(comp, dict):
            return None
        summary = comp.get("compensationTierSummary") or comp.get("scrape_compensation")
        if summary and isinstance(summary, str):
            return summary.strip()

        components = comp.get("summaryComponents") or []
        parts: List[str] = []
        for c in components:
            if not isinstance(c, dict):
                continue
            min_v = c.get("minValue")
            max_v = c.get("maxValue")
            curr = c.get("currencyCode") or "USD"
            interval = c.get("interval") or "YEAR"
            if min_v and max_v:
                parts.append(f"{curr} {int(min_v):,} - {int(max_v):,} / {interval.lower()}")
            elif min_v:
                parts.append(f"{curr} {int(min_v):,} / {interval.lower()}")
        return " | ".join(parts) if parts else None

    @staticmethod
    def _parse_iso_datetime(val: Any) -> Optional[datetime]:
        if not val or not isinstance(val, str):
            return None
        try:
            cleaned = val.strip().replace("Z", "+00:00")
            dt = datetime.fromisoformat(cleaned)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return None

    @staticmethod
    def _instantiate_normalized(**fields: Any) -> OpportunityNormalized:
        data = dict(fields)
        if "id" not in data:
            data["id"] = None
        if "salary" in data and "salary_range" not in data:
            data["salary_range"] = data.pop("salary")
        if "tags" in data and "skills_required" not in data:
            data["skills_required"] = data.get("tags") or []
        if "skills" in data and "skills_required" not in data:
            data["skills_required"] = data.get("skills") or []

        allowed = set(OpportunityNormalized.__dataclass_fields__.keys())
        filtered = {k: v for k, v in data.items() if k in allowed}
        return OpportunityNormalized(**filtered)


__all__ = ["AshbyConnector"]
