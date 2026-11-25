"""
Greenhouse ATS Connector (High-Signal Student & Early Career Edition)
=====================================================================
Fetches corporate career opportunities from Greenhouse ATS public board
endpoints. Combines deep compensation mining and collision-resistant
deduplication with student signal sanitation:
  1. Filters out senior/leadership roles and 4+ years experience walls.
  2. Rejects 'Prospect' resume-harvesting posts (missing active requisition).
  3. Resolves custom metadata location (remote vs hybrid vs on-site).
  4. Extracts target graduation years (2025-2030) and degree badges.
  5. Mines structured pay from metadata custom fields & content.

Board tokens are read from source ``config`` (``{"boards": [...]}``) or
fall back to a curated default list of high-value public Greenhouse boards.
"""

from __future__ import annotations

import html
import logging
import re
from typing import Any, Dict, List, Optional, Set, Tuple

from bs4 import BeautifulSoup
import httpx

from backend.pipeline.models import OpportunityRaw
from backend.pipeline.connectors.base_connector import BaseConnector, ConnectorFetchError
from backend.pipeline.connectors.capabilities import Capability
from backend.pipeline.connectors._http import ConnectorHttpClient, parse_guarded
from backend.pipeline.connectors._pay import mine_pay_text, mine_structured_pay

logger = logging.getLogger(__name__)

# Keywords to identify internships and student-friendly programs
INTERNSHIP_KEYWORDS: Tuple[str, ...] = (
    "intern",
    "internship",
    "co-op",
    "coop",
    "trainee",
    "apprentice",
    "summer analyst",
    "student",
    "graduate",
    "fresher",
)

# Keywords indicating senior/leadership roles that must be excluded for students/freshers
SENIOR_EXCLUSION_KEYWORDS: Tuple[str, ...] = (
    "senior",
    "sr.",
    "sr",
    "staff",
    "principal",
    "lead",
    "manager",
    "director",
    "vp",
    "vice president",
    "head of",
    "architect",
    "iii",
    "iv",
)

# Shape of a usable requisition id: an ID token (no whitespace) containing a digit
_REQ_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def _stable_requisition_id(*candidates: Any, fallback: str) -> str:
    """Return the first ID-shaped requisition id, else ``fallback``.

    Some employers publish a shared placeholder requisition id for every job
    on the board (Stripe: "See Opening ID" x765, Airbnb: "ONE" x167). Keying
    ``external_id`` on a placeholder collapses hundreds of distinct jobs into
    one key, which store-stage dedup then silently reduces to a single row.
    ``fallback`` is the job's own id: always unique, so the worst case for a
    misjudged id is a less stable key, never a collapsed one.
    """
    for candidate in candidates:
        if candidate is None:
            continue
        value = str(candidate).strip()
        if value and _REQ_ID_RE.match(value) and any(ch.isdigit() for ch in value):
            return value
    return fallback


class GreenhouseConnector(BaseConnector):
    """Greenhouse ATS connector for student and entry-level corporate career opportunities."""

    priority = 900
    version = "2.0.0"
    owner = "platform"
    capabilities: Set[Capability] = {
        Capability.JOB,
        Capability.INTERNSHIP,
        Capability.REMOTE,
        Capability.HYBRID,
        Capability.ONSITE,
    }
    supports_full_sync = True
    supports_incremental_sync = False
    estimated_rate_limit = 2.0

    DEFAULT_BOARDS = [
        {"name": "Stripe", "token": "stripe"},
        {"name": "Figma", "token": "figma"},
        {"name": "Razorpay", "token": "razorpay"},
        {"name": "Cloudflare", "token": "cloudflare"},
        {"name": "Databricks", "token": "databricks"},
        {"name": "Airbnb", "token": "airbnb"},
        {"name": "Vercel", "token": "vercel"},
        {"name": "Datadog", "token": "datadog"},
        {"name": "Coinbase", "token": "coinbase"},
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
        return "greenhouse"

    @property
    def display_name(self) -> str:
        return "Greenhouse ATS"

    @property
    def default_source_reputation(self) -> int:
        return 100

    # -------------------------------------------------------------------------
    # HTML and Text Sanitation
    # -------------------------------------------------------------------------

    @staticmethod
    def clean_html_description(raw_content: Optional[str]) -> str:
        """Unescape HTML entities and extract clean, multi-line plain text."""
        if not raw_content:
            return ""
        unescaped = html.unescape(raw_content)
        if "<" not in unescaped:
            return unescaped.strip()
        soup = BeautifulSoup(unescaped, "html.parser")
        lines = [
            line.strip()
            for line in soup.get_text(separator="\n").splitlines()
            if line.strip()
        ]
        return "\n".join(lines)

    # -------------------------------------------------------------------------
    # Student Signal Sanitation & Anti-Noise Gatekeeping
    # -------------------------------------------------------------------------

    @staticmethod
    def is_student_friendly(
        title: str,
        clean_text: str,
        internal_job_id: Any = None,
        has_internal_id_key: bool = False,
    ) -> Tuple[bool, str]:
        """
        Anti-noise & eligibility filter:
        1. Rejects 'Prospect' posts (internal_job_id is None) used only for resume harvesting.
        2. Rejects senior/leadership titles (word-boundary matched).
        3. Flags non-internship roles explicitly requiring 4+ years of experience.
        """
        # Prospect posts check (when internal_job_id is explicitly None)
        if has_internal_id_key and internal_job_id is None:
            return False, "Prospect/Evergreen resume pool (no active requisition)"

        title_lower = title.lower()
        is_internship = any(kw in title_lower for kw in INTERNSHIP_KEYWORDS)

        # Word-boundary check for senior/leadership titles
        for kw in SENIOR_EXCLUSION_KEYWORDS:
            pattern = r"\b" + re.escape(kw) + r"\b"
            if re.search(pattern, title_lower):
                return False, f"Senior/Leadership role ({kw})"

        # Check description for heavy experience walls (e.g. '5+ years of experience')
        # Skip experience wall check if role is an explicit internship/co-op
        if not is_internship and clean_text:
            text_lower = clean_text.lower()
            exp_matches = re.findall(
                r"(\d+)\s*\+?\s*(?:to|-)\s*\d*\s*years?\s+(?:of\s+)?experience",
                text_lower,
            )
            single_exp_matches = re.findall(
                r"(\d+)\+\s*years?\s+(?:of\s+)?experience", text_lower
            )
            all_years = [int(y) for y in exp_matches + single_exp_matches if y.isdigit()]
            if all_years and min(all_years) >= 4:
                return False, f"Requires {min(all_years)}+ years experience"

        return True, "Verified student/entry-level eligible"

    # -------------------------------------------------------------------------
    # Location & Metadata Enrichment
    # -------------------------------------------------------------------------

    @staticmethod
    def resolve_location(job: Dict[str, Any]) -> Dict[str, str]:
        """Resolve accurate location and work mode from primary location and custom metadata."""
        primary_loc = ""
        loc_val = job.get("location")
        if isinstance(loc_val, dict):
            primary_loc = (loc_val.get("name") or "").strip()
        elif loc_val:
            primary_loc = str(loc_val).strip()

        metadata = job.get("metadata") or []

        # Greenhouse often stores city/country in custom metadata when location is just 'Remote'
        metadata_loc = ""
        if isinstance(metadata, list):
            for item in metadata:
                if isinstance(item, dict) and item.get("name") == "Job Posting Location":
                    val = item.get("value")
                    if isinstance(val, list):
                        metadata_loc = ", ".join(str(v) for v in val if v)
                    elif val:
                        metadata_loc = str(val).strip()
                    break

        final_location = primary_loc or metadata_loc or "Remote"
        if metadata_loc and primary_loc.lower() in ("remote", "hybrid", "in-office", "onsite"):
            final_location = f"{primary_loc} ({metadata_loc})"

        combined_lower = f"{primary_loc} {metadata_loc}".lower()
        if any(term in combined_lower for term in ("remote", "wfh", "work from home")):
            work_mode = "Remote"
        elif "hybrid" in combined_lower:
            work_mode = "Hybrid"
        elif primary_loc:
            work_mode = "On-Site"
        else:
            work_mode = "Remote"

        return {"location": final_location, "work_mode": work_mode}

    @staticmethod
    def extract_eligibility_highlights(clean_text: str) -> Dict[str, Any]:
        """Extract degree mentions and target graduation years for card badges."""
        grad_years = sorted(set(re.findall(r"\b(202[5-9]|2030)\b", clean_text)))
        degrees: List[str] = []
        degree_patterns = {
            "B.Tech / B.E.": r"\b(b\.?tech|b\.?e\.?|bachelor of engineering|bachelor of technology)\b",
            "B.Sc / BCA": r"\b(b\.?sc|bca|bachelor of science|computer applications)\b",
            "M.Tech / MCA": r"\b(m\.?tech|mca|master of technology|master of computer)\b",
            "CS / IT Background": r"\b(computer science|information technology|software engineering)\b",
        }
        text_lower = clean_text.lower()
        for label, pattern in degree_patterns.items():
            if re.search(pattern, text_lower):
                degrees.append(label)

        return {
            "target_grad_years": grad_years,
            "degree_tags": degrees,
        }

    # -------------------------------------------------------------------------
    # Core Fetch & Parse Pipeline
    # -------------------------------------------------------------------------

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
                items = self._fetch_board(board.get("name", token), token, query)
            except Exception as exc:
                logger.warning("Greenhouse board '%s' failed: %s", token, exc)
                failed_boards += 1
                continue
            successful_boards += 1
            results.extend(items)

        if failed_boards and not successful_boards:
            raise ConnectorFetchError(
                f"Greenhouse: all {failed_boards} configured boards failed "
                "(API blocked or unreachable)."
            )

        logger.info("GreenhouseConnector fetched %d total student-eligible items.", len(results))
        return results

    def _fetch_board(
        self, company_name: str, token: str, query: Optional[str]
    ) -> List[OpportunityRaw]:
        url = f"https://api.greenhouse.io/v1/boards/{token}/embed/jobs"
        params = {"content": "true"}
        if query:
            params["query"] = query

        data = self._http.get_json(url, params=params)

        jobs = data.get("jobs") or []
        items: List[OpportunityRaw] = []
        for job in jobs:
            parsed = parse_guarded(
                lambda j: self._parse_job(company_name, token, j),
                job,
                context="greenhouse",
            )
            if parsed is not None:
                items.append(parsed)
        return items

    @classmethod
    def _parse_job(
        cls, company_name: str, token: str, job: Dict[str, Any]
    ) -> Optional[OpportunityRaw]:
        job_id = str(job.get("id") or "")
        if not job_id:
            return None
        title = (job.get("title") or "").strip()
        if not title:
            return None

        raw_content = job.get("content") or f"<p>Career opportunity at {company_name} for {title}.</p>"
        clean_desc = cls.clean_html_description(raw_content)

        # 1. Student-friendly eligibility check (filters out seniors & prospect resume pools)
        internal_job_id = job.get("internal_job_id")
        has_internal_id = "internal_job_id" in job
        is_valid, filter_reason = cls.is_student_friendly(
            title, clean_desc, internal_job_id, has_internal_id
        )
        if not is_valid:
            logger.debug(
                "Filtered out Greenhouse job '%s' (%s): %s", title, company_name, filter_reason
            )
            return None

        # 2. Requisition ID with placeholder collision protection
        requisition_id = _stable_requisition_id(
            job.get("requisition_id"),
            internal_job_id,
            fallback=job_id,
        )
        external_id = f"gh_{token}_{requisition_id}"

        # 3. Location and Work Mode Resolution
        loc_info = cls.resolve_location(job)
        apply_url = job.get("absolute_url") or f"https://boards.greenhouse.io/{token}/jobs/{job_id}"

        # 4. Department hierarchy
        departments = job.get("departments") or []
        department = None
        if isinstance(departments, list) and departments:
            first = departments[0]
            department = first.get("name") if isinstance(first, dict) else str(first)
        offices = job.get("offices") or []

        # 5. Application deadline & pay mining
        raw_deadline = job.get("application_deadline") or job.get("deadline")
        raw_salary = cls._extract_pay(job, raw_content)

        # 6. Classification & Eligibility Badges
        title_lower = title.lower()
        is_internship = any(kw in title_lower for kw in INTERNSHIP_KEYWORDS)
        opp_type = "internship" if is_internship else "job"
        subcategory = "internship" if is_internship else "private/corporate"

        eligibility = cls.extract_eligibility_highlights(clean_desc)

        raw_payload = {
            "board": token,
            "ats": "greenhouse",
            "department": department,
            "departments": departments if isinstance(departments, list) else [],
            "offices": offices if isinstance(offices, list) else [],
            "requisition_id": requisition_id,
            "updated_at": job.get("updated_at"),
            "work_mode": loc_info["work_mode"],
            "eligibility": eligibility,
            "target_grad_years": eligibility.get("target_grad_years", []),
            "trust_badges": [
                "Direct Official ATS",
                "Active Requisition",
                filter_reason,
            ],
            "raw_job": job,
        }

        return OpportunityRaw(
            source_slug="greenhouse",
            external_id=external_id,
            title=title,
            company_name=company_name,
            apply_url=apply_url,
            raw_description=clean_desc,
            raw_location=loc_info["location"],
            raw_salary=raw_salary,
            raw_deadline=raw_deadline,
            raw_posted_at=job.get("first_published") or None,
            opportunity_type=opp_type,
            requisition_id=requisition_id,
            department=department,
            subcategory_raw=subcategory,
            raw_payload=raw_payload,
        )

    @staticmethod
    def _extract_pay(job: Dict[str, Any], content: str) -> Optional[str]:
        """Best-effort pay text: metadata[] custom fields, then content."""
        metadata = job.get("metadata") or []
        if isinstance(metadata, list):
            named = {}
            for entry in metadata:
                if isinstance(entry, dict) and entry.get("name") is not None:
                    named[str(entry.get("name"))] = entry.get("value")
            mined = mine_structured_pay(named)
            if mined:
                return mined
        if isinstance(content, str) and content:
            plain = re.sub(r"<[^>]+>", " ", content)
            mined = mine_pay_text(html.unescape(plain))
            if mined:
                return mined
        return None


__all__ = ["GreenhouseConnector"]
