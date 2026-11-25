"""
Entity Normalizer
=================
Composes the single-responsibility normalization components into one
``OpportunityRaw -> OpportunityNormalized`` transformation:

StructuralNormalizer -> SkillExtractor -> CompanyNormalizer ->
LocationNormalizer -> DegreeExtractor -> branch/category/salary enrichment.

All taxonomy (skills, locations, companies, degrees, aliases) is read from the
data-driven ``TaxonomyService``; updating a JSON file requires zero code
changes. Unknown terms are returned to the caller for the unknown-entities
queue instead of being silently discarded.
"""

from typing import List, Dict, Any, Tuple, Optional
import re
import calendar
from datetime import datetime, timedelta

from backend.pipeline.models import OpportunityRaw, OpportunityNormalized
from backend.pipeline.data_contract import derive_geo_eligibility
from backend.pipeline.degrees import extract_degrees
from backend.pipeline.connectors._pay import mine_pay_text
from backend.pipeline.taxonomy_service import TaxonomyService, taxonomy as default_taxonomy
from backend.pipeline.normalizers import (
    CompanyNormalizer,
    DegreeExtractor,
    LocationNormalizer,
    SkillExtractor,
    StructuralNormalizer,
    OpportunityTypeNormalizer,
    CanonicalSuggester,
    get_sample_context,
)

VALID_DB_EXPERIENCE_LEVELS = {"entry", "junior", "mid", "senior", "lead", "any"}

EXPERIENCE_LEVEL_MAP = {
    "fresher": "entry",
    "freshers": "entry",
    "entry level": "entry",
    "entry-level": "entry",
    "intern": "entry",
    "internship": "entry",
    "graduate": "entry",
    "new grad": "entry",
    "trainee": "entry",
    "0-1 years": "entry",
    "0-2 years": "entry",
    "junior": "junior",
    "associate": "junior",
    "mid": "mid",
    "mid-level": "mid",
    "intermediate": "mid",
    "senior": "senior",
    "staff": "senior",
    "principal": "lead",
    "lead": "lead",
    "manager": "lead",
    "executive": "lead",
    "any": "any",
    "all": "any",
}

def normalize_experience_level(raw_level: Optional[str]) -> str:
    """Normalize experience string to canonical PostgreSQL experience_level enum."""
    if not raw_level or not str(raw_level).strip():
        return "entry"
    cleaned = str(raw_level).strip().lower()
    if cleaned in EXPERIENCE_LEVEL_MAP:
        return EXPERIENCE_LEVEL_MAP[cleaned]
    if "fresher" in cleaned or "intern" in cleaned or "entry" in cleaned:
        return "entry"
    if "senior" in cleaned or "sr" in cleaned or "staff" in cleaned:
        return "senior"
    if "lead" in cleaned or "principal" in cleaned or "manager" in cleaned:
        return "lead"
    if "mid" in cleaned:
        return "mid"
    return cleaned if cleaned in VALID_DB_EXPERIENCE_LEVELS else "entry"

COUNTRY_LOCK_RE = re.compile(
    r"\b(us\s+only|united\s+states\s+only|u\.s\.\s+only|us\s+citizens?\s+only|"
    r"authorized\s+to\s+work\s+in\s+the\s+u\.?s\.?|security\s+clearance|"
    r"uk\s+only|canada\s+only|eu\s+residents?\s+only)\b",
    re.IGNORECASE,
)

def resolve_geo_eligibility(
    category: str,
    location: str,
    is_remote: bool,
    description: str,
    detected_countries: List[str],
) -> List[str]:
    """Resolve geographic eligibility for international/remote student & freelance roles."""
    text = f"{location or ''} {description or ''}"
    is_locked = bool(COUNTRY_LOCK_RE.search(text))
    if category == "freelance" and not is_locked:
        if not detected_countries or detected_countries == ["US"]:
            return ["GLOBAL"]
    if is_remote and not is_locked:
        loc_lower = (location or "").lower()
        if any(k in loc_lower for k in ("worldwide", "anywhere", "global")) and not any(
            k in loc_lower for k in ("us only", "usa only", "uk only", "canada only")
        ):
            return ["GLOBAL"]
    return detected_countries


class EntityNormalizer:
    """Production Entity Normalizer & Taxonomy Mapping Engine."""

    # Branch taxonomy rules (kept here as domain logic, not taxonomy data).
    BRANCH_KEYWORDS: Dict[str, List[str]] = {
        'Software': ['software', 'python', 'java', 'c++', 'flutter', 'react', 'node', 'full stack', 'backend', 'frontend', 'ai', 'ml', 'data science', 'web'],
        'ECE': ['embedded', 'vlsi', 'verilog', 'firmware', 'microcontroller', 'signal processing', 'qualcomm', 'intel', 'semiconductor', 'ece'],
        'EEE': ['power electronics', 'plc', 'scada', 'electrical', 'powertrain', 'substation', 'grid', 'automation', 'eee'],
        'Mechanical': ['cad', 'solidworks', 'ansys', 'robotics', 'thermal', 'automotive', 'catia', 'mechanical', 'hvac'],
        'Civil': ['civil', 'structural', 'bim', 'staad', 'gis', 'construction', 'surveying', 'infrastructure', 'urban planning'],
    }

    def __init__(self, taxonomy: Optional[TaxonomyService] = None) -> None:
        self._taxonomy = taxonomy or default_taxonomy
        self.structural = StructuralNormalizer()
        self.skills = SkillExtractor(self._taxonomy)
        self.companies = CompanyNormalizer(self._taxonomy)
        self.locations = LocationNormalizer(self._taxonomy)
        self.degrees = DegreeExtractor(self._taxonomy)
        self.opp_types = OpportunityTypeNormalizer()
        self.suggester = CanonicalSuggester(self._taxonomy)

    def normalize(
        self,
        raw: OpportunityRaw,
        source_reputation: int = 85,
    ) -> Tuple[OpportunityNormalized, List[Dict[str, Any]]]:
        """Convert an OpportunityRaw into a clean OpportunityNormalized.

        Returns:
            Tuple of (OpportunityNormalized, List of Unknown Entities)
        """
        # 1. Structural cleaning.
        clean_desc = self.structural.strip_html(raw.raw_description)
        clean_title = self.structural.clean_text(raw.title)
        clean_company = self.structural.clean_text(raw.company_name)

        # 2. Skills, tools, frameworks, tech & context extraction.
        # Sprint-1: adapter-supplied structured skills participate directly —
        # no NLP regex needed when the source gives an explicit array.
        structured = self._structured_fields(raw.raw_payload)
        structured.extend(s for s in (raw.required_skills or []) if str(s).strip())
        extracted_skills, _ = self.skills.extract(clean_title, clean_desc, structured)
        unknown_details = self.skills.extract_unknown_details(
            clean_title, clean_desc, structured, known=extracted_skills
        )
        
        unknown_entities: List[Dict[str, Any]] = []
        for item in unknown_details:
            item_copy = dict(item)
            item_copy["source"] = raw.source_slug
            unknown_entities.append(item_copy)

        # 3. Company + location + degree normalization.
        company_id, company_display, is_company_unknown = self.companies.normalize(clean_company, include_unknown=True)
        if is_company_unknown and clean_company:
            sugg, mconf = self.suggester.suggest(clean_company, "company")
            unknown_entities.append({
                "entity_type": "company",
                "raw_value": clean_company,
                "confidence": 0.85,
                "suggested_canonical": sugg,
                "match_confidence": mconf,
                "sample_context": f"Company: {clean_company}",
                "field_source": "company_name",
                "source": raw.source_slug,
            })

        location, is_remote, _ = self.locations.normalize(
            raw.raw_location, clean_title, clean_desc
        )
        degree_text = f"{clean_title} {clean_desc} " + " ".join(structured)
        # Taxonomy matching misses common phrasings ("Bachelor's degree"), so
        # union it with the canonical extractor. Order is deterministic and
        # duplicates are removed.
        degrees_required = list(
            dict.fromkeys(
                self.degrees.extract(degree_text)
                + extract_degrees(degree_text).degrees
            )
        )
        degree_tokens = self.degrees.covered_tokens(degrees_required)
        unknown_entities = [
            entity for entity in unknown_entities
            if entity["raw_value"].lower() not in degree_tokens
        ]

        # Extract unknown degrees (reuses the degree scan above).
        unknown_degrees = self.degrees.extract_unknowns(
            f"{clean_title} {clean_desc}", known_degrees=degrees_required
        )
        for deg in unknown_degrees:
            deg_copy = dict(deg)
            deg_copy["source"] = raw.source_slug
            unknown_entities.append(deg_copy)

        # Extract unknown opportunity type
        unk_opp_type = self.opp_types.extract_unknown(raw.opportunity_type, clean_title)
        if unk_opp_type:
            unk_opp_type["source"] = raw.source_slug
            unknown_entities.append(unk_opp_type)

        # 4. Branch, category, remote & salary enrichment.
        detected_branch = self._detect_branch(clean_title + " " + clean_desc)
        category = self._detect_category(
            raw.opportunity_type, clean_title, raw.raw_payload,
            subcategory_raw=raw.subcategory_raw,
            source_slug=raw.source_slug,
        )
        # Sources such as Greenhouse put pay inside the description, not a
        # salary field. Mine it only when the adapter supplied nothing.
        salary_text = raw.raw_salary
        if not salary_text:
            salary_text = mine_pay_text(f"{clean_title}. {clean_desc}")

        annual_salary_inr, cleaned_salary_range = self.structural.parse_salary(
            salary_text,
            opportunity_type=raw.opportunity_type,
            category=category,
        )
        monthly_stipend = self.structural.parse_monthly_stipend(
            salary_text,
            salary_min=raw.salary_min,
            salary_max=raw.salary_max,
            salary_currency=raw.salary_currency,
            salary_period=raw.salary_period,
            opportunity_type=raw.opportunity_type,
            category=category,
        )
        deadline = self._parse_deadline(raw.raw_deadline)
        # Honour the source's real publication time. Only fall back to crawl
        # time when the adapter has no authoritative date, so old listings do
        # not get re-stamped as fresh on every ingestion.
        posted_at = self._parse_deadline(raw.raw_posted_at)

        # Geo eligibility: adapter-provided codes win. Otherwise derive from
        # the raw location text with the shared contract (never from the
        # title, where "US shift" would wrongly restrict an Indian job).
        geo_eligibility = list(raw.geo_eligibility or [])
        if not geo_eligibility:
            geo_eligibility = derive_geo_eligibility(
                raw.raw_location or location, raw.source_slug
            )
        geo_eligibility = resolve_geo_eligibility(
            category=category,
            location=raw.raw_location or location,
            is_remote=is_remote,
            description=clean_desc,
            detected_countries=geo_eligibility,
        )

        normalized = OpportunityNormalized(
            id=None,
            source_slug=raw.source_slug,
            external_id=raw.external_id,
            title=clean_title,
            company=company_display or clean_company,
            company_id=company_id,
            apply_url=raw.apply_url,
            description=clean_desc,
            location=location,
            opportunity_type=raw.opportunity_type,
            category=category,
            branch=detected_branch,
            salary_range=cleaned_salary_range,
            stipend_amount=annual_salary_inr,
            experience_level=self._map_experience_level(raw.experience_level_raw),
            posted_at=posted_at or datetime.utcnow(),
            deadline=deadline,
            is_remote=is_remote,
            skills_required=extracted_skills,
            degrees_required=degrees_required,
            ppo_offered=raw.ppo_offered,
            duration=raw.duration,
            vacancy_count=raw.vacancy_count,
            geo_eligibility=geo_eligibility,
            salary_period=raw.salary_period,
            normalized_monthly_stipend=monthly_stipend,
            requisition_id=raw.requisition_id,
            department=raw.department,
            subcategory_raw=raw.subcategory_raw,
            metadata={
                "prize_text": raw.prize_text,
                "prize_amount_inr": raw.prize_amount_inr,
                "regn_open": raw.regn_open,
                "views_count": raw.views_count,
                "registers_count": raw.registers_count,
            },
            source_reputation=source_reputation,
            taxonomy_version=self._taxonomy.version(),
            parser_version="2.0",
            raw_payload=raw.raw_payload,
            extraction_confidence={
                'title': 1.0 if len(clean_title) > 3 else 0.5,
                'company': 1.0 if len(clean_company) > 2 else 0.5,
                'description': 0.95 if len(clean_desc) > 50 else 0.4,
                'overall': 0.90 if len(clean_desc) > 50 else 0.6,
            },
        )

        return normalized, unknown_entities

    @staticmethod
    def _map_experience_level(raw_level: Optional[str]) -> str:
        """Map a source-native seniority label to the canonical enum.

        Sprint-1: Jobicy jobLevel flows straight through instead of regex
        inference. Unknown labels fall back to 'entry', never throw.
        """
        return normalize_experience_level(raw_level)

    @staticmethod
    def _structured_fields(raw_payload: Dict[str, Any]) -> List[str]:
        """Pull structured skill/tag/requirement fields from the raw payload."""
        fields: List[str] = []
        for key in ("requirements", "tags", "skills", "benefits", "required_skills"):
            value = raw_payload.get(key)
            if isinstance(value, list):
                fields.extend(str(item) for item in value if str(item).strip())
            elif isinstance(value, str) and value.strip():
                fields.append(value)
        return fields

    def _detect_branch(self, text: str) -> Optional[str]:
        text_lower = text.lower()
        scores: Dict[str, int] = {}
        for branch, keywords in self.BRANCH_KEYWORDS.items():
            count = sum(
                1 for k in keywords
                if re.search(r'\b' + re.escape(k) + r'\b', text_lower)
            )
            if count > 0:
                scores[branch] = count
        if not scores:
            return "Software"  # Default fallback
        return max(scores, key=scores.get)

    def _detect_category(
        self,
        opp_type: str,
        title: str,
        raw_payload: Optional[Dict[str, Any]] = None,
        subcategory_raw: Optional[str] = None,
        source_slug: Optional[str] = None,
    ) -> str:
        payload = raw_payload or {}
        payload_cat = payload.get("category")
        if payload_cat in ('freelance', 'government', 'student', 'private'):
            return payload_cat
        if payload.get("is_government") or (source_slug and source_slug in ("govt_research", "govt_rss", "upsc", "ssc")):
            return 'government'

        # Private boards & ATS feeds are strictly private-sector unless explicit
        if source_slug in (
            "naukri", "greenhouse", "lever", "jobicy", "wellfound",
            "arbeitnow", "remoteok", "himalayas", "remoteco", "generic_careers", "greenhouse_lever"
        ):
            explicit = (subcategory_raw or "").strip().lower()
            if explicit in ("hackathon", "hiring-challenge", "hiring_challenge", "competition"):
                return 'student'
            if explicit in ("freelance", "contract", "remote"):
                return 'freelance'
            title_lower = title.lower()
            if 'freelance' in title_lower or 'contract' in title_lower or opp_type == 'freelance':
                return 'freelance'
            if opp_type in ['internship', 'competition', 'scholarship']:
                return 'student'
            return 'private'

        # Sprint-1: explicit source taxonomy (Unstop type/subtype) wins over
        # title-keyword guessing. Only fall back to regex when absent.
        explicit = (subcategory_raw or "").strip().lower()
        if explicit in ("hackathon", "hiring-challenge", "hiring_challenge", "competition"):
            return 'student'
        if explicit in ("freelance", "contract", "remote"):
            return 'freelance'
        title_lower = title.lower()
        if 'freelance' in title_lower or 'contract' in title_lower or opp_type == 'freelance':
            return 'freelance'
        elif re.search(r'\b(drdo|isro|upsc|ssc|psu|barc|ongc|bhel|ntpc)\b', title_lower) or re.search(r'\b(?:govt|government)\s+(?:recruitment|exam|job|vacancy|post|board|dept|department)\b', title_lower):
            return 'government'
        elif opp_type in ['internship', 'competition', 'scholarship']:
            return 'student'
        return 'private'

    @staticmethod
    def _parse_salary_number(salary_str: Optional[str]) -> Optional[float]:
        if not salary_str:
            return None
        numbers = re.findall(r'\d+(?:\.\d+)?', salary_str.replace(',', ''))
        if numbers:
            val = float(numbers[0])
            if 'lpa' in salary_str.lower() or 'lac' in salary_str.lower():
                return val * 100000 / 12  # Convert LPA to monthly stipend equivalent
            return val
        return None

    @staticmethod
    def _parse_deadline(raw_deadline: Optional[str]) -> Optional[datetime]:
        """Parse a raw deadline string into a timezone-naive UTC datetime.

        Accepts the common formats emitted by connectors (ISO ``YYYY-MM-DD``,
        ``DD-MM-YYYY``, ``DD/MM/YYYY``, ISO datetime, and full text months) and
        clamps impossible day values (e.g. ``31-02-2025`` -> Feb 28) instead of
        rejecting the record. Returns None when nothing date-like is present.
        """
        if not raw_deadline or not raw_deadline.strip():
            return None
        text = raw_deadline.strip()
        normalized = re.sub(r'[.,]', '', text)

        # ISO datetime / date (YYYY-MM-DD or YYYY-MM-DD HH:MM[:SS][offset]).
        # Use the raw text here: the comma-stripped copy would destroy the
        # colon in "+05:30" offsets. Timezone-aware values (Greenhouse sends
        # "-04:00", RSS sends "+0530"/"Z") are converted to UTC instead of
        # being read as if they were already UTC.
        iso_match = re.match(
            r'^(\d{4})-(\d{1,2})-(\d{1,2})'
            r'(?:[ T](\d{1,2}):(\d{2})(?::(\d{2}))?(?:[.,]\d+)?'
            r'(Z|[+-]\d{2}:?\d{2})?)?',
            text,
        )
        if iso_match:
            year, month, day = int(iso_match.group(1)), int(iso_match.group(2)), int(iso_match.group(3))
            hour = int(iso_match.group(4) or 0)
            minute = int(iso_match.group(5) or 0)
            second = int(iso_match.group(6) or 0)
            dt = EntityNormalizer._clamp_datetime(year, month, day, hour, minute, second)
            return EntityNormalizer._shift_to_utc(dt, iso_match.group(7))

        # Text month: "1 March 2026", "March 1, 2026", or RFC-822
        # "Mon, 01 Sep 2026 10:00:00 +0530" (govt/generic RSS pubDate).
        month_map = {m.lower(): i for i, m in enumerate(
            ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
             'jul', 'aug', 'sep', 'oct', 'nov', 'dec'], start=1)}
        text_month = re.search(
            r'(\d{1,2})\s+([A-Za-z]{3,9}),?\s+(\d{4})'
            r'(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?'
            r'(?:\s*(Z|[+-]\d{2}:?\d{2}))?',
            text,
        )
        if text_month:
            day = int(text_month.group(1))
            month_name = text_month.group(2)
            year = int(text_month.group(3))
            month = month_map.get(month_name[:3].lower())
            if month:
                dt = EntityNormalizer._clamp_datetime(
                    year, month, day,
                    hour=int(text_month.group(4) or 0),
                    minute=int(text_month.group(5) or 0),
                    second=int(text_month.group(6) or 0),
                )
                return EntityNormalizer._shift_to_utc(dt, text_month.group(7))
        month_first = re.search(r'([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{4})', normalized)
        if month_first:
            month_name, day, year = month_first.groups()
            month = month_map.get(month_name[:3].lower())
            if month:
                dt = EntityNormalizer._clamp_datetime(int(year), month, int(day))
                return dt

        # Day-first: DD-MM-YYYY or DD/MM/YYYY.
        dm_y = re.match(r'^(\d{1,2})[-/](\d{1,2})[-/](\d{4})$', normalized)
        if dm_y:
            day, month, year = int(dm_y.group(1)), int(dm_y.group(2)), int(dm_y.group(3))
            dt = EntityNormalizer._clamp_datetime(year, month, day)
            return dt

        return None

    @staticmethod
    def _shift_to_utc(dt: Optional[datetime], tz: Optional[str]) -> Optional[datetime]:
        """Convert a locally-stamped naive datetime to naive UTC.

        ``tz`` is an offset token from the source (``Z``, ``+0530``,
        ``-04:00``). Missing or malformed offsets leave the value as-is, which
        matches the historical behavior for sources without a timezone.
        """
        if dt is None or not tz:
            return dt
        if tz.upper() == "Z":
            return dt
        try:
            sign = -1 if tz[0] == "-" else 1
            digits = tz[1:].replace(":", "")
            return dt - timedelta(
                minutes=sign * (int(digits[:2]) * 60 + int(digits[2:]))
            )
        except (ValueError, IndexError):
            return dt

    @staticmethod
    def _clamp_datetime(
        year: int, month: int, day: int,
        hour: int = 0, minute: int = 0, second: int = 0,
    ) -> Optional[datetime]:
        """Build a datetime, clamping impossible days (31-02 -> last day of Feb).

        Returns None only when the month itself is impossible (outside 1..12).
        """
        if not (1 <= month <= 12):
            return None
        last_day = calendar.monthrange(year, month)[1]
        day = min(max(day, 1), last_day)
        hour = min(max(hour, 0), 23)
        minute = min(max(minute, 0), 59)
        second = min(max(second, 0), 59)
        return datetime(year, month, day, hour, minute, second)
