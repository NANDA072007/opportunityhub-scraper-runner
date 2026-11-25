"""
OpportunityHub Backend Models
============================
Universal Data Boundary Models for the Production Ingestion Pipeline.
Enforces strict separation between raw connector output (OpportunityRaw)
and normalized database entities (OpportunityNormalized).

IMMUTABLE BY DESIGN: Both models are frozen dataclasses. Transformations never
mutate instances in place; they build new instances via ``copy_with()``.
Nested dict/list fields are shallow-mutable containers; pipeline components
must treat them as read-only and pass replacement containers via ``copy_with()``.
"""

from dataclasses import dataclass, field, replace
from typing import List, Optional, Dict, Any
from datetime import datetime

RAW_OPPORTUNITY_TYPES = ("job", "internship", "research", "competition", "scholarship", "event")
NORMALIZED_OPPORTUNITY_TYPES = ("job", "internship", "research", "competition", "scholarship")
CATEGORIES = ("student", "freelance", "private", "government")
BRANCHES = ("Software", "ECE", "EEE", "Mechanical", "Civil")
LIFECYCLE_STATES = ("ACTIVE", "STALE", "CLOSING_SOON", "EXPIRED", "ARCHIVED")

_REQUIRED_RAW_FIELDS = ("source_slug", "external_id", "title", "company_name", "apply_url")
_REQUIRED_NORMALIZED_FIELDS = ("source_slug", "external_id", "title", "company", "apply_url")
_SCORE_FIELDS = ("trust_score", "quality_score", "freshness_score", "popularity_score", "source_reputation")


@dataclass(frozen=True)
class OpportunityRaw:
    """Raw payload returned directly by source connectors (zero branch logic).

    Sprint-1 enrichment: adapters translate source-native structured fields
    into the typed slots below instead of burying them in ``raw_payload``.
    Every slot is optional so legacy adapters keep working untouched.
    """

    source_slug: str
    external_id: str
    title: str
    company_name: str
    apply_url: str
    raw_description: str
    raw_location: Optional[str] = None
    raw_salary: Optional[str] = None
    raw_deadline: Optional[str] = None
    # Source-native publication timestamp. Without it every crawl stamped
    # ``posted_at = now``, making year-old listings look freshly posted.
    # Free-text on purpose: sources emit ISO datetimes, RFC-822 dates and
    # epoch-derived ISO strings; the normalizer parses all of them.
    raw_posted_at: Optional[str] = None
    opportunity_type: str = "job"  # 'job' | 'internship' | 'research' | 'competition' | 'scholarship' | 'event'
    raw_payload: Dict[str, Any] = field(default_factory=dict)
    fetched_at: datetime = field(default_factory=datetime.utcnow)

    # --- Sprint-1 structured slots (all optional, adapter-filled) ---
    required_skills: List[str] = field(default_factory=list)
    ppo_offered: Optional[bool] = None
    regn_open: Optional[bool] = None
    views_count: Optional[int] = None
    registers_count: Optional[int] = None
    prize_text: Optional[str] = None
    prize_amount_inr: Optional[float] = None
    duration: Optional[str] = None
    vacancy_count: Optional[int] = None
    geo_eligibility: List[str] = field(default_factory=list)
    salary_period: Optional[str] = None  # 'hour' | 'week' | 'month' | 'year'
    salary_min: Optional[float] = None
    salary_max: Optional[float] = None
    salary_currency: Optional[str] = None
    experience_level_raw: Optional[str] = None
    requisition_id: Optional[str] = None
    department: Optional[str] = None
    subcategory_raw: Optional[str] = None

    def __post_init__(self) -> None:
        # Type safety: reject clearly wrong types at the boundary.
        for name in ("source_slug", "external_id", "title", "company_name", "apply_url", "raw_description"):
            value = getattr(self, name)
            if not isinstance(value, str):
                raise TypeError(f"OpportunityRaw.{name} must be str, got {type(value).__name__}")

    def copy_with(self, **changes: Any) -> "OpportunityRaw":
        """Return a new OpportunityRaw with the given field overrides."""
        return replace(self, **changes)

    def validate(self) -> List[str]:
        """Return a list of structural validation errors (empty when valid)."""
        errors = []
        for name in _REQUIRED_RAW_FIELDS:
            if not str(getattr(self, name) or "").strip():
                errors.append(f"OpportunityRaw.{name} must be non-empty.")
        if self.opportunity_type not in RAW_OPPORTUNITY_TYPES:
            errors.append(f"Unknown opportunity_type '{self.opportunity_type}'.")
        return errors


@dataclass(frozen=True)
class OpportunityNormalized:
    """Cleaned, validated, normalized entity ready for database persistence."""
    id: Optional[str]
    source_slug: str
    external_id: str
    title: str
    company: str
    apply_url: str
    description: str
    company_id: Optional[str] = None  # stable canonical slug for dedup/search
    company_logo_url: Optional[str] = None
    location: Optional[str] = None
    opportunity_type: str = "job"  # 'job' | 'internship' | 'research' | 'competition' | 'scholarship'
    category: str = "student"      # 'student' | 'freelance' | 'private' | 'government'
    branch: Optional[str] = None   # 'Software' | 'ECE' | 'EEE' | 'Mechanical' | 'Civil'
    salary_range: Optional[str] = None
    stipend_amount: Optional[float] = None
    experience_level: str = "entry"
    posted_at: datetime = field(default_factory=datetime.utcnow)
    deadline: Optional[datetime] = None
    is_remote: bool = False
    # False until ApplyLinkVerifier confirms the apply URL is live
    # (see PipelineOrchestrator.verify_active_links). Never assume checked.
    is_verified: bool = False
    skills_required: List[str] = field(default_factory=list)
    degrees_required: List[str] = field(default_factory=list)
    benefits: List[str] = field(default_factory=list)

    # Metadata & Telemetry
    trust_score: int = 85
    quality_score: int = 75
    freshness_score: int = 100
    popularity_score: int = 50
    source_reputation: int = 85
    fraud_score: float = 0.0
    lifecycle_state: str = "ACTIVE"  # 'ACTIVE' | 'STALE' | 'CLOSING_SOON' | 'EXPIRED' | 'ARCHIVED'
    fingerprint_hash: Optional[str] = None

    # Sprint-1 enrichment (all optional; NULL/empty = unknown/unrestricted).
    ppo_offered: Optional[bool] = None
    duration: Optional[str] = None
    vacancy_count: Optional[int] = None
    geo_eligibility: List[str] = field(default_factory=list)
    salary_period: Optional[str] = None
    normalized_monthly_stipend: Optional[float] = None
    requisition_id: Optional[str] = None
    department: Optional[str] = None
    subcategory_raw: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    # Versioning & Explainability
    parser_version: str = "1.0"
    taxonomy_version: str = "1.0"
    quality_version: str = "1.0"
    raw_payload: Dict[str, Any] = field(default_factory=dict)
    quality_breakdown: Dict[str, int] = field(default_factory=dict)
    extraction_confidence: Dict[str, float] = field(default_factory=dict)
    score_vector: Dict[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # Type safety: reject clearly wrong types at the boundary.
        for name in ("source_slug", "external_id", "title", "company", "apply_url"):
            value = getattr(self, name)
            if not isinstance(value, str):
                raise TypeError(f"OpportunityNormalized.{name} must be str, got {type(value).__name__}")

    def copy_with(self, **changes: Any) -> "OpportunityNormalized":
        """Return a new OpportunityNormalized with the given field overrides."""
        return replace(self, **changes)

    def validate(self) -> List[str]:
        """Return a list of validation errors (empty when valid)."""
        errors = []
        for name in _REQUIRED_NORMALIZED_FIELDS:
            if not str(getattr(self, name) or "").strip():
                errors.append(f"OpportunityNormalized.{name} must be non-empty.")
        if self.opportunity_type not in NORMALIZED_OPPORTUNITY_TYPES:
            errors.append(f"Unknown opportunity_type '{self.opportunity_type}'.")
        if self.category not in CATEGORIES:
            errors.append(f"Unknown category '{self.category}'.")
        if self.branch is not None and self.branch not in BRANCHES:
            errors.append(f"Unknown branch '{self.branch}'.")
        if self.lifecycle_state not in LIFECYCLE_STATES:
            errors.append(f"Unknown lifecycle_state '{self.lifecycle_state}'.")
        for name in _SCORE_FIELDS:
            value = getattr(self, name)
            if not (0 <= value <= 100):
                errors.append(f"{name} must be within 0..100, got {value!r}.")
        if (
            self.fraud_score is None
            or isinstance(self.fraud_score, bool)
            or not isinstance(self.fraud_score, (int, float))
            or not (0.0 <= float(self.fraud_score) <= 1.0)
        ):
            errors.append(f"fraud_score must be within 0.0..1.0, got {self.fraud_score!r}.")
        return errors


__all__ = [
    "OpportunityRaw",
    "OpportunityNormalized",
    "RAW_OPPORTUNITY_TYPES",
    "NORMALIZED_OPPORTUNITY_TYPES",
    "CATEGORIES",
    "BRANCHES",
    "LIFECYCLE_STATES",
]
