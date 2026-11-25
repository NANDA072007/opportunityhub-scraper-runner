"""
Naukri Connector
================
Fetches private-sector job listings from naukri.com using city-specific XML
sitemaps (no public API, heavy anti-bot protection). Job URLs encode rich
structured metadata (title, company, location, experience) in their slug.

Returns generic OpportunityRaw objects; the raw sitemap URL + slug metadata are
preserved in ``raw_payload`` for audit.
"""

from __future__ import annotations

import logging
import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import httpx

from backend.pipeline.models import OpportunityRaw
from backend.pipeline.connectors.base_connector import BaseConnector, ConnectorFetchError
from backend.pipeline.connectors.capabilities import Capability
from backend.pipeline.connectors._http import ConnectorHttpClient

try:
    import defusedxml.ElementTree as DET
    from defusedxml.common import DefusedXmlException
    _XML_PARSE_ERRORS = (DET.ParseError, DefusedXmlException)
except ImportError:  # pragma: no cover
    DET = None
    _XML_PARSE_ERRORS = (Exception,)

logger = logging.getLogger(__name__)

# City-specific job description sitemaps (top-tier Indian cities).
_CITY_SITEMAPS: List[Dict[str, str]] = [
    {"city": "Pune", "url": "https://www.naukri.com/sitemap/jobDescPagesPune.xml"},
    {"city": "Bengaluru", "url": "https://www.naukri.com/sitemap/jobDescPagesBengaluru.xml"},
    {"city": "Delhi", "url": "https://www.naukri.com/sitemap/jobDescPagesDelhi.xml"},
    {"city": "Mumbai", "url": "https://www.naukri.com/sitemap/jobDescPagesMumbai.xml"},
    {"city": "Hyderabad", "url": "https://www.naukri.com/sitemap/jobDescPagesHyderabad.xml"},
    {"city": "Chennai", "url": "https://www.naukri.com/sitemap/jobDescPagesChennai.xml"},
]

# URL slug -> structured data regex.
_FULL_URL_PATTERN = re.compile(
    r"https://www\.naukri\.com//?job-listings-"
    r"(.+?)"
    r"-(\d+)-to-(\d+)-years"
    r"-(\d+)$"
)

# Known Indian cities appearing in Naukri URL slugs. Only tokens at the end
# of the slug are treated as locations, so a city word inside a title or an
# employer name is left untouched.
_INDIAN_CITIES: set = {
    "pune", "bengaluru", "bangalore", "delhi", "mumbai", "hyderabad",
    "chennai", "kolkata", "ahmedabad", "noida", "gurugram", "gurgaon",
    "jaipur", "chandigarh", "lucknow", "indore", "coimbatore",
    "nashik", "nagpur", "aurangabad", "thane", "surat", "vadodara",
    "visakhapatnam", "vijayawada", "kochi", "cochin", "trivandrum",
    "thiruvananthapuram", "mysuru", "mysore", "mangaluru", "mangalore",
    "bhubaneswar", "patna", "ranchi", "raipur", "dehradun", "goa",
    "bhopal", "jabalpur", "gwalior", "udaipur", "jodhpur", "ajmer",
    "kanpur", "agra", "meerut", "varanasi", "prayagraj", "allahabad",
    "amritsar", "ludhiana", "jalandhar", "faridabad", "ghaziabad",
    "madurai", "salem", "warangal", "tiruchirappalli", "trichy",
    # Maharashtra + multi-city tags seen in the live Pune sitemap.
    "washim", "akola", "amravati", "jalgaon", "kolhapur", "satara",
    "sangli", "solapur", "ahmednagar", "latur", "nanded", "parbhani",
    "ratnagiri", "palghar", "dhule", "buldhana", "wardha", "yavatmal",
    "chandrapur", "gondia", "bhandara", "jalna", "beed", "osmanabad",
    # Other states, common in cross-city listings.
    "rajkot", "bhavnagar", "jamnagar", "junagadh", "anand", "bharuch",
    "navsari", "valsad", "panipat", "rohtak", "hisar", "sonipat",
    "karnal", "ambala", "panchkula", "mohali", "jammu", "srinagar",
    "shimla", "jamshedpur", "dhanbad", "bokaro", "cuttack", "rourkela",
    "siliguri", "durgapur", "asansol", "howrah", "kharagpur", "guwahati",
    "shillong", "imphal", "aizawl", "agartala", "kohima", "gangtok",
    "nellore", "kurnool", "tirupati", "rajahmundry", "kakinada", "guntur",
    "ongole", "anantapur", "karimnagar", "nizamabad", "khammam",
    "secunderabad", "ernakulam", "kozhikode", "calicut", "thrissur",
    "kollam", "palakkad", "kannur", "kottayam", "hubli", "dharwad",
    "belgaum", "belagavi", "kalaburagi", "davangere", "shivamogga",
    "tumkur", "udupi", "vellore", "erode", "tirunelveli", "thanjavur",
    "tiruppur", "dindigul", "puducherry", "pondicherry", "bhilai",
    "bilaspur", "korba",
}

# Multi-token place names. Checked before single-token cities so the whole
# compound is consumed ("navi-mumbai" must not leave "navi" in the employer).
_COMPOUND_CITIES: Dict[tuple, str] = {
    ("navi", "mumbai"): "Navi Mumbai",
    ("greater", "noida"): "Greater Noida",
    ("new", "delhi"): "New Delhi",
    ("pimpri", "chinchwad"): "Pimpri Chinchwad",
    ("chhatrapati", "sambhajinagar"): "Chhatrapati Sambhajinagar",
    ("north", "goa"): "North Goa",
    ("south", "goa"): "South Goa",
}

_COMPANY_SUFFIXES: set = {
    "limited", "ltd", "pvt", "private", "inc", "corp", "corporation",
    "solutions", "services", "technologies", "technology", "consulting",
    "consultants", "software", "systems", "group", "india", "global",
    "international", "enterprises", "associates", "ventures", "labs",
    "digital", "infosystems", "infotech", "staffing",
    # Industry / legal descriptors seen in live Naukri sitemaps that were
    # previously mistaken for title words ("...-florentis-pharmaceuticals").
    "infosolutions", "solution", "equipments", "industries", "engineering",
    "com", "bank", "finance", "capital", "pharma", "pharmaceuticals",
    "healthcare", "hospital", "school", "university", "college", "media",
    "studio", "studios", "foods", "retail", "automotive", "motors",
    "textiles", "steel", "cement", "builders", "infra", "realty",
    "logistics", "shipping", "traders", "trading", "exports", "imports",
    "overseas", "agency", "agencies", "insurance", "hospitality", "hotels",
    "travels", "education", "foundation", "trust", "society", "association",
    "engineers", "contractors", "suppliers", "manufacturers", "distributors",
    "manpower",
}

# Staffing/consulting suffixes name pure intermediaries; tech keywords in
# front of them belong to the title ("...-cisco-f5-ltm-staffing-services").
_AGENCY_SUFFIXES: set = {
    "staffing", "placement", "placements", "consulting", "consultancy",
    "consultants", "recruiters", "recruitment", "manpower",
}

# Role words that mark the boundary between job title and employer. Naukri
# SEO slugs join title and company with hyphens, so role words are the only
# reliable separator ("qa-engineer-leuwint-technologies" must split after
# "engineer", not two tokens before the "technologies" suffix).
_ROLE_KEYWORDS: set = {
    "engineer", "developer", "manager", "analyst", "designer", "architect",
    "consultant", "executive", "officer", "intern", "specialist", "associate",
    "head", "director", "tester", "programmer", "administrator", "scientist",
    "accountant", "recruiter", "recruitment", "lead", "qa", "qc", "trainee",
    "technician", "supervisor", "operator", "sales", "marketing", "hr",
    "support", "admin", "coordinator", "advisor", "strategist", "writer",
    "editor", "producer", "assistant", "agent", "clerk", "faculty", "teacher",
    "professor", "doctor", "nurse", "pharmacist", "advocate", "auditor",
    "buyer", "merchandiser", "merchandising", "planner", "estimator",
    "surveyor", "electrician", "mechanic", "welder", "fitter", "driver",
    "counsellor", "counselor", "engineers", "developers",
    "managers", "analysts", "designers", "architects", "consultants",
    "executives", "officers", "interns", "specialists", "associates",
    "directors", "testers", "programmers", "administrators", "scientists",
    "accountants", "recruiters", "trainees", "technicians", "supervisors",
    "operators", "coordinators", "advisors", "writers", "editors",
    "producers", "assistants", "agents", "clerks", "teachers", "professors",
    "doctors", "nurses", "pharmacists", "advocates", "auditors", "buyers",
    "merchandisers", "planners", "estimators", "surveyors", "electricians",
    "mechanics", "welders", "fitters", "drivers", "counsellors",
    "counselors", "leader", "underwriter",
}

# Department/function words that belong to the title, never to the employer.
# They stop leftwards employer expansion even when no role word separates the
# two parts ("lead-analyst-finance-eaton-technologies-pvt-ltd" must keep
# "finance" in the title and "eaton technologies pvt ltd" as the employer).
_TITLE_HINTS: set = {
    "finance", "accounts", "accounting", "sales", "marketing", "hr",
    "operations", "production", "quality", "purchase", "procurement",
    "supply", "chain", "logistics", "maintenance", "design", "development",
    "software", "hardware", "testing", "research", "clinical", "regulatory",
    "customer", "support", "management", "project", "program", "product",
    "business", "data", "cloud", "network", "security", "infrastructure",
    "digital", "content", "brand", "growth", "strategy", "legal", "audit",
    "tax", "compliance", "mechanical", "electrical", "civil", "chemical",
    "industrial", "manufacturing", "packaging", "warehouse", "retail",
    "store", "plant", "site", "field", "area", "regional", "national",
    "state", "zonal", "cluster", "territory", "channel", "distribution",
    "export", "import", "technology", "engineering", "simulation",
    "salary", "lpa", "urgent", "hiring", "opening", "openings", "walk",
    "shift", "int", "bpo", "app", "apps", "call", "caller",
    "callers", "calling", "professional", "level", "banking", "lap",
    "module", "modules", "location", "ii", "iii", "iv", "chemistry",
}

# A company made only of legal/industry descriptors ("Services", "Staffing
# Services") carries no brand; showing "Confidential" is more honest than
# presenting a generic legal tail as the employer.
_GENERIC_COMPANY_TOKENS: set = _COMPANY_SUFFIXES | {
    "and", "of", "the", "staffing", "placement", "placements", "consulting",
    "consultancy", "recruitment", "hr", "it", "hiring", "company",
}

# Short role abbreviations that double as company-name words ("Glorious HR
# Services", "IT Solutions"). They may be crossed when recovering a brand
# in front of a generic suffix tail.
_BRAND_ROLE_TOKENS: set = {"hr", "it", "qa", "qc"}

# Department words that are nevertheless common inside legal company names
# ("NTT Data Business Solutions", "SV Management Consultants"). They may be
# crossed during brand recovery — unlike other title hints.
_COMPANY_CONNECTOR_HINTS: set = {
    "management", "business", "data", "network", "hiring", "quality",
}

# Connector words that are also genuine employer names on this source
# ("Quality HR India Pvt Ltd"). They count as a brand for honesty checks.
_BRAND_CONNECTOR_TOKENS: set = {"quality"}

# Other tokens that mark the title side of the boundary when recovering a
# brand. Includes qualifiers, language lists, and locality fragments that
# live Naukri slugs put next to the employer.
_RECOVERY_HARD_STOPS: set = _TITLE_HINTS | {
    "pass", "fresher", "freshers", "only", "male", "female", "females",
    "gender", "experience", "experienced", "year", "years", "salary",
    "nagar", "colony", "sector", "layout", "chowk", "road", "village",
    "hindi", "marathi", "english", "tamil", "telugu", "kannada", "gujarati",
    "bengali", "malayalam", "punjabi", "urdu", "gl", "rtr", "js",
}

# Recovered brand spans beyond this are almost always the tail of the job
# title, not the employer ("...sales-executive-direct-marketing-services").
_MAX_BRAND_RECOVERY = 3

_TITLE_EDGE_STOPWORDS: set = {
    "and", "for", "at", "of", "the", "in", "on", "to", "with", "a", "an",
}

# Work-mode / freshness tags appended to Naukri SEO slugs. They belong to
# neither the title nor the employer ("...solutions-direct-pune").
_TRAILING_TAGS: set = {
    "direct", "wfh", "remote", "hybrid", "onsite", "on-site", "permanent",
    "urgent", "immediate", "hiring", "full-time", "part-time", "contract",
    "work", "from", "home",
}

_ACRONYMS: Dict[str, str] = {
    "qa": "QA", "qc": "QC", "it": "IT", "hr": "HR", "api": "API",
    "sql": "SQL", "ui": "UI", "ux": "UX", "aws": "AWS", "gcp": "GCP",
    "devops": "DevOps", "ios": "iOS", "ml": "ML", "ai": "AI", "v": "V",
    "js": "JS", "ts": "TS", "php": "PHP", "css": "CSS", "html": "HTML",
    "bi": "BI", "etl": "ETL", "sap": "SAP", "crm": "CRM", "erp": "ERP",
    "tcs": "TCS", "kyc": "KYC", "aml": "AML", "seo": "SEO", "wfh": "WFH",
    "bpo": "BPO", "hsc": "HSC", "dsa": "DSA", "dst": "DST", "lap": "LAP",
    "rtr": "RTR", "pl": "PL", "ca": "CA", "mnc": "MNC", "ntt": "NTT",
    "fis": "FIS", "sv": "SV", "kpi": "KPI", "ot": "OT",
}


def _title_word(token: str) -> str:
    lowered = token.lower()
    if lowered in _ACRONYMS:
        return _ACRONYMS[lowered]
    return token.capitalize()


def _posted_at_from_job_id(job_id: str) -> Optional[str]:
    """Recover the posting date encoded in a Naukri job ID.

    Every live Pune sitemap ID (21,118/21,118) starts with a valid DDMMYY
    date followed by a sequence number. Interpreting it as IST and emitting
    UTC ISO keeps old listings from being re-stamped as freshly posted on
    every crawl. Returns None for IDs that do not match the format.
    """
    if not job_id or len(job_id) < 6 or not job_id[:6].isdigit():
        return None
    try:
        ist = timezone(timedelta(hours=5, minutes=30))
        posted = datetime.strptime(job_id[:6], "%d%m%y").replace(tzinfo=ist)
    except ValueError:
        return None
    return posted.astimezone(timezone.utc).isoformat()


def _has_brand_token(tokens: List[str]) -> bool:
    """True when a company span carries at least one real brand word.

    Generic legal tails ("Staffing Services", "Management Consultants") name
    no employer; presenting them as the company would be fabricated. A brand
    is any token that is not itself a generic descriptor, a short role
    abbreviation, or a bare numeric job reference.
    """
    return any(
        token.lower() not in _GENERIC_COMPANY_TOKENS
        and token.lower() not in _BRAND_ROLE_TOKENS
        and (
            token.lower() not in _COMPANY_CONNECTOR_HINTS
            or token.lower() in _BRAND_CONNECTOR_TOKENS
        )
        and not token.isdigit()
        for token in tokens
    )


def _is_locality_token(token: str) -> bool:
    """True for a place fragment that can follow a city ("pune-chakpachuria")."""
    low = token.lower()
    return (
        low not in _INDIAN_CITIES
        and low != "india"
        and low not in _ROLE_KEYWORDS
        and low not in _TITLE_HINTS
        and low not in _COMPANY_SUFFIXES
        and low not in _TRAILING_TAGS
        and low not in _TITLE_EDGE_STOPWORDS
        and not low.isdigit()
    )


def _strip_trailing_location(tokens: List[str]):
    """Return (remaining_tokens, location_string) for a slug token list.

    Handles the three live tail shapes: plain cities, multi-city lists, and
    ``<city>-<locality>`` ("...-pune-chakpachuria"). ``india`` counts as a
    location token so nationwide tags do not poison the employer split.
    """
    tokens = list(tokens)
    while tokens and tokens[-1].lower() in _TRAILING_TAGS:
        tokens.pop()
    if not tokens:
        return ([], "")

    location_names: List[str] = []
    loc_start = len(tokens)
    while loc_start > 0:
        if loc_start >= 2:
            compound = _COMPOUND_CITIES.get(
                (tokens[loc_start - 2].lower(), tokens[loc_start - 1].lower())
            )
            if compound:
                location_names.insert(0, compound)
                loc_start -= 2
                continue
        if tokens[loc_start - 1].lower() in _INDIAN_CITIES or tokens[loc_start - 1].lower() == "india":
            location_names.insert(0, _title_word(tokens[loc_start - 1]))
            loc_start -= 1
            continue
        break

    if loc_start == len(tokens):
        # No plain city tail: accept a short "<city>-<locality>" tail. The
        # city must sit in the final window and every token after it must
        # look like a place fragment, never a role or company word.
        for i in range(max(0, len(tokens) - 4), len(tokens) - 1):
            if i <= 0 or tokens[i].lower() not in _INDIAN_CITIES:
                continue
            if all(_is_locality_token(t) for t in tokens[i + 1 :]):
                location_names = [_title_word(t) for t in tokens[i:]]
                loc_start = i
                break

    seen_locations: set = set()
    deduped_locations: List[str] = []
    for name in location_names:
        if name.lower() not in seen_locations:
            seen_locations.add(name.lower())
            deduped_locations.append(name)
    location = ", ".join(deduped_locations)

    pre_loc = tokens[:loc_start]
    while pre_loc and pre_loc[-1].lower() in _TRAILING_TAGS:
        pre_loc.pop()
    return (pre_loc, location.strip())


def _split_slug_tokens(
    tokens: List[str],
    company_hints: Optional[Set[Tuple[str, ...]]] = None,
):
    """Split URL slug tokens into (title, company, location).

    Naukri SEO slugs have no separator between title and employer, so the
    split is inferred from the source's own vocabulary:

    0. If the tail matches a repeated employer phrase learned from the same
       sitemap (``consult-asia``, ``golden-opportunities-pvt-ltd``), trust it.
    1. Strip work-mode tags and consume the trailing location block
       (multi-city, compound cities, and city+locality included).
    2. Handle the explicit ``...-at-<city>-<employer>...`` form, where the
       city inside the slug is a hard title/employer separator.
    3. When the slug ends in a run of company suffixes
       (``...-software-services-private-limited``), walk left over ordinary
       brand words, stopping at role words and department words. Staffing
       suffixes stop earlier because tech keywords precede them.
    4. Otherwise the employer starts after the last role keyword
       (``qa-engineer-leuwint-technologies`` -> "Leuwint Technologies").
    5. Everything before the employer is the title.
    """
    if not tokens:
        return ("", "", "")

    tokens = list(tokens)
    pre_loc, location = _strip_trailing_location(tokens)
    if not pre_loc:
        return ("", "", location)

    if company_hints:
        for n in range(min(5, len(pre_loc) - 1), 1, -1):
            phrase = tuple(t.lower() for t in pre_loc[-n:])
            if phrase in company_hints:
                return _finish_split(pre_loc[:-n], pre_loc[-n:], location)

    # Explicit "...-at-pune-<employer>-..." separator ("at" must have both a
    # real title before it and a plausible employer after the city).
    for i, token in enumerate(pre_loc):
        if token.lower() != "at" or i < 2:
            continue
        if i + 3 > len(pre_loc):
            continue
        if pre_loc[i + 1].lower() not in _INDIAN_CITIES:
            continue
        title_tokens = pre_loc[:i]
        company_tokens = pre_loc[i + 2:]
        if len(company_tokens) < 2:
            continue
        return _finish_split(title_tokens, company_tokens, location)

    last_role = -1
    for i, token in enumerate(pre_loc):
        if token.lower() in _ROLE_KEYWORDS:
            last_role = i

    suffix_run_start = len(pre_loc)
    for i in range(len(pre_loc) - 1, -1, -1):
        if pre_loc[i].lower() in _COMPANY_SUFFIXES:
            suffix_run_start = i
        else:
            break

    if suffix_run_start < len(pre_loc):
        company_start = suffix_run_start
        max_extra = (
            2 if pre_loc[suffix_run_start].lower() in _AGENCY_SUFFIXES else 4
        )
        for _ in range(max_extra):
            # Always leave at least one token for the title: a full-slug
            # company span ("musketeer-red-bull-india-pvt-ltd") otherwise
            # triggers the empty-title swap and loses the real employer.
            if company_start <= 1:
                break
            candidate = pre_loc[company_start - 1].lower()
            if candidate in _ROLE_KEYWORDS or candidate in _TITLE_HINTS:
                break
            company_start -= 1
        title_tokens = pre_loc[:company_start]
        company_tokens = pre_loc[company_start:]
    elif last_role >= 0 and last_role + 1 < len(pre_loc):
        title_tokens = pre_loc[: last_role + 1]
        company_tokens = pre_loc[last_role + 1:]
    elif (
        len(pre_loc) == 2
        and pre_loc[-1].lower() not in _ROLE_KEYWORDS
        and pre_loc[-1].lower() not in _TITLE_HINTS
    ):
        # "phlebotomist-1mg" -> title + single-token employer.
        title_tokens = pre_loc[:1]
        company_tokens = pre_loc[1:]
    elif len(pre_loc) > 4:
        title_tokens = pre_loc[:-3]
        company_tokens = pre_loc[-3:]
    elif len(pre_loc) > 2:
        title_tokens = pre_loc[:-2]
        company_tokens = pre_loc[-2:]
    else:
        title_tokens = pre_loc
        company_tokens = []

    return _finish_split(title_tokens, company_tokens, location)


def _finish_split(
    title_tokens: List[str], company_tokens: List[str], location: str
):
    """Clean title/company token lists and apply honesty fallbacks."""
    title_tokens = list(title_tokens)
    while title_tokens and title_tokens[-1].lower() in _TITLE_EDGE_STOPWORDS:
        title_tokens.pop()

    company_tokens = list(company_tokens)
    original_company = (
        " ".join(_title_word(t) for t in company_tokens) if company_tokens else ""
    )

    # Brand recovery for generic legal tails. Naukri slugs like
    # "...-glorious-hr-services" leave the brand in the title because the
    # splitter stops at the first hint word. Walk back a bounded span and
    # accept it only when a real brand token is found — otherwise the honest
    # "Confidential" fallback below still applies.
    if company_tokens and all(
        t.lower() in _GENERIC_COMPANY_TOKENS for t in company_tokens
    ):
        recovered: List[str] = []
        while len(recovered) < _MAX_BRAND_RECOVERY and len(title_tokens) > 1:
            candidate = title_tokens[-1].lower()
            if candidate in _BRAND_ROLE_TOKENS or candidate in _COMPANY_CONNECTOR_HINTS:
                recovered.insert(0, title_tokens.pop())
                continue
            if (
                candidate.isdigit()
                or candidate in _ROLE_KEYWORDS
                or candidate in _RECOVERY_HARD_STOPS
                or candidate in _INDIAN_CITIES
            ):
                break
            recovered.insert(0, title_tokens.pop())
        if recovered and _has_brand_token(recovered + company_tokens):
            company_tokens = recovered + company_tokens

    while title_tokens and title_tokens[-1].lower() in _TITLE_EDGE_STOPWORDS:
        title_tokens.pop()
    title = " ".join(_title_word(t) for t in title_tokens)

    if company_tokens and all(
        t.lower() in _GENERIC_COMPANY_TOKENS for t in company_tokens
    ):
        company_tokens = []

    if not title and company_tokens:
        title_tokens, company_tokens = company_tokens, []
        title = " ".join(_title_word(t) for t in title_tokens)

    if not company_tokens:
        if not title and original_company:
            title = original_company
        return (title.strip(), "Confidential", location.strip())

    company_parts = [_title_word(t) for t in company_tokens]
    company = " ".join(company_parts)
    if len(company_parts) >= 2 and company_parts[-1].lower() == "com":
        company = " ".join(company_parts[:-1]) + ".com"

    return (title.strip(), company.strip(), location.strip())


def _hint_phrase_is_usable(phrase: Tuple[str, ...]) -> bool:
    """Reject title boilerplate from the learned-employer dictionary."""
    if len(phrase) < 2 or phrase[0] in _GENERIC_COMPANY_TOKENS:
        return False
    for token in phrase:
        if any(ch.isdigit() for ch in token):
            return False
        if token in _TRAILING_TAGS or token in _TITLE_EDGE_STOPWORDS:
            return False
        if token in _ROLE_KEYWORDS and token not in _BRAND_ROLE_TOKENS:
            return False
        if (
            token in _RECOVERY_HARD_STOPS
            and token not in _COMPANY_CONNECTOR_HINTS
            and token not in _BRAND_ROLE_TOKENS
        ):
            return False
    return True


def _build_company_hints(
    entries: List[Dict[str, str]], *, min_count: int = 5
) -> Set[Tuple[str, ...]]:
    """Learn repeated employer phrases from one city sitemap.

    Naukri gives no separator between title and employer, but recruiters post
    dozens of listings that all end with the same employer tokens
    ("...-consult-asia", "...-golden-opportunities-pvt-ltd"). Counting those
    terminal phrases recovers the boundary that per-slug heuristics cannot,
    without hardcoding any company name.

    A phrase is trusted only when it is a complete company the plain splitter
    already produced more than once, or when it is the tail of such a company
    behind nothing but generic legal words (the "Solutions" in
    "Solutions Tech Think Infotech" is a dropped title word, not a brand).
    Longer title+employer phrases therefore never become hints.
    """
    counts: Counter = Counter()
    outputs: Counter = Counter()
    for entry in entries:
        match = _FULL_URL_PATTERN.match(entry.get("url", ""))
        if not match:
            continue
        tokens = NaukriConnector._tokenize_slug(match.group(1))
        pre_loc, _ = _strip_trailing_location(tokens)
        if len(pre_loc) < 3:
            continue
        _, plain_company, _ = _split_slug_tokens(tokens)
        if plain_company and plain_company != "Confidential":
            outputs[tuple(t.lower() for t in plain_company.split())] += 1
        for n in range(2, min(5, len(pre_loc) - 1) + 1):
            counts[tuple(t.lower() for t in pre_loc[-n:])] += 1

    full_names = {phrase for phrase, seen in outputs.items() if seen >= 2}
    # Tails of known companies whose dropped prefix is purely a legal/industry
    # suffix ("solutions tech think infotech" -> "tech think infotech").
    # Deliberately narrower than _GENERIC_COMPANY_TOKENS: words like "hiring"
    # are real brand tokens ("Hiring Plus HR Solution"), so treating them as
    # droppable prefixes steals the brand from the employer.
    hint_prefix_tokens = _COMPANY_SUFFIXES | {"and", "of", "the"}
    plain_tails: Set[Tuple[str, ...]] = set()
    for phrase in outputs:
        for n in range(2, min(5, len(phrase) - 1) + 1):
            prefix, tail = phrase[:-n], phrase[-n:]
            if prefix and all(t in hint_prefix_tokens for t in prefix):
                plain_tails.add(tail)

    hints: Set[Tuple[str, ...]] = set()
    for phrase, count in counts.items():
        if count < min_count or not _hint_phrase_is_usable(phrase):
            continue
        if phrase in full_names or phrase in plain_tails:
            hints.add(phrase)
    return hints


class NaukriConnector(BaseConnector):
    """Naukri connector for Indian private-sector job listings."""

    priority = 700
    version = "1.0.0"
    owner = "platform"
    capabilities = {Capability.JOB}
    supports_full_sync = True
    supports_incremental_sync = False
    estimated_rate_limit = 0.5

    _MAX_JOBS_PER_CITY = 300
    _MAX_TOTAL_JOBS = 2000

    def __init__(
        self,
        *,
        client: Optional[ConnectorHttpClient] = None,
        http_client: Optional[httpx.Client] = None,
    ) -> None:
        if client is not None:
            self._http = client
        else:
            self._http = ConnectorHttpClient(client=http_client, rate_limit_per_sec=0.5)

    @property
    def source_slug(self) -> str:
        return "naukri"

    @property
    def display_name(self) -> str:
        return "Naukri"

    @property
    def default_source_reputation(self) -> int:
        return 70

    def fetch_raw_opportunities(
        self, config: Dict[str, Any], query: Optional[str] = None
    ) -> List[OpportunityRaw]:
        sitemaps = config.get("sitemaps") or _CITY_SITEMAPS
        results: List[OpportunityRaw] = []
        seen_ids: set = set()
        successful_sitemaps = 0
        failed_sitemaps = 0

        for sitemap in sitemaps:
            if len(results) >= self._MAX_TOTAL_JOBS:
                break
            city = sitemap["city"]
            try:
                xml_text = self._http.get_text(sitemap["url"])
            except Exception as exc:
                logger.warning("Naukri sitemap '%s' failed: %s", city, exc)
                failed_sitemaps += 1
                continue

            try:
                sitemap_entries = self._parse_sitemap(xml_text)
            except ConnectorFetchError as exc:
                logger.warning("Naukri sitemap '%s' unusable: %s", city, exc)
                failed_sitemaps += 1
                continue
            successful_sitemaps += 1
            company_hints = _build_company_hints(sitemap_entries)
            if company_hints:
                logger.info(
                    "Naukri '%s': learned %d repeated employer phrases.",
                    city,
                    len(company_hints),
                )

            city_count = 0
            for entry in sitemap_entries:
                if city_count >= self._MAX_JOBS_PER_CITY:
                    break
                if len(results) >= self._MAX_TOTAL_JOBS:
                    break
                try:
                    parsed = self._parse_job_url(entry["url"], company_hints)
                    if parsed is None:
                        continue
                    job_id = parsed["job_id"]
                    if job_id in seen_ids:
                        continue
                    seen_ids.add(job_id)
                    results.append(
                        self._to_raw(
                            parsed,
                            entry["url"],
                            city,
                            entry.get("lastmod", ""),
                        )
                    )
                    city_count += 1
                except Exception as exc:
                    logger.warning("Skipping malformed Naukri entry: %s", exc)
                    continue

        if failed_sitemaps and not successful_sitemaps:
            raise ConnectorFetchError(
                f"Naukri: all {failed_sitemaps} configured city sitemaps failed "
                "(blocked, unreachable, or invalid XML)."
            )

        logger.info("NaukriConnector fetched %d total items.", len(results))
        return results

    @staticmethod
    def _parse_sitemap(xml_text: str) -> List[Dict[str, str]]:
        entries: List[Dict[str, str]] = []
        if DET is None:
            raise ConnectorFetchError("Naukri sitemap skipped: defusedxml missing.")
        try:
            root = DET.fromstring(xml_text)
        except _XML_PARSE_ERRORS as exc:
            raise ConnectorFetchError(f"Naukri sitemap XML parse failed: {exc}") from exc

        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        for url_elem in root.findall("sm:url", ns):
            loc_elem = url_elem.find("sm:loc", ns)
            lastmod_elem = url_elem.find("sm:lastmod", ns)
            if loc_elem is None or not loc_elem.text:
                continue
            loc = loc_elem.text.strip()
            if "/job-listings-" in loc:
                lastmod = (lastmod_elem.text or "").strip() if lastmod_elem is not None else ""
                entries.append({"url": loc, "lastmod": lastmod})
        return entries

    @staticmethod
    def _tokenize_slug(full_slug: str) -> List[str]:
        """Split a slug and rejoin hyphenated words that Naukri encodes with
        an interior hyphen ("delivery-co-ordinator")."""
        tokens = full_slug.split("-")
        merged: List[str] = []
        for token in tokens:
            if (
                merged
                and merged[-1].lower() == "co"
                and token.lower().startswith("ordinat")
            ):
                merged[-1] = f"co{token.lower()}"
            else:
                merged.append(token)
        return merged

    @staticmethod
    def _parse_job_url(
        url: str,
        company_hints: Optional[Set[Tuple[str, ...]]] = None,
    ) -> Optional[Dict[str, Any]]:
        match = _FULL_URL_PATTERN.match(url)
        if not match:
            return None
        full_slug = match.group(1)
        exp_min = int(match.group(2))
        exp_max = int(match.group(3))
        job_id = match.group(4)

        title, company, location = _split_slug_tokens(
            NaukriConnector._tokenize_slug(full_slug),
            company_hints,
        )
        return {
            "title": title,
            "company": company,
            "location": location,
            "exp_min": exp_min,
            "exp_max": exp_max,
            "job_id": job_id,
        }

    @staticmethod
    def _to_raw(
        parsed: Dict[str, Any],
        job_url: str,
        fallback_city: str,
        lastmod: str = "",
    ) -> OpportunityRaw:
        title = parsed["title"]
        location = parsed["location"] or fallback_city
        company = parsed["company"] or "Confidential"

        description = (
            f"{title} at {company}. Location: {location}. "
            f"Experience required: {parsed['exp_min']}-{parsed['exp_max']} years. "
            "Apply on Naukri.com for this private sector opportunity."
        )
        # Mine title and company separately: joining them turns "…Noida 5" +
        # "5lpa Rtr…" into a fake "5.5 LPA" across the boundary.
        pay_text = extract_title_pay(title) or extract_title_pay(company)

        return OpportunityRaw(
            source_slug="naukri",
            external_id=f"nk_{parsed['job_id']}",
            title=title,
            company_name=company,
            apply_url=job_url,
            raw_description=description,
            raw_location=location,
            raw_salary=pay_text,
            raw_posted_at=_posted_at_from_job_id(parsed["job_id"]),
            opportunity_type="job",
            raw_payload={
                "source": "naukri",
                "category": "private",
                "exp_min": parsed["exp_min"],
                "exp_max": parsed["exp_max"],
                "job_id": parsed["job_id"],
                # Sitemap lastmod is a "last changed" hint, not a posting
                # date; kept for audit instead of being presented as posted_at.
                "sitemap_lastmod": lastmod or None,
            },
        )


_LPA_RANGE_RE = re.compile(
    r"(?<![\d.])(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2}(?:\.\d+)?)\s*lpa\b",
    re.IGNORECASE,
)
_LPA_SLUG_PAIR_RE = re.compile(
    r"(?<![\d.])(\d{1,2})\s+(\d{1,2})\s*lpa\b", re.IGNORECASE
)
_LPA_SINGLE_RE = re.compile(
    r"(?<![\d.])(\d{1,2}(?:\.\d+)?)\s*lpa\b", re.IGNORECASE
)
_LAC_RANGE_RE = re.compile(
    r"(?<![\d.])(\d{1,2}(?:\.\d+)?)\s*(?:-|–|to)\s*"
    r"(\d{1,2}(?:\.\d+)?)\s*(?:lac|lakh|lakhs)\b",
    re.IGNORECASE,
)
_LAC_SLUG_PAIR_RE = re.compile(
    r"(?<![\d.])(\d{1,2})\s+(\d{1,2})\s*(?:lac|lakh|lakhs)\b",
    re.IGNORECASE,
)
_LAC_SINGLE_RE = re.compile(
    r"(?<![\d.])(\d{1,2}(?:\.\d+)?)\s*(?:lac|lakh|lakhs)\b", re.IGNORECASE
)
_K_RANGE_RE = re.compile(
    r"\b(?:sal(?:ary)?|ctc|pay)\b[^0-9]{0,15}"
    r"(\d{1,3})\s*k\s*(?:-|to)?\s*(\d{1,3})\s*k\b",
    re.IGNORECASE,
)
_K_SINGLE_RE = re.compile(
    r"\b(?:sal(?:ary)?|ctc|pay)\b[^0-9]{0,15}(\d{1,3})\s*k(?:\s*ctc)?\b",
    re.IGNORECASE,
)
_K_BEFORE_KEYWORD_RE = re.compile(
    r"(?<![\d.])(\d{1,3})\s*k\s*(?:ctc|sal(?:ary)?)\b", re.IGNORECASE
)


def _slug_pair_amount(whole: int, fraction: int, prefix: str) -> str:
    """Resolve a two-number slug pair ("4 5lac") to a decimal or a range.

    "Upto 4 5lac" is a ceiling (4.5), while a bare ascending pair ("3 5 Lpa")
    is the slug form of a range. Equal or descending pairs are decimals.
    """
    capped = re.search(
        r"\b(?:up\s*to|upto|max(?:imum)?)\b", prefix, re.IGNORECASE
    )
    if capped or whole == fraction or fraction < whole:
        return f"{whole}.{fraction}"
    return f"{whole}-{fraction}"


def extract_title_pay(text: Optional[str]) -> Optional[str]:
    """Mine pay from Naukri's hyphen-stripped titles ("Upto 8 Lpa", "Sal 15k 40k").

    Returns canonical text with an explicit period so ``parse_salary`` cannot
    guess wrong: LPA/LAC are annual, bare K amounts are monthly. Every pattern
    requires a currency/unit or a pay keyword, so years, headcounts and
    location fragments never become pay. ``None`` means undisclosed.
    """
    if not text or not isinstance(text, str):
        return None

    match = _LPA_RANGE_RE.search(text)
    if match:
        return f"{match.group(1)}-{match.group(2)} LPA"

    match = _LPA_SLUG_PAIR_RE.search(text)
    if match:
        # "5 5 Lpa" is 5.5, "Upto 4 5 Lpa" is 4.5, "3 5 Lpa" is the range 3-5.
        amount = _slug_pair_amount(
            int(match.group(1)), int(match.group(2)), text[: match.start()]
        )
        return f"{amount} LPA"

    match = _LPA_SINGLE_RE.search(text)
    if match:
        return f"{match.group(1)} LPA"

    match = _LAC_RANGE_RE.search(text)
    if match:
        return f"{match.group(1)}-{match.group(2)} LAC"

    match = _LAC_SLUG_PAIR_RE.search(text)
    if match:
        amount = _slug_pair_amount(
            int(match.group(1)), int(match.group(2)), text[: match.start()]
        )
        return f"{amount} LAC"

    match = _LAC_SINGLE_RE.search(text)
    if match:
        return f"{match.group(1)} LAC"

    match = _K_RANGE_RE.search(text)
    if match:
        return f"{match.group(1)}K-{match.group(2)}K per month"

    match = _K_SINGLE_RE.search(text)
    if match:
        return f"{match.group(1)}K per month"

    match = _K_BEFORE_KEYWORD_RE.search(text)
    if match:
        return f"{match.group(1)}K per month"

    return None


__all__ = ["NaukriConnector"]
