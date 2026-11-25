"""Shared data contract for ingestion and replay.

These helpers define the semantics the database and feed rely on:

- ``geo_eligibility``: ``['IN']`` = India-eligible, ``['GLOBAL']`` = worldwide,
  other ISO-3166 alpha-2 codes = restricted to that country, empty = unknown.
  The feed RPCs currently admit unknown/empty as worldwide, so this module is
  conservative: it only emits a value when the stored location text is
  unambiguous. Guessing is worse than leaving a row unknown.
- Salary columns: ``salary`` is annual INR, ``normalized_monthly_stipend`` is
  monthly INR, and neither is ever overwritten with an unparsed/estimated
  value.
- ``ENRICHMENT_VERSION`` stamps which extractor rules produced a row, so
  ``replay_enrichment.py`` can rebuild rows without one-off scripts.

The module is intentionally dependency-free (stdlib only) so the pipeline,
operators scripts, and tests can all import it safely.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

# Re-export core models for connector/pipeline convenience
try:
    from backend.pipeline.models import OpportunityNormalized, OpportunityRaw
except ImportError:
    pass

# Bump when extractor behaviour changes in a way that should be replayed.
ENRICHMENT_VERSION = 2

# Sources that only publish Indian opportunities. For these, an unrecognised
# or empty location still means India, because the source itself is India-only.
INDIA_ONLY_SOURCES = frozenset(
    {
        "naukri",
        "internshala",
        "unstop",
        "govt_rss",
        "govt_research",
        "aicte",
    }
)

_WORLDWIDE_TERMS: Tuple[str, ...] = (
    "worldwide",
    "anywhere in the world",
    "work from anywhere",
    "remote - worldwide",
    "remote worldwide",
    "global",
)

_INDIA_TERMS: Tuple[str, ...] = (
    "india",
    "bharat",
    "pan india",
    "remote (india)",
    "remote - india",
    # states / union territories
    "andhra pradesh",
    "arunachal pradesh",
    "assam",
    "bihar",
    "chhattisgarh",
    "goa",
    "gujarat",
    "haryana",
    "himachal pradesh",
    "jharkhand",
    "karnataka",
    "kerala",
    "madhya pradesh",
    "maharashtra",
    "manipur",
    "meghalaya",
    "mizoram",
    "nagaland",
    "odisha",
    "orissa",
    "punjab",
    "rajasthan",
    "sikkim",
    "tamil nadu",
    "telangana",
    "tripura",
    "uttar pradesh",
    "uttarakhand",
    "west bengal",
    "jammu and kashmir",
    "ladakh",
    "puducherry",
    "chandigarh",
    # cities
    "bengaluru",
    "bangalore",
    "blr",
    "mumbai",
    "bombay",
    "navi mumbai",
    "thane",
    "delhi",
    "new delhi",
    "delhi ncr",
    "ncr",
    "gurgaon",
    "gurugram",
    "noida",
    "greater noida",
    "faridabad",
    "ghaziabad",
    "pune",
    "hyderabad",
    "secunderabad",
    "chennai",
    "madras",
    "kolkata",
    "calcutta",
    "ahmedabad",
    "gandhinagar",
    "jaipur",
    "jodhpur",
    "udaipur",
    "kota",
    "ajmer",
    "indore",
    "bhopal",
    "gwalior",
    "jabalpur",
    "coimbatore",
    "madurai",
    "tiruchirappalli",
    "trichy",
    "tirunelveli",
    "thanjavur",
    "salem",
    "vellore",
    "kochi",
    "cochin",
    "thiruvananthapuram",
    "trivandrum",
    "kozhikode",
    "calicut",
    "thrissur",
    "kollam",
    "kannur",
    "lucknow",
    "kanpur",
    "varanasi",
    "prayagraj",
    "allahabad",
    "agra",
    "meerut",
    "moradabad",
    "bareilly",
    "aligarh",
    "gorakhpur",
    "nagpur",
    "nashik",
    "aurangabad",
    "kolhapur",
    "solapur",
    "amravati",
    "akola",
    "surat",
    "vadodara",
    "rajkot",
    "patna",
    "ranchi",
    "jamshedpur",
    "dhanbad",
    "bhubaneswar",
    "cuttack",
    "rourkela",
    "visakhapatnam",
    "vizag",
    "vijayawada",
    "guntur",
    "nellore",
    "tirupati",
    "warangal",
    "kakinada",
    "mysore",
    "mysuru",
    "mangalore",
    "mangaluru",
    "hubli",
    "belgaum",
    "dehradun",
    "haridwar",
    "roorkee",
    "shimla",
    "solan",
    "jammu",
    "srinagar",
    "leh",
    "guwahati",
    "shillong",
    "imphal",
    "agartala",
    "aizawl",
    "kohima",
    "gangtok",
    "itanagar",
    "dispur",
    "panaji",
    "siliguri",
    "raipur",
    "bilaspur",
    "pondicherry",
)

# Country/city markers -> ISO 3166-1 alpha-2. Deliberately conservative:
# ambiguous cities (e.g. Cambridge, Salem) are omitted rather than guessed.
_FOREIGN_MARKERS: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    (
        "US",
        (
            "united states",
            "usa",
            "u.s.",
            "remote - us",
            "remote (us)",
            "remote us",
            "new york",
            "san francisco",
            "seattle",
            "austin",
            "boston",
            "chicago",
            "los angeles",
            "denver",
            "atlanta",
            "dallas",
            "houston",
            "washington dc",
            "philadelphia",
            "san diego",
            "silicon valley",
            "bay area",
            "palo alto",
            "mountain view",
            "menlo park",
            "cupertino",
            "sunnyvale",
            "redwood city",
        ),
    ),
    (
        "GB",
        (
            "united kingdom",
            "uk",
            "england",
            "london",
            "manchester",
            "edinburgh",
            "bristol",
        ),
    ),
    (
        "CA",
        (
            "canada",
            "toronto",
            "vancouver",
            "montreal",
            "ottawa",
            "calgary",
        ),
    ),
    ("IE", ("ireland", "dublin")),
    ("SG", ("singapore",)),
    (
        "DE",
        (
            "germany",
            "berlin",
            "munich",
            "hamburg",
            "frankfurt",
            "cologne",
            "stuttgart",
        ),
    ),
    ("FR", ("france", "paris", "lyon", "toulouse")),
    ("NL", ("netherlands", "amsterdam", "rotterdam", "utrecht")),
    (
        "AU",
        (
            "australia",
            "sydney",
            "melbourne",
            "brisbane",
            "perth",
            "adelaide",
        ),
    ),
    ("AE", ("united arab emirates", "uae", "dubai", "abu dhabi")),
    ("JP", ("japan", "tokyo", "osaka", "kyoto")),
    ("CN", ("china", "shanghai", "beijing", "shenzhen", "hangzhou")),
    ("HK", ("hong kong",)),
    ("IL", ("israel", "tel aviv")),
    ("SE", ("sweden", "stockholm")),
    ("ES", ("spain", "madrid", "barcelona")),
    ("PL", ("poland", "warsaw", "krakow")),
    ("BR", ("brazil", "sao paulo", "são paulo", "rio de janeiro")),
    ("MX", ("mexico", "mexico city")),
    ("ZA", ("south africa", "cape town", "johannesburg")),
    ("NZ", ("new zealand", "auckland", "wellington")),
    ("CH", ("switzerland", "zurich", "geneva")),
    ("IT", ("italy", "milan", "rome")),
    ("KR", ("south korea", "seoul")),
    ("TW", ("taiwan", "taipei")),
    ("PT", ("portugal", "lisbon")),
    ("DK", ("denmark", "copenhagen")),
    ("NO", ("norway", "oslo")),
    ("FI", ("finland", "helsinki")),
    ("AT", ("austria", "vienna")),
    ("BE", ("belgium", "brussels")),
    ("CZ", ("czech republic", "czechia", "prague")),
    ("RO", ("romania", "bucharest")),
    ("AR", ("argentina", "buenos aires")),
    ("CL", ("chile", "santiago")),
    ("CO", ("colombia", "bogota", "bogotá")),
)


def _contains(text: str, term: str) -> bool:
    return re.search(
        rf"(?<![a-z0-9]){re.escape(term)}(?![a-z0-9])", text
    ) is not None


def derive_geo_eligibility(
    location: Optional[str], source_slug: Optional[str]
) -> List[str]:
    """Return explicit eligibility codes, or ``[]`` when the location is unknown.

    Order of precedence: worldwide > India > foreign country/city > source
    default (India-only sources) > unknown.
    """
    text = " ".join(str(location or "").lower().split())
    source = str(source_slug or "").strip().lower()

    if text and any(_contains(text, term) for term in _WORLDWIDE_TERMS):
        return ["GLOBAL"]

    if text and any(_contains(text, term) for term in _INDIA_TERMS):
        return ["IN"]

    if text:
        matched: List[str] = []
        for code, terms in _FOREIGN_MARKERS:
            if any(_contains(text, term) for term in terms):
                matched.append(code)
        if matched:
            return sorted(set(matched))

    if source in INDIA_ONLY_SOURCES:
        return ["IN"]

    return []


def _compact_metadata(value: Any, *, max_keys: int = 20) -> Dict[str, Any]:
    """Keep small scalar values from source metadata; drop nested/oversized data."""
    if not isinstance(value, Mapping):
        return {}
    compact: Dict[str, Any] = {}
    for key, item in value.items():
        if len(compact) >= max_keys:
            break
        if isinstance(item, (str, int, float, bool)) or item is None:
            text = item if not isinstance(item, str) else item[:300]
            compact[str(key)[:60]] = text
        elif isinstance(item, (list, tuple)):
            scalars = [
                entry if not isinstance(entry, str) else entry[:120]
                for entry in item
                if isinstance(entry, (str, int, float, bool))
            ]
            if scalars and len(scalars) <= 10:
                compact[str(key)[:60]] = scalars
    return compact


def build_enrichment_input(
    *,
    source_slug: Optional[str],
    title: Optional[str] = None,
    company: Optional[str] = None,
    location: Optional[str] = None,
    salary_text: Optional[str] = None,
    description: Optional[str] = None,
    apply_url: Optional[str] = None,
    source_metadata: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Build the compact, replayable input stored in ``metadata.enrichment_input``.

    Bounded on purpose: this is persisted for every row, and the full
    ``raw_payload`` is deliberately reduced for approved rows.
    """
    return {
        "v": ENRICHMENT_VERSION,
        "source": str(source_slug or ""),
        "title": str(title or "")[:300],
        "company": str(company or "")[:200],
        "location": str(location or "")[:200],
        "salary_text": str(salary_text or "")[:300],
        "description_excerpt": str(description or "")[:2000],
        "apply_url": str(apply_url or "")[:2048],
        "source_metadata": _compact_metadata(source_metadata),
    }


def decide_salary_update(
    *,
    current_salary: Optional[float],
    current_monthly: Optional[float],
    new_salary: Optional[float],
    new_monthly: Optional[float],
    category: Optional[str],
) -> Tuple[Dict[str, Any], bool]:
    """Return ``(fields_to_update, nulled_legacy_value)``.

    Non-destructive by contract: a stored value is only overwritten when the
    parser produced a replacement. Legacy 0/negative/sub-30k (non-student)
    rows are still cleaned up. Shared by the online pipeline replay path and
    operator scripts so the rule cannot drift.
    """
    update: Dict[str, Any] = {}
    nulled = False

    if new_salary is not None:
        if new_salary != current_salary:
            update["salary"] = new_salary
    elif current_salary is not None and (
        current_salary <= 0
        or (current_salary < 30000 and category != "student")
    ):
        update["salary"] = None
        nulled = True

    if new_monthly is not None:
        if new_monthly != current_monthly:
            update["normalized_monthly_stipend"] = new_monthly
    elif current_monthly is not None and (
        current_monthly <= 0 or current_monthly < 1000
    ):
        update["normalized_monthly_stipend"] = None
        nulled = True

    return update, nulled
