"""
Single-Responsibility Normalizer Components
===========================================
Each component owns exactly one normalization concern and reads all of its
taxonomy from the data-driven ``TaxonomyService`` (JSON files), never from
hardcoded lists:

- ``StructuralNormalizer`` : HTML stripping + whitespace/text cleaning.
- ``AliasEngine``          : case/alias-insensitive canonicalization.
- ``SkillExtractor``       : canonical skill extraction + unknown-skill queue.
- ``CompanyNormalizer``    : company_id + display (alias + suffix stripping).
- ``LocationNormalizer``   : canonical city/state/country + remote/hybrid.
- ``DegreeExtractor``      : degree eligibility extraction.

``EntityNormalizer`` composes these components in order.
"""

from __future__ import annotations

import html
import re
from typing import Any, Dict, List, Optional, Tuple

from backend.pipeline.taxonomy_service import (
    TaxonomyService,
    normalize_token,
    slugify,
    taxonomy as default_taxonomy,
)


# ----------------------------------------------------------------------
# Structural cleaning
# ----------------------------------------------------------------------

class StructuralNormalizer:
    """Stage: STRIP_HTML / CLEAN_TEXT — no domain knowledge."""

    # Sprint-1: single centrally-updatable FX peg for salary normalization.
    # Phase-2 will feed a daily rate from the orchestrator boot sequence;
    # until then every conversion flows through this constant so a rupee
    # move is a one-line change, not silent data drift across call sites.
    USD_TO_INR_PEG: float = 84.0
    EUR_TO_INR_PEG: float = 90.0
    GBP_TO_INR_PEG: float = 105.0

    # Full-time equivalent hours per year backing hourly → monthly math.
    FTE_HOURS_PER_YEAR: float = 2080.0

    @staticmethod
    def strip_html(raw_html: Optional[str], max_chars: int = 5000) -> str:
        if not raw_html:
            return ""
        # Some sources (Greenhouse) double-escape their HTML: the stored value
        # is `&lt;p&gt;...`, so stripping tags first leaves literal `<p>` in
        # the description. Unescape before stripping, and repeat once so a
        # second layer of entities is also handled. The final pass strips any
        # tag introduced by the last unescape — output is plain text.
        text = raw_html
        for _ in range(2):
            text = html.unescape(text)
            text = re.sub(
                r"<(script|style|noscript|iframe)[^>]*>.*?</\1>",
                " ",
                text,
                flags=re.IGNORECASE | re.DOTALL,
            )
            text = re.sub(r"<[^>]+>", " ", text)
            # Truncated source text can end mid-tag (`<li data-section-id="x`)
            # with no closing `>`; drop that fragment too.
            text = re.sub(r"<[a-zA-Z][^>]*$", " ", text)
        text = html.unescape(text)
        text = re.sub(r"\s+", " ", text).strip()
        if len(text) > max_chars:
            text = text[:max_chars].rsplit(" ", 1)[0] + "..."
        return text

    @staticmethod
    def clean_text(text: Optional[str]) -> str:
        if not text:
            return ""
        return re.sub(r"\s+", " ", html.unescape(text)).strip()

    @staticmethod
    def parse_salary(
        salary_str: Optional[str],
        *,
        opportunity_type: Optional[str] = None,
        category: Optional[str] = None,
    ) -> Tuple[Optional[float], Optional[str]]:
        """Parse raw salary string into (annual_inr_salary, cleaned_salary_range_text)."""
        if not salary_str or not salary_str.strip():
            return None, None

        cleaned_text = re.sub(r"\s+", " ", html.unescape(salary_str)).strip()
        lower_text = cleaned_text.lower()

        unparseable_keywords = ["competitive", "undisclosed", "as per industry", "best in industry", "not specified", "negotiable", "tbd"]
        if any(kw in lower_text for kw in unparseable_keywords) and not re.search(r'\d+', lower_text):
            return None, cleaned_text

        fx_rates = {
            "$": StructuralNormalizer.USD_TO_INR_PEG,
            "usd": StructuralNormalizer.USD_TO_INR_PEG,
            "€": StructuralNormalizer.EUR_TO_INR_PEG,
            "eur": StructuralNormalizer.EUR_TO_INR_PEG,
            "£": StructuralNormalizer.GBP_TO_INR_PEG,
            "gbp": StructuralNormalizer.GBP_TO_INR_PEG,
            "₹": 1.0, "inr": 1.0, "rs": 1.0, "rs.": 1.0,
        }
        fx = 1.0
        for symbol, rate in fx_rates.items():
            if symbol in cleaned_text or symbol in lower_text:
                fx = rate
                break

        norm_period_text = re.sub(r"/\s+", "/", lower_text)
        period_mult = 1.0
        if any(p in norm_period_text for p in ["/hour", "/hr", "hourly", "per hour", "p.h."]):
            # Sprint-1 root-cause fix: hourly rates previously fell through
            # with period_mult=1.0, storing "$50/hr" as ~Rs 4,175 "annual".
            period_mult = StructuralNormalizer.FTE_HOURS_PER_YEAR
        elif any(p in norm_period_text for p in ["/month", "/mo", "monthly", "per month", "p.m.", "stipend"]):
            period_mult = 12.0
        elif any(p in norm_period_text for p in ["/week", "/wk", "weekly", "per week"]):
            period_mult = 52.0
        elif any(p in norm_period_text for p in ["/day", "daily", "per day"]):
            period_mult = 365.0
        elif any(p in norm_period_text for p in ["lpa", "lac", "lakh", "lacs", "per annum", "/year", "/yr", "ctc", "p.a."]):
            if any(l in norm_period_text for l in ["lpa", "lac", "lakh", "lacs"]):
                period_mult = 100000.0
            else:
                period_mult = 1.0
        elif any(p in norm_period_text for p in ["level-", "pay level", "pay scale", "scale of pay", "cpc"]):
            # Indian civil service / research / PSU pay bands are monthly basic pay scales
            period_mult = 12.0

        # Strip pay commission / pay level numbers so Level-11 or 7th CPC don't corrupt salary average
        text_for_nums = re.sub(r'\blevel[-\s]*\d+\b', '', cleaned_text, flags=re.IGNORECASE)
        text_for_nums = re.sub(r'\b\d+(?:st|nd|rd|th)\s+cpc\b', '', text_for_nums, flags=re.IGNORECASE)
        text_for_nums = re.sub(r'\bcpc\b', '', text_for_nums, flags=re.IGNORECASE)

        # Replace 20k -> 20000
        text_for_nums = re.sub(r'(\d+(?:\.\d+)?)\s*k\b', lambda m: str(float(m.group(1)) * 1000), text_for_nums, flags=re.IGNORECASE)
        # European grouping: "€112.200" and "1.234.567" use dots as thousands
        # separators. Only exactly-3-digit dot groups qualify, and never when
        # an Indian lakh/LPA unit follows ("3.500 LPA" is 3.5 lakh).
        text_for_nums = re.sub(
            r'(\d{1,3}(?:\.\d{3})+)(?!\s*(?:lpa|lakh|lakhs|lac|crore|cr)\b)',
            lambda m: m.group(1).replace('.', ''),
            text_for_nums,
            flags=re.IGNORECASE,
        )
        cleaned_digits = re.sub(r'(?<=\d),(?=\d)', '', text_for_nums)
        numbers = [float(n) for n in re.findall(r'\d+(?:\.\d+)?', cleaned_digits)]

        if not numbers:
            return None, cleaned_text

        val = sum(numbers) / len(numbers) if len(numbers) > 1 else numbers[0]
        if period_mult == 1.0 and (category == "student" or opportunity_type == "internship") and val <= 75000:
            # Student internships in India: bare un-suffixed figures <= 75k are monthly stipends
            period_mult = 12.0
        annual_inr = val * period_mult * fx

        return round(annual_inr, 2), cleaned_text

    @staticmethod
    def parse_monthly_stipend(
        salary_str: Optional[str],
        *,
        salary_min: Optional[float] = None,
        salary_max: Optional[float] = None,
        salary_currency: Optional[str] = None,
        salary_period: Optional[str] = None,
        opportunity_type: Optional[str] = None,
        category: Optional[str] = None,
    ) -> Optional[float]:
        """Single monthly-INR baseline for client filters (Sprint-1).

        Returns None when undisclosed — never 0.0, so NULL semantics survive
        to the DB (filters exclude NULL instead of comparing against zero).
        Structured min/max/currency/period (Jobicy etc.) take precedence over
        free-text parsing; hourly rates annualize via FTE hours first.
        """
        if (
            salary_min is not None or salary_max is not None
        ) and salary_currency:
            vals = [
                v
                for v in (salary_min, salary_max)
                if isinstance(v, (int, float))
                and not isinstance(v, bool)
                and v > 0
            ]
            if vals:
                base = sum(vals) / len(vals)
                cur = salary_currency.strip().lower()
                fx = {
                    "usd": StructuralNormalizer.USD_TO_INR_PEG,
                    "$": StructuralNormalizer.USD_TO_INR_PEG,
                    "eur": StructuralNormalizer.EUR_TO_INR_PEG,
                    "€": StructuralNormalizer.EUR_TO_INR_PEG,
                    "gbp": StructuralNormalizer.GBP_TO_INR_PEG,
                    "£": StructuralNormalizer.GBP_TO_INR_PEG,
                }.get(cur, 1.0)
                period = (salary_period or "year").strip().lower()
                if period in ("hour", "hr", "hourly"):
                    annual = base * StructuralNormalizer.FTE_HOURS_PER_YEAR * fx
                elif period in ("week", "wk", "weekly"):
                    annual = base * 52.0 * fx
                elif period in ("month", "mo", "monthly"):
                    annual = base * 12.0 * fx
                else:
                    annual = base * fx
                if annual <= 0:
                    return None
                return round(annual / 12.0, 2)

        annual, _ = StructuralNormalizer.parse_salary(
            salary_str,
            opportunity_type=opportunity_type,
            category=category,
        )
        if annual is None or annual <= 0:
            return None
        return round(annual / 12.0, 2)


# ----------------------------------------------------------------------
# Alias engine
# ----------------------------------------------------------------------

class AliasEngine:
    """Finds canonical taxonomy terms present in free text.

    Matching uses a word boundary on both sides so aliases embedded inside
    other words are not matched (e.g. ``arm`` inside ``army``). Aliases that
    collide with common lowercase words (``c``, ``r``, ``be``, ``ba``, ...)
    are matched case-sensitively to avoid false positives.
    """

    _AMBIGUOUS = frozenset(
        {"c", "r", "be", "ba", "go", "is", "on", "at", "it", "as", "of",
         "to", "or", "in", "an", "if", "me", "my", "us", "we"}
    )

    def __init__(self, taxonomy: Optional[TaxonomyService] = None) -> None:
        self._taxonomy = taxonomy or default_taxonomy
        self._skill_patterns = self._build(self._taxonomy.skills())
        self._degree_patterns = self._build(self._taxonomy.degrees())

    def _build(self, items: Dict[str, Any]) -> List[Tuple[Any, Any, bool]]:
        """Return [(compiled_pattern, canonical, is_ambiguous)] tuples."""
        patterns: List[Tuple[Any, Any, bool]] = []
        for canonical, meta in items.items():
            aliases = meta if isinstance(meta, list) else meta.get("aliases", [])
            for alias in [canonical] + list(aliases):
                ambiguous = alias.lower() in self._AMBIGUOUS
                flags = 0 if ambiguous else re.IGNORECASE
                pattern = re.compile(r"(?<!\w)" + re.escape(alias) + r"(?!\w)", flags)
                patterns.append((pattern, canonical, ambiguous))
        return patterns

    @staticmethod
    def _match(pattern: Any, alias: str, ambiguous: bool, text: str) -> bool:
        if pattern.search(text):
            return True
        if ambiguous:
            for variant in {alias, alias.capitalize(), alias.upper()}:
                if re.search(r"(?<!\w)" + re.escape(variant) + r"(?!\w)", text):
                    return True
        return False

    def find(self, patterns: List[Tuple[Any, Any, bool]], text: str) -> List[str]:
        """Return canonical matches present in ``text``, deduped in order."""
        seen = set()
        found: List[str] = []
        for pattern, canonical, ambiguous in patterns:
            if canonical in seen:
                continue
            if self._match(pattern, pattern.pattern, ambiguous, text):
                seen.add(canonical)
                found.append(canonical)
        return found

    def find_skills(self, text: str) -> List[str]:
        return self.find(self._skill_patterns, text)

    def find_degrees(self, text: str) -> List[str]:
        return self.find(self._degree_patterns, text)


# ----------------------------------------------------------------------
# Skill extraction
# ----------------------------------------------------------------------

import difflib


def get_sample_context(text: str, term: str, max_window: int = 40) -> str:
    """Extract a surrounding snippet around term in text."""
    if not text or not term:
        return ""
    pos = text.lower().find(term.lower())
    if pos == -1:
        return ""
    start = max(0, pos - max_window)
    end = min(len(text), pos + len(term) + max_window)
    snippet = text[start:end].strip()
    prefix = "..." if start > 0 else ""
    suffix = "..." if end < len(text) else ""
    return f"{prefix}{snippet}{suffix}"


class CanonicalSuggester:
    """Fuzzy matching engine to suggest canonical taxonomy entries for unknown terms.

    Candidate pools are normalized ONCE per entity type (taxonomy is immutable
    for the process lifetime) instead of re-normalizing every canonical value
    for every unknown term, and ``difflib.get_close_matches`` prunes clearly
    unsuitable candidates before computing a full ratio. This keeps per-item
    suggestion cost bounded when a source yields thousands of records.
    """

    _SKILL_ENTITY_TYPES = frozenset(
        {"skill", "tool", "framework", "technology", "programming_language",
         "certification", "research_area", "competition"}
    )

    def __init__(self, taxonomy: Optional[TaxonomyService] = None) -> None:
        self._taxonomy = taxonomy or default_taxonomy
        self._pools: Dict[str, Tuple[List[str], Dict[str, str]]] = {}

    def _candidate_pool(self, entity_type: str) -> Tuple[List[str], Dict[str, str]]:
        """Return (normalized_candidates, normalized_to_display), cached."""
        pool_key = (
            "skill"
            if entity_type in self._SKILL_ENTITY_TYPES
            else entity_type
        )
        cached = self._pools.get(pool_key)
        if cached is not None:
            return cached

        if pool_key == "skill":
            candidates: List[str] = list(self._taxonomy.canonical_skills())
        elif pool_key == "degree":
            candidates = list(self._taxonomy.degrees().keys())
        elif pool_key == "company":
            company_meta = self._taxonomy.company_alias_map()
            candidates = sorted({m["display"] for m in company_meta.values()})
        else:
            candidates = []

        normalized: List[str] = []
        display_by_norm: Dict[str, str] = {}
        for candidate in candidates:
            norm = normalize_token(candidate)
            if not norm or norm in display_by_norm:
                continue
            display_by_norm[norm] = candidate
            normalized.append(norm)
        pool = (normalized, display_by_norm)
        self._pools[pool_key] = pool
        return pool

    def suggest(self, raw_value: str, entity_type: str = "skill") -> Tuple[Optional[str], float]:
        """Suggest canonical name and similarity confidence score (0.0 to 1.0)."""
        if not raw_value or not raw_value.strip():
            return None, 0.0

        raw_norm = normalize_token(raw_value)
        candidates, display_by_norm = self._candidate_pool(entity_type)
        if not candidates:
            return None, 0.0

        exact = display_by_norm.get(raw_norm)
        if exact is not None:
            return exact, 1.0

        # cutoff matches the historical 0.65 acceptance threshold; the
        # real_quick/quick ratio bounds used by difflib guarantee no
        # acceptable candidate is pruned.
        matches = difflib.get_close_matches(raw_norm, candidates, n=1, cutoff=0.65)
        if not matches:
            return None, 0.0
        best_norm = matches[0]
        best_score = difflib.SequenceMatcher(None, raw_norm, best_norm).ratio()
        return display_by_norm[best_norm], round(best_score, 2)


# ----------------------------------------------------------------------
# Skill extraction
# ----------------------------------------------------------------------

class SkillExtractor:
    """Stage: SKILL_EXTRACT — canonical skills + unknown skill discovery."""

    _STOPWORDS = frozenset(
        """the and for with requirements apply full time developer intern
        engineer job remote india location work looking fresher experience
        student graduate company about role day week month year good strong
        our their this that from have will should into over under about than
        software hardware team what who when where why how per not are was
        being been native opportunities opportunity candidates candidate
        joining immediately start date letter ctc perks benefits stipend paid
        unpaid training certification build using need must join help create
        learn understand able strong solid working know good excellent
        finance tech support manager business""".split()
    )

    def __init__(self, taxonomy: Optional[TaxonomyService] = None,
                 alias_engine: Optional[AliasEngine] = None) -> None:
        self._taxonomy = taxonomy or default_taxonomy
        self._engine = alias_engine or AliasEngine(self._taxonomy)
        self._suggester = CanonicalSuggester(self._taxonomy)
        # Taxonomy is immutable for the process lifetime; build these once
        # instead of re-deriving them for every ingested record.
        self._alias_map = self._taxonomy.skill_alias_map()
        self._skills = self._taxonomy.skills()

    def extract(
        self,
        title: str = "",
        description: str = "",
        structured: Optional[List[Any]] = None,
    ) -> Tuple[List[str], List[str]]:
        """Return (known canonical skills, unknown candidate skills)."""
        text = " ".join(filter(None, [title, description]))
        structured_texts = [str(s) for s in (structured or []) if str(s).strip()]
        all_text = " ".join([text] + structured_texts)
        known = self._engine.find_skills(all_text)
        unknown = self._unknown_skills(all_text, known)
        return known, unknown

    def extract_unknown_details(
        self,
        title: str = "",
        description: str = "",
        structured: Optional[List[Any]] = None,
        known: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Return enriched list of unknown entities (skills, tools, frameworks, tech, etc.).

        ``known`` may be supplied by a caller that already ran :meth:`extract`
        so the full-text skill scan is not repeated (normalizer hot path).
        """
        text = " ".join(filter(None, [title, description]))
        structured_texts = [str(s) for s in (structured or []) if str(s).strip()]
        all_text = " ".join([text] + structured_texts)
        if known is None:
            known = self._engine.find_skills(all_text)
        unknown_terms = self._unknown_skills(all_text, known)

        details: List[Dict[str, Any]] = []
        for term in unknown_terms:
            entity_type, confidence = self._classify_term(term, all_text)
            context = get_sample_context(all_text, term)
            suggested, match_conf = self._suggester.suggest(term, entity_type)
            details.append({
                "entity_type": entity_type,
                "raw_value": term,
                "confidence": confidence,
                "suggested_canonical": suggested,
                "match_confidence": match_conf,
                "sample_context": context,
                "field_source": "description" if term in description else "title",
            })
        return details

    def _classify_term(self, term: str, context_text: str) -> Tuple[str, float]:
        term_lower = term.lower()
        context_lower = context_text.lower()

        if any(k in term_lower or k in context_lower for k in ["certified", "certification", "practitioner", "pmp"]):
            return "certification", 0.92
        if any(k in term_lower for k in ["cursor", "copilot", "postman", "docker", "figma", "jira", "chatgpt", "claude"]):
            return "tool", 0.95
        if term_lower.endswith(" ai") or term_lower.endswith(" tool") or term_lower.endswith(" studio") or term_lower.endswith(" ide"):
            return "tool", 0.90
        if term_lower.endswith(".js") or term_lower.endswith(".py") or "graph" in term_lower or "framework" in term_lower or term_lower in ["react", "vue", "angular", "nextjs", "langgraph", "fastapi", "django", "flask"]:
            return "framework", 0.92
        if term_lower in ["mcp", "kubernetes", "kafka", "aws", "gcp", "azure", "docker", "terraform", "graphql"] or "protocol" in term_lower:
            return "technology", 0.90
        if term_lower in ["mojo", "zig", "elixir", "rust", "kotlin", "typescript", "golang", "scala", "haskell", "clojure"]:
            return "programming_language", 0.95
        if any(k in term_lower or k in context_lower for k in ["interpretability", "quantization", "llm", "rlhf", "nlp", "genai"]):
            return "research_area", 0.88
        if any(k in term_lower for k in ["hackathon", "challenge", "grand prix", "contest"]):
            return "competition", 0.90

        return "skill", 0.75

    def _unknown_skills(self, text: str, known: List[str]) -> List[str]:
        known_lower = {k.lower() for k in known}
        alias_map = self._alias_map
        covered = self._covered_tokens(known)
        candidates = re.findall(r"\b[A-Z][a-zA-Z0-9+#'-]{2,}\b", text)
        seen = set()
        unknown: List[str] = []
        for term in candidates:
            lowered = term.lower()
            if lowered in known_lower or lowered in alias_map:
                continue
            if lowered in covered or lowered in self._STOPWORDS:
                continue
            if lowered in seen:
                continue
            seen.add(lowered)
            unknown.append(term)
            if len(unknown) >= 10:
                break
        return unknown

    def _covered_tokens(self, known: List[str]) -> set:
        """Tokens from matched multi-word skills, so their parts are not
        reported as unknown (e.g. 'Machine' from 'Machine Learning')."""
        skills = self._skills
        covered: set = set()
        for canonical in known:
            aliases = [canonical] + list(skills.get(canonical, []))
            for alias in aliases:
                covered.update(
                    w for w in re.findall(r"\w+", alias.lower()) if len(w) > 2
                )
        return covered


# ----------------------------------------------------------------------
# Company normalization
# ----------------------------------------------------------------------

class CompanyNormalizer:
    """Stage: COMPANY_NORMALIZE — stable company_id + display name."""

    def __init__(self, taxonomy: Optional[TaxonomyService] = None) -> None:
        self._taxonomy = taxonomy or default_taxonomy
        # Alias map + longest-first order are immutable for the process
        # lifetime; cache them instead of rebuilding per record.
        self._aliases = self._taxonomy.company_alias_map()
        self._sorted_aliases = sorted(self._aliases, key=len, reverse=True)

    def normalize(self, company_raw: Optional[str], include_unknown: bool = False) -> Tuple[Any, ...]:
        """Return (company_id, display_name) or (company_id, display_name, is_unknown)."""
        cleaned = StructuralNormalizer.clean_text(company_raw)
        if not cleaned:
            return (None, None, False) if include_unknown else (None, None)
        norm = normalize_token(cleaned)
        aliases = self._aliases

        canonical = None
        display = None
        for alias in self._sorted_aliases:
            if norm == alias or norm.startswith(alias + " "):
                canonical = aliases[alias]["canonical"]
                display = aliases[alias]["display"]
                break

        is_unknown = False
        if canonical is None:
            base = self._strip_suffixes(norm)
            meta = aliases.get(base)
            if meta:
                canonical = meta["canonical"]
                display = meta["display"]
            else:
                display = self._title_case(base) or self._title_case(norm)
                canonical = display
                is_unknown = True

        if include_unknown:
            return slugify(canonical), display, is_unknown
        return slugify(canonical), display

    def _strip_suffixes(self, norm: str) -> str:
        tokens = norm.split()
        suffixes = [s.split() for s in self._taxonomy.company_suffixes()]
        changed = True
        while changed and len(tokens) > 1:
            changed = False
            for s_tokens in suffixes:
                if len(tokens) >= len(s_tokens) and tokens[-len(s_tokens):] == s_tokens:
                    remaining = tokens[:-len(s_tokens)]
                    if remaining:
                        tokens = remaining
                        changed = True
                    break
        return " ".join(tokens)

    @staticmethod
    def _title_case(value: str) -> str:
        return " ".join(w.capitalize() for w in value.split() if w)


# ----------------------------------------------------------------------
# Location normalization
# ----------------------------------------------------------------------

class LocationNormalizer:
    """Stage: LOCATION_NORMALIZE — canonical location + remote/hybrid mode."""

    def __init__(self, taxonomy: Optional[TaxonomyService] = None) -> None:
        self._taxonomy = taxonomy or default_taxonomy
        # Compile the longest-alias-first city patterns once: _find_city runs
        # for every record and must not recompile hundreds of regexes.
        aliases = self._taxonomy.location_alias_map()
        self._city_patterns = [
            (re.compile(r"(?<!\w)" + re.escape(alias) + r"(?!\w)"), canonical)
            for alias, canonical in sorted(
                aliases.items(), key=lambda item: len(item[0]), reverse=True
            )
        ]

    def normalize(
        self,
        raw_location: Optional[str],
        title: str = "",
        description: str = "",
    ) -> Tuple[str, bool, bool]:
        """Return (canonical_location, is_remote, is_hybrid)."""
        combined = " ".join(filter(None, [raw_location, title, description]))
        lower = combined.lower()
        is_remote = any(kw in lower for kw in self._taxonomy.remote_keywords())
        is_hybrid = (not is_remote) and any(
            kw in lower for kw in self._taxonomy.hybrid_keywords()
        )

        if not raw_location:
            return ("Remote" if is_remote else "India"), is_remote, is_hybrid

        if is_remote:
            return "Remote", True, False

        city = self._find_city(raw_location)
        if city:
            return self._format(city), is_remote, is_hybrid

        if any(syn in lower for syn in self._taxonomy.india_synonyms()):
            return "India", is_remote, is_hybrid

        cleaned = StructuralNormalizer.clean_text(raw_location)
        return cleaned or "India", is_remote, is_hybrid

    def _find_city(self, raw_location: str) -> Optional[str]:
        norm = normalize_token(raw_location)
        for pattern, canonical in self._city_patterns:
            if pattern.search(norm):
                return canonical
        return None

    def _format(self, city: str) -> str:
        entry = self._taxonomy.locations()[city]
        parts = [city]
        if entry.get("state"):
            parts.append(entry["state"])
        if entry.get("kind") == "city" and entry.get("country") and entry["country"] not in parts:
            parts.append(entry["country"])
        return ", ".join(parts)

    def get_location_details(
        self,
        raw_location: Optional[str],
        title: str = "",
        description: str = "",
    ) -> Dict[str, Any]:
        """Return structured 3-tier location breakdown dict."""
        canonical_loc, is_remote, is_hybrid = self.normalize(raw_location, title, description)
        city = self._find_city(raw_location or "")
        if city and city in self._taxonomy.locations():
            entry = self._taxonomy.locations()[city]
            return {
                "city": city,
                "district": entry.get("district", city),
                "state": entry.get("state", "India"),
                "country": entry.get("country", "India"),
                "tier": entry.get("tier", "Tier-1"),
                "is_remote": is_remote,
                "is_hybrid": is_hybrid,
            }
        return {
            "city": canonical_loc,
            "district": None,
            "state": "India",
            "country": "India",
            "tier": "Remote" if is_remote else "Tier-1",
            "is_remote": is_remote,
            "is_hybrid": is_hybrid,
        }


# ----------------------------------------------------------------------
# Degree extraction
# ----------------------------------------------------------------------

class DegreeExtractor:
    """Stage: DEGREE_EXTRACT — degree eligibility from free text."""

    def __init__(self, taxonomy: Optional[TaxonomyService] = None,
                 alias_engine: Optional[AliasEngine] = None) -> None:
        self._taxonomy = taxonomy or default_taxonomy
        self._engine = alias_engine or AliasEngine(self._taxonomy)
        self._suggester = CanonicalSuggester(self._taxonomy)
        # Immutable taxonomy data cached outside the per-record hot path.
        self._degree_map = self._taxonomy.degree_alias_map()
        self._degree_items = self._taxonomy.degrees()

    def extract(self, text: str) -> List[str]:
        return self._engine.find_degrees(text or "")

    def extract_unknowns(
        self,
        text: str,
        known_degrees: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Identify candidate degree terms in text that are not in degrees taxonomy.

        ``known_degrees`` may be supplied by a caller that already ran
        :meth:`extract`, avoiding a duplicate full-text scan.
        """
        if not text:
            return []
        known_degrees = (
            self.extract(text) if known_degrees is None else known_degrees
        )
        known_tokens = self.covered_tokens(known_degrees)
        degree_map = self._degree_map

        candidates = re.findall(
            r"\b(?:B|M|D|Ph)\.[A-Za-z0-9.]+\b|\b(?:BTech|MTech|BSc|MSc|BCom|MCom|BArch|MArch|BDes|MDes|PGDCA)\b",
            text,
        )
        unknowns: List[Dict[str, Any]] = []
        seen = set()
        for cand in candidates:
            norm = normalize_token(cand)
            if len(norm) < 2 or norm in degree_map or norm in known_tokens:
                continue
            if norm in seen:
                continue
            seen.add(norm)
            context = get_sample_context(text, cand)
            suggested, match_conf = self._suggester.suggest(cand, "degree")
            unknowns.append({
                "entity_type": "degree",
                "raw_value": cand,
                "confidence": 0.85,
                "suggested_canonical": suggested,
                "match_confidence": match_conf,
                "sample_context": context,
                "field_source": "description",
            })
        return unknowns

    def covered_tokens(self, degrees: List[str]) -> set:
        """Tokens from matched degrees, so their parts (e.g. 'MBA') are not
        reported as unknown skills by other components."""
        items = self._degree_items
        covered: set = set()
        for canonical in degrees:
            meta = items.get(canonical, {})
            aliases = meta if isinstance(meta, list) else meta.get("aliases", [])
            for alias in [canonical] + list(aliases):
                covered.update(
                    w for w in re.findall(r"\w+", alias.lower()) if len(w) > 2
                )
        return covered


# ----------------------------------------------------------------------
# Opportunity Type Normalization
# ----------------------------------------------------------------------

class OpportunityTypeNormalizer:
    """Stage: OPPORTUNITY_TYPE_NORMALIZE — standard opportunity type + unknown detection."""

    STANDARD_TYPES = frozenset(
        {"job", "internship", "competition", "scholarship", "freelance",
         "full_time", "part_time", "contract", "student", "private", "government"}
    )

    KNOWN_EMERGING = {
        "residency": "AI Residency",
        "fellowship": "Fellowship",
        "bootcamp": "Bootcamp",
        "apprenticeship": "Apprenticeship",
        "hackathon": "Hackathon",
        "incubation": "Incubation Program",
    }

    def extract_unknown(self, opp_type: Optional[str], title: str = "") -> Optional[Dict[str, Any]]:
        text = f"{opp_type or ''} {title}".strip()
        lower = text.lower()
        if opp_type and opp_type.lower() in self.STANDARD_TYPES:
            return None

        for kw, canonical in self.KNOWN_EMERGING.items():
            if kw in lower:
                return {
                    "entity_type": "opportunity_type",
                    "raw_value": canonical,
                    "confidence": 0.90,
                    "suggested_canonical": canonical,
                    "match_confidence": 1.0,
                    "sample_context": get_sample_context(text, kw),
                    "field_source": "opportunity_type" if opp_type else "title",
                }
        return None


__all__ = [
    "StructuralNormalizer",
    "AliasEngine",
    "CanonicalSuggester",
    "SkillExtractor",
    "CompanyNormalizer",
    "LocationNormalizer",
    "DegreeExtractor",
    "OpportunityTypeNormalizer",
    "get_sample_context",
]

