"""
Taxonomy Service
================
Single source of truth for all data-driven taxonomy used during normalization.

All lists, aliases and canonical names live in JSON files under
``backend/data/`` so updating taxonomy (adding skills, locations, companies,
degrees, aliases) requires ZERO code changes:

- ``skills_aliases.json``   : canonical skill -> list of aliases
- ``location_aliases.json`` : canonical location -> {aliases, state, country}
- ``companies.json``        : company suffixes + brand alias map
- ``degrees.json``          : canonical degree -> {aliases, level}

Every normalization component (``SkillExtractor``, ``CompanyNormalizer``,
``LocationNormalizer``, ``DegreeExtractor``) calls this service instead of
embedding its own keyword lists. ``EntityNormalizer`` reports the aggregated
``version()`` as the ``taxonomy_version`` telemetry field.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"

SKILLS_FILE = "skills_aliases.json"
LOCATIONS_FILE = "location_aliases.json"
COMPANIES_FILE = "companies.json"
DEGREES_FILE = "degrees.json"

_TAXONOMY_FILES = (SKILLS_FILE, LOCATIONS_FILE, COMPANIES_FILE, DEGREES_FILE)


def normalize_token(value: str) -> str:
    """Lowercase, drop periods, and collapse whitespace for alias lookups."""
    return re.sub(r"\s+", " ", re.sub(r"\.", "", value)).strip().lower()


def slugify(value: str) -> str:
    """Lowercase alphanumeric slug used for stable ids (e.g. company_id)."""
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


class TaxonomyService:
    """Cached, data-driven taxonomy loader.

    Components share a single instance (module-level ``taxonomy``) so the JSON
    files are parsed once per process.
    """

    def __init__(self, data_dir: Optional[Path] = None) -> None:
        self._data_dir = data_dir or _DATA_DIR
        self._cache: Dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Low-level loading
    # ------------------------------------------------------------------

    def _load(self, filename: str) -> Dict[str, Any]:
        if filename not in self._cache:
            with open(self._data_dir / filename, encoding="utf-8") as f:
                self._cache[filename] = json.load(f)
        return self._cache[filename]

    def version(self) -> str:
        """Aggregate taxonomy version string for telemetry.

        Compact form to fit the ``taxonomy_version VARCHAR(20)`` database
        column: ``skills<major>|loc<major>|c<major>|d<major>``.
        """
        versions = {
            name: str(self._load(name).get("version", "1.0"))
            for name in _TAXONOMY_FILES
        }

        def _major(value: str) -> str:
            return value.split(".", 1)[0]

        return (
            f"skills{_major(versions[SKILLS_FILE])}"
            f"|loc{_major(versions[LOCATIONS_FILE])}"
            f"|c{_major(versions[COMPANIES_FILE])}"
            f"|d{_major(versions[DEGREES_FILE])}"
        )

    # ------------------------------------------------------------------
    # Skills
    # ------------------------------------------------------------------

    def skills(self) -> Dict[str, List[str]]:
        return self._load(SKILLS_FILE)["skills"]

    def canonical_skills(self) -> List[str]:
        return list(self.skills().keys())

    def skill_alias_map(self) -> Dict[str, str]:
        """Normalized alias -> canonical skill display name."""
        result: Dict[str, str] = {}
        for canonical, aliases in self.skills().items():
            result[normalize_token(canonical)] = canonical
            for alias in aliases:
                result[normalize_token(alias)] = canonical
        return result

    # ------------------------------------------------------------------
    # Locations
    # ------------------------------------------------------------------

    def locations(self) -> Dict[str, Dict[str, Any]]:
        return self._load(LOCATIONS_FILE)["locations"]

    def location_alias_map(self) -> Dict[str, str]:
        """Normalized alias -> canonical location name."""
        result: Dict[str, str] = {}
        for canonical, entry in self.locations().items():
            result[normalize_token(canonical)] = canonical
            for alias in entry.get("aliases", []):
                result[normalize_token(alias)] = canonical
        return result

    def remote_keywords(self) -> List[str]:
        return self._load(LOCATIONS_FILE).get("remote_keywords", [])

    def hybrid_keywords(self) -> List[str]:
        return self._load(LOCATIONS_FILE).get("hybrid_keywords", [])

    def india_synonyms(self) -> List[str]:
        return self._load(LOCATIONS_FILE).get("india_synonyms", [])

    # ------------------------------------------------------------------
    # Companies
    # ------------------------------------------------------------------

    def company_suffixes(self) -> List[str]:
        """Company name suffixes, normalized and ordered longest-first."""
        suffixes = self._load(COMPANIES_FILE).get("suffixes", [])
        normalized = sorted(
            {normalize_token(s) for s in suffixes if s.strip()},
            key=len,
            reverse=True,
        )
        return normalized

    def company_alias_map(self) -> Dict[str, Dict[str, str]]:
        """Normalized company alias -> {"canonical", "display"}."""
        result: Dict[str, Dict[str, str]] = {}
        for alias, meta in self._load(COMPANIES_FILE).get("aliases", {}).items():
            result[normalize_token(alias)] = {
                "canonical": meta["canonical"],
                "display": meta.get("display", meta["canonical"]),
            }
        return result

    # ------------------------------------------------------------------
    # Degrees
    # ------------------------------------------------------------------

    def degrees(self) -> Dict[str, Dict[str, Any]]:
        return self._load(DEGREES_FILE)["degrees"]

    def degree_alias_map(self) -> Dict[str, str]:
        """Normalized degree alias -> canonical degree name."""
        result: Dict[str, str] = {}
        for canonical, entry in self.degrees().items():
            result[normalize_token(canonical)] = canonical
            for alias in entry.get("aliases", []):
                result[normalize_token(alias)] = canonical
        return result


# Shared process-wide taxonomy instance.
taxonomy = TaxonomyService()

__all__ = [
    "TaxonomyService",
    "taxonomy",
    "normalize_token",
    "slugify",
]
