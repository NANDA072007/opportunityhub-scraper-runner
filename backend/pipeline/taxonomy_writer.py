"""
Taxonomy Writer
===============
Makes safe, atomic, data-driven updates to the taxonomy JSON files under
``backend/data/``. This is the only code path that modifies taxonomy, and it is
used by the ``taxonomy_admin`` CLI when an operator approves an unknown entity.

Each update:
- reads the JSON file fresh from disk,
- applies the requested addition (canonical + aliases),
- bumps the ``version`` minor field (telemetry reflects the change),
- atomically replaces the file (temp file + ``os.replace``) so a crash never
  leaves a half-written taxonomy file.

No hardcoded lists live here: every insertion targets a specific data file and
validates against the file's existing schema before writing.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"

SKILLS_FILE = "skills_aliases.json"
LOCATIONS_FILE = "location_aliases.json"
COMPANIES_FILE = "companies.json"
DEGREES_FILE = "degrees.json"


class TaxonomyWriteError(ValueError):
    """Raised when an approval cannot be applied to the taxonomy data."""


class TaxonomyWriter:
    """Validated, atomic updates to the data-driven taxonomy JSON files."""

    def __init__(self, data_dir: Optional[Path] = None) -> None:
        self._data_dir = Path(data_dir) if data_dir else _DATA_DIR

    # ------------------------------------------------------------------
    # Loading
    # ------------------------------------------------------------------

    def _path(self, filename: str) -> Path:
        return self._data_dir / filename

    def _load(self, filename: str) -> Dict[str, Any]:
        with open(self._path(filename), encoding="utf-8") as f:
            return json.load(f)

    def _save(self, filename: str, data: Dict[str, Any]) -> None:
        """Atomically replace ``filename`` with ``data``."""
        path = self._path(filename)
        fd, tmp = tempfile.mkstemp(
            dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
                f.write("\n")
            os.replace(tmp, path)
        except Exception:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    @staticmethod
    def _bump_version(version: str) -> str:
        major, _, minor = version.partition(".")
        try:
            return f"{major}.{int(minor or 0) + 1}"
        except ValueError:
            return f"{float(version) + 1:.1f}"

    # ------------------------------------------------------------------
    # Skills
    # ------------------------------------------------------------------

    def add_skill(
        self,
        canonical: str,
        aliases: Optional[List[str]] = None,
        *,
        skip_if_exists: bool = True,
    ) -> bool:
        """Add a canonical skill with aliases to ``skills_aliases.json``.

        Returns True when the taxonomy changed, False when ``canonical``
        already exists and ``skip_if_exists`` is True.
        """
        canonical = canonical.strip()
        if not canonical:
            raise TaxonomyWriteError("Canonical skill name is required.")
        data = self._load(SKILLS_FILE)
        skills: Dict[str, List[str]] = data["skills"]
        if canonical in skills:
            if skip_if_exists:
                return False
            raise TaxonomyWriteError(f"Skill '{canonical}' already exists.")
        existing = {alias.lower() for aliases_list in skills.values() for alias in aliases_list}
        clean_aliases = self._dedupe_aliases(aliases or [], canonical, existing)
        skills[canonical] = clean_aliases
        data["version"] = self._bump_version(data.get("version", "1.0"))
        self._save(SKILLS_FILE, data)
        return True

    # ------------------------------------------------------------------
    # Locations
    # ------------------------------------------------------------------

    def add_location(
        self,
        canonical: str,
        *,
        aliases: Optional[List[str]] = None,
        state: Optional[str] = None,
        country: str = "India",
        kind: str = "city",
        skip_if_exists: bool = True,
    ) -> bool:
        """Add a canonical location to ``location_aliases.json``."""
        canonical = canonical.strip()
        if not canonical:
            raise TaxonomyWriteError("Canonical location name is required.")
        data = self._load(LOCATIONS_FILE)
        locations: Dict[str, Any] = data["locations"]
        if canonical in locations:
            if skip_if_exists:
                return False
            raise TaxonomyWriteError(f"Location '{canonical}' already exists.")
        existing = {
            alias.lower()
            for entry in locations.values()
            for alias in entry.get("aliases", [])
        }
        clean_aliases = self._dedupe_aliases(aliases or [], canonical, existing)
        if kind == "city":
            entry: Dict[str, Any] = {
                "aliases": clean_aliases,
                "state": state or canonical,
                "country": country,
                "kind": kind,
            }
        else:
            entry = {"aliases": clean_aliases, "kind": kind}
        locations[canonical] = entry
        data["version"] = self._bump_version(data.get("version", "1.0"))
        self._save(LOCATIONS_FILE, data)
        return True

    # ------------------------------------------------------------------
    # Companies
    # ------------------------------------------------------------------

    def add_company_alias(
        self,
        alias: str,
        canonical: str,
        *,
        display: Optional[str] = None,
        skip_if_exists: bool = True,
    ) -> bool:
        """Add a company alias -> canonical mapping to ``companies.json``."""
        alias = alias.strip()
        canonical = canonical.strip()
        if not alias or not canonical:
            raise TaxonomyWriteError("Company alias and canonical are required.")
        data = self._load(COMPANIES_FILE)
        aliases: Dict[str, Any] = data.setdefault("aliases", {})
        if alias in aliases and skip_if_exists:
            return False
        aliases[alias] = {
            "canonical": canonical,
            "display": display or canonical,
        }
        data["version"] = self._bump_version(data.get("version", "1.0"))
        self._save(COMPANIES_FILE, data)
        return True

    # ------------------------------------------------------------------
    # Degrees
    # ------------------------------------------------------------------

    def add_degree(
        self,
        canonical: str,
        aliases: Optional[List[str]] = None,
        *,
        level: str = "bachelors",
        skip_if_exists: bool = True,
    ) -> bool:
        """Add a canonical degree to ``degrees.json``."""
        canonical = canonical.strip()
        if not canonical:
            raise TaxonomyWriteError("Canonical degree name is required.")
        data = self._load(DEGREES_FILE)
        degrees: Dict[str, Any] = data["degrees"]
        if canonical in degrees:
            if skip_if_exists:
                return False
            raise TaxonomyWriteError(f"Degree '{canonical}' already exists.")
        existing = {
            alias.lower()
            for entry in degrees.values()
            for alias in entry.get("aliases", [])
        }
        clean_aliases = self._dedupe_aliases(aliases or [], canonical, existing)
        degrees[canonical] = {"aliases": clean_aliases, "level": level}
        data["version"] = self._bump_version(data.get("version", "1.0"))
        self._save(DEGREES_FILE, data)
        return True

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _dedupe_aliases(
        aliases: List[str], canonical: str, existing: set
    ) -> List[str]:
        """Filter alias collisions and normalize casing/whitespace."""
        result: List[str] = []
        for alias in aliases:
            cleaned = " ".join(alias.strip().split())
            if not cleaned:
                continue
            key = cleaned.lower()
            if key in existing or key == canonical.lower() or key in {
                a.lower() for a in result
            }:
                continue
            result.append(cleaned)
        return result


__all__ = ["TaxonomyWriter", "TaxonomyWriteError"]
