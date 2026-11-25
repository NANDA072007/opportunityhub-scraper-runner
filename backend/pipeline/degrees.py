"""Canonical degree extraction from job text.

Only explicit, high-confidence degree tokens count. Bare words such as "PG",
"master" (Scrum Master) or "degree" are deliberately not mapped: showing the
wrong required degree on a listing is worse than showing none.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

_DEGREE_PATTERNS: Tuple[Tuple[str, re.Pattern], ...] = (
    ("PhD", re.compile(r"\bph\.?\s?d\b", re.I)),
    ("MBBS", re.compile(r"\bmbbs\b", re.I)),
    ("M.Tech", re.compile(r"\bm\.?\s?tech\b", re.I)),
    ("M.Sc", re.compile(r"\bm\.?\s?sc\b", re.I)),
    ("MBA", re.compile(r"\bmba\b|\bpgdm\b", re.I)),
    ("MCA", re.compile(r"\bmca\b", re.I)),
    ("M.Pharm", re.compile(r"\bm\.?\s?pharm\b", re.I)),
    ("B.Pharm", re.compile(r"\bb\.?\s?pharm\b", re.I)),
    ("Pharm.D", re.compile(r"\bpharm\.?\s?d\b", re.I)),
    ("B.Arch", re.compile(r"\bb\.?\s?arch\b", re.I)),
    ("B.Des", re.compile(r"\bb\.?\s?des\b", re.I)),
    ("B.Ed", re.compile(r"\bb\.?\s?ed\b", re.I)),
    (
        "CA",
        re.compile(
            r"\bchartered\s+accountant\b|"
            r"\bca\s*\(\s*chartered\s+accountant\s*\)|"
            r"\bca\s+(?:inter|final|articleship|cpt)\b",
            re.I,
        ),
    ),
    (
        "Master's",
        re.compile(r"\bmaster'?s?\s+degree\b|\bpost\s*graduate\b", re.I),
    ),
    ("B.Tech", re.compile(r"\bb\.?\s?tech\b|\bb\.e\.?\b", re.I)),
    ("B.Sc", re.compile(r"\bb\.?\s?sc\b|\bb\.s\.?\b", re.I)),
    ("BCA", re.compile(r"\bbca\b", re.I)),
    ("B.Com", re.compile(r"\bb\.?\s?com\b", re.I)),
    ("B.A", re.compile(r"\bb\.a\.?\b|\bbachelor\s+of\s+arts\b", re.I)),
    (
        "Bachelor's",
        re.compile(
            r"\bbachelor'?s?\b|\bany\s+graduate\b|(?<!post-)\bgraduation\b|"
            r"\bba\s*/\s*bs\b|\b(?:ba|bs)\s+degree\b",
            re.I,
        ),
    ),
    ("Diploma", re.compile(r"\bdiploma\b", re.I)),
    ("ITI", re.compile(r"\biti\b", re.I)),
    (
        "12th",
        re.compile(
            r"\b12th\b|\bhsc\s+pass\b|"
            r"\bintermediate\s+(?:pass|examination|certificate)\b",
            re.I,
        ),
    ),
    (
        "10th",
        re.compile(
            r"\b10th\b|\bmatric(?:ulation)?\b|\bssc\s+(?:pass|certificate)\b",
            re.I,
        ),
    ),
)

_NEGATION = re.compile(
    r"\b(?:no|not)\s+(?:a\s+)?(?:specific\s+)?degree\s+(?:is\s+)?"
    r"(?:required|necessary)\b|\bdegree\s+not\s+required\b",
    re.I,
)

_EVIDENCE_WINDOW = 90


@dataclass(frozen=True)
class DegreeExtraction:
    degrees: List[str] = field(default_factory=list)
    evidence: Optional[str] = None


def extract_degrees(text: Optional[str]) -> DegreeExtraction:
    if not text:
        return DegreeExtraction()

    normalized = re.sub(r"\s+", " ", str(text))
    if _NEGATION.search(normalized):
        return DegreeExtraction()

    found: List[str] = []
    first_match: Optional[re.Match] = None
    for label, pattern in _DEGREE_PATTERNS:
        match = pattern.search(normalized)
        if match is None or label in found:
            continue
        found.append(label)
        if first_match is None or match.start() < first_match.start():
            first_match = match

    if not found or first_match is None:
        return DegreeExtraction()

    start = max(0, first_match.start() - 40)
    end = min(len(normalized), first_match.end() + _EVIDENCE_WINDOW)
    evidence = normalized[start:end].strip()
    return DegreeExtraction(degrees=found, evidence=evidence[:160])
