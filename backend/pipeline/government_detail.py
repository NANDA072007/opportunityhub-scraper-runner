"""Structured field extraction from government recruitment detail pages.

The RSS summary often stops at "visit the website", so the deadline, vacancy
count, qualification, age limit and fee never reach the database. This module
reads a detail page that we are allowed to fetch (the caller enforces
robots.txt and rate limits) and returns only high-confidence facts plus the
phrase each fact came from. FreeJobAlert pages expose these in tables; other
official pages are handled with conservative text patterns.

Nothing here copies page prose into the database. Only the extracted facts and
a short evidence string are stored.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from backend.pipeline.degrees import extract_degrees
from backend.pipeline.government_deadline import extract_application_deadline

_QUALIFICATION_WINDOW = re.compile(
    r"\bqualification\b\s*[:\-]?\s*(.{3,180}?)(?:\bvacanc|\bage\b|\bsalary\b|$)",
    re.I | re.S,
)


@dataclass(frozen=True)
class DetailEnrichment:
    deadline: Optional[date] = None
    vacancies: Optional[int] = None
    degrees: List[str] = field(default_factory=list)
    max_age: Optional[int] = None
    application_fee: Optional[int] = None
    qualification_text: Optional[str] = None
    evidence: Dict[str, str] = field(default_factory=dict)


def is_pdf_url(url: Optional[str]) -> bool:
    return str(url or "").split("?")[0].lower().endswith(".pdf")


def host_of(url: Optional[str]) -> str:
    try:
        return (urlparse(str(url or "")).hostname or "").lower().replace("www.", "")
    except ValueError:
        return ""


def _clean(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _table_rows(soup: BeautifulSoup) -> List[List[str]]:
    rows: List[List[str]] = []
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            cells = [
                _clean(cell.get_text(" ", strip=True))
                for cell in tr.find_all(["td", "th"])
            ]
            if cells:
                rows.append(cells)
    return rows


def _find_row(rows: List[List[str]], keyword: str) -> Optional[List[str]]:
    needle = keyword.lower()
    for row in rows:
        if row and needle in row[0].lower():
            return row
    return None


def _to_int(value: str) -> Optional[int]:
    match = re.search(r"\d[\d,]*", value or "")
    if not match:
        return None
    try:
        return int(match.group(0).replace(",", ""))
    except ValueError:
        return None


def _valid_age(value: int) -> bool:
    return 18 <= value <= 70


def _parse_age(text: str) -> Optional[int]:
    # Only an explicit age-limit statement counts, and the value must look like
    # an age. Cut-off dates ("upper age limit 03-11-2026") otherwise parse as
    # the range 03-11.
    for match in re.finditer(
        r"\bage\s+limit\b[^0-9]{0,25}(\d{2})\s*(?:-|to)\s*(\d{2})\b",
        text,
        re.I,
    ):
        candidate = int(match.group(2))
        if _valid_age(candidate):
            return candidate
    for match in re.finditer(
        r"\bage\s+limit\b[^0-9]{0,25}(\d{2})\b", text, re.I
    ):
        candidate = int(match.group(1))
        if _valid_age(candidate):
            return candidate
    for match in re.finditer(
        r"\b(?:upper|maximum)\s+age(?:\s+limit)?\b[^0-9]{0,25}(\d{2})\b",
        text,
        re.I,
    ):
        candidate = int(match.group(1))
        if _valid_age(candidate):
            return candidate
    return None


def _parse_fee(text: str) -> Optional[int]:
    match = re.search(
        r"\bapplication\s+fee\b[^0-9]{0,25}(?:rs\.?|inr|\u20b9)\s*([\d,]+)",
        text,
        re.I,
    )
    if not match:
        return None
    try:
        return int(match.group(1).replace(",", ""))
    except ValueError:
        return None


def _parse_vacancies(text: str) -> Optional[int]:
    # A bare "N posts" inside unrelated prose is not a vacancy count.
    match = re.search(
        r"\btotal\s+vacanc(?:y|ies)\b\s*[:\-]?\s*(\d{1,5})\b", text, re.I
    )
    if not match:
        # "VACANCY 91 Posts" is a vacancy count; "(458 Vacancy) GSSSB 84"
        # is a sidebar link, so the number must follow the word directly.
        match = re.search(
            r"\bvacanc(?:y|ies)\b\s*[:\-]?\s*(\d{1,5})\s*(?:posts?|openings?)\b",
            text,
            re.I,
        )
    if not match:
        return None
    value = int(match.group(1))
    return value if 0 < value <= 100000 else None


def enrich_detail_html(
    html: str,
    *,
    posted_at: object = None,
    source_url: Optional[str] = None,
) -> DetailEnrichment:
    """Extract high-confidence government facts from a detail page."""
    soup = BeautifulSoup(html, "html.parser")
    text = _clean(soup.get_text(" ", strip=True))
    rows = _table_rows(soup)
    evidence: Dict[str, str] = {}

    deadline = None
    last_date_row = _find_row(rows, "last date")
    if last_date_row and len(last_date_row) > 1:
        parsed = extract_application_deadline(
            " ".join(last_date_row), posted_at=posted_at
        )
        if parsed:
            deadline = parsed.deadline
            evidence["deadline"] = _clean(" ".join(last_date_row))[:120]
    if deadline is None:
        parsed = extract_application_deadline(text, posted_at=posted_at)
        if parsed:
            deadline = parsed.deadline
            evidence["deadline"] = parsed.phrase

    vacancies = None
    vacancy_row = _find_row(rows, "total vacancies")
    if vacancy_row and len(vacancy_row) > 1:
        vacancies = _to_int(vacancy_row[1])
    if vacancies is None:
        vacancies = _parse_vacancies(text)
    if vacancies is not None:
        evidence["vacancies"] = f"total vacancies: {vacancies}"

    qualification_text = None
    qualification_row = _find_row(rows, "qualification")
    if qualification_row and len(qualification_row) > 1:
        qualification_text = qualification_row[1][:180]
    if not qualification_text:
        match = _QUALIFICATION_WINDOW.search(text)
        if match:
            qualification_text = _clean(match.group(1))[:180]
    if qualification_text:
        evidence["qualification"] = qualification_text

    degrees = (
        extract_degrees(qualification_text).degrees if qualification_text else []
    )

    max_age = _parse_age(text)
    if max_age is not None:
        evidence["max_age"] = f"age limit: {max_age}"

    fee = _parse_fee(text)
    if fee is not None:
        evidence["application_fee"] = f"application fee: {fee}"

    return DetailEnrichment(
        deadline=deadline,
        vacancies=vacancies,
        degrees=degrees,
        max_age=max_age,
        application_fee=fee,
        qualification_text=qualification_text,
        evidence=evidence,
    )
