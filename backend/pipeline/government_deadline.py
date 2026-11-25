"""Deterministic application-deadline extraction for government notices.

Government RSS summaries state the closing date in a small number of stock
phrases ("last date to apply is 03 November 2026", "applications will be
accepted till 07 November 2026"). Only an explicit application-deadline phrase
counts: interview, walk-in and result dates are deliberately ignored, because
publishing those as the deadline would hide a live listing.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Optional

_MONTHS = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}

_MONTH = (
    r"jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
    r"jul(?:y)?|aug(?:ust)?|sep(?:t|tember)?|oct(?:ober)?|nov(?:ember)?|"
    r"dec(?:ember)?"
)
_WORD_DATE = re.compile(rf"\b(\d{{1,2}})\s+({_MONTH})\.?,?\s+(\d{{4}})\b", re.I)
_WORD_DATE_NO_YEAR = re.compile(rf"\b(\d{{1,2}})\s+({_MONTH})\.?\b", re.I)
_NUMERIC_DATE = re.compile(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{4})\b")

_DEADLINE_PHRASES = (
    re.compile(r"\blast\s+date(?:\s+(?:to|for)\s+[^.]{0,45}?)?\s*(?:is|:)?\s*", re.I),
    re.compile(
        r"\bapplications?\s+(?:will\s+be\s+|are\s+|is\s+)?"
        r"(?:accepted|received|invited|submitted)\s+"
        r"(?:till|until|upto|up\s+to)\s*",
        re.I,
    ),
    re.compile(
        r"\b(?:apply(?:ing)?|submit(?:ting)?|send(?:ing)?)[^.]{0,45}?"
        r"\b(?:on\s+or\s+)?before\s*",
        re.I,
    ),
    re.compile(r"\bapply(?:ing)?\s+(?:online\s+)?by\s*", re.I),
    re.compile(
        r"\bapplications?\s+(?:close|closes|closed)\s+(?:on|by)?\s*", re.I
    ),
    re.compile(r"\bopen\s+until\s*", re.I),
    re.compile(r"\bclosing\s+date\s*(?:is|:)?\s*", re.I),
    re.compile(r"\bdeadline\s*(?:is|:)?\s*", re.I),
    re.compile(r"\bextended\s+(?:to|till|until)\s*", re.I),
)

_SEARCH_WINDOW = 60
_MAX_FUTURE_DAYS = 400
_MAX_PAST_DAYS = 3

# "Last date to pay the application fee" is not the application deadline.
_NEGATIVE_CONTEXT = re.compile(
    r"\b(fee|payment|correction|exam|result|admit|interview|"
    r"walk[-\s]?in|document\s+verification|counselling)\b",
    re.I,
)


@dataclass(frozen=True)
class ExtractedDeadline:
    deadline: date
    phrase: str


def _coerce_date(value: object) -> Optional[date]:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value is None:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
    except ValueError:
        pass
    try:
        return parsedate_to_datetime(str(value)).date()
    except (TypeError, ValueError):
        return None


def _plausible(candidate: date, base: date) -> bool:
    return (
        candidate >= base - timedelta(days=_MAX_PAST_DAYS)
        and candidate <= base + timedelta(days=_MAX_FUTURE_DAYS)
    )


def _find_date(window: str, base: date) -> Optional[tuple[date, int]]:
    match = _WORD_DATE.search(window)
    if match:
        day, month_name, year = match.groups()
        month = _MONTHS.get(month_name.lower().rstrip("."))
        if month:
            try:
                return date(int(year), month, int(day)), match.start()
            except ValueError:
                return None

    match = _NUMERIC_DATE.search(window)
    if match:
        day, month, year = (int(g) for g in match.groups())
        if 1 <= day <= 31 and 1 <= month <= 12:
            try:
                return date(year, month, day), match.start()
            except ValueError:
                return None

    match = _WORD_DATE_NO_YEAR.search(window)
    if match:
        day, month_name = match.groups()
        month = _MONTHS.get(month_name.lower().rstrip("."))
        if month:
            try:
                candidate = date(base.year, month, int(day))
            except ValueError:
                return None
            if candidate < base - timedelta(days=30):
                try:
                    candidate = date(base.year + 1, month, int(day))
                except ValueError:
                    return None
            return candidate, match.start()

    return None


def extract_application_deadline(
    text: Optional[str],
    posted_at: object = None,
    today: Optional[date] = None,
) -> Optional[ExtractedDeadline]:
    """Return the first plausible application deadline stated in ``text``."""
    if not text:
        return None

    base = _coerce_date(posted_at) or today or date.today()
    normalized = re.sub(r"\s+", " ", str(text))

    for pattern in _DEADLINE_PHRASES:
        for phrase_match in pattern.finditer(normalized):
            window = normalized[
                phrase_match.end() : phrase_match.end() + _SEARCH_WINDOW
            ]
            found = _find_date(window, base)
            if found is None:
                continue
            candidate, offset = found
            # Context between the phrase and its date decides whether this is
            # an application deadline or a fee/exam/correction date.
            context = normalized[
                phrase_match.start() : phrase_match.end() + offset
            ]
            if _NEGATIVE_CONTEXT.search(context):
                continue
            if not _plausible(candidate, base):
                continue
            evidence = normalized[
                phrase_match.start() : phrase_match.end() + offset + 12
            ].strip()
            return ExtractedDeadline(deadline=candidate, phrase=evidence[:120])

    return None
