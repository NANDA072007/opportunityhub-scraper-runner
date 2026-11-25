"""
Pay-Signal Mining Helper (Sprint: private-pay honesty)
======================================================
Shared, dependency-free extraction of compensation text from unstructured
ATS/job payloads (Greenhouse metadata + content HTML, Lever descriptions,
generic JSON/RSS feeds).

Root cause it addresses: connectors hardcoded
``raw_salary="Competitive Package"`` without inspecting fields that DO carry
pay (Greenhouse ``metadata[]`` custom fields, Lever description pay sections,
generic feed ``salary``/``compensation`` keys), so most private listings
ingested as undisclosed even when the source published a range.

Strictness contract (never fabricate pay):
- A candidate MUST contain an explicit currency symbol/code + digits, OR be
  the value of an explicitly pay-named key.
- Free-text mining additionally requires a pay keyword
  (salary/stipend/pay/compensation/CTC/LPA/...) within the anchor window, so
  headcounts ("team of 12"), experience ("3+ years"), fees ("₹500
  registration fee"), and funding ("raised $5M") can never match.
- ``parse_salary`` downstream remains the final arbiter: unparseable text
  yields undisclosed (None), never a number.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

# Explicitly pay-named keys in generic JSON feeds (case-insensitive match).
PAY_KEY_NAMES = frozenset({
    "salary",
    "salaryrange",
    "salary_range",
    "annualsalarymin",
    "annualsalarymax",
    "compensation",
    "basecompensation",
    "totalcompensation",
    "pay",
    "payrange",
    "pay_range",
    "stipend",
    "stipendamount",
    "monthlystipend",
    "ctc",
    "lpa",
    "emoluments",
    "remuneration",
    "package",
    "hourlyrate",
    "hourly_rate",
    "hourly",
    "budget",
    "fixedbudget",
    "fixed_budget",
    "rate",
    "fixedrate",
})

# Anchor words that must appear near a free-text figure for it to count.
_PAY_KEYWORDS_RE = re.compile(
    r"(?:\bsalar(?:y|ies)\b|\bstipends?\b|\bcompensation\b|\bctc\b|\blpa\b|\bemoluments?\b|"
    r"\bremuneration\b|\bpay\b|\bpackage\b|\bhourly\s+rate\b|\bpay\s+rate\b|\bbase\s+rate\b|"
    r"\bbase\s+pay\b|\bbase\s+salary\b|\bwages?\b|\bpay\s+scale\b|\bscale\s+of\s+pay\b|"
    r"\b(?:hourly|day|project|pay|base|fixed)\s+rates?\b|\brates?\s*[:\-]|\brates?\b(?=\s*[:\-])|"
    r"\bbudgets?\b|\bfixed\s+budget\b|\bhiring\s+ranges?\b|\bpay\s+ranges?\b|\bsalary\s+ranges?\b)",
    re.IGNORECASE,
)

_CURR_SYMBOLS = r"(?:₹|\$|€|£|\bINR\b|\bUSD\b|\bEUR\b|\bGBP\b|\bCAD\b|\bRs\.?)"
_NUM_CHUNK = r"\d[\d,]*(?:\.\d+)?\s*(?:k|K)?"
_FREQ_CHUNK = (
    r"(?:\s*(?:/|per|a|-)?\s*"
    r"(?:year|yr|month|mo|week|wk|hour|hr|day|annum|annual|annually|hourly|monthly|p\.m\.|p\.a\.|CTC|LPA|lpa))"
)
_SEP_CHUNK = r"(?:\s*(?:-|–|—|to)\s*)"
_POST_CURR = r"(?:\s*(?:USD|INR|EUR|GBP|CAD))"

# Currency-denominated figure or range (strict: symbol/code + digits required; revenue/funding in B/M excluded).
_FIGURE_RE = re.compile(
    rf"(?:"
    rf"{_CURR_SYMBOLS}\s*{_NUM_CHUNK}(?!\s*(?:billion|million|\b[bm]\b|\+))(?:{_SEP_CHUNK}(?:{_CURR_SYMBOLS}\s*)?{_NUM_CHUNK})?(?:/-)?{_POST_CURR}?{_FREQ_CHUNK}?"
    rf"|"
    rf"\d[\d,]*(?:\.\d+)?\s*(?:{_SEP_CHUNK}\s*\d[\d,]*(?:\.\d+)?\s*)?(?:lpa|lakh|lakhs)\b{_FREQ_CHUNK}?"
    rf")",
    re.IGNORECASE,
)

_ANCHOR_WINDOW = 400


def mine_pay_text(text: Optional[str], window: int = _ANCHOR_WINDOW) -> Optional[str]:
    """Return a pay snippet from free text, or None.

    Finds currency figures occurring within ``window`` chars AFTER a pay
    keyword (e.g. "Salary: $150,000 - $200,000 per year"). Bare figures with
    no pay context are ignored here (other connectors handle explicit
    salary nodes); ``parse_salary`` validates whatever is returned.
    """
    if not text or not isinstance(text, str):
        return None
    for kw in _PAY_KEYWORDS_RE.finditer(text):
        snippet = text[kw.start():kw.start() + window]
        match = _FIGURE_RE.search(snippet)
        if match:
            # Check if match was truncated at snippet boundary
            if match.end() < len(snippet) and snippet[match.end()] in "0123456789,":
                continue
            found = match.group(0).strip()
            if found:
                # If surrounding anchor mentions hourly/per hour and found lacks frequency, propagate /hr
                if re.search(r"\b(?:hourly|per hour|/hr)\b", snippet, re.IGNORECASE):
                    if not re.search(r"(?:/|per|a)\s*(?:year|yr|month|mo|week|wk|hour|hr|day|annum)", found, re.IGNORECASE):
                        found = f"{found} /hr"
                return found[:120]
    return None


def mine_structured_pay(item: Dict[str, Any]) -> Optional[str]:
    """Return pay text from explicitly pay-named keys of a JSON payload.

    Values may be strings ("$150k-$200k"), numbers (treated as annual in
    source currency only when paired with a currency key — otherwise
    returned bare for ``parse_salary`` to judge), or lists (joined).
    Keys are matched case-insensitively with punctuation stripped.
    """
    if not isinstance(item, dict):
        return None
    texts: List[str] = []
    currency_hint = ""
    for key, value in item.items():
        norm_key = re.sub(r"[^a-z]", "", str(key).lower())
        if "currency" in norm_key and isinstance(value, str) and value.strip():
            currency_hint = value.strip()[:8]
        if norm_key not in PAY_KEY_NAMES:
            continue
        if value is None:
            continue
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            texts.append(f"{currency_hint + ' ' if currency_hint else ''}{value}")
        elif isinstance(value, str) and value.strip():
            texts.append(value.strip()[:200])
        elif isinstance(value, list):
            joined = ", ".join(str(v).strip() for v in value if str(v).strip())
            if joined:
                texts.append(joined[:200])
    if not texts:
        return None
    return "; ".join(texts)[:500] or None


__all__ = ["mine_pay_text", "mine_structured_pay", "PAY_KEY_NAMES"]
