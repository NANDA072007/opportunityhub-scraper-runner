"""
Student Signal Extractor
========================
Zero-LLM deterministic extractor that extracts student-specific eligibility
signals from job/internship listings:
  1. eligible_batches: List[int] (e.g., [2025, 2026, 2027])
  2. min_experience_months: int (detects required experience in months)
  3. is_experience_trap: bool (flags 'entry'/'any' jobs demanding >= 24 months)
  4. is_direct_ats: bool (verifies Greenhouse, Lever, Ashby, AICTE, .gov.in, etc.)
  5. pre_flight_checklist: List[str] (3 actionable readiness bullets for the Apply sheet)
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

# 1. Graduation batch patterns:
# Prefix: "batch of 2026", "Class of 2027", "graduating in 2025"
BATCH_PREFIX_RE = re.compile(
    r"\b(?:batch(?:es)?\s*(?:of)?|class\s*of|graduating\s*in|pass(?:ing)?\s*out\s*(?:in)?|"
    r"eligible\s*batches?)[:\s\-]*((?:202[4-9](?:\s*[,/&|-]\s*|\s+or\s+|\s+and\s+)?)+)",
    re.IGNORECASE,
)
# Suffix: "2025, 2026 & 2027 passouts", "2026 batch", "2025/2026 graduates"
BATCH_SUFFIX_RE = re.compile(
    r"((?:202[4-9](?:\s*[,/&|-]\s*|\s+or\s+|\s+and\s+)?)+)\s*(?:batch(?:es)?|pass(?:ing)?\s*outs?|passouts?|graduates?|candidates?)",
    re.IGNORECASE,
)
YEAR_TOKEN_RE = re.compile(r"\b(202[4-9])\b")

# 2. Experience trap patterns: "2+ years of experience", "minimum 3 yrs exp", "1-3 years experience"
EXP_YEARS_RE = re.compile(
    r"\b(?:minimum|min\.?|at\s*least)?\s*(\d{1,2})\s*(?:\+|\-\s*\d{1,2})?\s*"
    r"(?:years?|yrs?)\s*(?:of\s+)?(?:relevant\s+|professional\s+|industry\s+|work\s+)?experience\b",
    re.IGNORECASE,
)

DIRECT_ATS_DOMAINS = (
    "greenhouse.io",
    "lever.co",
    "ashbyhq.com",
    "myworkdayjobs.com",
    "smartrecruiters.com",
    "gov.in",
    "nic.in",
    "ac.in",
    "pmrf.in",
    "aicte-india.org",
)


def extract_student_signals(
    title: str,
    description: str,
    apply_url: str,
    experience_level: str,
    tags: List[str],
    required_degrees: List[str],
    ppo_offered: bool,
    normalized_monthly_stipend: Optional[float],
) -> Dict[str, Any]:
    text = f"{title or ''}\n{description or ''}"

    # A. Extract Eligible Graduation Years
    batches = set()
    for match in BATCH_PREFIX_RE.finditer(text):
        for yr in YEAR_TOKEN_RE.findall(match.group(1)):
            batches.add(int(yr))
    for match in BATCH_SUFFIX_RE.finditer(text):
        for yr in YEAR_TOKEN_RE.findall(match.group(1)):
            batches.add(int(yr))
    eligible_batches = sorted(batches)

    # B. Extract Minimum Required Experience (in months) & Fake-Fresher Trap Flag
    min_exp_years = 0
    exp_matches = [int(m.group(1)) for m in EXP_YEARS_RE.finditer(text)]
    if exp_matches:
        min_exp_years = min(exp_matches)
    min_experience_months = min_exp_years * 12

    # Flag if marked 'entry' or 'any' but demands >= 2 years experience
    is_experience_trap = (
        experience_level in ("entry", "any") and min_experience_months >= 24
    )

    # C. Direct ATS / Official Portal Verification
    url_lower = (apply_url or "").lower()
    is_direct_ats = any(domain in url_lower for domain in DIRECT_ATS_DOMAINS)

    # D. Deterministic 3-Bullet Pre-Flight Checklist for the Apply Sheet
    checklist: List[str] = []
    if tags:
        top_skills = ", ".join(tags[:4])
        checklist.append(f"Highlight core skills on your resume: {top_skills}")
    if required_degrees:
        deg_str = "/".join(required_degrees[:3])
        batch_str = f" ({', '.join(map(str, eligible_batches))} batch)" if eligible_batches else ""
        checklist.append(f"Verify degree eligibility: {deg_str}{batch_str}")
    elif eligible_batches:
        checklist.append(f"Target graduation batch: {', '.join(map(str, eligible_batches))}")

    if ppo_offered:
        checklist.append("PPO offered: include production GitHub/portfolio links for full-time evaluation")
    elif is_direct_ats:
        checklist.append("Direct ATS application: use a single-column ATS-parsable PDF resume")
    else:
        checklist.append("Keep your GitHub, LinkedIn, and latest resume PDF ready before clicking Apply")

    return {
        "eligible_batches": eligible_batches,
        "min_experience_months": min_experience_months,
        "is_experience_trap": is_experience_trap,
        "is_direct_ats": is_direct_ats,
        "pre_flight_checklist": checklist[:3],
    }


__all__ = ["extract_student_signals", "DIRECT_ATS_DOMAINS"]
