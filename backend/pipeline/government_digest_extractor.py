"""
Deterministic 10-Second Government & PSU Notification Digest Extractor.
Path: `backend/pipeline/government_digest_extractor.py`

Extracts structured fast-facts (Pay Level, Max Age Limit, Application Fee,
Selection Stages, Exam Tags, Vacancies, and Official PDF/Portal verification)
into `metadata["govt_digest"]` for instant mobile card rendering.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

# 1. 7th CPC Pay Level / Stipend / Fellowship Regex
PAY_LEVEL_RE = re.compile(
    r"\b(pay\s+matrix\s+level\s*[-–:]?\s*\d{1,2}[A-Z]?|level\s*[-–:]?\s*\d{1,2}\s+of\s+7th\s+cpc|"
    r"pb\s*[-–]?\s*[1-4]|grade\s+pay\s*(?:of\s*)?(?:rs\.?|₹)?\s*\d{4,5})\b",
    re.IGNORECASE,
)

# 2. Age Limit Regex ("Max age: 28 years", "Age Limit: 18-30 years", "not exceeding 32 years")
AGE_RANGE_RE = re.compile(
    r"\b(?:age\s*limit|maximum\s*age|upper\s*age\s*limit|not\s*exceeding|age\s*between)"
    r"(?:[^.\n0-9]{0,30}?)[:\s\-]*(?:(\d{2})\s*(?:to|[-–])\s*)?(\d{2})\s*(?:years?|yrs?)\b",
    re.IGNORECASE,
)

# 3. Vacancies / Posts Count Regex ("Total Vacancies: 150", "45 Posts")
VACANCY_RE = re.compile(
    r"\b(?:total\s+)?(?:vacancies|posts|seats|openings)[:\s\-]*(\d{1,5})\b|"
    r"\b(\d{1,4})\s+(?:vacancies|posts)\b",
    re.IGNORECASE,
)

# 4. Application Fee Regex
NO_FEE_RE = re.compile(
    r"\b(no\s+application\s+fee|nil\s+fee|exempted\s+from\s+fee|fee\s*:\s*nil|free\s+application)\b",
    re.IGNORECASE,
)
FEE_AMOUNT_RE = re.compile(
    r"\b(?:application\s+fee|examination\s+fee|processing\s+fee)[^.\n]{0,60}?(?:rs\.?|inr|₹)\s*(\d{2,4})\b",
    re.IGNORECASE,
)

# 5. Canonical Government Exams & Recruitment Bodies (matches `user_preferences.government_exam_targets`)
EXAM_TARGETS_MAP: Dict[str, str] = {
    "gate": "GATE",
    "upsc": "UPSC",
    "ssc cgl": "SSC CGL",
    "ssc chsl": "SSC CHSL",
    "ssc je": "SSC JE",
    "isro": "ISRO",
    "drdo": "DRDO",
    "barc": "BARC",
    "nielit": "NIELIT",
    "ibps": "IBPS",
    "sbi po": "SBI PO",
    "rbi grade b": "RBI Grade B",
    "ugc net": "UGC NET",
    "csir net": "CSIR NET",
    "pmrf": "PMRF",
}

# 6. Selection Stage Keywords
SELECTION_STAGE_PATTERNS: List[tuple[str, str]] = [
    (r"\bgate\s+score\b", "GATE Score Shortlisting"),
    (r"\b(?:cbt|computer\s+based\s+test|written\s+(?:test|exam)|prelims)\b", "Written / CBT Exam"),
    (r"\b(?:mains\s+exam|descriptive\s+paper)\b", "Mains Examination"),
    (r"\b(?:skill\s+test|trade\s+test|coding\s+test)\b", "Skill / Trade Assessment"),
    (r"\b(?:personal\s+interview|walk[- ]in\s+interview|interview)\b", "Personal Interview"),
    (r"\b(?:document\s+verification|medical\s+examination)\b", "Document Verification"),
]

OFFICIAL_GOV_DOMAIN_RE = re.compile(
    r"^https?://([a-zA-Z0-9\-]+\.)*(gov\.in|nic\.in|ac\.in|pmrf\.in|res\.in|ernet\.in)(/|$)",
    re.IGNORECASE,
)


def extract_government_digest(
    title: str,
    company: str,
    description: str,
    requirements: str,
    apply_url: str,
    salary_text: Optional[str],
    required_degrees: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Builds a structured 10-second summary card payload for `category == 'government'`."""
    blob = f"{title or ''}\n{company or ''}\n{description or ''}\n{requirements or ''}"
    blob_lower = blob.lower()

    # 1. Pay Scale / Fellowship Summary
    pay_match = PAY_LEVEL_RE.search(blob)
    pay_summary = (
        pay_match.group(1).upper()
        if pay_match
        else (salary_text or "As per Central / State Govt Pay Matrix Rules")
    )

    # 2. Age Limit Extraction
    age_match = AGE_RANGE_RE.search(blob)
    min_age: Optional[int] = None
    max_age: Optional[int] = None
    if age_match:
        if age_match.group(1):
            min_age = int(age_match.group(1))
        max_age = int(age_match.group(2))
    age_limit_text = (
        f"{min_age}–{max_age} years (Relaxation as per Govt norms)"
        if min_age and max_age
        else (f"Up to {max_age} years (Relaxation for SC/ST/OBC)" if max_age else "Check official notification")
    )

    # 3. Vacancies Count
    vac_match = VACANCY_RE.search(blob)
    vacancies: Optional[int] = None
    if vac_match:
        raw_num = vac_match.group(1) or vac_match.group(2)
        if raw_num and int(raw_num) > 0:
            vacancies = int(raw_num)

    # 4. Application Fee
    fee_match = FEE_AMOUNT_RE.search(blob)
    if fee_match:
        fee_text = f"₹{fee_match.group(1)} (SC/ST/PwD/Women exemptions may apply)"
    elif NO_FEE_RE.search(blob):
        fee_text = "₹0 (No Application Fee)"
    else:
        fee_text = "Refer to official notification PDF"

    # 5. Selection Stages
    stages: List[str] = []
    for pattern, label in SELECTION_STAGE_PATTERNS:
        if re.search(pattern, blob, re.IGNORECASE):
            stages.append(label)
    if not stages:
        stages = ["Merit / Application Shortlisting", "Interview / Document Verification"]

    # 6. Matched Exam / Recruitment Tags (for `user_preferences.government_exam_targets`)
    matched_exams: List[str] = []
    for key, canonical in EXAM_TARGETS_MAP.items():
        if re.search(r"\b" + re.escape(key) + r"\b", blob_lower):
            matched_exams.append(canonical)

    # 7. Official Domain & PDF Check
    url_clean = (apply_url or "").strip()
    is_official_domain = bool(OFFICIAL_GOV_DOMAIN_RE.search(url_clean))
    is_pdf_link = url_clean.lower().endswith(".pdf")

    return {
        "pay_summary": pay_summary,
        "age_limit_text": age_limit_text,
        "max_age": max_age,
        "vacancies": vacancies,
        "application_fee": fee_text,
        "selection_stages": stages[:4],
        "exam_targets": matched_exams,
        "eligible_degrees": required_degrees or ["Any Graduate / B.Tech (See Notification)"],
        "is_official_gov_domain": is_official_domain,
        "is_direct_pdf": is_pdf_link,
    }


__all__ = ["extract_government_digest"]
