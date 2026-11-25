"""
OpportunityHub — URL & Domain Security Validator
==================================================

Provides strict URL parsing and domain validation to prevent domain spoofing
and query parameter manipulation attacks (e.g. https://scam.com/apply?ref=greenhouse.io).
"""

from __future__ import annotations

import logging
from urllib.parse import urlparse
from typing import Optional

logger = logging.getLogger(__name__)

TRUSTED_ATS_DOMAINS: frozenset[str] = frozenset({
    "greenhouse.io",
    "lever.co",
    "myworkdayjobs.com",
    "workday.com",
    "ashbyhq.com",
    "smartrecruiters.com",
    "bamboohr.com",
    "jobvite.com",
})

TRUSTED_GOVT_DOMAINS: frozenset[str] = frozenset({
    "gov.in",
    "nic.in",
    "ac.in",
    "pmrf.in",
    "drdo.gov.in",
    "upsc.gov.in",
    "ssc.nic.in",
    "isro.gov.in",
})

TRUSTED_FREELANCE_DOMAINS: frozenset[str] = frozenset({
    "upwork.com",
    "fiverr.com",
    "freelancer.com",
    "freelancer.in",
    "toptal.com",
    "remotive.com",
    "weworkremotely.com",
    "jobicy.com",
})


def extract_clean_hostname(url: Optional[str]) -> Optional[str]:
    """Strictly extract normalized hostname from URL without query or path poisoning."""
    if not url or not isinstance(url, str):
        return None
    cleaned = url.strip()
    if not cleaned:
        return None

    # Handle schemeless URLs gracefully
    if "://" not in cleaned:
        cleaned = f"https://{cleaned}"

    try:
        parsed = urlparse(cleaned)
        host = (parsed.hostname or "").lower().strip()
        # Remove trailing periods if present
        return host.rstrip(".") or None
    except Exception as exc:
        logger.debug("Failed to parse URL hostname for '%s': %s", url, exc)
        return None


def is_trusted_ats_url(url: Optional[str]) -> bool:
    """Strictly verify if URL belongs to an authoritative ATS domain.
    
    Guards against query-string spoofing:
    - 'https://scam.com/job?source=greenhouse.io' -> FALSE
    - 'https://boards.greenhouse.io/acme/jobs/123' -> TRUE
    - 'https://jobs.lever.co/corp' -> TRUE
    """
    host = extract_clean_hostname(url)
    if not host:
        return False

    return any(
        host == domain or host.endswith(f".{domain}")
        for domain in TRUSTED_ATS_DOMAINS
    )


def is_trusted_govt_url(url: Optional[str]) -> bool:
    """Strictly verify if URL hostname exactly is or ends with .gov.in, .nic.in, or .ac.in.
    
    Guards against look-alike hosts:
    - 'gov.in.evil.com' -> FALSE
    - 'evilgov.in' -> FALSE
    - 'https://scam.com/path?domain=drdo.gov.in' -> FALSE
    - 'https://rac.gov.in/download/advt.pdf' -> TRUE
    - 'https://iitb.ac.in/recruitment' -> TRUE
    """
    host = extract_clean_hostname(url)
    if not host:
        return False

    # Exact domain or dot-separated subdomain
    official_suffixes = (".gov.in", ".nic.in", ".ac.in")
    exact_allowed = ("gov.in", "nic.in", "ac.in", "pmrf.in")

    if host in exact_allowed or host.endswith(".pmrf.in"):
        return True

    return any(host.endswith(suffix) for suffix in official_suffixes)


def is_trusted_freelance_url(url: Optional[str]) -> bool:
    """Strictly verify if URL belongs to an authoritative freelance platform."""
    host = extract_clean_hostname(url)
    if not host:
        return False

    return any(
        host == domain or host.endswith(f".{domain}")
        for domain in TRUSTED_FREELANCE_DOMAINS
    )
