"""
Government & Academic Research Opportunities Connector
======================================================
Canonical connector for Indian Government research labs (DRDO, ISRO, IITs) and official
defence & academic research notifications from DRDO RAC (rac.gov.in).
"""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from bs4 import BeautifulSoup

from backend.pipeline.connectors.base_connector import BaseConnector, ConnectorFetchError
from backend.pipeline.connectors.capabilities import Capability
from backend.pipeline.connectors._http import ConnectorHttpClient, parse_guarded
from backend.pipeline.connectors._pay import mine_pay_text
from backend.pipeline.models import OpportunityRaw

logger = logging.getLogger(__name__)

_RAC_BASE_URL = "https://rac.gov.in"


class GovtResearchConnector(BaseConnector):
    """Connector for Indian Government & Academic Research Notifications (DRDO RAC)."""

    priority = 800
    version = "1.1.0"
    owner = "platform"
    capabilities: Set[Capability] = {Capability.GOVERNMENT, Capability.JOB, Capability.RESEARCH, Capability.INTERNSHIP}

    def __init__(self, http_client: Optional[ConnectorHttpClient] = None) -> None:
        self._http = http_client or ConnectorHttpClient(max_retries=2, rate_limit_per_sec=2.0)

    @property
    def source_slug(self) -> str:
        return "govt_research"

    @property
    def display_name(self) -> str:
        return "Govt & Academic Research Portal"

    @property
    def default_source_reputation(self) -> int:
        return 95

    def fetch_raw_opportunities(
        self, config: Dict[str, Any], query: Optional[str] = None
    ) -> List[OpportunityRaw]:
        """Fetch live Government Research opportunities from DRDO RAC (rac.gov.in)."""
        results: List[OpportunityRaw] = []
        target_url = config.get("url") or f"{_RAC_BASE_URL}/"

        try:
            html = self._http.get_text(target_url)
        except Exception as exc:
            raise ConnectorFetchError(
                f"GovtResearch: DRDO RAC fetch failed for '{target_url}': {exc}"
            ) from exc

        parsed_items = self._parse_rac_html(html)
        if not parsed_items:
            # A live RAC page always carries notices. Zero parsed items means
            # the markup changed or a bot wall served a substitute page — do
            # not report that as a healthy, empty source.
            raise ConnectorFetchError(
                f"GovtResearch: no parseable notices on '{target_url}' "
                "(markup change or bot challenge)."
            )

        seen_urls: Set[str] = set()

        for item in parsed_items:
            parsed = parse_guarded(lambda it: self._to_raw(it, seen_urls), item, context="govt_research")
            if parsed is not None:
                results.append(parsed)

        logger.info("GovtResearchConnector fetched %d live items from DRDO RAC.", len(results))
        return results

    def _parse_rac_html(self, html: str) -> List[Dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        items: List[Dict[str, Any]] = []

        links = soup.find_all("a")
        for a in links:
            href = (a.get("href") or "").strip()
            text = a.get_text(" ", strip=True)
            if not text or len(text) < 5:
                continue

            # Prioritize official recruitment PDFs and advertisements
            is_pdf = href.lower().endswith(".pdf") or "download/" in href.lower()
            is_recruitment = any(
                k in href.lower() or k in text.lower()
                for k in ["advt", "recruitment", "jrf", "scientist", "fellow", "gate"]
            )
            if not (is_pdf or is_recruitment):
                continue

            # Skip generic FAQ / alert / caution documents if not a primary notice/advt
            if any(k in href.lower() or k in text.lower() for k in ["cautionapplicants", "alertapplicants", "faq"]):
                continue

            # Ensure PDF extension for PDF downloads
            if "download/" in href.lower() and not href.lower().endswith(".pdf") and "?" not in href:
                href = f"{href}.pdf"

            full_url = href if href.startswith("http") else f"{_RAC_BASE_URL}/{href.lstrip('/')}"

            # Look up parent container for rich context (card title and closing date)
            container = a.find_parent(class_=lambda c: c and any(k in str(c) for k in ["p-4", "GiDiv", "col-12", "card"]))
            container_text = container.get_text(" ", strip=True) if container else ""

            # Extract published date if present in link text or container
            pub_date_str = None
            date_match = re.search(r"(?:Published on|Last updated on):\s*([0-9]{1,2}\s+[A-Za-z]{3},?\s+[0-9]{4})", container_text or text, re.IGNORECASE)
            if date_match:
                pub_date_str = date_match.group(1)

            # Extract deadline: check extended date first, then closing date, then link text
            deadline_str = None
            ext_match = re.search(r"Extended Form Submission date:\s*([0-9]{1,2}\s*[-/ ]\s*[A-Za-z]{3}\s*[-/ ]\s*[0-9]{4}|[0-9]{4}-[0-9]{2}-[0-9]{2})", container_text, re.IGNORECASE)
            if ext_match:
                deadline_str = ext_match.group(1).strip()
            else:
                dl_match = re.search(r"(?:Last Date|Deadline|Closing Date):\s*([0-9]{1,2}\s*[-/ ]\s*[A-Za-z]{3},?\s*[-/ ]\s*[0-9]{4}|[0-9]{4}-[0-9]{2}-[0-9]{2})", container_text or text, re.IGNORECASE)
                if dl_match:
                    deadline_str = dl_match.group(1).strip()

            # Enrich title with container header if text is just generic "Advertisement" or "Brief Advertisement"
            full_title = text
            if container and text.lower().strip() in ["advertisement", "brief advertisement", "view details", "download"]:
                header_match = re.search(r"^(.*?)(?:Check latest updates|Frequently Asked|Brief Advertisement|$)", container_text)
                if header_match and len(header_match.group(1).strip()) > 5:
                    full_title = f"{header_match.group(1).strip()} - {text}"

            items.append({
                "title": full_title,
                "url": full_url,
                "is_pdf": full_url.endswith(".pdf"),
                "published_date": pub_date_str,
                "deadline": deadline_str,
                "raw_text": container_text or text,
            })

        # Sort items so official PDF notifications come first
        items.sort(key=lambda x: 0 if x["is_pdf"] else 1)
        return items

    def _to_raw(self, item: Dict[str, Any], seen_urls: Set[str]) -> Optional[OpportunityRaw]:
        apply_url = item["url"]
        if apply_url in seen_urls:
            return None
        seen_urls.add(apply_url)

        title = item["title"]
        # Clean title text if it contains published date suffix or KB prefix
        title_clean = re.sub(r"Published on:.*$", "", title, flags=re.IGNORECASE).strip()
        title_clean = re.sub(r"^[0-9.]+\s*KB\s*", "", title_clean).strip()

        if len(title_clean) < 5:
            return None

        # Prepend DRDO for clarity if not already present
        if not title_clean.upper().startswith("DRDO"):
            title_clean = f"DRDO {title_clean}"

        # Extract external ID from PDF filename or URL params
        pdf_match = re.search(r"/download/([^?#/]+)", apply_url)
        if pdf_match:
            external_id = f"drdo_rac_{pdf_match.group(1).replace('.', '_')}"
        else:
            url_hash = hashlib.sha256(apply_url.encode("utf-8")).hexdigest()[:16]
            external_id = f"drdo_rac_{url_hash}"

        # Classify opportunity type
        opp_type = "job"
        if any(k in title_clean.lower() for k in ["jrf", "srf", "fellow", "internship", "apprentice"]):
            opp_type = "internship"

        context_text = f"{title_clean}. {item.get('raw_text') or ''}"
        description = (
            f"{title_clean}. Official Defence & Academic Research Notification published by DRDO RAC "
            f"(Defence Research and Development Organisation, Govt of India). "
            f"Official Link: {apply_url}"
        )

        return OpportunityRaw(
            source_slug=self.source_slug,
            external_id=external_id,
            title=title_clean,
            company_name="Defence Research and Development Organisation (DRDO)",
            apply_url=apply_url,
            raw_description=description,
            # RAC recruits for DRDO labs across India; a single hardcoded city
            # mislabelled every posting as Bengaluru.
            raw_location="India",
            # Pay and GATE eligibility are notice-specific. The old hardcoded
            # "₹37,000 - ₹56,100" + gate_required=True invented compensation
            # and eligibility for every row; now only what the notice states.
            raw_salary=mine_pay_text(context_text),
            raw_deadline=item.get("deadline"),
            opportunity_type=opp_type,
            raw_payload={
                "source": self.source_slug,
                "is_government": True,
                "gate_required": bool(re.search(r"\bGATE\b", context_text)),
                "official_source_domain": "rac.gov.in",
                "published_date_raw": item.get("published_date"),
                "is_mock": False,
            },
        )


__all__ = ["GovtResearchConnector"]
