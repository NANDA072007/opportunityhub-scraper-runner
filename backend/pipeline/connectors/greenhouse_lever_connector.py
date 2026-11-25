"""
Greenhouse & Lever ATS Connector
=================================
High-ROI Universal ATS Connector.
Fetches corporate career opportunities directly from Greenhouse and Lever ATS public endpoints.
Returns generic OpportunityRaw objects with ZERO branch/domain hardcoding.
"""

import html
import logging
import re
import urllib.request
import json
from typing import List, Dict, Any, Optional
from backend.pipeline.models import OpportunityRaw
from backend.pipeline.connectors.base_connector import BaseConnector
from backend.pipeline.connectors._pay import mine_pay_text, mine_structured_pay

logger = logging.getLogger(__name__)

class GreenhouseLeverConnector(BaseConnector):
    """Universal ATS Connector for Greenhouse & Lever corporate career pages."""

    # High-value company boards powered by Greenhouse & Lever
    DEFAULT_BOARDS = [
        {"name": "Stripe", "token": "stripe", "ats": "greenhouse"},
        {"name": "Airbnb", "token": "airbnb", "ats": "greenhouse"},
        {"name": "Figma", "token": "figma", "ats": "greenhouse"},
        {"name": "Vercel", "token": "vercel", "ats": "greenhouse"},
        {"name": "Datadog", "token": "datadog", "ats": "greenhouse"},
        {"name": "Coinbase", "token": "coinbase", "ats": "greenhouse"}
    ]

    @property
    def source_slug(self) -> str:
        return "greenhouse_lever"

    @property
    def display_name(self) -> str:
        return "Universal ATS (Greenhouse & Lever)"

    @property
    def default_source_reputation(self) -> int:
        return 100 # Highest trust for direct employer ATS feeds

    def fetch_raw_opportunities(
        self, 
        config: Dict[str, Any], 
        query: Optional[str] = None
    ) -> List[OpportunityRaw]:
        """Fetch raw listings across corporate ATS boards."""
        results: List[OpportunityRaw] = []

        boards = config.get("boards", self.DEFAULT_BOARDS)

        for board in boards:
            company_name = board["name"]
            board_token = board["token"]
            ats_type = board.get("ats", "greenhouse")

            if ats_type == "greenhouse":
                items = self._fetch_greenhouse_board(company_name, board_token)
                results.extend(items)
            elif ats_type == "lever":
                items = self._fetch_lever_board(company_name, board_token)
                results.extend(items)

        logger.info(f"GreenhouseLeverConnector fetched {len(results)} total items across {len(boards)} boards.")
        return results

    def _fetch_greenhouse_board(self, company_name: str, board_token: str) -> List[OpportunityRaw]:
        url = f"https://api.greenhouse.io/v1/boards/{board_token}/embed/jobs?content=true"
        items: List[OpportunityRaw] = []

        try:
            req = urllib.request.Request(url, headers={"User-Agent": "OpportunityHub-IngestionEngine/2.0"})
            with urllib.request.urlopen(req, timeout=10) as response:
                if response.status == 200:
                    data = json.loads(response.read().decode("utf-8"))
                    jobs = data.get("jobs", [])

                    for job in jobs:
                        try:
                            job_id = str(job.get("id"))
                            title = job.get("title", "")
                            apply_url = job.get("absolute_url", f"https://boards.greenhouse.io/{board_token}/jobs/{job_id}")
                            location_obj = job.get("location", {})
                            location = location_obj.get("name", "Remote") if isinstance(location_obj, dict) else str(location_obj)
                            content = job.get("content", f"<p>Career opportunity at {company_name} for {title}.</p>")

                            # Private-pay honesty: metadata[] custom fields
                            # first, then content pay sections. "Competitive
                            # Package" only when nothing is published.
                            pay_text = None
                            metadata = job.get("metadata") or []
                            if isinstance(metadata, list):
                                named = {
                                    str(e.get("name")): e.get("value")
                                    for e in metadata
                                    if isinstance(e, dict) and e.get("name") is not None
                                }
                                pay_text = mine_structured_pay(named)
                            if pay_text is None and isinstance(content, str):
                                plain = re.sub(r"<[^>]+>", " ", content)
                                pay_text = mine_pay_text(html.unescape(plain))

                            items.append(OpportunityRaw(
                                source_slug=self.source_slug,
                                external_id=f"gh_{board_token}_{job_id}",
                                title=title,
                                company_name=company_name,
                                apply_url=apply_url,
                                raw_description=content,
                                raw_location=location,
                                raw_salary=pay_text,
                                opportunity_type="job",
                                raw_payload={"board": board_token, "ats": "greenhouse", "raw_job": job}
                            ))
                        except Exception as e:
                            logger.warning(f"Skipping malformed greenhouse item on board '{board_token}': {e}")
                            continue
        except Exception as e:
            logger.warning(f"Could not fetch Greenhouse board '{board_token}': {e}")

        return items

    def _fetch_lever_board(self, company_name: str, board_token: str) -> List[OpportunityRaw]:
        url = f"https://api.lever.co/v0/postings/{board_token}?mode=json"
        items: List[OpportunityRaw] = []

        try:
            req = urllib.request.Request(url, headers={"User-Agent": "OpportunityHub-IngestionEngine/2.0"})
            with urllib.request.urlopen(req, timeout=10) as response:
                if response.status == 200:
                    jobs = json.loads(response.read().decode("utf-8"))

                    for job in jobs:
                        try:
                            job_id = str(job.get("id"))
                            title = job.get("text", "")
                            apply_url = job.get("hostedUrl", f"https://jobs.lever.co/{board_token}/{job_id}")
                            categories = job.get("categories", {})
                            location = categories.get("location", "Remote")
                            description = job.get("descriptionPlain", f"Career opportunity at {company_name} for {title}.")

                            # Same pay honesty as above: description pay
                            # section or explicit salary keys, else undisclosed.
                            lev_pay = mine_pay_text(description) or mine_structured_pay(
                                {k: job.get(k) for k in ("salary", "compensation", "pay")}
                            )

                            items.append(OpportunityRaw(
                                source_slug=self.source_slug,
                                external_id=f"lev_{board_token}_{job_id}",
                                title=title,
                                company_name=company_name,
                                apply_url=apply_url,
                                raw_description=description,
                                raw_location=location,
                                raw_salary=lev_pay,
                                opportunity_type="job",
                                raw_payload={"board": board_token, "ats": "lever", "raw_job": job}
                            ))
                        except Exception as e:
                            logger.warning(f"Skipping malformed lever item on board '{board_token}': {e}")
                            continue
        except Exception as e:
            logger.warning(f"Could not fetch Lever board '{board_token}': {e}")

        return items
