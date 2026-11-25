"""
Unstop Connector
================
Fetches hackathons, competitions, and hiring challenges from Unstop's public
opportunity search API. Returns generic OpportunityRaw objects with ZERO
branch/domain hardcoding beyond the site's own payload shape.
"""

from __future__ import annotations

import logging
import random
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx

from backend.pipeline.models import OpportunityRaw
from backend.pipeline.connectors.base_connector import BaseConnector, ConnectorFetchError
from backend.pipeline.connectors.capabilities import Capability
from backend.pipeline.connectors._http import ConnectorHttpClient, parse_guarded

logger = logging.getLogger(__name__)


class UnstopConnector(BaseConnector):
    """Unstop connector for hackathons, competitions and hiring challenges."""

    priority = 700
    version = "1.0.0"
    owner = "platform"
    capabilities = {Capability.COMPETITION, Capability.SCHOLARSHIP, Capability.FELLOWSHIP}
    supports_full_sync = True
    supports_incremental_sync = False
    estimated_rate_limit = 1.0

    API_BASE = "https://unstop.com/api/public/opportunity/search-result"

    # Sprint-1: polite multi-page crawl. Rate limiting lives in
    # ConnectorHttpClient (1 req/s); the jittered inter-page pause below keeps
    # long runs under Cloudflare/Akamai 429 thresholds.
    # Budget: 3 subcategories x 70 pages x 50 = ~10,500 rows headroom over the
    # ~6,655 live competitions. Hitting the cap logs at ERROR (alert-worthy,
    # never a silent graceful exit) so data loss pages the team via Sentry.
    _PER_PAGE = 50
    _MAX_PAGES_PER_TYPE = 70
    _INTER_PAGE_DELAY_S = 0.5

    _OPPORTUNITY_TYPES = [
        ("hackathon", "hackathons"),
        ("competition", "competitions"),
        ("hiring_challenge", "hiring-challenges"),
    ]

    def __init__(
        self,
        *,
        client: Optional[ConnectorHttpClient] = None,
        http_client: Optional[httpx.Client] = None,
    ) -> None:
        if client is not None:
            self._http = client
        else:
            self._http = ConnectorHttpClient(client=http_client, rate_limit_per_sec=1.0)

    @property
    def source_slug(self) -> str:
        return "unstop"

    @property
    def display_name(self) -> str:
        return "Unstop"

    @property
    def default_source_reputation(self) -> int:
        return 80

    def fetch_raw_opportunities(
        self, config: Dict[str, Any], query: Optional[str] = None
    ) -> List[OpportunityRaw]:
        import time

        opportunity_types = config.get("opportunity_types") or self._OPPORTUNITY_TYPES
        per_page = int(config.get("per_page") or self._PER_PAGE)
        max_pages = int(config.get("max_pages") or self._MAX_PAGES_PER_TYPE)
        # The public search API serves the full multi-year archive (~85% of a
        # live sample was already closed). Ingesting those rows burned the
        # crawl budget and tripped the circuit breaker on "deadline outside a
        # sane date window" quarantines that are expected, not defects. Keep
        # only actionable rows by default; `include_closed` is an explicit
        # opt-in for backfill/audit runs that really want the archive.
        include_closed = bool(config.get("include_closed", False))
        results: List[OpportunityRaw] = []
        seen_ids = set()
        successful_fetches = 0
        failed_fetches = 0
        skipped_closed = 0

        for subcategory, slug in opportunity_types:
            for page in range(1, max_pages + 1):
                params = {"opportunity": slug, "per_page": per_page, "page": page}
                try:
                    data = self._http.get_json(self.API_BASE, params=params)
                except Exception as exc:
                    logger.warning("Unstop '%s' page %d failed: %s", slug, page, exc)
                    failed_fetches += 1
                    break
                successful_fetches += 1
                raw_page_items = self._parse_payload(data, subcategory)
                if include_closed:
                    page_items = raw_page_items
                else:
                    page_items = [
                        it for it in raw_page_items if self._is_actionable(it)
                    ]
                    skipped_closed += len(raw_page_items) - len(page_items)
                fresh = [it for it in page_items if it.external_id not in seen_ids]
                for it in fresh:
                    seen_ids.add(it.external_id)
                results.extend(fresh)
                logger.info("Unstop '%s' page %d: %d items (%d total).", slug, page, len(fresh), len(results))
                # Termination must use the RAW page size: a page can be all
                # closed rows while upstream still has more open ones behind
                # it. Comparing the filtered count would stop pagination early.
                if len(raw_page_items) < per_page:
                    break
                if page >= max_pages:
                    # Cap exhausted with a full last page: upstream likely
                    # holds more rows than our budget. Loud, not silent.
                    logger.error(
                        "Unstop '%s' hit max_pages=%d with a full last page "
                        "(%d total) — upstream inventory exceeds crawl "
                        "budget, segment by subcategory/date before next run.",
                        slug, max_pages, len(results),
                    )
                    break
                time.sleep(self._INTER_PAGE_DELAY_S + random.uniform(0, 0.5))

        if failed_fetches and not successful_fetches:
            raise ConnectorFetchError(
                f"Unstop: all {failed_fetches} configured opportunity types "
                "failed to fetch (API blocked or unreachable)."
            )

        if skipped_closed:
            logger.info(
                "Unstop: skipped %d closed/expired items at fetch "
                "(include_closed=false).",
                skipped_closed,
            )
        logger.info("UnstopConnector fetched %d total items.", len(results))
        return results

    @staticmethod
    def _is_actionable(item: OpportunityRaw) -> bool:
        """True when a user can still act on the listing.

        Closed registration or a deadline in the past means the row belongs
        to the archive, not the feed. Unknown signals stay actionable rather
        than being silently dropped.
        """
        if item.regn_open is False:
            return False
        if UnstopConnector._deadline_is_past(item.raw_deadline) is True:
            return False
        return True

    @classmethod
    def _parse_payload(cls, data: Dict[str, Any], subcategory: str) -> List[OpportunityRaw]:
        raw_items = data.get("data") or []
        if isinstance(raw_items, dict):
            raw_items = raw_items.get("data") or []
        if not isinstance(raw_items, list):
            return []

        items: List[OpportunityRaw] = []
        for item in raw_items:
            parsed = parse_guarded(
                lambda it: cls._parse_item(it, subcategory),
                item,
                context="unstop",
            )
            if parsed is not None:
                items.append(parsed)
        return items

    @staticmethod
    def _parse_item(item: Dict[str, Any], subcategory: str) -> OpportunityRaw:
        title = (item.get("title") or "").strip()
        if not title:
            return None

        org = item.get("organisation") or item.get("organiser") or {}
        company = org.get("name") if isinstance(org, dict) else ""
        if not company:
            company = "Unstop Partner"

        # Stable external id: prefer the platform id, fall back to URL.
        # Explicit None checks (not `or`-chaining): a platform id of 0 is
        # a valid id and must not fall through to the URL fallback.
        platform_id = item.get("id")
        if platform_id is None:
            platform_id = item.get("opportunity_id")
        if platform_id is None:
            platform_id = item.get("uuid")
        seo_url = item.get("seo_url") or ""
        apply_url = UnstopConnector._resolve_apply_url(seo_url)
        external_id = f"unstop_{platform_id}" if platform_id is not None else apply_url

        location = item.get("job_location") or ""
        if not location:
            location = "Online" if item.get("region") == "online" else "India"

        desc = item.get("summary") or item.get("details") or f"{title} hosted on Unstop by {company}."

        # Sprint-1: explicit source taxonomy wins over title regex downstream
        # (entity_normalizer._detect_category prefers subcategory_raw).
        explicit_type = (
            item.get("type") or item.get("opportunity_type") or subcategory
        )
        explicit_subtype = item.get("subtype") or item.get("assignedTag") or ""
        subcategory_raw = f"{explicit_type}/{explicit_subtype}".strip("/").lower()

        # Registration window: the live API nests camelCase
        # ``regnRequirements`` with ``end_regn_dt``. The old snake_case
        # lookup missed it entirely, so deadlines fell back to the event end
        # date and already-closed challenges looked active (a hiring
        # challenge whose registration ended 2026-09-11 kept an event end of
        # 2026-11-30). Registration close is the date users can actually act
        # on, so it wins; event end_date is the fallback.
        regn = item.get("regnRequirements") or item.get("regn_requirements") or {}
        deadline_str = None
        reg_status = ""
        if isinstance(regn, dict):
            deadline_str = (
                regn.get("end_regn_dt")
                or regn.get("end_regn_date")
                or item.get("end_date")
            )
            reg_status = str(regn.get("reg_status") or "").strip().upper()
        else:
            deadline_str = item.get("end_date")
        regn_open = UnstopConnector._coerce_optional_bool(item.get("regn_open"))
        if regn_open is None and isinstance(regn, dict):
            regn_open = UnstopConnector._coerce_optional_bool(regn.get("regn_open"))
        # ``reg_status`` is authoritative when present: the listing API marks
        # 2022-era rows ``regn_open: 1`` while registration is FINISHED.
        if reg_status in {"FINISHED", "CLOSED", "ENDED", "COMPLETED"}:
            regn_open = False
        elif reg_status in {"LIVE", "OPEN", "ONGOING", "STARTED"} and regn_open is None:
            regn_open = True
        if regn_open is None:
            past = UnstopConnector._deadline_is_past(deadline_str)
            if past is not None:
                regn_open = not past

        # Structured skills array — no NLP regex needed downstream.
        raw_skills = item.get("required_skills") or item.get("skills") or []
        required_skills = [str(s).strip() for s in raw_skills if str(s).strip()]

        # Social proof counters for popularity ranking.
        views_count = item.get("viewsCount") or item.get("views_count")
        register_count = item.get("registerCount") or item.get("register_count")
        try:
            views_count = int(views_count) if views_count is not None else None
        except (TypeError, ValueError):
            views_count = None
        try:
            register_count = int(register_count) if register_count is not None else None
        except (TypeError, ValueError):
            register_count = None

        # Prize envelope: cash total (INR-normalized best effort), PPO/PPI flags.
        prize_text, prize_amount_inr, ppo_offered = UnstopConnector._parse_prizes(item)

        # Closed competitions: keep the row (expiry cron owns lifecycle) but
        # mark it so feeds can demote it immediately.
        is_closed = regn_open is False

        return OpportunityRaw(
            source_slug="unstop",
            external_id=external_id,
            title=title,
            company_name=company,
            apply_url=apply_url,
            raw_description=desc[:2000],
            raw_location=location,
            raw_deadline=deadline_str,
            opportunity_type="competition"
            if subcategory in ("hackathon", "competition")
            else "event",
            required_skills=required_skills,
            ppo_offered=ppo_offered,
            regn_open=regn_open,
            views_count=views_count,
            registers_count=register_count,
            prize_text=prize_text,
            prize_amount_inr=prize_amount_inr,
            subcategory_raw=subcategory_raw,
            raw_payload={
                "source": "unstop",
                "subcategory": subcategory,
                "is_closed": is_closed,
                "raw_item": item,
            },
        )

    @staticmethod
    def _coerce_optional_bool(value: Any) -> Optional[bool]:
        """Unstop emits booleans as 0/1 ints, strings, or true bools."""
        if isinstance(value, bool):
            return value
        if isinstance(value, int) and value in (0, 1):
            return bool(value)
        return None

    @staticmethod
    def _deadline_is_past(raw: Optional[str]) -> Optional[bool]:
        """Return True/False for a parseable ISO deadline, None otherwise."""
        text = (raw or "").strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed < datetime.now(timezone.utc)

    @staticmethod
    def _resolve_apply_url(seo_url: str) -> str:
        """Build a valid Unstop apply URL from the payload's ``seo_url``.

        The live API returns ``seo_url`` as an ABSOLUTE URL
        (``https://unstop.com/hackathons/...``), so blindly prefixing the
        domain produced ``https://unstop.com/https://unstop.com/...``. Accept
        absolute, protocol-relative, and bare-path shapes exactly once.
        """
        raw = (seo_url or "").strip()
        if not raw:
            return "https://unstop.com/hackathons"
        if raw.startswith(("http://", "https://")):
            return raw
        if raw.startswith("//"):
            return f"https:{raw}"
        return f"https://unstop.com/{raw.lstrip('/')}"

    @staticmethod
    def _parse_prizes(item: Dict[str, Any]) -> tuple:
        """Return (prize_text, prize_amount_inr, ppo_offered) from a payload.

        Never throws: unknown shapes yield (None, None, None) and the row
        still ingests. Cash parsing is best-effort INR normalization; the raw
        text is always preserved for display.
        """
        prizes = item.get("prizes") or item.get("prize") or []
        if isinstance(prizes, dict):
            prizes = [prizes]
        if not isinstance(prizes, list):
            prizes = []

        texts: List[str] = []
        cash_total = 0.0
        found_cash = False
        ppo_offered: Optional[bool] = None

        for prize in prizes:
            if isinstance(prize, str):
                texts.append(prize)
                if any(k in prize.lower() for k in ("ppo", "ppi", "pre-placement", "placement offer")):
                    ppo_offered = True
                continue
            if not isinstance(prize, dict):
                continue
            label = str(prize.get("label") or prize.get("title") or prize.get("rank") or "").strip()
            amount = prize.get("amount") or prize.get("cash") or prize.get("value")
            currency = str(prize.get("currency") or "INR").upper()
            kind = str(prize.get("type") or prize.get("kind") or "").lower()
            if any(k in f"{label} {kind}".lower() for k in ("ppo", "ppi", "pre-placement", "placement offer")):
                ppo_offered = True
                texts.append(label or "PPO / PPI")
                continue
            if amount is not None:
                try:
                    num = float(str(amount).replace(",", "").strip().split()[0])
                except (TypeError, ValueError, IndexError):
                    num = None
                if num is not None:
                    norm_curr = currency.upper().replace("FA-", "")
                    rates = {"USD": 84.0, "INR": 1.0, "RS": 1.0, "RUPEE": 1.0, "EUR": 92.0, "GBP": 108.0}
                    symbols = {"USD": "$", "INR": "₹", "RS": "₹", "RUPEE": "₹", "EUR": "€", "GBP": "£"}
                    rate = rates.get(norm_curr, 1.0)
                    cash_total += num * rate
                    found_cash = True
                    curr_symbol = symbols.get(norm_curr, f"{norm_curr} ")
                    texts.append(f"{label + ': ' if label else ''}{curr_symbol}{num:,.0f}")
                    continue
            if label:
                texts.append(label)
            elif prize.get("certificate"):
                texts.append("Certificate")

        # Top-level PPO/PPI flags outside the prizes array.
        for key in ("ppo", "ppi", "ppo_offered", "is_ppo"):
            if item.get(key) is True:
                ppo_offered = True
        blob = f"{item.get('summary', '')} {item.get('details', '')}".lower()
        if ppo_offered is None and any(k in blob for k in ("ppo", "ppi")):
            ppo_offered = None  # prose mention only — do not assert

        prize_text = "; ".join(texts) if texts else None
        return prize_text, (round(cash_total, 2) if found_cash else None), ppo_offered


__all__ = ["UnstopConnector"]
