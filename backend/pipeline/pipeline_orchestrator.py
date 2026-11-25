"""
PipelineOrchestrator
====================
Central Master Controller for the Production Opportunity Ingestion Pipeline.

Stages Orchestrated:
1. FETCH       : Dynamic config load from company_sources & ConnectorRegistry invocation.
2. NORMALIZE   : Structural cleaner, Entity Normalizer & unknown_entities queueing.
3. VALIDATE    : OpportunityValidator (Separates Valid, Quarantine Queue & Retry Queue).
4. DEDUPLICATE : OpportunityDeduplicator (Stage 1 Fingerprint + Stage 2 Graduated Fuzzy).
5. SCORE       : QualityEvaluator (Explainable Quality, Source Reputation, Freshness, Vector Composition).
6. STORE       : Raw Payload JSONB retention & Supabase upsert.
7. PUBLISH     : Increments system feed_version & updates telemetry in daily_ingestion_stats.

Processing stages are executed through the pluggable ``PipelineStageRegistry``
instead of hardcoded calls, so the sequence can be extended or re-ordered
without editing this class.
"""

import logging
import os
import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Tuple, Optional, Set
from supabase import create_client, Client

from backend.pipeline.models import OpportunityRaw, OpportunityNormalized
from backend.pipeline.connectors.connector_registry import ConnectorRegistry
from backend.pipeline.connectors.devfolio_connector import DevfolioConnector
from backend.pipeline.connectors.ashby_connector import AshbyConnector
from backend.pipeline.entity_normalizer import EntityNormalizer, normalize_experience_level
from backend.pipeline.opportunity_validator import OpportunityValidator
from backend.pipeline.opportunity_deduplicator import OpportunityDeduplicator
from backend.pipeline.quality_evaluator import QualityEvaluator
from backend.pipeline.pipeline_stage_registry import PipelineStageRegistry
from backend.pipeline.pipeline_stages import register_concrete_stages
from backend.pipeline.unknown_entity_service import UnknownEntityService
from backend.pipeline.normalizers import StructuralNormalizer
from backend.pipeline.domain_circuit_breaker import DomainCircuitBreaker
from backend.pipeline.data_contract import (
    ENRICHMENT_VERSION,
    build_enrichment_input,
)
from backend.pipeline.student_signal_extractor import extract_student_signals
from backend.pipeline.government_digest_extractor import extract_government_digest
from backend.pipeline.notification_worker import drain_notification_delivery_queue

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("PipelineOrchestrator")

class PipelineOrchestrator:
    """Production Pipeline Orchestrator."""

    def __init__(self, dry_run: bool = False):
        url = os.environ.get("SUPABASE_URL", "").strip("\"'")
        key = (
            os.environ.get("SUPABASE_SERVICE_KEY")
            or os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
            or os.environ.get("SUPABASE_KEY")
            or os.environ.get("SUPABASE_ANON_KEY")
            or ""
        )
        key = key.strip("\"'")
        self.supabase: Client = create_client(url, key)
        self._dry_run = dry_run
        self.normalizer = EntityNormalizer()
        self.validator = OpportunityValidator()
        self.deduplicator = OpportunityDeduplicator()
        self.quality_evaluator = QualityEvaluator()
        self.circuit_breaker = DomainCircuitBreaker()
        self.unknown_entity_service = UnknownEntityService(self.supabase)
        # Counter of records quarantined during the STORE stage, used for the
        # final pipeline summary log. Reset every run.
        self._last_store_quarantined = 0
        self._last_store_skipped_duplicate_fingerprint = 0
        # In-memory cross-batch fingerprint tracking across runs/chunks
        self._seen_fingerprints: Set[str] = set()
        self._maintenance_ran_this_session: bool = False
        # Automatic connector discovery at startup: newly added connector
        # modules are registered without any hardcoded registration here.
        ConnectorRegistry.discover()
        self.connectors: Dict[str, Any] = {
            "devfolio": ConnectorRegistry.get("devfolio") or DevfolioConnector(),
            "ashby": ConnectorRegistry.get("ashby") or AshbyConnector(),
        }
        # Wire the concrete processing stages into the pluggable registry so
        # run_pipeline_for_source executes whatever stages are registered.
        # In dry-run mode all persistence/telemetry callables become no-ops.
        register_concrete_stages(
            fetch_fn=self._fetch_for_source,
            normalizer=self.normalizer,
            validator=self.validator,
            deduplicator=self.deduplicator,
            evaluator=self.quality_evaluator,
            save_unknown_fn=(lambda unknowns: None) if dry_run else self._save_unknown_entities,
            malformed_quarantine_fn=(
                lambda raw, reason: None
            ) if dry_run else self._quarantine_raw_item,
            quarantine_fn=(lambda opp, reason: None) if dry_run else self._quarantine_item,
            retry_fn=(lambda opp, reason: None) if dry_run else self._queue_retry_item,
            store_fn=(lambda items: (len(items), len(items))) if dry_run else self._store_opportunities,
            publish_fn=(lambda: 1) if dry_run else self._publish_feed_version,
        )

    def run_pipeline_for_source(self, source_slug: str, query: str = None) -> Dict[str, Any]:
        """Run the complete ingestion pipeline for a single source connector."""
        # 0. Check circuit breaker and connector status before touching the connector
        if self.circuit_breaker.is_tripped(source_slug) or not ConnectorRegistry.is_enabled(source_slug):
            logger.error(
                "Scraping halted for '%s': Circuit breaker is TRIPPED or connector is disabled.",
                source_slug,
            )
            return {
                "status": "error",
                "source_slug": source_slug,
                "message": f"Circuit breaker tripped or connector '{source_slug}' disabled.",
            }

        connector = ConnectorRegistry.get(source_slug)
        if not connector:
            logger.error(f"Connector '{source_slug}' not found in ConnectorRegistry.")
            return {"status": "error", "message": f"Connector '{source_slug}' not registered."}

        # 1. Load Dynamic Source Config from company_sources table
        source_config, source_reputation = self._load_source_config(source_slug)

        # 1-7. Run the pluggable stages (FETCH -> NORMALIZE -> VALIDATE ->
        # DEDUPLICATE -> QUALITY -> STORE -> PUBLISH) in registration order.
        context: Dict[str, Any] = {
            "source_slug": source_slug,
            "source_config": source_config,
            "source_reputation": source_reputation,
            "query": query,
        }
        for stage in PipelineStageRegistry.get_all():
            logger.info(f"=== STAGE: {stage.name.upper()} ===")
            context = stage.run(context) or context

        # If the FETCH stage recorded a failure, short-circuit with telemetry.
        fetch_error = context.get("fetch_error")
        fetch_latency_ms = context.get("fetch_latency_ms") or 0.0
        if fetch_error:
            logger.error(f"Fetch failed for '{source_slug}': {fetch_error}")
            if not self._dry_run:
                ConnectorRegistry.record_run(
                    source_slug,
                    latency_ms=fetch_latency_ms,
                    retry_count=1,
                )
                ConnectorRegistry.mark_failure(source_slug, fetch_error)
                self._record_source_failure(source_slug, fetch_error)
            return {"status": "error", "message": fetch_error}

        raw_items = context.get("raw_items") or []
        logger.info(f"Fetched {len(raw_items)} raw items from '{source_slug}'.")

        normalized_list = context.get("normalized_list") or []
        valid_items = context.get("valid_items") or []
        unique_items = context.get("unique_items") or []
        scored_items = context.get("scored_items") or []
        quarantined_count = context.get("quarantined_count") or 0
        duplicates_removed = context.get("duplicates_removed") or 0
        stored_count = context.get("stored_count") or 0
        new_count = context.get("new_count") or 0
        new_feed_version = context.get("new_feed_version") or 1

        # Circuit breaker evaluation
        total_processed = len(raw_items)
        malformed_count = context.get("malformed_count") or max(0, len(raw_items) - len(normalized_list))
        total_rejections = malformed_count + quarantined_count + self._last_store_quarantined

        tripped = self.circuit_breaker.record(
            source_slug,
            processed_count=total_processed,
            rejected_count=total_rejections,
        )

        if tripped:
            self._handle_circuit_breaker_tripped(source_slug)
            return {
                "status": "error",
                "source_slug": source_slug,
                "raw_fetched": total_processed,
                "rejections": total_rejections,
                "message": f"Circuit breaker tripped for '{source_slug}': connector disabled and scraping halted.",
            }

        if self._dry_run:
            logger.info(
                f"DRY RUN: would store {stored_count} / publish v{new_feed_version} "
                f"(quarantine={quarantined_count}, duplicates={duplicates_removed})."
            )
            return {
                "status": "success",
                "source_slug": source_slug,
                "raw_fetched": len(raw_items),
                "valid_items": len(valid_items),
                "duplicates_removed": duplicates_removed,
                "quarantined": quarantined_count,
                "stored": stored_count,
                "published_feed_version": new_feed_version,
                "dry_run": True,
            }

        self._record_source_success(source_slug, len(scored_items))
        self._record_daily_stats(
            total=len(raw_items),
            new_cnt=new_count,
            quarantined_cnt=quarantined_count,
            duplicates_cnt=duplicates_removed
        )

        logger.info(
            "Pipeline summary for '%s': fetched=%d inserted=%d updated=%d quarantined=%d",
            source_slug,
            len(raw_items),
            new_count,
            stored_count - new_count,
            quarantined_count + self._last_store_quarantined,
        )

        ConnectorRegistry.record_run(
            source_slug,
            items_fetched=len(raw_items),
            items_stored=stored_count,
            items_updated=stored_count - new_count,
            duplicates_removed=duplicates_removed,
            rejected=quarantined_count,
            latency_ms=fetch_latency_ms,
        )
        ConnectorRegistry.mark_success(source_slug)

        return {
            "status": "success",
            "source_slug": source_slug,
            "raw_fetched": len(raw_items),
            "valid_items": len(valid_items),
            "duplicates_removed": duplicates_removed,
            "quarantined": quarantined_count,
            "stored": stored_count,
            "published_feed_version": new_feed_version
        }

    def _handle_circuit_breaker_tripped(self, source_slug: str) -> None:
        """Handle actions when circuit breaker trips for a source."""
        slug = (source_slug or "").lower()
        ConnectorRegistry.disable(slug)
        self._record_source_failure(
            slug,
            f"Circuit breaker tripped: rejection rate exceeded threshold for '{slug}'"
        )
        if not self._dry_run:
            try:
                self.supabase.table("company_sources").update({
                    "status": "disabled",
                    "is_active": False,
                    "last_failure_at": datetime.now(timezone.utc).isoformat(),
                }).eq("source_slug", slug).execute()
            except Exception as e:
                logger.warning(f"Could not update disabled status in company_sources for '{slug}': {e}")

    def _fetch_for_source(
        self,
        source_slug: str,
        source_config: Dict[str, Any],
        query: Optional[str] = None,
    ) -> List[OpportunityRaw]:
        """Resolve the connector for ``source_slug`` and fetch raw items.

        This is the ``fetch_fn`` injected into the FETCH pipeline stage. It is
        the only place the orchestrator touches ``ConnectorRegistry`` for
        fetching; raising here lets the stage capture the error into the
        context for the orchestrator's failure handling.
        """
        connector = ConnectorRegistry.get(source_slug)
        if not connector:
            raise ValueError(f"Connector '{source_slug}' not registered.")
        return connector.fetch_raw_opportunities(source_config, query)

    def _load_source_config(self, source_slug: str) -> Tuple[Dict[str, Any], int]:
        default_rep = 85
        connector = ConnectorRegistry.get(source_slug)
        if connector and hasattr(connector, "default_source_reputation"):
            default_rep = connector.default_source_reputation

        try:
            res = self.supabase.table("company_sources").select("*").eq("source_slug", source_slug).limit(1).execute()
            if res.data:
                row = res.data[0]
                reputation = row.get("source_reputation", default_rep)
                return row, reputation
            logger.warning(
                "No company_sources row for '%s'; using connector defaults for "
                "config/reputation. Health telemetry for this source will not persist.",
                source_slug,
            )
        except Exception as e:
            logger.warning(f"Could not load config for '{source_slug}' from Supabase: {e}")
        return {}, default_rep

    def _purge_old_quarantine(self) -> None:
        """Purge quarantine records older than 3 days in timeout-safe batches."""
        total_deleted = 0
        try:
            for _ in range(10):  # up to 25,000 rows per run
                res = self.supabase.rpc(
                    "purge_quarantine_batch",
                    {"p_retention_days": 3, "p_batch_size": 2500},
                ).execute()
                deleted = res.data if isinstance(res.data, int) else 0
                total_deleted += deleted
                if deleted < 2500:
                    break
            if total_deleted:
                logger.info(f"[maintenance] Purged {total_deleted} old quarantine rows")
        except Exception as exc:
            logger.warning(f"[maintenance] Batched quarantine purge failed: {exc}")

    def _cleanup_expired_opportunities(self) -> None:
        """Expire past-deadline listings and hard-prune unreferenced expired rows >14d."""
        try:
            res = self.supabase.rpc(
                "maintain_opportunity_lifecycle",
                {"p_delete_after_days": 14, "p_batch_size": 2000},
            ).execute()
            if res and hasattr(res, "data") and isinstance(res.data, dict) and "hard_pruned" in res.data:
                logger.info(f"[maintenance] Opportunity lifecycle result: {res.data}")
                return
        except Exception as exc:
            logger.debug(f"[maintenance] Batched lifecycle RPC unhandled, falling back: {exc}")

        # Table update fallback (also satisfies unit tests mocking .table("opportunities"))
        try:
            cutoff = datetime.utcnow().isoformat()
            self.supabase.table("opportunities").update({
                "status": "expired",
                "is_active": False,
                "expired_at": cutoff,
                "lifecycle_state": "EXPIRED",
                "review_status": "expired"
            }).lt("deadline", cutoff).eq("status", "active").execute()

            null_cutoff = (datetime.utcnow() - timedelta(days=30)).isoformat()
            self.supabase.table("opportunities").update({
                "status": "expired",
                "is_active": False,
                "expired_at": cutoff,
                "lifecycle_state": "EXPIRED",
                "review_status": "expired"
            }).is_("deadline", "null").lt("posted_at", null_cutoff).eq("status", "active").execute()
        except Exception as e:
            logger.warning(f"expired cleanup skipped/failed: {e}")

    def _enqueue_bookmark_deadline_reminders(self) -> None:
        """Trigger SQL RPC to queue 48-hour closing reminders for un-applied bookmarks."""
        try:
            res = self.supabase.rpc("enqueue_expiring_bookmark_reminders").execute()
            queued = res.data if isinstance(res.data, int) else 0
            if queued:
                logger.info(f"[maintenance] Queued {queued} expiring bookmark deadline reminders.")
        except Exception as exc:
            logger.warning(f"[maintenance] Failed to enqueue expiring bookmark reminders: {exc}")

    def _run_session_maintenance(self) -> None:
        """Runs once per pipeline invocation: purges quarantine, expires stale rows,
        enqueues 48h bookmark reminders, and drains notification_delivery_queue."""
        if getattr(self, "_maintenance_ran_this_session", False):
            return
        self._maintenance_ran_this_session = True

        self._purge_old_quarantine()
        self._cleanup_expired_opportunities()

        # 1. Enqueue 48-hour closing reminders for un-applied bookmarks
        self._enqueue_bookmark_deadline_reminders()

        # 2. Drain pending notifications via FCM HTTP v1
        try:
            drain_notification_delivery_queue(self.supabase, batch_size=200)
        except Exception as exc:
            logger.warning(f"[maintenance] drain_notification_delivery_queue failed: {exc}")

    @staticmethod
    def _is_valid_fingerprint(fp: Optional[str]) -> bool:
        """Validate whether a fingerprint string is non-empty, non-degenerate SHA-256."""
        if not fp or not isinstance(fp, str):
            return False
        cleaned = fp.strip()
        if len(cleaned) != 64:
            return False
        degenerate_hashes = {
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",  # sha256(b"")
            "898b671192e2ebaa80c441c8c5c7602324976cf36bc6bc9ca1a3f65e5e6fb469",  # sha256(b"||||")
        }
        return cleaned not in degenerate_hashes

    @staticmethod
    def _extract_salary(opp: OpportunityNormalized) -> Optional[float]:
        """Extract annual salary in INR as a float; None when undisclosed.

        Root-cause fix (0/month display bug): the old `0.0` default for
        undisclosed pay flowed into the DB `salary` column, where the app
        rendered it as a real amount ("Rs 0/month"). No legitimate listing
        pays exactly 0, so non-positive values are undisclosed (None → NULL).
        """
        if opp.stipend_amount is not None:
            try:
                val = float(opp.stipend_amount)
                if val > 0:
                    return round(val, 2)
            except (ValueError, TypeError):
                pass

        if opp.salary_range:
            parsed_val, _ = StructuralNormalizer.parse_salary(opp.salary_range)
            if parsed_val is not None and parsed_val > 0:
                return round(float(parsed_val), 2)

        if opp.raw_payload and isinstance(opp.raw_payload, dict):
            raw_val = (
                opp.raw_payload.get("salary")
                or opp.raw_payload.get("raw_salary")
                or opp.raw_payload.get("stipend")
            )
            if raw_val is not None:
                if isinstance(raw_val, bool):
                    pass
                elif isinstance(raw_val, (int, float)) and raw_val > 0:
                    return round(float(raw_val), 2)
                elif isinstance(raw_val, str):
                    parsed_val, _ = StructuralNormalizer.parse_salary(raw_val)
                    if parsed_val is not None and parsed_val > 0:
                        return round(float(parsed_val), 2)

        return None

    @staticmethod
    def _extract_monthly_stipend(opp: OpportunityNormalized) -> Optional[float]:
        """Monthly-INR baseline for client filters; None when undisclosed.

        Sprint-1: prefers adapter-structured min/max/currency/period, falls
        back to free-text parse. Never returns 0.0 — NULL preserves the
        exclude-on-salaryMin filter contract.
        """
        try:
            from backend.pipeline.normalizers import StructuralNormalizer
        except Exception:
            return None
        try:
            raw = opp.raw_payload if isinstance(opp.raw_payload, dict) else {}
            if opp.normalized_monthly_stipend is not None:
                try:
                    val = float(opp.normalized_monthly_stipend)
                    if val > 0:
                        return round(val, 2)
                except (ValueError, TypeError):
                    pass
            monthly = StructuralNormalizer.parse_monthly_stipend(
                opp.salary_range,
                salary_min=raw.get("salary_min"),
                salary_max=raw.get("salary_max"),
                salary_currency=raw.get("salary_currency"),
                salary_period=raw.get("salary_period"),
            )
            if monthly is not None and monthly > 0:
                return monthly
        except Exception:
            pass
        return None

    @staticmethod
    def _content_hash_v2(record: Dict[str, Any]) -> str:
        """Content hash computed from the FINAL upsert dict (red-team fix).

        Hashing pre-builder dataclass fields while the builder drops columns
        causes an infinite update loop (stored NULL != scraped value forever).
        v3 adds ``posted_at`` so a source-date correction is a content change
        instead of an "unchanged" skip; the prefix retires older hashes, so the
        first post-deploy run re-upserts once, then stabilizes.
        """
        tags = sorted(record.get("tags") or [])
        reqs = sorted(record.get("requirements") or [])
        geo = sorted(record.get("geo_eligibility") or [])
        canonical = "|".join([
            "v3",
            str(record.get("title") or ""),
            str(record.get("company") or ""),
            str(record.get("description") or ""),
            str(record.get("location") or ""),
            str(record.get("category") or ""),
            str(record.get("salary") or ""),
            str(record.get("normalized_monthly_stipend") or ""),
            ",".join(tags),
            ",".join(reqs),
            str(record.get("deadline") or ""),
            str(record.get("posted_at") or ""),
            str(record.get("experience_level") or ""),
            str(record.get("ppo_offered") or ""),
            str(record.get("duration") or ""),
            str(record.get("vacancy_count") or ""),
            ",".join(geo),
            str(record.get("subcategory") or ""),
        ])
        return hashlib.md5(canonical.encode("utf-8")).hexdigest()

    @staticmethod
    def _build_lifecycle_update(record: Dict[str, Any]) -> Dict[str, Any]:
        """Fields allowed to change on an approved/expired row.

        Review/status fields are deliberately excluded so a content refresh
        cannot bypass triage. Null values are omitted so a missing source
        signal never erases a previously known deadline/salary/geo.
        """
        update: Dict[str, Any] = {
            "raw_payload": record.get("raw_payload"),
            "updated_at": datetime.utcnow().isoformat(),
        }
        if record.get("posted_at"):
            update["posted_at"] = record["posted_at"]
        for key in (
            "deadline",
            "salary",
            "normalized_monthly_stipend",
            "geo_eligibility",
        ):
            value = record.get(key)
            if value is not None:
                update[key] = value
        return update

    @staticmethod
    def _extract_tags(opp: OpportunityNormalized) -> List[str]:
        """Construct a clean, deduplicated list of tags; defaults to [opp.category]."""
        tags_list: List[str] = []
        seen = set()

        def _add(t: Any) -> None:
            if not t:
                return
            cleaned = str(t).strip()
            if cleaned and cleaned.lower() not in seen:
                seen.add(cleaned.lower())
                tags_list.append(cleaned)

        for skill in (opp.skills_required or []):
            _add(skill)

        if opp.raw_payload and isinstance(opp.raw_payload, dict):
            raw_tags = opp.raw_payload.get("tags")
            if isinstance(raw_tags, list):
                for t in raw_tags:
                    _add(t)
            elif isinstance(raw_tags, str):
                for t in raw_tags.split(","):
                    _add(t)

        if opp.branch:
            _add(opp.branch)
        if opp.category:
            _add(opp.category)

        if not tags_list:
            fallback = (opp.category or "student").strip()
            tags_list = [fallback]

        return tags_list

    @staticmethod
    def _extract_requirements(opp: OpportunityNormalized) -> List[str]:
        """Extract or synthesize requirements; defaults to ['<Level> experience required']."""
        reqs: List[str] = []
        seen = set()

        def _add(r: Any) -> None:
            if not r:
                return
            cleaned = str(r).strip()
            if cleaned and cleaned.lower() not in seen:
                seen.add(cleaned.lower())
                reqs.append(cleaned)

        if opp.raw_payload and isinstance(opp.raw_payload, dict):
            raw_reqs = opp.raw_payload.get("requirements")
            if isinstance(raw_reqs, list):
                for item in raw_reqs:
                    _add(item)
            elif isinstance(raw_reqs, str):
                for line in raw_reqs.splitlines():
                    stripped = line.strip("•-* \t")
                    if stripped:
                        _add(stripped)

        if not reqs:
            for deg in (opp.degrees_required or []):
                _add(f"Degree in {deg}")
            for skill in (opp.skills_required or [])[:3]:
                _add(f"Proficiency in {skill}")
            if opp.experience_level:
                _add(f"{opp.experience_level.capitalize()} experience level")

        if not reqs:
            lvl = (opp.experience_level or "entry").capitalize()
            reqs = [f"{lvl} experience required"]

        return reqs

    def _store_opportunities(self, items: List[OpportunityNormalized]) -> Tuple[int, int]:
        if self._dry_run or not items:
            return len(items), len(items)

        stored = 0
        new_count = 0
        quarantined = 0
        skipped_hash = 0
        skipped_duplicate_fingerprint = 0

        # Run start purge, expired cleanup & notification queue drain once per session
        self._run_session_maintenance()

        CHUNK_SIZE = 100
        total_items = len(items)

        for chunk_idx in range(0, total_items, CHUNK_SIZE):
            chunk = items[chunk_idx : chunk_idx + CHUNK_SIZE]

            chunk_records: List[Dict[str, Any]] = []
            chunk_hashes: Dict[str, str] = {}
            ext_ids: List[str] = []
            opp_by_key: Dict[Tuple[str, str], OpportunityNormalized] = {}

            for opp in chunk:
                if not opp.external_id:
                    continue
                opp_key = (opp.source_slug, opp.external_id)
                if opp_key in opp_by_key:
                    continue  # Deduplicate within chunk
                opp_by_key[opp_key] = opp
                ext_ids.append(opp.external_id)

                # DB trigger owns review_status; Python only recommends.
                review_recommendation = (
                    opp.extraction_confidence or {}
                ).get("review_recommendation", "pending")
                if review_recommendation == "approved":
                    payload_to_store = {
                        "source_url": opp.apply_url,
                        "fetched_at": datetime.utcnow().isoformat(),
                        "parser_version": opp.parser_version,
                    }
                else:
                    payload_to_store = opp.raw_payload

                c_hash = None  # computed from the final record below (v2)
                chunk_hashes[opp.external_id] = ""

                updated_confidence = dict(opp.extraction_confidence or {})
                updated_confidence["enrichment_version"] = ENRICHMENT_VERSION
                updated_confidence.setdefault(
                    "review_recommendation", review_recommendation
                )
                if self._is_valid_fingerprint(opp.fingerprint_hash):
                    updated_confidence["fingerprint_hash"] = opp.fingerprint_hash

                # Compact, replayable input. The full raw_payload is reduced
                # for approved rows, so replay needs its own stable snapshot.
                enrichment_input = build_enrichment_input(
                    source_slug=opp.source_slug,
                    title=opp.title,
                    company=opp.company,
                    location=opp.location,
                    salary_text=opp.salary_range,
                    description=opp.description,
                    apply_url=opp.apply_url,
                    source_metadata=(
                        opp.raw_payload
                        if isinstance(opp.raw_payload, dict)
                        else None
                    ),
                )

                salary_val = self._extract_salary(opp)
                monthly_val = self._extract_monthly_stipend(opp)
                tags_val = self._extract_tags(opp)
                reqs_val = self._extract_requirements(opp)

                if not opp.source_slug or not str(opp.source_slug).strip():
                    logger.error("Rejecting item '%s': source_slug is empty or missing provenance.", opp.external_id)
                    continue

                # Student Signals extraction (guaranteed in metadata)
                student_signals = (
                    opp.metadata
                    if opp.metadata.get("pre_flight_checklist")
                    else extract_student_signals(
                        title=opp.title or "",
                        description=opp.description or "",
                        apply_url=opp.apply_url or "",
                        experience_level=opp.experience_level or "entry",
                        tags=tags_val,
                        required_degrees=list(opp.degrees_required or []),
                        ppo_offered=bool(opp.ppo_offered),
                        normalized_monthly_stipend=monthly_val,
                    )
                )

                # Government notification digest extraction
                govt_digest = None
                if opp.category == "government":
                    req_text = " ".join(reqs_val) if reqs_val else ""
                    sal_text = str(salary_val or opp.salary_range or "")
                    govt_digest = extract_government_digest(
                        title=opp.title or "",
                        company=opp.company or "",
                        description=opp.description or "",
                        requirements=req_text,
                        apply_url=opp.apply_url or "",
                        salary_text=sal_text,
                        required_degrees=list(opp.degrees_required or []),
                    )
                    if govt_digest.get("exam_targets"):
                        for exam in govt_digest["exam_targets"]:
                            if exam not in tags_val:
                                tags_val.append(exam)

                record = {
                    "source": str(opp.source_slug).strip(),
                    "source_slug": str(opp.source_slug).strip(),
                    "external_id": opp.external_id,
                    "title": opp.title,
                    "company": opp.company,
                    "apply_url": opp.apply_url,
                    "description": opp.description,
                    "location": opp.location,
                    "category": opp.category,
                    "experience_level": normalize_experience_level(opp.experience_level),
                    "salary": salary_val,
                    "tags": tags_val,
                    "requirements": reqs_val,
                    "posted_at": opp.posted_at.isoformat(),
                    "deadline": opp.deadline.isoformat() if opp.deadline else None,
                    "is_verified": opp.is_verified,
                    "quality_score": opp.quality_score,
                    "trust_score": opp.trust_score,
                    "fraud_score": float(max(0.0, min(1.0, getattr(opp, "fraud_score", 0.0) or 0.0))),
                    "popularity_score": opp.popularity_score,
                    "lifecycle_state": opp.lifecycle_state,
                    "raw_payload": payload_to_store,
                    "quality_breakdown": opp.quality_breakdown,
                    "extraction_confidence": updated_confidence,
                    "score_vector": opp.score_vector,
                    "parser_version": opp.parser_version,
                    "taxonomy_version": opp.taxonomy_version,
                    "quality_version": opp.quality_version,
                    # review_status is intentionally omitted: the DB trigger is
                    # the single authority on insert and score changes.
                    # Sprint-1 enrichment passthrough (nullable; NULL = unknown).
                    "ppo_offered": opp.ppo_offered,
                    "duration": opp.duration,
                    "vacancy_count": opp.vacancy_count,
                    "geo_eligibility": list(opp.geo_eligibility or []) or None,
                    "normalized_monthly_stipend": monthly_val,
                    "metadata": {
                        **(opp.metadata or {}),
                        **student_signals,
                        **({"govt_digest": govt_digest} if govt_digest else {}),
                        # Projected so the feed RPCs (which already return
                        # metadata) deliver degree eligibility to the client.
                        "required_degrees": list(opp.degrees_required or []) or None,
                        "salary_period": opp.salary_period,
                        "requisition_id": opp.requisition_id,
                        "department": opp.department,
                        "subcategory_raw": opp.subcategory_raw,
                        "enrichment_input": enrichment_input,
                        "enrichment_version": ENRICHMENT_VERSION,
                    },
                }
                # Red-team fix: hash what is actually stored, not the
                # pre-builder dataclass. Retires v1 hashes (one bounded
                # re-upsert wave on first post-deploy run, then stable).
                c_hash = self._content_hash_v2(record)
                chunk_hashes[opp.external_id] = c_hash
                updated_confidence["content_hash"] = c_hash
                record["extraction_confidence"] = updated_confidence
                rec_fp = opp.fingerprint_hash or (updated_confidence.get("fingerprint_hash"))
                if rec_fp:
                    record["fingerprint_hash"] = rec_fp
                if isinstance(opp.raw_payload, dict):
                    if "subcategory" in opp.raw_payload:
                        record["subcategory"] = opp.raw_payload["subcategory"]
                    if "expired_at" in opp.raw_payload:
                        record["expired_at"] = opp.raw_payload["expired_at"]
                    if "is_active" in opp.raw_payload:
                        record["is_active"] = opp.raw_payload["is_active"]
                chunk_records.append(record)

            if not chunk_records:
                continue

            # Batch query existing records in ONE roundtrip
            existing_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
            try:
                res = (
                    self.supabase.table("opportunities")
                    .select("id,source_slug,external_id,status,review_status,extraction_confidence")
                    .in_("external_id", ext_ids)
                    .execute()
                )
                if isinstance(res.data, list):
                    for row in res.data:
                        existing_map[(row.get("source_slug"), row.get("external_id"))] = row
            except Exception as e:
                logger.warning("Batch check of existing opportunities failed: %s", e)

            # Persistent fingerprint lookup: query database for existing fingerprints in this chunk
            chunk_valid_fps = [
                rec["extraction_confidence"]["fingerprint_hash"]
                for rec in chunk_records
                if "fingerprint_hash" in rec.get("extraction_confidence", {})
                and self._is_valid_fingerprint(rec["extraction_confidence"]["fingerprint_hash"])
            ]
            unique_chunk_fps = list(set(chunk_valid_fps))
            db_fingerprint_map: Dict[str, Dict[str, Any]] = {}

            if unique_chunk_fps:
                FP_BATCH_SIZE = 50
                for fp_offset in range(0, len(unique_chunk_fps), FP_BATCH_SIZE):
                    sub_fps = unique_chunk_fps[fp_offset : fp_offset + FP_BATCH_SIZE]
                    try:
                        res_fp = (
                            self.supabase.table("opportunities")
                            .select("id,source_slug,external_id,status,review_status,fingerprint_hash,extraction_confidence")
                            .filter("fingerprint_hash", "in", f"({','.join(sub_fps)})")
                            .execute()
                        )
                        if isinstance(res_fp.data, list):
                            for row in res_fp.data:
                                row_conf = row.get("extraction_confidence") or {}
                                row_fp = row.get("fingerprint_hash") or row_conf.get("fingerprint_hash")
                                if row_fp:
                                    db_fingerprint_map[row_fp] = row
                    except Exception as e:
                        logger.warning("Batch fingerprint check failed for sub-batch: %s", e)

            upsert_batch: List[Dict[str, Any]] = []
            update_lifecycle_only: List[Tuple[str, Dict[str, Any]]] = []
            chunk_new_count = 0

            for record in chunk_records:
                key = (record["source_slug"], record["external_id"])
                existing_row = existing_map.get(key)
                rec_fp = (record.get("extraction_confidence") or {}).get("fingerprint_hash")
                has_valid_fp = self._is_valid_fingerprint(rec_fp)

                if existing_row is not None:
                    if has_valid_fp:
                        self._seen_fingerprints.add(rec_fp)

                    ex_conf = existing_row.get("extraction_confidence") or {}
                    existing_hash = ex_conf.get("content_hash")
                    c_hash = chunk_hashes.get(record["external_id"])
                    if existing_hash and existing_hash == c_hash:
                        skipped_hash += 1
                        stored += 1
                        continue

                    existing_rev = existing_row.get("review_status")
                    existing_status = existing_row.get("status")

                    if existing_rev == "expired" or existing_status == "expired" or existing_rev == "approved":
                        # Field-ownership merge: refresh dates/pay/geo without
                        # touching review_status or status.
                        update_lifecycle_only.append(
                            (
                                existing_row["id"],
                                self._build_lifecycle_update(record),
                            )
                        )
                    else:
                        upsert_batch.append(record)
                else:
                    if has_valid_fp and (rec_fp in self._seen_fingerprints or rec_fp in db_fingerprint_map):
                        matched_row = db_fingerprint_map.get(rec_fp)
                        matched_src = matched_row.get("source_slug") if matched_row else "in_run_cache"
                        matched_id = matched_row.get("id") if matched_row else "in_run_cache"
                        logger.info(
                            "Persistent deduplication: skipped duplicate opportunity '%s' from source '%s' "
                            "(ext_id='%s', fp='%s', matched_source='%s', matched_id=%s)",
                            record.get("title"),
                            record.get("source_slug"),
                            record.get("external_id"),
                            rec_fp,
                            matched_src,
                            matched_id,
                        )
                        skipped_duplicate_fingerprint += 1
                        self._seen_fingerprints.add(rec_fp)
                        continue

                    chunk_new_count += 1
                    upsert_batch.append(record)
                    if has_valid_fp:
                        self._seen_fingerprints.add(rec_fp)

            # Bulk upsert in ONE roundtrip
            if upsert_batch:
                # Deduplicate by apply_url within the batch to prevent 23505 collisions
                seen_batch_urls = set()
                deduped_upsert = []
                for b_item in upsert_batch:
                    b_url = (b_item.get("apply_url") or "").strip()
                    if b_url:
                        if b_url in seen_batch_urls:
                            continue
                        seen_batch_urls.add(b_url)
                    deduped_upsert.append(b_item)
                upsert_batch = deduped_upsert
                try:
                    self.supabase.table("opportunities").upsert(
                        upsert_batch,
                        on_conflict="source_slug,external_id",
                    ).execute()
                    stored += len(upsert_batch)
                    new_count += chunk_new_count
                except Exception as e:
                    logger.warning("Chunk bulk upsert failed: %s. Falling back to per-item isolation.", e)
                    for rec in upsert_batch:
                        opp_obj = opp_by_key.get((rec["source_slug"], rec["external_id"]))
                        try:
                            self.supabase.table("opportunities").upsert(
                                rec,
                                on_conflict="source_slug,external_id",
                            ).execute()
                            stored += 1
                            if (rec["source_slug"], rec["external_id"]) not in existing_map:
                                new_count += 1
                        except Exception as item_err:
                            err_str = str(item_err)
                            # Never quarantine duplicate URLs — simply skip them in memory
                            if "23505" in err_str or "duplicate key" in err_str.lower():
                                continue
                            quarantined += 1
                            logger.error("Failed to store opportunity '%s': %s", rec.get("title"), item_err)
                            if opp_obj:
                                self._quarantine_item(opp_obj, f"store_error: {type(item_err).__name__}: {item_err}")

            # Apply lifecycle/content updates for expired/approved listings
            for row_id, update_fields in update_lifecycle_only:
                try:
                    self.supabase.table("opportunities").update(
                        update_fields
                    ).eq("id", row_id).execute()
                    stored += 1
                except Exception as e:
                    logger.warning(
                        "Failed lifecycle update for opportunity id=%s: %s",
                        row_id,
                        e,
                    )

        self._last_store_quarantined = quarantined
        self._last_store_skipped_duplicate_fingerprint = skipped_duplicate_fingerprint
        updated = stored - new_count - skipped_hash
        summary_line = (
            f"Store summary: processed={len(items)} inserted={new_count} "
            f"updated={updated} skipped_hash={skipped_hash} "
            f"skipped_duplicate_fingerprint={skipped_duplicate_fingerprint} "
            f"quarantined={quarantined}"
        )
        logger.info(summary_line)
        print(summary_line)
        return stored, new_count

    def _publish_feed_version(self) -> int:
        """Increment system feed_version to notify mobile app clients of fresh delta updates."""
        try:
            res = self.supabase.table("system_state").select("id,feed_version").limit(1).execute()
            if res.data:
                row = res.data[0]
                new_version = (row.get("feed_version") or 1) + 1
                self.supabase.table("system_state").update({
                    "feed_version": new_version,
                    "last_scrape_completed_at": datetime.utcnow().isoformat(),
                    "updated_at": datetime.utcnow().isoformat()
                }).eq("id", row["id"]).execute()
                logger.info(f"Feed version incremented to: {new_version}")
                return new_version
        except Exception as e:
            logger.warning(f"Could not increment system feed version: {e}")
        return 1

    def _quarantine_raw_item(self, raw: OpportunityRaw, reason: str):
        """S3: Quarantine a malformed raw payload trimmed to ~2000 chars."""
        try:
            trimmed_payload = str(raw.raw_payload)[:2000]
            self.supabase.table("quarantine_queue").insert({
                "source_slug": raw.source_slug,
                "raw_payload": trimmed_payload,
                "quarantine_reason": reason,
            }).execute()
            logger.warning(
                "Quarantined malformed raw item '%s' (%s): %s",
                raw.external_id, raw.source_slug, reason,
            )
        except Exception as e:
            logger.error("Failed to quarantine raw item '%s': %s", raw.external_id, e)

    def _quarantine_item(self, opp: OpportunityNormalized, reason: str):
        """S3: Quarantine an normalized payload trimmed to ~2000 chars."""
        try:
            trimmed_payload = str(opp.raw_payload)[:2000]
            self.supabase.table("quarantine_queue").insert({
                "source_slug": opp.source_slug,
                "raw_payload": trimmed_payload,
                "quarantine_reason": reason
            }).execute()
        except Exception as e:
            logger.error(f"Failed to quarantine item '{opp.external_id}': {e}")

    def _queue_retry_item(self, opp: OpportunityNormalized, error_msg: str):
        """Retry-queue handling (V1.2: disabled).

        Network retries happen in the connector HTTP layer; writing items to a
        dead ``retry_queue`` table with no consumer is just storage. For V1.2 we
        log and drop instead of enqueuing. A real retry worker can re-enable
        this later.
        """
        logger.warning(
            "Skipping retry enqueue for '%s' (retry worker not implemented): %s",
            opp.external_id, error_msg,
        )

    def _save_unknown_entities(self, unknowns: List[Dict[str, Any]]):
        """Insert-based accumulation of unknown entities (occurrence_count++)."""
        if not unknowns:
            return
        self.unknown_entity_service.accumulate(unknowns)

    def _record_source_success(self, source_slug: str, items_cnt: int):
        # Dry runs are read-only by contract: reporting a source as healthy or
        # rewriting its last-run count would change production telemetry.
        if self._dry_run:
            logger.info(
                "DRY RUN: skipping company_sources success write for '%s' (%d items).",
                source_slug,
                items_cnt,
            )
            return
        now_iso = datetime.utcnow().isoformat()
        try:
            res = self.supabase.table("company_sources").update({
                "status": "healthy",
                "items_ingested_last_run": items_cnt,
                "last_scraped_at": now_iso,
                "last_success_at": now_iso,
            }).eq("source_slug", source_slug).execute()
            if not res.data:
                # A source can be run without a config row (manual invocation
                # or a newly added connector). Update silently did nothing, so
                # crawl health was unmeasurable for exactly those sources.
                self.supabase.table("company_sources").insert({
                    "source_slug": source_slug,
                    "display_name": source_slug.replace("_", " ").title(),
                    "base_url": "",
                    "scraper_type": source_slug,
                    "status": "healthy",
                    "items_ingested_last_run": items_cnt,
                    "last_scraped_at": now_iso,
                    "last_success_at": now_iso,
                }).execute()
                logger.info(
                    "Created company_sources health row for '%s'.",
                    source_slug,
                )
        except Exception as e:
            logger.error(f"Failed to record success for '{source_slug}': {e}")

    def _record_source_failure(self, source_slug: str, error_msg: str):
        # A dry run must not mutate production telemetry: an exploratory
        # `--dry-run` previously PATCHed company_sources to status='degraded',
        # which could flip a healthy connector offline for real runs.
        if self._dry_run:
            logger.info(
                "DRY RUN: skipping company_sources failure write for '%s' (%s).",
                source_slug,
                error_msg,
            )
            return
        now_iso = datetime.utcnow().isoformat()
        try:
            res = self.supabase.table("company_sources").update({
                "status": "degraded",
                "last_scraped_at": now_iso,
                "last_failure_at": now_iso,
            }).eq("source_slug", source_slug).execute()
            if not res.data:
                self.supabase.table("company_sources").insert({
                    "source_slug": source_slug,
                    "display_name": source_slug.replace("_", " ").title(),
                    "base_url": "",
                    "scraper_type": source_slug,
                    "status": "degraded",
                    "last_scraped_at": now_iso,
                    "last_failure_at": now_iso,
                }).execute()
                logger.info(
                    "Created degraded company_sources health row for '%s' (%s).",
                    source_slug,
                    error_msg,
                )
        except Exception as e:
            logger.error(f"Failed to record failure for '{source_slug}': {e}")

    def _record_daily_stats(self, total: int, new_cnt: int, quarantined_cnt: int, duplicates_cnt: int):
        # Dry-run totals are exploratory and must not pollute the rolling
        # daily ingestion dashboard.
        if self._dry_run:
            logger.info(
                "DRY RUN: skipping daily_ingestion_stats write "
                "(total=%d, new=%d, quarantined=%d, duplicates=%d).",
                total,
                new_cnt,
                quarantined_cnt,
                duplicates_cnt,
            )
            return
        try:
            today_str = datetime.utcnow().strftime("%Y-%m-%d")
            self.supabase.table("daily_ingestion_stats").upsert({
                "stat_date": today_str,
                "total_ingested": total,
                "new_opportunities": new_cnt,
                "quarantined_count": quarantined_cnt,
                "duplicates_merged": duplicates_cnt
            }, on_conflict="stat_date").execute()
        except Exception as e:
            logger.error(f"Failed to record daily stats: {e}")

    def verify_active_links(self, limit: int = 500) -> Dict[str, Any]:
        """Runs ApplyLinkVerifier against active opportunities in Supabase.

        Confirmed DEAD (404/410) or CLOSED results transition the record to status='expired', is_active=False.
        Transient errors (timeouts, 5xx, anti-bot) do NOT modify any records.
        """
        from backend.pipeline.apply_link_verifier import ApplyLinkVerifier, VerificationStatus

        logger.info("Starting active link verification (limit=%d, dry_run=%s)...", limit, self._dry_run)
        try:
            res = self.supabase.table("opportunities").select("id, apply_url").eq("status", "active").eq("is_active", True).order("updated_at").limit(limit).execute()
            rows = res.data or []
        except Exception as e:
            logger.error("Failed to load active opportunities for link verification: %s", e)
            return {"status": "error", "message": str(e)}

        urls = [r["apply_url"] for r in rows if r.get("apply_url")]
        if not urls:
            logger.info("No active opportunity URLs found to verify.")
            return {"status": "success", "total_checked": 0, "expired_count": 0}

        verifier = ApplyLinkVerifier(max_results=len(urls))
        results = verifier.verify_batch(urls, check_content=True)

        expired_count = 0
        dead_count = 0
        closed_count = 0
        unreachable_count = 0

        now_iso = datetime.utcnow().isoformat()

        for r in results:
            if r.status in (VerificationStatus.DEAD, VerificationStatus.CLOSED):
                if r.status == VerificationStatus.DEAD:
                    dead_count += 1
                else:
                    closed_count += 1

                if not self._dry_run:
                    try:
                        self.supabase.table("opportunities").update({
                            "status": "expired",
                            "is_active": False,
                            "expired_at": now_iso,
                            "lifecycle_state": "EXPIRED",
                            "review_status": "expired"
                        }).eq("apply_url", r.apply_url).eq("status", "active").execute()
                        expired_count += 1
                    except Exception as exc:
                        logger.error("Failed to update expired status for URL '%s': %s", r.apply_url, exc)
                else:
                    expired_count += 1
            elif r.status == VerificationStatus.UNREACHABLE:
                unreachable_count += 1
            elif r.is_live:
                # The link check passed: record that this URL was confirmed
                # live. Transient states never touch the flag.
                if not self._dry_run:
                    try:
                        self.supabase.table("opportunities").update({
                            "is_verified": True,
                        }).eq("apply_url", r.apply_url).eq("status", "active").execute()
                    except Exception as exc:
                        logger.error("Failed to mark verified URL '%s': %s", r.apply_url, exc)

        logger.info(
            "Link verification complete: checked=%d, dead=%d, closed=%d, unreachable=%d, updated_expired=%d",
            len(results), dead_count, closed_count, unreachable_count, expired_count
        )
        return {
            "status": "success",
            "total_checked": len(results),
            "dead_found": dead_count,
            "closed_found": closed_count,
            "unreachable": unreachable_count,
            "expired_count": expired_count,
        }
