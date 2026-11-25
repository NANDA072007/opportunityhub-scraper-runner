"""
OpportunityHub — Production Ingestion CLI (Pipeline C)
=======================================================

Runs the modern ``PipelineOrchestrator`` across every registered connector,
or a single ``--source``. This replaces the removed legacy
``backend.pipeline.pipeline_runner`` and ``strategy_ingestion_runner``.

Usage
-----
.. code-block:: bash

    python backend/scripts/run_pipeline.py
    python backend/scripts/run_pipeline.py --source greenhouse
    python backend/scripts/run_pipeline.py --dry-run

Exit code is 0 when every requested source succeeded, 1 if any errored, and
2 for usage errors (unknown source slug).
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

env_backend = Path(__file__).resolve().parent.parent / ".env"
if env_backend.exists():
    load_dotenv(env_backend)
else:
    load_dotenv()

from backend.pipeline.connectors.connector_registry import ConnectorRegistry
from backend.pipeline.pipeline_orchestrator import PipelineOrchestrator

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("RunPipeline")


def _enabled_slugs() -> List[str]:
    return sorted(c.source_slug for c in ConnectorRegistry.get_all())


def _resolve_sources(source: str | None) -> tuple[List[str], str | None]:
    """Return (sources, usage_error).

    ``usage_error`` is non-None for an unknown or disabled ``--source`` so the
    CLI can exit 2 instead of silently reporting success on zero sources.
    """
    ConnectorRegistry.discover()
    enabled = _enabled_slugs()
    if source:
        slug = source.strip().lower()
        if not ConnectorRegistry.is_registered(slug):
            return [], (
                f"Unknown source_slug '{slug}'. Enabled sources: {', '.join(enabled)}"
            )
        if not ConnectorRegistry.is_enabled(slug):
            return [], (
                f"Source '{slug}' is registered but disabled "
                f"(see DISABLED_SOURCE_SLUGS). Enabled sources: {', '.join(enabled)}"
            )
        return [slug], None
    return enabled, None


def _print_summary(results: Dict[str, Any]) -> None:
    print("\n" + "=" * 64)
    print(" PIPELINE SUMMARY")
    print("=" * 64)
    header = f"{'source_slug':<20} {'status':<10} {'raw':<6} {'valid':<6} {'dup':<5} {'stored':<7} {'v':<4}"
    print(header)
    print("-" * 64)
    for slug, res in results.items():
        print(
            f"{slug:<20} {str(res.get('status', '?')):<10} "
            f"{res.get('raw_fetched', 0):<6} {res.get('valid_items', 0):<6} "
            f"{res.get('duplicates_removed', 0):<5} {res.get('stored', 0):<7} "
            f"{res.get('published_feed_version', '-'):<4}"
        )
    print("=" * 64)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the OpportunityHub ingestion pipeline.")
    parser.add_argument("--source", help="Only run this source_slug (default: all registered sources).")
    parser.add_argument("--dry-run", action="store_true", help="Skip all database writes and telemetry.")
    parser.add_argument("--verify-links", action="store_true", help="Verify active application link liveness.")
    args = parser.parse_args()

    if args.verify_links:
        logger.info("Executing active application link verification...")
        orchestrator = PipelineOrchestrator(dry_run=args.dry_run)
        res = orchestrator.verify_active_links()
        if res.get("status") == "error":
            return 1
        return 0

    # Resolve (and validate) the source selection BEFORE constructing the
    # orchestrator so usage errors do not depend on Supabase credentials.
    sources, usage_error = _resolve_sources(args.source)
    if usage_error:
        logger.error(usage_error)
        return 2
    if not sources:
        logger.error("No enabled sources are registered; nothing to run.")
        return 2

    orchestrator = PipelineOrchestrator(dry_run=args.dry_run)

    results: Dict[str, Any] = {}
    for slug in sources:
        try:
            results[slug] = orchestrator.run_pipeline_for_source(slug)
        except Exception as e:  # noqa: BLE001
            logger.exception(f"Unhandled pipeline error for '{slug}'.")
            results[slug] = {"status": "error", "message": str(e)}

    _print_summary(results)

    if any(res.get("status") == "error" for res in results.values()):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
