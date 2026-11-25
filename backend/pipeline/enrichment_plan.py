"""Pure enrichment planning shared by replay and operator scripts.

The pipeline enriches at ingestion; this module recomputes the same pay/geo
rules from a stored row so ``replay_enrichment.py`` and the backfill script
cannot drift. It never writes to the database and never erases a valid value.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Mapping, Optional, Tuple

from backend.pipeline.connectors._pay import mine_pay_text
from backend.pipeline.connectors.naukri_connector import extract_title_pay
from backend.pipeline.data_contract import (
    ENRICHMENT_VERSION,
    build_enrichment_input,
    decide_salary_update,
    derive_geo_eligibility,
)
from backend.pipeline.normalizers import StructuralNormalizer


def _as_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            decoded = json.loads(value)
        except ValueError:
            return {}
        return dict(decoded) if isinstance(decoded, Mapping) else {}
    return {}


def interpret_pay_text(
    pay_text: Optional[str], category: Optional[str]
) -> Tuple[Optional[float], Optional[float]]:
    """Return (annual INR, monthly INR) for a pay snippet.

    Mirrors the ingestion rules: monthly baselines are derived from the annual
    value only when the snippet itself did not state a period, and sub-30k
    non-student values are rejected as corrupt legacy amounts.
    """
    if not pay_text:
        return None, None

    annual, _ = StructuralNormalizer.parse_salary(
        pay_text, opportunity_type="job", category=category
    )
    if annual is not None and annual <= 0:
        annual = None
    monthly = StructuralNormalizer.parse_monthly_stipend(
        pay_text, opportunity_type="job", category=category
    )
    if annual is not None and (monthly is None or monthly <= 0):
        monthly = round(annual / 12.0, 2)

    if annual is not None and annual < 30000:
        if category == "student" and annual >= 1000:
            monthly = annual
            annual = round(annual * 12.0, 2)
        else:
            annual = None
            monthly = None
    if monthly is not None and monthly < 1000:
        monthly = None

    return annual, monthly


def _pay_candidates(
    row: Mapping[str, Any], enrichment_input: Mapping[str, Any]
) -> List[Optional[str]]:
    source = str(row.get("source_slug") or "")
    title = str(row.get("title") or "")
    company = str(row.get("company") or "")
    description = str(row.get("description") or "")
    # Greenhouse/Lever descriptions are HTML. Mining the raw markup stops at
    # the first </span>, so a "$135,320 — $159,200" range collapses to its
    # lower bound and silently rewrites the stored midpoint.
    clean_description = StructuralNormalizer.strip_html(description)

    if source == "naukri":
        # Title CTC is higher precision than the generated description. Keep
        # title and company separate so a trailing title digit and a company
        # starting with "5lpa" cannot combine into a fake decimal.
        return [
            extract_title_pay(title) or extract_title_pay(company),
            enrichment_input.get("salary_text"),
            mine_pay_text(f"{title} {company} {clean_description}"),
        ]
    return [
        enrichment_input.get("salary_text"),
        mine_pay_text(f"{title} {company} {clean_description}"),
    ]


def plan_row_update(
    row: Mapping[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """Return ``(fields_to_update, change_summary)`` for one stored row.

    ``change_summary`` maps a field name to a human-readable "old -> new"
    string, used by the CLI's dry-run report. An empty update means the row is
    already at the current contract and replay is idempotent for it.
    """
    update: Dict[str, Any] = {}
    changes: Dict[str, str] = {}

    source = str(row.get("source_slug") or "")
    location = str(row.get("location") or "")
    category = row.get("category")

    metadata = _as_dict(row.get("metadata"))
    existing_input = _as_dict(metadata.get("enrichment_input"))
    if existing_input:
        enrichment_input = existing_input
    else:
        enrichment_input = build_enrichment_input(
            source_slug=source,
            title=row.get("title"),
            company=row.get("company"),
            location=row.get("location"),
            salary_text=None,
            description=row.get("description"),
            apply_url=row.get("apply_url"),
            source_metadata={
                key: value
                for key, value in metadata.items()
                if key not in ("enrichment_input", "enrichment_version")
            },
        )

    pay_text = next(
        (text for text in _pay_candidates(row, enrichment_input) if text),
        None,
    )
    if not existing_input and pay_text:
        # Persist what this replay actually mined so the next version does not
        # depend on re-parsing HTML or a generated description.
        enrichment_input["salary_text"] = str(pay_text)[:300]
    annual, monthly = interpret_pay_text(pay_text, category)
    salary_update, _ = decide_salary_update(
        current_salary=row.get("salary"),
        current_monthly=row.get("normalized_monthly_stipend"),
        new_salary=annual,
        new_monthly=monthly,
        category=category,
    )
    for field, value in salary_update.items():
        old = (
            row.get("salary")
            if field == "salary"
            else row.get("normalized_monthly_stipend")
        )
        changes[field] = f"{old} -> {value}"
        update[field] = value

    current_geo = row.get("geo_eligibility") or []
    if not current_geo:
        derived = derive_geo_eligibility(location, source)
        if derived:
            update["geo_eligibility"] = derived
            changes["geo_eligibility"] = f"[] -> {derived}"

    # Stamp the snapshot only when there is something to record: a value
    # correction, a legacy row that already carries a snapshot, or an outdated
    # version. Untouched rows are not rewritten just to bump a version.
    version_stale = bool(existing_input) and (
        metadata.get("enrichment_version") != ENRICHMENT_VERSION
    )
    if update or version_stale:
        new_metadata = dict(metadata)
        if not existing_input:
            new_metadata["enrichment_input"] = enrichment_input
        if new_metadata.get("enrichment_version") != ENRICHMENT_VERSION:
            new_metadata["enrichment_version"] = ENRICHMENT_VERSION
            changes["metadata"] = (
                f"version {metadata.get('enrichment_version')} -> "
                f"{ENRICHMENT_VERSION}"
            )
        if new_metadata != metadata:
            update["metadata"] = new_metadata

        confidence = _as_dict(row.get("extraction_confidence"))
        if confidence.get("enrichment_version") != ENRICHMENT_VERSION:
            new_confidence = dict(confidence)
            new_confidence["enrichment_version"] = ENRICHMENT_VERSION
            update["extraction_confidence"] = new_confidence
            changes["extraction_confidence"] = (
                f"version {confidence.get('enrichment_version')} -> "
                f"{ENRICHMENT_VERSION}"
            )

    return update, changes
