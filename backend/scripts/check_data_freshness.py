"""
Data Freshness & Coverage Probe
===============================
READ-ONLY diagnostic over the production ``opportunities`` table.

The silent failure this exists to catch: every scraper stopped writing for 18
days and nothing noticed, because nothing watches the DATA (only the app).

Default gate (always enforced):
- every monitored source must have a successful crawl recorded within
  ``FRESHNESS_MAX_AGE`` (36h) in ``company_sources.last_success_at``.
  Row ``updated_at`` is deliberately not used: a backfill or cleanup write
  would make a dead crawler look healthy.

``--strict`` additionally enforces the relevance targets that are still gaps:
- geo coverage >= 95% of feed-visible rows
- pay coverage >= 50% of feed-visible rows
- zero duplicate (title, company, location) groups

Usage:
    python -m backend.scripts.check_data_freshness
    python -m backend.scripts.check_data_freshness --strict
    python -m backend.scripts.check_data_freshness --json

Exit codes: 0 = pass, 1 = failures, 2 = configuration error.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from dotenv import load_dotenv
from supabase import create_client

sys.path.insert(
    0, str(Path(__file__).resolve().parents[2])
)

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

PAGE_SIZE = 1000
FRESHNESS_MAX_AGE = timedelta(hours=36)
GEO_MIN_COVERAGE = 0.95
PAY_MIN_COVERAGE = 0.50

# Tiny dormant/legacy sources (1-3 rows) are reported but never fail the gate:
# their absence of updates is not a crawler outage, and treating them as such
# would train everyone to ignore the probe.
MIN_ROWS_FOR_FRESHNESS = 20

# ``unknown`` is the bucket for rows with no source_slug (a data-hygiene bug),
# not a crawler that can go quiet. Report it, never alarm on it.
NON_MONITORED_SOURCES = {"unknown"}

_OFFICIAL_GOVT_URL = re.compile(
    r"^https?://([a-zA-Z0-9\-]+\.)*(gov\.in|nic\.in|ac\.in|pmrf\.in)"
    r"(:[0-9]+)?(/|$|\?|#)",
    re.IGNORECASE,
)


def _parse_dt(value: Any) -> Optional[datetime]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _has_pay(row: Mapping[str, Any]) -> bool:
    salary = row.get("salary") or 0
    monthly = row.get("normalized_monthly_stipend") or 0
    try:
        return float(salary) > 0 or float(monthly) > 0
    except (TypeError, ValueError):
        return False


def _has_geo(row: Mapping[str, Any]) -> bool:
    return bool(row.get("geo_eligibility") or [])


def is_feed_visible(row: Mapping[str, Any], now: datetime) -> bool:
    """Mirror of the feed RPC eligibility gates, for diagnostics only.

    Kept deliberately close to ``get_personalized_feed``/``get_composed_feed``
    so coverage numbers describe what users actually see. The authoritative
    implementation remains the SQL RPC.
    """
    if row.get("status") != "active" or row.get("is_active") is not True:
        return False
    if row.get("review_status") not in (None, "approved"):
        return False

    expired_at = _parse_dt(row.get("expired_at"))
    if expired_at is not None and expired_at < now:
        return False

    fraud = row.get("fraud_score")
    if fraud is not None:
        try:
            if float(fraud) >= 0.8:
                return False
        except (TypeError, ValueError):
            pass

    geo = row.get("geo_eligibility") or []
    if geo and not ({"IN", "GLOBAL"} & {str(code).upper() for code in geo}):
        return False

    deadline = _parse_dt(row.get("deadline"))
    if deadline is not None:
        return deadline >= now

    posted = _parse_dt(row.get("posted_at")) or _parse_dt(row.get("created_at"))
    if posted is None:
        return False

    if (
        row.get("category") == "government"
        and _OFFICIAL_GOVT_URL.match(str(row.get("apply_url") or ""))
        and now - posted <= timedelta(days=90)
    ):
        return True
    return now - posted <= timedelta(days=30)


def summarize(
    rows: Iterable[Mapping[str, Any]],
    *,
    health: Optional[Mapping[str, Mapping[str, Any]]] = None,
    now: Optional[datetime] = None,
    freshness_max_age: timedelta = FRESHNESS_MAX_AGE,
) -> Dict[str, Any]:
    """Pure aggregation used by the CLI and the unit tests.

    ``health`` is the ``company_sources`` map (source_slug -> row). Freshness
    comes from ``last_success_at`` only.
    """
    now = now or datetime.now(timezone.utc)
    rows = list(rows)
    health = health or {}

    by_source: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        by_source[str(row.get("source_slug") or "unknown")].append(row)

    failures: List[str] = []
    sources: List[Dict[str, Any]] = []

    for source, items in sorted(by_source.items()):
        health_row = health.get(source) or {}
        last_success = _parse_dt(health_row.get("last_success_at"))
        age_hours = (
            round((now - last_success).total_seconds() / 3600.0, 1)
            if last_success is not None
            else None
        )
        stale = last_success is None or (now - last_success) > freshness_max_age
        health_active = health_row.get("is_active")
        active_items = [
            item
            for item in items
            if item.get("status") == "active" and item.get("is_active") is True
        ]
        monitored = (
            len(items) >= MIN_ROWS_FOR_FRESHNESS
            and source not in NON_MONITORED_SOURCES
            and health_active is not False
            # A disabled/retired source with no active inventory cannot go
            # stale in a way users can see; its historical rows are ignored.
            and len(active_items) > 0
        )
        if stale and monitored:
            if not health_row:
                reason = "no company_sources health row"
            elif last_success is None:
                reason = "no successful crawl recorded"
            else:
                reason = (
                    f"{age_hours}h since last successful crawl (limit "
                    f"{int(freshness_max_age.total_seconds() // 3600)}h)"
                )
            failures.append(f"{source}: {reason}")

        visible = [item for item in items if is_feed_visible(item, now)]
        latest_updated = max(
            (
                dt
                for dt in (_parse_dt(item.get("updated_at")) for item in items)
                if dt
            ),
            default=None,
        )
        sources.append(
            {
                "source": source,
                "rows": len(items),
                "visible_rows": len(visible),
                "last_success_at": (
                    last_success.isoformat() if last_success else None
                ),
                "latest_updated_at": (
                    latest_updated.isoformat() if latest_updated else None
                ),
                "health_status": health_row.get("status") or "missing",
                "age_hours": age_hours,
                "stale": stale,
                "monitored": monitored,
            }
        )

    visible_rows = [row for row in rows if is_feed_visible(row, now)]
    pay_covered = sum(1 for row in visible_rows if _has_pay(row))
    geo_covered = sum(1 for row in visible_rows if _has_geo(row))

    groups = Counter(
        (
            _normalize_text(row.get("title")),
            _normalize_text(row.get("company")),
            _normalize_text(row.get("location")),
        )
        for row in visible_rows
    )
    duplicate_groups = sum(
        1 for key, count in groups.items() if key[0] and count > 1
    )
    duplicate_extra_rows = sum(
        count - 1 for key, count in groups.items() if key[0] and count > 1
    )

    visible_count = len(visible_rows)
    pay_coverage = round(pay_covered / visible_count, 4) if visible_count else 0.0
    geo_coverage = round(geo_covered / visible_count, 4) if visible_count else 0.0

    strict_failures: List[str] = []
    if geo_coverage < GEO_MIN_COVERAGE:
        strict_failures.append(
            f"geo coverage {geo_coverage:.1%} < {GEO_MIN_COVERAGE:.0%}"
        )
    if pay_coverage < PAY_MIN_COVERAGE:
        strict_failures.append(
            f"pay coverage {pay_coverage:.1%} < {PAY_MIN_COVERAGE:.0%}"
        )
    if duplicate_extra_rows > 0:
        strict_failures.append(
            f"{duplicate_extra_rows} duplicate visible rows "
            f"across {duplicate_groups} same title/company/location groups"
        )

    return {
        "generated_at": now.isoformat(),
        "thresholds": {
            "freshness_hours": int(freshness_max_age.total_seconds() // 3600),
            "geo_min_coverage": GEO_MIN_COVERAGE,
            "pay_min_coverage": PAY_MIN_COVERAGE,
        },
        "total_rows": len(rows),
        "visible_rows": visible_count,
        "pay_covered_rows": pay_covered,
        "pay_coverage": pay_coverage,
        "geo_covered_rows": geo_covered,
        "geo_coverage": geo_coverage,
        "duplicate_groups": duplicate_groups,
        "duplicate_extra_rows": duplicate_extra_rows,
        "sources": sources,
        "failures": failures,
        "strict_failures": strict_failures,
        "passed": not failures,
        "strict_passed": not failures and not strict_failures,
    }


def fetch_source_health(client: Any) -> Dict[str, Dict[str, Any]]:
    rows = (
        client.table("company_sources")
        .select("source_slug,is_active,status,last_scraped_at,last_success_at")
        .limit(1000)
        .execute()
        .data
        or []
    )
    return {
        str(row.get("source_slug")): row
        for row in rows
        if row.get("source_slug")
    }


def fetch_rows(client: Any) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    offset = 0
    while True:
        batch = (
            client.table("opportunities")
            .select(
                "id,source_slug,status,is_active,review_status,expired_at,"
                "deadline,posted_at,created_at,fraud_score,geo_eligibility,"
                "category,apply_url,salary,normalized_monthly_stipend,"
                "title,company,location,updated_at"
            )
            .order("id")
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
            .data
            or []
        )
        rows.extend(batch)
        offset += PAGE_SIZE
        if len(batch) < PAGE_SIZE:
            break
    return rows


def _print_report(report: Mapping[str, Any], strict: bool) -> None:
    print("\n" + "=" * 82)
    print(" OPPORTUNITYHUB — DATA FRESHNESS & COVERAGE")
    print("=" * 82)
    print(f"  Generated: {report['generated_at']}")
    print(
        f"  Rows: {report['total_rows']} | visible: {report['visible_rows']} "
        f"| pay: {report['pay_covered_rows']} ({report['pay_coverage']:.1%}) "
        f"| geo: {report['geo_covered_rows']} ({report['geo_coverage']:.1%}) "
        f"| duplicate extra rows: {report['duplicate_extra_rows']}"
    )
    print("-" * 82)
    print(f"  {'SOURCE':<20}{'ROWS':>6}{'VISIBLE':>9}  {'LAST CRAWL':<26}{'AGE(H)':>7}  STATUS")
    for source in report["sources"]:
        latest = source["last_success_at"] or "never"
        age = "?" if source["age_hours"] is None else f"{source['age_hours']:.1f}"
        if source["stale"] and source["monitored"]:
            status = "STALE"
        elif source["stale"]:
            status = "stale(unmonitored)"
        else:
            status = "ok"
        print(
            f"  {source['source']:<20}{source['rows']:>6}{source['visible_rows']:>9}  "
            f"{latest:<26}{age:>7}  {status}"
        )
    print("-" * 82)
    for failure in report["failures"]:
        print(f"  [FAIL] {failure}")
    if strict:
        for failure in report["strict_failures"]:
            print(f"  [FAIL:strict] {failure}")
    elif report["strict_failures"]:
        for failure in report["strict_failures"]:
            print(f"  [gap] {failure}")
    print("=" * 82 + "\n")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Read-only freshness/coverage probe for the opportunities table."
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON only.")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Also fail on geo/pay coverage and duplicate groups.",
    )
    args = parser.parse_args(argv)

    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ.get(
        "SUPABASE_SERVICE_ROLE_KEY"
    )
    if not url or not key:
        print(
            "ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY (or "
            "SUPABASE_SERVICE_ROLE_KEY) must be set in backend/.env",
            file=sys.stderr,
        )
        return 2

    try:
        client = create_client(url, key)
        rows = fetch_rows(client)
        health = fetch_source_health(client)
    except Exception as exc:  # noqa: BLE001 - CLI boundary
        print(f"ERROR: probe failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2

    report = summarize(rows, health=health)

    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        _print_report(report, strict=args.strict)

    if not report["passed"]:
        return 1
    if args.strict and not report["strict_passed"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
