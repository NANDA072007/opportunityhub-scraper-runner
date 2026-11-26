"""
Zero-cost Pipeline Health & Supabase Storage Email Alerter.
Path: backend/pipeline/health_alerter.py
Sends a Gmail SMTP alert if DB size > 375 MB, quarantine > 2,000 rows,
any domain drops below 50 live listings, or maintenance fails.
"""
from __future__ import annotations

import logging
import os
import smtplib
from datetime import datetime, timezone
from email.message import EmailMessage
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

DB_SIZE_WARN_MB = 375.0
QUARANTINE_WARN_ROWS = 2000
MIN_LIVE_PER_CATEGORY = 50
FAILED_NOTIF_WARN_24H = 50


def run_health_check_and_alert(
    supabase_client: Any,
    extra_errors: Optional[List[str]] = None,
    force_send: Optional[bool] = None,
) -> Dict[str, Any]:
    """Queries get_pipeline_health_snapshot() and sends an alert email if needed."""
    alerts: List[str] = list(extra_errors or [])
    snapshot: Dict[str, Any] = {}

    try:
        res = supabase_client.rpc("get_pipeline_health_snapshot").execute()
        snapshot = res.data if isinstance(res.data, dict) else {}
    except Exception as exc:
        alerts.append(f"Health RPC failed: {exc}")

    db_mb = float(snapshot.get("db_size_mb") or 0.0)
    quarantine_rows = int(snapshot.get("quarantine_rows") or 0)
    failed_notifs = int(snapshot.get("failed_notifications_24h") or 0)
    live_counts: Dict[str, int] = snapshot.get("live_counts") or {}

    if db_mb >= DB_SIZE_WARN_MB:
        alerts.append(
            f"Supabase Free-Tier storage high: {db_mb:.1f} MB / 500 MB limit."
        )
    if quarantine_rows >= QUARANTINE_WARN_ROWS:
        alerts.append(
            f"quarantine_queue growing again: {quarantine_rows} rows."
        )
    if failed_notifs >= FAILED_NOTIF_WARN_24H:
        alerts.append(
            f"High FCM failure rate in last 24h: {failed_notifs} failed rows."
        )

    for domain in ("student", "private", "freelance", "government"):
        count = int(live_counts.get(domain, 0))
        if count < MIN_LIVE_PER_CATEGORY:
            alerts.append(
                f"Low live feed inventory in '{domain}': {count} rows (< {MIN_LIVE_PER_CATEGORY})."
            )

    should_force = (
        force_send
        if force_send is not None
        else os.environ.get("FORCE_HEALTH_EMAIL", "false").lower() == "true"
    )

    if alerts or should_force:
        _send_email_report(snapshot=snapshot, alerts=alerts)
    else:
        logger.info(
            "[health] All checks green | db=%.1fMB | quarantine=%d | live=%s",
            db_mb,
            quarantine_rows,
            live_counts,
        )

    return {"snapshot": snapshot, "alerts": alerts}


def _send_email_report(snapshot: Dict[str, Any], alerts: List[str]) -> None:
    smtp_user = os.environ.get("ALERT_SMTP_USER", "").strip()
    smtp_pass = os.environ.get("ALERT_SMTP_APP_PASSWORD", "").strip()
    to_email = os.environ.get("ALERT_TO_EMAIL", smtp_user).strip()

    if not smtp_user or not smtp_pass or not to_email:
        logger.warning(
            "[health] SMTP credentials not configured; alerts=%s", alerts
        )
        return

    status_tag = "ALERT" if alerts else "OK (Manual Check)"
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    db_mb = snapshot.get("db_size_mb", "?")
    quarantine_rows = snapshot.get("quarantine_rows", "?")
    live_counts = snapshot.get("live_counts") or {}
    pending_notifs = snapshot.get("pending_notifications", 0)
    failed_notifs = snapshot.get("failed_notifications_24h", 0)

    lines = [
        f"OpportunityHub Pipeline Health Report ({now_utc})",
        "=" * 54,
        "",
        f"Status: {status_tag}",
        f"Database Size: {db_mb} MB / 500 MB (Free Tier)",
        f"Quarantine Rows: {quarantine_rows}",
        f"Notifications: {pending_notifs} pending | {failed_notifs} failed (24h)",
        "",
        "Live Approved Feed Counts (IN + GLOBAL):",
        f"  - Student:    {live_counts.get('student', 0)}",
        f"  - Private:    {live_counts.get('private', 0)}",
        f"  - Freelance:  {live_counts.get('freelance', 0)}",
        f"  - Government: {live_counts.get('government', 0)}",
        "",
    ]

    if alerts:
        lines.append("Triggered Alerts:")
        for item in alerts:
            lines.append(f"  [!] {item}")
    else:
        lines.append("All thresholds passed with zero issues.")

    msg = EmailMessage()
    msg["Subject"] = f"[OpportunityHub {status_tag}] DB {db_mb}MB | {len(alerts)} alert(s)"
    msg["From"] = smtp_user
    msg["To"] = to_email
    msg.set_content("\n".join(lines))

    try:
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=15) as server:
            server.ehlo()
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
        logger.info("[health] Sent health email (%s) to %s", status_tag, to_email)
    except Exception as exc:
        logger.warning("[health] Failed sending SMTP alert email: %s", exc)
