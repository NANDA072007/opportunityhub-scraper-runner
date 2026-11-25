"""
Notification Delivery Queue Worker for OpportunityHub.
Path: `backend/pipeline/notification_worker.py`

Drains `public.notification_delivery_queue` (0043), resolves active FCM tokens
from `public.user_device_tokens` (0034), dispatches push alerts via Firebase
Cloud Messaging (HTTP v1 API), and records outcomes + `notification_analytics`
in bulk via `finalize_notification_deliveries`.
"""

from __future__ import annotations

import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

# Optional Firebase Admin SDK import (graceful degradation if unconfigured in local dev)
try:
    import firebase_admin
    from firebase_admin import credentials, messaging

    HAS_FIREBASE_ADMIN = True
except ImportError:
    firebase_admin = None  # type: ignore[assignment]
    credentials = None  # type: ignore[assignment]
    messaging = None  # type: ignore[assignment]
    HAS_FIREBASE_ADMIN = False

MAX_RETRY_ATTEMPTS = 3
FCM_BATCH_LIMIT = 500


class NotificationQueueWorker:
    """Batch worker that drains `notification_delivery_queue` and sends FCM pushes."""

    def __init__(
        self,
        supabase_client: Any,
        batch_size: int = 200,
        max_attempts: int = MAX_RETRY_ATTEMPTS,
        dry_run: bool = False,
    ) -> None:
        self.supabase = supabase_client
        self.batch_size = min(max(batch_size, 1), FCM_BATCH_LIMIT)
        self.max_attempts = max_attempts
        self.dry_run = dry_run
        self._fcm_ready = self._ensure_firebase_initialized()

    # -------------------------------------------------------------------------
    # Firebase Initialization
    # -------------------------------------------------------------------------
    @staticmethod
    def _ensure_firebase_initialized() -> bool:
        """Initializes Firebase Admin SDK once from env path or inline JSON."""
        if not HAS_FIREBASE_ADMIN:
            logger.warning(
                "[notifications] `firebase-admin` package not installed. "
                "Install via `pip install firebase-admin` to enable live FCM delivery."
            )
            return False

        if getattr(firebase_admin, "_apps", None):
            return True

        cred_path = os.environ.get("FIREBASE_SERVICE_ACCOUNT_PATH", "").strip()
        cred_json = os.environ.get("FIREBASE_CREDENTIALS_JSON", "").strip()

        try:
            if cred_path and os.path.isfile(cred_path):
                cred = credentials.Certificate(cred_path)
                firebase_admin.initialize_app(cred)
                return True
            if cred_json:
                cred_dict = json.loads(cred_json)
                cred = credentials.Certificate(cred_dict)
                firebase_admin.initialize_app(cred)
                return True

            logger.warning(
                "[notifications] Neither FIREBASE_SERVICE_ACCOUNT_PATH nor "
                "FIREBASE_CREDENTIALS_JSON is set; skipping live FCM dispatch."
            )
            return False
        except Exception as exc:
            logger.error(f"[notifications] Failed to initialize Firebase Admin SDK: {exc}")
            return False

    # -------------------------------------------------------------------------
    # Main Drain Entrypoint
    # -------------------------------------------------------------------------
    def drain_queue(self, max_batches: int = 5) -> Dict[str, int]:
        """Drains pending notifications in batches of `self.batch_size`."""
        stats = {
            "processed": 0,
            "sent": 0,
            "failed": 0,
            "retrying": 0,
            "dead_tokens_disabled": 0,
        }

        if not self._fcm_ready and not self.dry_run:
            return stats

        for _ in range(max_batches):
            queue_rows = self._fetch_pending_batch()
            if not queue_rows:
                break

            user_ids = list({str(r["user_id"]) for r in queue_rows if r.get("user_id")})
            tokens_by_user = self._fetch_active_tokens_by_user(user_ids)

            outcomes, dead_tokens = self._dispatch_batch(queue_rows, tokens_by_user)

            for item in outcomes:
                stats["processed"] += 1
                st = item["status"]
                if st == "sent":
                    stats["sent"] += 1
                elif st == "failed":
                    stats["failed"] += 1
                else:
                    stats["retrying"] += 1

            stats["dead_tokens_disabled"] += len(dead_tokens)
            self._persist_outcomes(outcomes, list(dead_tokens))

            if len(queue_rows) < self.batch_size:
                break

        if stats["processed"] > 0:
            logger.info(f"[notifications] Queue drain summary: {stats}")
        return stats

    # -------------------------------------------------------------------------
    # Step 1: Fetch Pending Notifications & Active Device Tokens (2 Queries)
    # -------------------------------------------------------------------------
    def _fetch_pending_batch(self) -> List[Dict[str, Any]]:
        try:
            res = (
                self.supabase.table("notification_delivery_queue")
                .select("id,user_id,opportunity_id,type,payload,status,attempts,created_at")
                .eq("status", "pending")
                .lt("attempts", self.max_attempts)
                .order("created_at", desc=False)
                .limit(self.batch_size)
                .execute()
            )
            return list(res.data or [])
        except Exception as exc:
            logger.error(f"[notifications] Failed fetching pending queue batch: {exc}")
            return []

    def _fetch_active_tokens_by_user(self, user_ids: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        """Fetches all enabled FCM tokens for the batch of users in a single IN query."""
        grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        if not user_ids:
            return grouped

        try:
            res = (
                self.supabase.table("user_device_tokens")
                .select("user_id,token,platform")
                .in_("user_id", user_ids)
                .eq("notification_enabled", True)
                .execute()
            )
            for row in res.data or []:
                uid = str(row.get("user_id") or "")
                token = str(row.get("token") or "").strip()
                if uid and token:
                    grouped[uid].append(row)
        except Exception as exc:
            logger.error(f"[notifications] Failed fetching user_device_tokens: {exc}")

        return grouped

    # -------------------------------------------------------------------------
    # Step 2: Build & Dispatch FCM Messages in Bulk (`messaging.send_each`)
    # -------------------------------------------------------------------------
    def _dispatch_batch(
        self,
        queue_rows: List[Dict[str, Any]],
        tokens_by_user: Dict[str, List[Dict[str, Any]]],
    ) -> Tuple[List[Dict[str, Any]], Set[str]]:
        outcomes: List[Dict[str, Any]] = []
        dead_tokens: Set[str] = set()

        # Map each outbound FCM Message back to its parent queue row ID and token
        fcm_messages: List[Any] = []
        message_meta: List[Tuple[str, str]] = []  # (queue_id, token)
        rows_by_id: Dict[str, Dict[str, Any]] = {}

        for row in queue_rows:
            qid = str(row["id"])
            uid = str(row["user_id"])
            attempts = int(row.get("attempts") or 0) + 1
            user_tokens = tokens_by_user.get(uid, [])

            if not user_tokens:
                # User has no active device tokens registered; mark failed to prevent infinite retries
                outcomes.append(
                    {
                        "id": qid,
                        "status": "failed",
                        "attempts": attempts,
                        "error_message": "No active FCM device tokens for user",
                        "tokens_reached": 0,
                    }
                )
                continue

            rows_by_id[qid] = {**row, "next_attempts": attempts}
            title, body, data_map = self._format_notification_content(row)

            for tk_row in user_tokens:
                token_str = str(tk_row["token"]).strip()
                if not self.dry_run and HAS_FIREBASE_ADMIN:
                    msg = self._build_fcm_message(
                        token=token_str,
                        title=title,
                        body=body,
                        data_map=data_map,
                    )
                    fcm_messages.append(msg)
                message_meta.append((qid, token_str))

        if not message_meta:
            return outcomes, dead_tokens

        # Dry-run path for local testing
        if self.dry_run:
            reached_per_qid: Dict[str, int] = defaultdict(int)
            for qid, _ in message_meta:
                reached_per_qid[qid] += 1
            for qid, row_info in rows_by_id.items():
                outcomes.append(
                    {
                        "id": qid,
                        "status": "sent",
                        "attempts": row_info["next_attempts"],
                        "error_message": None,
                        "tokens_reached": reached_per_qid[qid],
                    }
                )
            return outcomes, dead_tokens

        # Live FCM batch send in chunks of 500 (FCM HTTP v1 limit per send_each call)
        success_counts: Dict[str, int] = defaultdict(int)
        last_errors: Dict[str, str] = {}

        for start in range(0, len(fcm_messages), FCM_BATCH_LIMIT):
            chunk_msgs = fcm_messages[start : start + FCM_BATCH_LIMIT]
            chunk_meta = message_meta[start : start + FCM_BATCH_LIMIT]

            try:
                batch_response = messaging.send_each(chunk_msgs)
                for idx, send_resp in enumerate(batch_response.responses):
                    qid, token_str = chunk_meta[idx]
                    if send_resp.success:
                        success_counts[qid] += 1
                    else:
                        err = send_resp.exception
                        err_code = getattr(err, "code", "") or str(err)
                        last_errors[qid] = f"{err_code}: {err}"
                        if self._is_unregistered_token_error(err):
                            dead_tokens.add(token_str)
            except Exception as exc:
                logger.error(f"[notifications] FCM send_each transport failure: {exc}")
                for qid, _ in chunk_meta:
                    last_errors[qid] = f"TransportError: {exc}"

        # Aggregate per-notification outcome (if >= 1 user device succeeded -> 'sent')
        for qid, row_info in rows_by_id.items():
            attempts = row_info["next_attempts"]
            reached = success_counts.get(qid, 0)
            if reached > 0:
                outcomes.append(
                    {
                        "id": qid,
                        "status": "sent",
                        "attempts": attempts,
                        "error_message": None,
                        "tokens_reached": reached,
                    }
                )
            else:
                is_terminal = attempts >= self.max_attempts
                outcomes.append(
                    {
                        "id": qid,
                        "status": "failed" if is_terminal else "pending",
                        "attempts": attempts,
                        "error_message": (last_errors.get(qid) or "Unknown FCM error")[:500],
                        "tokens_reached": 0,
                    }
                )

        return outcomes, dead_tokens

    # -------------------------------------------------------------------------
    # Step 3: Format Notification Copy & Deep-Link Payload by Type
    # -------------------------------------------------------------------------
    @staticmethod
    def _format_notification_content(
        row: Dict[str, Any],
    ) -> Tuple[str, str, Dict[str, str]]:
        notif_type = str(row.get("type") or "new_match")
        payload = row.get("payload") or {}
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except ValueError:
                payload = {}

        opp_id = str(row.get("opportunity_id") or payload.get("opportunity_id") or "")
        company = str(payload.get("company") or "OpportunityHub").strip()
        raw_title = str(payload.get("title") or "New Opportunity").strip()
        apply_url = str(payload.get("apply_url") or "").strip()
        category = str(payload.get("category") or "student").strip()

        if notif_type == "deadline_reminder":
            hours_left = payload.get("hours_remaining") or 48
            checklist = payload.get("pre_flight_checklist") or payload.get("pre_flight") or []
            title = raw_title if raw_title.startswith("Closing") else f"Closing in <{hours_left}h: {raw_title}"
            if isinstance(checklist, list) and checklist:
                body = f"{company} • Pre-flight tip: {checklist[0]}"
            else:
                body = f"Your bookmarked role at {company} closes in {hours_left} hours. Tap to complete your application."
        elif notif_type == "daily_digest":
            count = payload.get("count") or "new"
            title = str(payload.get("title") or f"{count} High-Match Opportunities Today")
            body = str(
                payload.get("body")
                or "Fresh internships, hackathons, and verified roles matching your skills are live."
            )
        else:  # 'new_match'
            title = f"New Match: {raw_title}"
            body = f"{company} just posted a verified {category} role matching your profile."

        # FCM data map requires all keys and values to be strings
        data_map: Dict[str, str] = {
            "notification_id": str(row.get("id") or ""),
            "type": notif_type,
            "opportunity_id": opp_id,
            "category": category,
            "apply_url": apply_url,
            "click_action": "FLUTTER_NOTIFICATION_CLICK",
        }
        return title, body, data_map

    @staticmethod
    def _build_fcm_message(
        token: str,
        title: str,
        body: str,
        data_map: Dict[str, str],
    ) -> Any:
        return messaging.Message(
            token=token,
            notification=messaging.Notification(title=title, body=body),
            data=data_map,
            android=messaging.AndroidConfig(
                priority="high",
                notification=messaging.AndroidNotification(
                    channel_id="opportunity_alerts_high",
                    click_action="FLUTTER_NOTIFICATION_CLICK",
                ),
            ),
            apns=messaging.APNSConfig(
                payload=messaging.APNSPayload(
                    aps=messaging.Aps(sound="default", content_available=True)
                )
            ),
        )

    @staticmethod
    def _is_unregistered_token_error(err: Any) -> bool:
        code = (getattr(err, "code", "") or "").upper()
        msg = str(err or "").lower()
        return (
            "UNREGISTERED" in code
            or "NOT_FOUND" in code
            or "registration-token-not-registered" in msg
            or "invalid-registration-token" in msg
        )

    # -------------------------------------------------------------------------
    # Step 4: Bulk Persist via RPC (with PostgREST Fallback)
    # -------------------------------------------------------------------------
    def _persist_outcomes(
        self,
        outcomes: List[Dict[str, Any]],
        dead_tokens: List[str],
    ) -> None:
        if not outcomes and not dead_tokens:
            return

        try:
            self.supabase.rpc(
                "finalize_notification_deliveries",
                {
                    "p_outcomes": outcomes,
                    "p_dead_tokens": dead_tokens,
                },
            ).execute()
            return
        except Exception as rpc_exc:
            logger.warning(
                f"[notifications] RPC `finalize_notification_deliveries` unavailable, "
                f"using fallback table updates: {rpc_exc}"
            )

        # Fallback if SQL RPC hasn't been pushed yet
        now_iso = datetime.now(timezone.utc).isoformat()
        for item in outcomes:
            update_data: Dict[str, Any] = {
                "status": item["status"],
                "attempts": item["attempts"],
                "error_message": item["error_message"],
            }
            if item["status"] == "sent":
                update_data["sent_at"] = now_iso
            try:
                (
                    self.supabase.table("notification_delivery_queue")
                    .update(update_data)
                    .eq("id", item["id"])
                    .execute()
                )
            except Exception as exc:
                logger.error(f"[notifications] Fallback queue update failed for {item['id']}: {exc}")

        if dead_tokens:
            try:
                (
                    self.supabase.table("user_device_tokens")
                    .update({"notification_enabled": False, "updated_at": now_iso})
                    .in_("token", dead_tokens)
                    .execute()
                )
            except Exception as exc:
                logger.error(f"[notifications] Fallback dead token disable failed: {exc}")


def drain_notification_delivery_queue(
    supabase_client: Any,
    batch_size: int = 200,
    dry_run: bool = False,
) -> Dict[str, int]:
    """Convenience entrypoint for PipelineOrchestrator and CLI scripts."""
    worker = NotificationQueueWorker(
        supabase_client=supabase_client,
        batch_size=batch_size,
        dry_run=dry_run,
    )
    return worker.drain_queue()
