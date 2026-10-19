"""Synthetic fixture migration utility for Efso Radar ledgers.

Losslessly and idempotently imports legacy/offline JSON ledgers (telegram_digest_delivery_ledger.json
and experiment_budget_ledger.json) into repository tables (telegram_delivery_records
and experiment_budget_history) with strict two-phase validation, historical role/category preservation,
timestamp immutability (zero synthetic utc_now() invention, zero day-1 timestamp invention),
alias deduplication, complete conflict detection, raw chat ID masking, strict INSERT-only application,
and total transactional rollback on collision.

NOTE:
- Live non-SQLite migration (production Turso) is NOT IMPLEMENTED and requires separately
  approved transactional integration.
- In Phase 5B.3B, live migration is strictly confined to atomic transactions on SQLite.
- Read-only preflight dry-run is safely supported across repository contracts.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from pydantic import BaseModel, Field

from efso_radar.domain import (
    BudgetHistoryConflictError,
    DeliveryIdentityConflictError,
    DeliveryStatus,
    DeliveryStatusRegressionError,
    TelegramDeliveryRecord,
    TelegramNotificationCategory,
    mask_chat_id,
)
from efso_radar.repositories.base import StateRepository
from efso_radar.repositories.sqlite import SqliteStateRepository

logger = logging.getLogger("efso-radar.repository.migration_ledger")


def mask_text_chat_ids(text: str) -> str:
    """Mask any potential numeric chat IDs (e.g. 5+ digits or -100... channel IDs) found in text."""
    if not text:
        return ""
    return re.sub(r"-?\b\d{5,}\b", lambda m: mask_chat_id(m.group(0)), str(text))


def mask_entry_reference(entry_key: str, chat_id: Optional[str] = None) -> str:
    """Sanitize entry keys or references by masking any raw recipient chat IDs."""
    key_str = str(entry_key).strip()
    if chat_id:
        key_str = key_str.replace(chat_id, mask_chat_id(chat_id))
    return mask_text_chat_ids(key_str)


class MigrationLedgerResult(BaseModel):
    """Summary of ledger migration execution."""
    dry_run: bool = False
    deliveries_imported: int = 0
    deliveries_skipped: int = 0
    budget_records_imported: int = 0
    budget_records_skipped: int = 0
    conflicts_detected: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)

    @property
    def success(self) -> bool:
        return len(self.errors) == 0 and len(self.conflicts_detected) == 0

    def add_error(self, message: str) -> None:
        self.errors.append(mask_text_chat_ids(str(message)))

    def add_conflict(self, message: str) -> None:
        self.conflicts_detected.append(mask_text_chat_ids(str(message)))

    def summary(self) -> str:
        mode = "DRY-RUN (PRE-FLIGHT VALIDATION)" if self.dry_run else "LIVE MIGRATION"
        lines = [
            f"=== LEDGER MIGRATION REPORT [{mode}] ===",
            f"Status: {'SUCCESS' if self.success else 'FAILED'}",
            f"Deliveries: {self.deliveries_imported} imported, {self.deliveries_skipped} skipped",
            f"Budget records: {self.budget_records_imported} imported, {self.budget_records_skipped} skipped",
            f"Conflicts: {len(self.conflicts_detected)}",
            f"Errors: {len(self.errors)}",
        ]
        if self.conflicts_detected:
            lines.append("Conflicts:")
            for c in self.conflicts_detected:
                lines.append(f"  - {mask_text_chat_ids(c)}")
        if self.errors:
            lines.append("Errors:")
            for e in self.errors:
                lines.append(f"  - {mask_text_chat_ids(e)}")
        return "\n".join(lines)


def migrate_ledgers_to_repository(
    repo: StateRepository,
    delivery_ledger_path: Optional[Path] = None,
    budget_ledger_path: Optional[Path] = None,
    dry_run: bool = False,
) -> MigrationLedgerResult:
    """Losslessly migrate JSON ledgers into the repository database using strict two-phase validation.

    Phase 1: Pure in-memory pre-flight validation.
    - Validates file existence, JSON syntax, required fields, and historical statuses.
    - Preserves HELD_QUIET_HOURS as HELD_QUIET_HOURS, not FAILED.
    - Requires authoritative recipient_role and category (never assumes PRIMARY_EDITOR or EDITORIAL_DIGEST).
    - Requires authoritative timestamps (never invents utc_now() or month-start 01T00:00:00).
    - Performs full-field duplicate and conflict detection (status, role, category, timestamp, error, payload).
    - Masks raw chat IDs in all logs, reports, and error messages.
    - If ANY error or conflict is detected, or if dry_run=True, aborts with 0 database writes.

    Phase 2: Transactional application.
    - Live migration for non-SQLite repositories (such as production Turso) is NOT IMPLEMENTED
      and fails closed with 0 writes.
    - On SQLite, writes are applied inside a single atomic transaction.
    - Uses strict INSERT-only behavior (no ON CONFLICT DO UPDATE).
    - If any identity collision occurs after preflight, the entire transaction is rolled back.
    """
    result = MigrationLedgerResult(dry_run=dry_run)

    deliveries_to_import: list[TelegramDeliveryRecord] = []
    deliveries_to_skip_count = 0
    budget_to_import: list[dict[str, Any]] = []
    budget_to_skip_count = 0

    # ---------------------------------------------------------
    # PHASE 1: PRE-FLIGHT VALIDATION (STRICTLY IN-MEMORY & READ-ONLY)
    # ---------------------------------------------------------

    # 1. Delivery Ledger Pre-flight
    if delivery_ledger_path is not None:
        if not delivery_ledger_path.exists():
            result.add_error(f"Delivery ledger path does not exist: {delivery_ledger_path}")
        else:
            try:
                raw_text = delivery_ledger_path.read_text(encoding="utf-8")
                data = json.loads(raw_text)
                deliveries = data.get("deliveries", {}) if isinstance(data, dict) else {}

                # Track entries seen within this file to deduplicate aliases and catch intra-file conflicts
                seen_deliveries: dict[tuple[str, str], TelegramDeliveryRecord] = {}

                for entry_key, entry_val in deliveries.items():
                    if not isinstance(entry_val, dict):
                        result.add_error(
                            f"Malformed delivery ledger entry is not a dict: {mask_entry_reference(entry_key)}"
                        )
                        continue

                    # Identity extraction
                    chat_id = str(entry_val.get("chat_id", "")).strip()
                    digest_hash = str(entry_val.get("digest_hash", "") or entry_val.get("message_key", "")).strip()
                    if not chat_id or not digest_hash:
                        if ":" in entry_key:
                            parts = entry_key.split(":", 1)
                            chat_id = chat_id or parts[0].strip()
                            digest_hash = digest_hash or parts[1].strip()

                    masked_chat = mask_chat_id(chat_id) if chat_id else "unknown_chat"
                    masked_entry_ref = mask_entry_reference(entry_key, chat_id)
                    if not chat_id or not digest_hash:
                        result.add_error(
                            f"Malformed delivery ledger entry missing chat_id or digest_hash: {masked_entry_ref}"
                        )
                        continue

                    # Timestamp validation (strict - NO invented utc_now())
                    delivered_at_str = (
                        entry_val.get("delivered_at_utc")
                        or entry_val.get("sent_at")
                        or entry_val.get("created_at")
                        or entry_val.get("timestamp")
                    )
                    if not delivered_at_str:
                        result.add_error(
                            f"Missing delivery timestamp for entry '{masked_entry_ref}' ({masked_chat}:{digest_hash}). "
                            "Historical timestamps must not be invented with utc_now()."
                        )
                        continue

                    try:
                        delivered_at = datetime.fromisoformat(str(delivered_at_str).replace("Z", "+00:00"))
                        if delivered_at.tzinfo is None:
                            delivered_at = delivered_at.replace(tzinfo=timezone.utc)
                    except Exception as ts_exc:
                        result.add_error(
                            f"Invalid ISO timestamp '{delivered_at_str}' for entry '{masked_entry_ref}' ({masked_chat}): {ts_exc}"
                        )
                        continue

                    # Historical status mapping
                    raw_status = str(entry_val.get("status", "")).strip().upper()
                    if raw_status in ("DELIVERY_SUCCESS", "SENT", "SUCCESS"):
                        status = DeliveryStatus.SENT
                    elif raw_status == "DELIVERY_UNCERTAIN":
                        status = DeliveryStatus.DELIVERY_UNCERTAIN
                    elif raw_status == "FAILED":
                        status = DeliveryStatus.FAILED
                    elif raw_status == "HELD_QUIET_HOURS":
                        status = DeliveryStatus.HELD_QUIET_HOURS
                    elif raw_status == "PENDING":
                        status = DeliveryStatus.PENDING
                    else:
                        result.add_error(
                            f"Unknown historical delivery status '{raw_status}' for entry '{masked_entry_ref}' ({masked_chat}). "
                            "Unknown statuses cannot be silently converted."
                        )
                        continue

                    # Role and Category preservation (Never assume PRIMARY_EDITOR or EDITORIAL_DIGEST)
                    raw_role = entry_val.get("recipient_role") or entry_val.get("role")
                    if not raw_role or not str(raw_role).strip():
                        result.add_error(
                            f"Missing required recipient role for entry '{masked_entry_ref}' ({masked_chat}:{digest_hash}). "
                            "Recipient roles must not be assumed as PRIMARY_EDITOR without evidence."
                        )
                        continue
                    role = str(raw_role).strip()

                    category_val = entry_val.get("category") or entry_val.get("notification_category")
                    if not category_val or not str(category_val).strip():
                        result.add_error(
                            f"Missing required notification category for entry '{masked_entry_ref}' ({masked_chat}:{digest_hash}). "
                            "Categories must not be assumed as EDITORIAL_DIGEST without evidence."
                        )
                        continue

                    cat_str = str(category_val).strip().upper()
                    if cat_str in ("ADMIN_HEALTH_REPORT", "ADMIN_REPORT", "OPERATIONAL_HEALTH_REPORT"):
                        category = TelegramNotificationCategory.OPERATIONAL_HEALTH_REPORT
                    elif cat_str in ("EDITORIAL_DIGEST", "DIGEST"):
                        category = TelegramNotificationCategory.EDITORIAL_DIGEST
                    else:
                        try:
                            category = TelegramNotificationCategory(str(category_val))
                        except ValueError:
                            try:
                                category = TelegramNotificationCategory[cat_str]
                            except KeyError:
                                result.add_error(
                                    f"Unknown notification category '{category_val}' for entry '{masked_entry_ref}' ({masked_chat})"
                                )
                                continue

                    err_msg = entry_val.get("error_message")
                    if raw_status == "HELD_QUIET_HOURS" and not err_msg:
                        err_msg = "HELD_QUIET_HOURS"

                    delivery_id = f"del_{digest_hash[:16]}_{chat_id}"
                    record = TelegramDeliveryRecord(
                        delivery_id=delivery_id,
                        message_key=digest_hash,
                        recipient_role=role,
                        recipient_chat_id=chat_id,
                        category=category,
                        status=status,
                        attempt_count=int(entry_val.get("attempt_count") or 1),
                        max_attempts=int(entry_val.get("max_attempts") or 3),
                        error_message=err_msg,
                        payload=entry_val,
                        created_at=delivered_at,
                        updated_at=delivered_at,
                        sent_at=delivered_at if status == DeliveryStatus.SENT else None,
                    )

                    # In-file duplicate & alias deduplication (comparing status, role, category, timestamp, error, payload)
                    del_ident = (chat_id, digest_hash)
                    if del_ident in seen_deliveries:
                        prev_rec = seen_deliveries[del_ident]
                        if (
                            prev_rec.status == record.status
                            and prev_rec.recipient_role == record.recipient_role
                            and prev_rec.category == record.category
                            and prev_rec.created_at.isoformat() == record.created_at.isoformat()
                            and prev_rec.error_message == record.error_message
                            and prev_rec.payload == record.payload
                        ):
                            # Identical duplicate / alias in file -> skip safely
                            deliveries_to_skip_count += 1
                            continue
                        else:
                            result.add_conflict(
                                f"Conflicting in-file duplicate entry for {masked_chat}:{digest_hash} "
                                f"(status={prev_rec.status.value}, role={prev_rec.recipient_role}, cat={prev_rec.category.value}, "
                                f"ts={prev_rec.created_at.isoformat()} vs status={record.status.value}, "
                                f"role={record.recipient_role}, cat={record.category.value}, ts={record.created_at.isoformat()})"
                            )
                            continue
                    seen_deliveries[del_ident] = record

                    # Database check and conflict detection (read-only)
                    existing = repo.get_telegram_delivery(chat_id, digest_hash)
                    if existing:
                        existing_ts = existing.created_at
                        if existing_ts.tzinfo is None:
                            existing_ts = existing_ts.replace(tzinfo=timezone.utc)
                        is_identical = (
                            existing.status == record.status
                            and existing.recipient_role == record.recipient_role
                            and existing.category == record.category
                            and existing_ts.isoformat() == record.created_at.isoformat()
                            and existing.error_message == record.error_message
                            and existing.payload == record.payload
                        )
                        if is_identical:
                            deliveries_to_skip_count += 1
                        else:
                            result.add_conflict(
                                f"Delivery conflict for {masked_chat}:{digest_hash} (existing in db "
                                f"status={existing.status.value}, role={existing.recipient_role}, cat={existing.category.value}, "
                                f"ts={existing_ts.isoformat()} vs imported status={record.status.value}, "
                                f"role={record.recipient_role}, cat={record.category.value}, ts={record.created_at.isoformat()})"
                            )
                    else:
                        deliveries_to_import.append(record)

            except Exception as exc:
                logger.exception("Failed to validate delivery ledger: %s", exc)
                result.add_error(f"Delivery ledger migration error: {exc}")

    # 2. Budget Ledger Pre-flight
    if budget_ledger_path is not None:
        if not budget_ledger_path.exists():
            result.add_error(f"Budget ledger path does not exist: {budget_ledger_path}")
        else:
            try:
                raw_text = budget_ledger_path.read_text(encoding="utf-8")
                data = json.loads(raw_text)
                batches = data.get("batches", {}) if isinstance(data, dict) else {}
                exp_id = str(data.get("experiment_id", "default_exp"))

                seen_budget_records: dict[str, dict[str, Any]] = {}

                for batch_id, batch_val in batches.items():
                    if not isinstance(batch_val, dict):
                        result.add_error(f"Malformed budget batch entry is not a dict: {batch_id}")
                        continue

                    cost = float(batch_val.get("actual_cost_usd", 0.0))
                    in_tok = int(batch_val.get("input_tokens", 0))
                    out_tok = int(batch_val.get("output_tokens", 0))
                    total_tok = in_tok + out_tok
                    calls = int(batch_val.get("calls_count", 1))

                    period = batch_val.get("period_month")
                    ts_str = (
                        batch_val.get("timestamp_utc")
                        or batch_val.get("timestamp")
                        or batch_val.get("created_at")
                    )

                    if not ts_str:
                        result.add_error(
                            f"Missing required timestamp for budget batch '{batch_id}'. "
                            "Timestamps must not be derived or invented from period_month."
                        )
                        continue

                    try:
                        ts = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
                        if ts.tzinfo is None:
                            ts = ts.replace(tzinfo=timezone.utc)
                        if not period:
                            period = ts.strftime("%Y-%m")
                    except Exception as ts_exc:
                        result.add_error(
                            f"Invalid timestamp '{ts_str}' for budget batch '{batch_id}': {ts_exc}"
                        )
                        continue

                    clean_run = f"{exp_id}_{batch_id}".strip()
                    rec_id = f"bud_{period}_{clean_run}"

                    bud_data = {
                        "record_id": rec_id,
                        "period_month": period,
                        "clean_run": clean_run,
                        "worker_id": "migration_utility",
                        "calls_count": calls,
                        "prompt_tokens": in_tok,
                        "completion_tokens": out_tok,
                        "total_tokens": total_tok,
                        "estimated_cost_usd": cost,
                        "timestamp": ts,
                    }

                    # In-file duplicate check
                    if rec_id in seen_budget_records:
                        prev_bud = seen_budget_records[rec_id]
                        is_identical = (
                            prev_bud["period_month"] == period
                            and prev_bud["clean_run"] == clean_run
                            and prev_bud["timestamp"].isoformat() == ts.isoformat()
                            and prev_bud["calls_count"] == calls
                            and prev_bud["prompt_tokens"] == in_tok
                            and prev_bud["completion_tokens"] == out_tok
                            and prev_bud["total_tokens"] == total_tok
                            and abs(prev_bud["estimated_cost_usd"] - cost) < 1e-9
                        )
                        if is_identical:
                            budget_to_skip_count += 1
                            continue
                        else:
                            result.add_conflict(
                                f"Conflicting in-file duplicate budget batch for '{rec_id}'"
                            )
                            continue
                    seen_budget_records[rec_id] = bud_data

                    # Database check (read-only)
                    existing = repo.get_budget_record(rec_id)
                    if existing:
                        is_identical = (
                            existing["period_month"] == period
                            and existing["run_id"] == clean_run
                            and existing["worker_id"] == "migration_utility"
                            and existing["timestamp"] == ts.isoformat()
                            and int(existing["calls_count"]) == calls
                            and int(existing["prompt_tokens"]) == in_tok
                            and int(existing["completion_tokens"]) == out_tok
                            and int(existing["total_tokens"]) == total_tok
                            and abs(float(existing["estimated_cost_usd"]) - cost) < 1e-9
                        )
                        if is_identical:
                            budget_to_skip_count += 1
                        else:
                            result.add_conflict(
                                f"Budget conflict for batch '{rec_id}': existing in db "
                                f"(period={existing['period_month']}, run={existing['run_id']}, worker={existing['worker_id']}, "
                                f"ts={existing['timestamp']}, calls={existing['calls_count']}, prompt={existing['prompt_tokens']}, "
                                f"cost={existing['estimated_cost_usd']}) vs imported "
                                f"(period={period}, run={clean_run}, worker=migration_utility, ts={ts.isoformat()}, "
                                f"calls={calls}, prompt={in_tok}, cost={cost})"
                            )
                    else:
                        budget_to_import.append(bud_data)

            except Exception as exc:
                logger.exception("Failed to validate budget ledger: %s", exc)
                result.add_error(f"Budget ledger migration error: {exc}")

    # If any error or conflict occurred, or if running in dry-run mode:
    # FAIL CLOSED or RETURN PRE-FLIGHT REPORT WITH ZERO DATABASE WRITES
    if not result.success or dry_run:
        result.deliveries_imported = len(deliveries_to_import) if (result.success and dry_run) else 0
        result.deliveries_skipped = deliveries_to_skip_count
        result.budget_records_imported = len(budget_to_import) if (result.success and dry_run) else 0
        result.budget_records_skipped = budget_to_skip_count
        return result

    # ---------------------------------------------------------
    # PHASE 2: TRANSACTIONAL APPLICATION (ALL PRE-FLIGHT GATES PASSED)
    # ---------------------------------------------------------
    # Fail closed for all non-SQLite write-mode migrations during Phase 5B.3B.
    # Production Turso migration is NOT IMPLEMENTED and requires separately approved transactional integration.
    if not isinstance(repo, SqliteStateRepository):
        result.add_error(
            "Non-SQLite live migration is disabled: production Turso migration is NOT IMPLEMENTED "
            "and requires separately approved transactional integration."
        )
        result.deliveries_imported = 0
        result.budget_records_imported = 0
        return result

    try:
        with repo._connection() as conn:
            for rec in deliveries_to_import:
                conn.execute(
                    """
                    INSERT INTO telegram_delivery_records (
                        delivery_id, message_key, recipient_role, recipient_chat_id,
                        category, status, attempt_count, max_attempts, error_message,
                        payload_json, created_at, updated_at, sent_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        rec.delivery_id,
                        rec.message_key,
                        rec.recipient_role,
                        rec.recipient_chat_id,
                        rec.category.value,
                        rec.status.value,
                        rec.attempt_count,
                        rec.max_attempts,
                        rec.error_message,
                        json.dumps(rec.payload, ensure_ascii=False),
                        rec.created_at.isoformat(),
                        rec.updated_at.isoformat(),
                        rec.sent_at.isoformat() if rec.sent_at else None,
                    ),
                )
            for bud in budget_to_import:
                conn.execute(
                    """
                    INSERT INTO experiment_budget_history (
                        record_id, period_month, run_id, worker_id, timestamp,
                        calls_count, prompt_tokens, completion_tokens, total_tokens,
                        estimated_cost_usd
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        bud["record_id"],
                        bud["period_month"],
                        bud["clean_run"],
                        bud["worker_id"],
                        bud["timestamp"].isoformat(),
                        bud["calls_count"],
                        bud["prompt_tokens"],
                        bud["completion_tokens"],
                        bud["total_tokens"],
                        bud["estimated_cost_usd"],
                    ),
                )

        result.deliveries_imported = len(deliveries_to_import)
        result.deliveries_skipped = deliveries_to_skip_count
        result.budget_records_imported = len(budget_to_import)
        result.budget_records_skipped = budget_to_skip_count

    except Exception as exc:
        logger.exception("Failed during transactional application of ledgers: %s", exc)
        result.add_error(f"Transactional migration application failed: {exc}")
        result.deliveries_imported = 0
        result.budget_records_imported = 0

    return result
