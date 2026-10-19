"""Synthetic fixture migration utility for Efso Radar ledgers.

Losslessly and idempotently imports legacy/offline JSON ledgers (telegram_digest_delivery_ledger.json
and experiment_budget_ledger.json) into repository tables (telegram_delivery_records
and experiment_budget_history) with strict conflict detection, historical role/category preservation,
timestamp immutability (zero synthetic utc_now() invention), alias deduplication, and pre-flight dry-run reporting.
"""
from __future__ import annotations

import json
import logging
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
)
from efso_radar.repositories.base import StateRepository

logger = logging.getLogger("efso-radar.repository.migration_ledger")


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
                lines.append(f"  - {c}")
        if self.errors:
            lines.append("Errors:")
            for e in self.errors:
                lines.append(f"  - {e}")
        return "\n".join(lines)


def migrate_ledgers_to_repository(
    repo: StateRepository,
    delivery_ledger_path: Optional[Path] = None,
    budget_ledger_path: Optional[Path] = None,
    dry_run: bool = False,
) -> MigrationLedgerResult:
    """Losslessly migrate JSON ledgers into the repository database.

    Enforces:
    - Fail-closed if explicitly provided ledger file does not exist.
    - Zero timestamp invention: records without valid historical ISO timestamps fail validation.
    - Accurate historical status mapping (SENT, DELIVERY_UNCERTAIN, FAILED, HELD_QUIET_HOURS, PENDING);
      unknown statuses fail with explicit errors.
    - Full preservation of source recipient roles and notification categories.
    - In-file and database-level duplicate/alias deduplication and conflict detection.
    - Pre-flight dry-run reporting (dry_run=True) without database mutation.
    """
    result = MigrationLedgerResult(dry_run=dry_run)

    # 1. Delivery Ledger Migration
    if delivery_ledger_path is not None:
        if not delivery_ledger_path.exists():
            result.errors.append(f"Delivery ledger path does not exist: {delivery_ledger_path}")
        else:
            try:
                raw_text = delivery_ledger_path.read_text(encoding="utf-8")
                data = json.loads(raw_text)
                deliveries = data.get("deliveries", {}) if isinstance(data, dict) else {}

                # Track entries seen within this file to deduplicate aliases and catch intra-file conflicts
                seen_deliveries: dict[tuple[str, str], TelegramDeliveryRecord] = {}

                for entry_key, entry_val in deliveries.items():
                    if not isinstance(entry_val, dict):
                        result.errors.append(f"Malformed delivery ledger entry is not a dict: {entry_key}")
                        continue

                    # 1a. Identity extraction
                    chat_id = str(entry_val.get("chat_id", "")).strip()
                    digest_hash = str(entry_val.get("digest_hash", "") or entry_val.get("message_key", "")).strip()
                    if not chat_id or not digest_hash:
                        if ":" in entry_key:
                            parts = entry_key.split(":", 1)
                            chat_id = chat_id or parts[0].strip()
                            digest_hash = digest_hash or parts[1].strip()

                    if not chat_id or not digest_hash:
                        result.errors.append(f"Malformed delivery ledger entry missing chat_id or digest_hash: {entry_key}")
                        continue

                    # 1b. Timestamp validation (strict - NO invented utc_now())
                    delivered_at_str = (
                        entry_val.get("delivered_at_utc")
                        or entry_val.get("sent_at")
                        or entry_val.get("created_at")
                        or entry_val.get("timestamp")
                    )
                    if not delivered_at_str:
                        result.errors.append(
                            f"Missing delivery timestamp for entry '{entry_key}' ({chat_id}:{digest_hash}). "
                            "Historical timestamps must not be invented with utc_now()."
                        )
                        continue

                    try:
                        delivered_at = datetime.fromisoformat(str(delivered_at_str).replace("Z", "+00:00"))
                    except Exception as ts_exc:
                        result.errors.append(
                            f"Invalid ISO timestamp '{delivered_at_str}' for entry '{entry_key}': {ts_exc}"
                        )
                        continue

                    # 1c. Historical status mapping
                    raw_status = str(entry_val.get("status", "")).strip().upper()
                    if raw_status in ("DELIVERY_SUCCESS", "SENT", "SUCCESS"):
                        status = DeliveryStatus.SENT
                    elif raw_status == "DELIVERY_UNCERTAIN":
                        status = DeliveryStatus.DELIVERY_UNCERTAIN
                    elif raw_status == "FAILED":
                        status = DeliveryStatus.FAILED
                    elif raw_status == "HELD_QUIET_HOURS":
                        status = DeliveryStatus.FAILED
                    elif raw_status == "PENDING":
                        status = DeliveryStatus.PENDING
                    else:
                        result.errors.append(
                            f"Unknown historical delivery status '{raw_status}' for entry '{entry_key}'. "
                            "Unknown statuses cannot be silently converted to FAILED."
                        )
                        continue

                    # 1d. Role and Category preservation
                    role = str(entry_val.get("recipient_role") or entry_val.get("role") or "PRIMARY_EDITOR").strip()

                    category_val = entry_val.get("category") or entry_val.get("notification_category")
                    if category_val:
                        cat_str = str(category_val).strip().upper()
                        if cat_str in ("ADMIN_HEALTH_REPORT", "ADMIN_REPORT", "OPERATIONAL_HEALTH_REPORT"):
                            category = TelegramNotificationCategory.OPERATIONAL_HEALTH_REPORT
                        else:
                            try:
                                category = TelegramNotificationCategory(str(category_val))
                            except ValueError:
                                try:
                                    category = TelegramNotificationCategory[cat_str]
                                except KeyError:
                                    result.errors.append(
                                        f"Unknown notification category '{category_val}' for entry '{entry_key}'"
                                    )
                                    continue
                    else:
                        category = TelegramNotificationCategory.EDITORIAL_DIGEST

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

                    # 1e. In-file duplicate & alias deduplication
                    del_ident = (chat_id, digest_hash)
                    if del_ident in seen_deliveries:
                        prev_rec = seen_deliveries[del_ident]
                        if (
                            prev_rec.status == record.status
                            and prev_rec.recipient_role == record.recipient_role
                            and prev_rec.category == record.category
                            and prev_rec.error_message == record.error_message
                        ):
                            # Identical duplicate / alias in file -> skip safely
                            result.deliveries_skipped += 1
                            continue
                        else:
                            result.conflicts_detected.append(
                                f"Conflicting in-file duplicate entry for {chat_id}:{digest_hash} "
                                f"(status={prev_rec.status}, role={prev_rec.recipient_role} vs "
                                f"status={record.status}, role={record.recipient_role})"
                            )
                            continue
                    seen_deliveries[del_ident] = record

                    # 1f. Database check and conflict detection
                    existing = repo.get_telegram_delivery(chat_id, digest_hash)
                    if existing:
                        if (
                            existing.status == record.status
                            and existing.recipient_role == record.recipient_role
                            and existing.category == record.category
                        ):
                            result.deliveries_skipped += 1
                        else:
                            result.conflicts_detected.append(
                                f"Delivery conflict for {chat_id}:{digest_hash} (existing in db "
                                f"status={existing.status}, role={existing.recipient_role}, cat={existing.category} "
                                f"vs imported status={record.status}, role={record.recipient_role}, cat={record.category})"
                            )
                    else:
                        if dry_run:
                            result.deliveries_imported += 1
                        else:
                            try:
                                repo.record_telegram_delivery(record)
                                result.deliveries_imported += 1
                            except (DeliveryStatusRegressionError, DeliveryIdentityConflictError) as reg_err:
                                result.conflicts_detected.append(
                                    f"Delivery conflict for {chat_id}:{digest_hash}: {reg_err}"
                                )
                            except Exception as rec_exc:
                                result.errors.append(
                                    f"Failed to record delivery for {chat_id}:{digest_hash}: {rec_exc}"
                                )

            except Exception as exc:
                logger.exception("Failed to migrate delivery ledger: %s", exc)
                result.errors.append(f"Delivery ledger migration error: {exc}")

    # 2. Budget Ledger Migration
    if budget_ledger_path is not None:
        if not budget_ledger_path.exists():
            result.errors.append(f"Budget ledger path does not exist: {budget_ledger_path}")
        else:
            try:
                raw_text = budget_ledger_path.read_text(encoding="utf-8")
                data = json.loads(raw_text)
                batches = data.get("batches", {}) if isinstance(data, dict) else {}
                exp_id = str(data.get("experiment_id", "default_exp"))

                seen_budget_keys: set[str] = set()

                for batch_id, batch_val in batches.items():
                    if not isinstance(batch_val, dict):
                        result.errors.append(f"Malformed budget batch entry is not a dict: {batch_id}")
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

                    if ts_str:
                        try:
                            ts = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
                            if not period:
                                period = ts.strftime("%Y-%m")
                        except Exception as ts_exc:
                            result.errors.append(
                                f"Invalid timestamp '{ts_str}' for budget batch {batch_id}: {ts_exc}"
                            )
                            continue
                    elif period:
                        try:
                            ts = datetime.fromisoformat(f"{period}-01T00:00:00+00:00")
                        except Exception as p_exc:
                            result.errors.append(
                                f"Invalid period_month '{period}' for budget batch {batch_id}: {p_exc}"
                            )
                            continue
                    else:
                        result.errors.append(
                            f"Missing timestamp and period_month for budget batch {batch_id}. "
                            "Timestamps must not be invented with utc_now()."
                        )
                        continue

                    clean_run = f"{exp_id}_{batch_id}".strip()
                    rec_id = f"bud_{period}_{clean_run}"

                    # In-file duplicate check
                    if rec_id in seen_budget_keys:
                        result.budget_records_skipped += 1
                        continue
                    seen_budget_keys.add(rec_id)

                    # Check DB
                    existing = repo.get_budget_record(rec_id)
                    if existing:
                        is_identical = (
                            int(existing["calls_count"]) == calls
                            and int(existing["prompt_tokens"]) == in_tok
                            and int(existing["completion_tokens"]) == out_tok
                            and int(existing["total_tokens"]) == total_tok
                            and abs(float(existing["estimated_cost_usd"]) - cost) < 1e-9
                        )
                        if is_identical:
                            result.budget_records_skipped += 1
                        else:
                            result.conflicts_detected.append(
                                f"Budget conflict for batch {rec_id}: existing in db "
                                f"(calls={existing['calls_count']}, prompt={existing['prompt_tokens']}, cost={existing['estimated_cost_usd']}) "
                                f"vs imported (calls={calls}, prompt={in_tok}, cost={cost})"
                            )
                    else:
                        if dry_run:
                            result.budget_records_imported += 1
                        else:
                            try:
                                repo.record_budget_usage(
                                    period_month=period,
                                    run_id=clean_run,
                                    worker_id="migration_utility",
                                    calls_count=calls,
                                    prompt_tokens=in_tok,
                                    completion_tokens=out_tok,
                                    total_tokens=total_tok,
                                    estimated_cost_usd=cost,
                                    timestamp=ts,
                                    record_id=rec_id,
                                )
                                result.budget_records_imported += 1
                            except BudgetHistoryConflictError as conf_err:
                                result.conflicts_detected.append(
                                    f"Budget conflict for batch {rec_id}: {conf_err}"
                                )
                            except Exception as exc:
                                result.errors.append(
                                    f"Failed to record budget usage for {rec_id}: {exc}"
                                )

            except Exception as exc:
                logger.exception("Failed to migrate budget ledger: %s", exc)
                result.errors.append(f"Budget ledger migration error: {exc}")

    return result
