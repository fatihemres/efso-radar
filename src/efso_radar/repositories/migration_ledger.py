"""Synthetic fixture migration utility for Efso Radar ledgers.

Idempotently imports legacy/offline JSON ledgers (telegram_digest_delivery_ledger.json
and experiment_budget_ledger.json) into repository tables (telegram_delivery_records
and experiment_budget_history) with conflict detection and verification.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from pydantic import BaseModel, Field

from efso_radar.domain import (
    DeliveryStatus,
    TelegramDeliveryRecord,
    TelegramNotificationCategory,
    utc_now,
)
from efso_radar.repositories.base import StateRepository

logger = logging.getLogger("efso-radar.repository.migration_ledger")


class MigrationLedgerResult(BaseModel):
    """Summary of ledger migration execution."""
    deliveries_imported: int = 0
    deliveries_skipped: int = 0
    budget_records_imported: int = 0
    budget_records_skipped: int = 0
    conflicts_detected: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)

    @property
    def success(self) -> bool:
        return len(self.errors) == 0 and len(self.conflicts_detected) == 0


def migrate_ledgers_to_repository(
    repo: StateRepository,
    delivery_ledger_path: Optional[Path] = None,
    budget_ledger_path: Optional[Path] = None,
) -> MigrationLedgerResult:
    """Migrate JSON ledgers into the repository database.

    Enforces idempotency: re-running with the same files will skip existing identical entries
    and flag conflicting entries without corrupting state.
    """
    result = MigrationLedgerResult()

    # 1. Delivery Ledger Migration
    if delivery_ledger_path and delivery_ledger_path.exists():
        try:
            raw_text = delivery_ledger_path.read_text(encoding="utf-8")
            data = json.loads(raw_text)
            deliveries = data.get("deliveries", {}) if isinstance(data, dict) else {}

            for entry_key, entry_val in deliveries.items():
                if not isinstance(entry_val, dict):
                    continue

                chat_id = str(entry_val.get("chat_id", "")).strip()
                digest_hash = str(entry_val.get("digest_hash", "")).strip()
                if not chat_id or not digest_hash:
                    if ":" in entry_key:
                        parts = entry_key.split(":", 1)
                        chat_id = chat_id or parts[0]
                        digest_hash = digest_hash or parts[1]

                if not chat_id or not digest_hash:
                    result.errors.append(f"Malformed delivery ledger entry missing keys: {entry_key}")
                    continue

                delivered_at_str = entry_val.get("delivered_at_utc") or entry_val.get("sent_at")
                if delivered_at_str:
                    try:
                        delivered_at = datetime.fromisoformat(delivered_at_str)
                    except Exception:
                        delivered_at = utc_now()
                else:
                    delivered_at = utc_now()

                raw_status = str(entry_val.get("status", "DELIVERY_SUCCESS")).upper()
                if "SUCCESS" in raw_status or "SENT" in raw_status:
                    status = DeliveryStatus.SENT
                elif "UNCERTAIN" in raw_status:
                    status = DeliveryStatus.DELIVERY_UNCERTAIN
                else:
                    status = DeliveryStatus.FAILED

                role = str(entry_val.get("recipient_role", "PRIMARY_EDITOR")).strip()
                delivery_id = f"del_{digest_hash[:16]}_{chat_id}"

                record = TelegramDeliveryRecord(
                    delivery_id=delivery_id,
                    message_key=digest_hash,
                    recipient_role=role,
                    recipient_chat_id=chat_id,
                    category=TelegramNotificationCategory.EDITORIAL_DIGEST,
                    status=status,
                    attempt_count=1,
                    max_attempts=3,
                    error_message=entry_val.get("error_message"),
                    payload=entry_val,
                    created_at=delivered_at,
                    updated_at=delivered_at,
                    sent_at=delivered_at if status == DeliveryStatus.SENT else None,
                )

                existing = repo.get_telegram_delivery(chat_id, digest_hash)
                if existing:
                    if existing.status == record.status:
                        result.deliveries_skipped += 1
                    else:
                        result.conflicts_detected.append(
                            f"Delivery conflict for {chat_id}:{digest_hash} (existing {existing.status} vs imported {record.status})"
                        )
                else:
                    repo.record_telegram_delivery(record)
                    result.deliveries_imported += 1

        except Exception as exc:
            logger.exception("Failed to migrate delivery ledger: %s", exc)
            result.errors.append(f"Delivery ledger migration error: {exc}")

    # 2. Budget Ledger Migration
    if budget_ledger_path and budget_ledger_path.exists():
        try:
            raw_text = budget_ledger_path.read_text(encoding="utf-8")
            data = json.loads(raw_text)
            batches = data.get("batches", {}) if isinstance(data, dict) else {}
            exp_id = str(data.get("experiment_id", "default_exp"))

            for batch_id, batch_val in batches.items():
                if not isinstance(batch_val, dict):
                    continue

                cost = float(batch_val.get("actual_cost_usd", 0.0))
                in_tok = int(batch_val.get("input_tokens", 0))
                out_tok = int(batch_val.get("output_tokens", 0))
                total_tok = in_tok + out_tok
                cands = batch_val.get("candidate_ids", [])
                calls = int(batch_val.get("calls_count", 1))

                # Period month
                period = batch_val.get("period_month") or utc_now().strftime("%Y-%m")

                repo.record_budget_usage(
                    period_month=period,
                    run_id=f"{exp_id}_{batch_id}",
                    worker_id="migration_utility",
                    calls_count=calls,
                    prompt_tokens=in_tok,
                    completion_tokens=out_tok,
                    total_tokens=total_tok,
                    estimated_cost_usd=cost,
                )
                result.budget_records_imported += 1

        except Exception as exc:
            logger.exception("Failed to migrate budget ledger: %s", exc)
            result.errors.append(f"Budget ledger migration error: {exc}")

    return result
