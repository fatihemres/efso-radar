# Phase 5B.3C-2: Cloud Staging Preflight & Recovery Readiness Specification

## 1. Executive Summary & Architecture Context
Phase 5B.3C-2 establishes the offline preflight validation, delivery recovery inspection, and staging cutover/rollback protocols required prior to connecting Efso Radar to any future, separately authorized Turso staging environment.

In strict adherence to the project safety charter:
- **Zero Real Cloud Operations:** No network calls to Turso, Telegram, Gemini, or external RSS sources.
- **Strict Isolation:** Existing operational database (`data/radar.db`), JSON ledgers, `.env`, and encrypted backups remain untouched and unmounted for writing.
- **Financial Invariant:** Strictly 0 TRY / $0.00 operational expenditure.
- **Fail-Closed Staging Gate:** Read-only inspection and preflight mechanisms abort with zero writes upon detecting any schema drift, unapplied migrations, or ambiguous delivery state.

---

## 2. Non-Mutating Cloud Preflight Validator
The `CloudPreflightValidator` (`src/efso_radar/staging/preflight.py`) provides non-mutating schema and contract verification across both local SQLite and remote Turso (libSQL) backends.

### 2.1. Schema v6 Verification Scope
The preflight validator introspects all 16 tables defined in schema v6:
1. `schema_migrations`: Version tracking (requires `MAX(version) >= 6`).
2. `run_locks`: Mutual exclusion and distributed worker lease tracking.
3. `processed_articles`: Deduplication and article lifecycle history (`idx_processed_articles_processed_at`).
4. `stories`: Editorial story clusters (`idx_stories_last_updated`).
5. `story_updates`: Significant story developments (`idx_story_updates_story_id`).
6. `notifications`: Alert history and lease tracking (`idx_notifications_status`).
7. `editorial_assessments`: AI evaluation records (`idx_assessments_story_id`).
8. `owner_feedback`: Calibration rating records.
9. `pending_candidates`: Backlog persistence (`idx_pending_candidates_queued_at`).
10. `ai_usage_ledger`: Token usage and cost tracking (`idx_ai_usage_run_id`, `idx_ai_usage_timestamp`).
11. `ai_batch_evaluations`: Evaluation cache (`idx_ai_batch_expires_at`).
12. `telegram_delivery_records`: Ephemeral runner outbox, atomic delivery claims, and unique constraint `uq_recipient_message` (`idx_telegram_delivery_records_status`, `idx_telegram_delivery_records_role`).
13. `durable_recovery_state`: Resilient runner pipeline checkpointing (`idx_durable_recovery_stage`).
14. `admin_operational_reports`: Nightly operational health dispatches.
15. `experiment_budget_history`: Immutable monthly budget event history (`idx_budget_history_period`).
16. `scheduled_execution_history`: Run lifecycle audit ledger (`idx_scheduled_execution_started`).

### 2.2. Non-Mutating Invariant
Preflight execution guarantees zero database mutation:
- Pre-validation schema fingerprint is calculated via an MD5 hash of all `sqlite_master` objects.
- Post-validation schema fingerprint is compared against the pre-validation fingerprint.
- If fingerprints do not match or if any write operation is attempted, preflight fails immediately.

---

## 3. Delivery Recovery Inspection Protocol
The `DeliveryRecoveryInspector` (`src/efso_radar/staging/recovery_inspection.py`) provides read-only diagnostic visibility for operators without risking duplicate alert dispatch.

### 3.1. Diagnostic Checks
1. **`DELIVERY_UNCERTAIN` Deliveries:** Identifies alerts where network timeouts occurred during dispatch.
2. **Stuck `DISPATCHING` Claims:** Identifies delivery claims held by crashed runners exceeding `stuck_claim_threshold_minutes`.
3. **Duplicate Identity Keys:** Scans for potential unique constraint violations on `(recipient_chat_id, message_key)`.
4. **Expired Worker Locks:** Scans `run_locks` for leases where `expires_at < now`.
5. **Incomplete Executions & Checkpoints:** Scans `scheduled_execution_history` and `durable_recovery_state` for interrupted stages (`STARTED`, `RUNNING`, `FAILED`).

### 3.2. Safety Rules
- **No Automatic State Regression:** `DELIVERY_UNCERTAIN` is NEVER transitioned automatically to `SENT`, `FAILED`, or `PENDING`.
- **No Automatic Resend:** Ambiguous messages are never automatically re-dispatched.
- **Strict Privacy Sanitization:** All recipient chat IDs are masked via `mask_chat_id()` (e.g. `11...11` or `***`), and bot tokens are stripped from diagnostic output.

---

## 4. Staging Cutover Protocol & Rollback Procedures
The `StagingCutoverReconciler` (`src/efso_radar/staging/cutover.py`) enforces strict validation before authorizing any transition from an offline fixture to staging.

### 4.1. Reconciliation Metrics
- Table-by-table record count comparison across all 16 tables.
- Primary key ordering and sample key alignment.
- Schema version compatibility.

### 4.2. Migration Abort Conditions
Cutover is immediately ABORTED if:
- `ERR_PREFLIGHT_FAILED`: Target database fails preflight checks.
- `ERR_SCHEMA_INCOMPATIBLE`: Source or target schema version < 6.
- `ERR_DATA_MISMATCH`: Record counts differ between source and target for any table.
- `ERR_NO_VERIFIED_BACKUP`: Verified encrypted backup is not present prior to cutover.
- `ERR_NO_ROLLBACK_PLAN`: Rollback restore point is unavailable.
- `ERR_COST_INVARIANT_VIOLATION`: Any non-zero cost is detected ($0.00 / 0 TRY violation).
- `ERR_NO_PO_SIGNOFF`: Explicit Product Owner sign-off is absent.

### 4.3. Multi-Factor Go/No-Go Gate
The `GoNoGoGate.evaluate()` function requires affirmative verification on all 7 criteria before emitting a `GO` decision (`can_proceed=True`).

---

## 5. Schedule and Budget Constraints (Europe/Istanbul)
The approved scheduling architecture remains inactive in this phase, but is codified as the future operating model:
- **Timezone:** Europe/Istanbul (`UTC+03:00`).
- **RSS Discovery:** Every 3 hours starting at 00:17 (e.g., 00:17, 03:17, 06:17, 09:17, 12:17, 15:17, 18:17, 21:17).
- **Primary Editor Digest Windows:** 09:30, 15:30, and 21:30.
- **Admin Operational Health Report:** Nightly around 22:00.
- **Quiet Hours:** 00:00–08:30 (breaking news with score >= 95 allowed if configured; normal alerts deferred or held).
- **Execution Policy:** No catch-up execution upon runner restart.
- **Budget Invariant:** 0 TRY / $0.00 operational cost.
