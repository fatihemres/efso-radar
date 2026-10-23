# Phase 5B.3C-2: Cloud Staging Preflight & Recovery Readiness Specification

## 1. Executive Summary & Architecture Context
Phase 5B.3C-2 establishes the offline preflight validation, delivery recovery inspection, and staging cutover/rollback protocols required prior to connecting Efso Radar to any future, separately authorized Turso staging environment.

In strict adherence to the project safety charter:
- **Zero Real Cloud Operations:** No network calls to Turso, Telegram, Gemini, or external RSS sources.
- **Strict Isolation:** Existing operational database (`data/radar.db`), JSON ledgers, `.env`, and encrypted backups remain untouched and unmounted for writing.
- **Financial Invariant:** Strictly 0 TRY / \$0.00 operational expenditure.
- **Fail-Closed Staging Gate:** Read-only inspection and preflight mechanisms abort with zero writes upon detecting any schema drift, unapplied migrations, missing/inaccessible SQLite files, or ambiguous delivery state.
- **Offline Planning Evidence Only — No Live Cutover Authorization:** Staging cutover, rollback, and live Turso behavior have **NOT** been tested on real cloud services. All tools, reports, and `GoNoGoDecision` outcomes (`can_proceed=True`) in this phase provide offline verification and planning evidence only; they **never** authorize or trigger live cloud deployment, production cutover, or external network connections. Live staging activation requires separate, explicit Product Owner authorization.

---

## 2. Non-Mutating Cloud Preflight Validator
The `CloudPreflightValidator` (`src/efso_radar/staging/preflight.py`) provides non-mutating schema and contract verification across both local SQLite (`file:...?mode=ro` on an existing regular file; never creating a missing database) and remote Turso (libSQL) backends, guarded by `validate_read_only_sql()`.

### 2.1. Schema v6 Verification Scope (15 Tables)
The preflight validator introspects all 15 tables defined in schema v6 (`SCHEMA_V6_TABLES`):
1. `schema_migrations`: Version and migration history tracking (`version`, `applied_at`, `description`; requires `MAX(version) >= 6`).
2. `run_locks`: Mutual exclusion and distributed worker lease tracking (`lock_key`, `worker_id`, `acquired_at`, `expires_at`, `heartbeat_at`).
3. `processed_articles`: Deduplication and article lifecycle history (`idx_processed_articles_processed_at`).
4. `stories`: Editorial story clusters (`idx_stories_last_updated`).
5. `story_updates`: Significant story developments (`idx_story_updates_story_id`).
6. `notifications`: Alert history and lease tracking (`idx_notifications_status`).
7. `editorial_assessments`: AI evaluation records (`idx_assessments_story_id`).
8. `owner_feedback`: Calibration rating records (`idx_owner_feedback_rating`).
9. `candidate_backlog`: Backlog persistence (`idx_candidate_backlog_status`).
10. `ai_usage_ledger`: Token usage and cost tracking (`idx_ai_usage_ledger_created_at`, `idx_ai_usage_ledger_batch_hash`).
11. `telegram_delivery_records`: Ephemeral runner outbox, atomic delivery claims, indexes (`idx_telegram_delivery_records_status`, `idx_telegram_delivery_records_role`), and critical uniqueness semantics across `(recipient_chat_id, message_key)` verified directly via `PRAGMA index_list`, `PRAGMA index_xinfo`, and `PRAGMA table_info` (requiring a non-partial `UNIQUE` index on exactly those two columns regardless of index/constraint name, plus `NOT NULL` enforcement on both identity columns).
12. `durable_recovery_state`: Resilient runner pipeline checkpointing (`idx_durable_recovery_stage`).
13. `admin_operational_reports`: Nightly operational health dispatches.
14. `experiment_budget_history`: Immutable monthly budget event history (`idx_budget_history_period`).
15. `scheduled_execution_history`: Run lifecycle audit ledger (`idx_scheduled_execution_started`).

### 2.2. Non-Mutating Invariant vs Data Integrity
- **Schema Fingerprint Check:** Calculates an MD5 hash of all `sqlite_master` DDL definitions before and after preflight. This proves that the preflight inspection itself was strictly non-mutating with respect to database schema objects (tables, columns, indexes, triggers, views).
- **Data Integrity Distinction:** Unchanged schema hashes do **NOT** prove unchanged row contents. Row content integrity and source/target data parity are separately verified via deterministic, lossless SHA-256 cryptographic content hashing in the `StagingCutoverReconciler`.

---

## 3. Delivery Recovery Inspection Protocol
The `DeliveryRecoveryInspector` (`src/efso_radar/staging/recovery_inspection.py`) provides read-only diagnostic visibility for operators across all 6 required recovery inspections (`REQUIRED_RECOVERY_INSPECTIONS = 6`) without risking duplicate alert dispatch.

### 3.1. Diagnostic Checks
1. **`DELIVERY_UNCERTAIN` Deliveries:** Identifies alerts where network timeouts occurred during dispatch.
2. **Stuck `DISPATCHING` Claims:** Identifies delivery claims held by crashed runners exceeding `stuck_claim_threshold_minutes`.
3. **Duplicate Identity Keys:** Scans for potential unique constraint violations on `(recipient_chat_id, message_key)`.
4. **Expired Worker Locks:** Scans `run_locks` for leases where `expires_at < now`, failing closed on empty or unparseable ISO expiration timestamps.
5. **Incomplete Executions:** Scans `scheduled_execution_history` for interrupted or failed runs (`STARTED`, `RUNNING`, `FAILED`).
6. **Recovery Checkpoints:** Scans `durable_recovery_state` for active pipeline checkpoints.

### 3.2. Fail-Closed Diagnostics & Safety Rules
- **Fail-Closed on Query or Parse Failure:** An unexecuted, empty, or partially completed `RecoveryInspectionReport` defaults to `is_inspection_complete = False` and never reports `ALL CLEAR`. `ALL CLEAR` requires all 6 required inspections to succeed with zero errors and zero recovery anomalies; otherwise `summary()` reports `Status: INSPECTION FAILED / INCOMPLETE` (or `ISSUES DETECTED`).
- **Safe Error Categories & Privacy Sanitization:** Query and timestamp parse failures report only safe exception categories (`type(exc).__name__`, `empty_timestamp`, `invalid_iso_timestamp`) rather than raw exception strings or arbitrary database cell values. All recipient chat IDs are masked via `mask_chat_id()` (e.g. `11***11`), bot tokens are redacted (`[REDACTED_BOT_TOKEN]`), and auth tokens are stripped from diagnostic output.
- **No Automatic State Regression:** `DELIVERY_UNCERTAIN` is NEVER transitioned automatically to `SENT`, `FAILED`, or `PENDING`.
- **No Automatic Resend:** Ambiguous messages are never automatically re-dispatched.

---

## 4. Staging Cutover Protocol & Rollback Procedures
The `StagingCutoverReconciler` (`src/efso_radar/staging/cutover.py`) enforces strict non-mutating validation before any offline cutover readiness evaluation.

### 4.1. Reconciliation Metrics & Lossless Comparison
- **15 Schema-v6 Tables:** Reconciles all 15 tables defined in `SCHEMA_V6_TABLES` (`schema_migrations`, `run_locks`, `processed_articles`, `stories`, `story_updates`, `notifications`, `editorial_assessments`, `owner_feedback`, `candidate_backlog`, `ai_usage_ledger`, `telegram_delivery_records`, `durable_recovery_state`, `admin_operational_reports`, `experiment_budget_history`, `scheduled_execution_history`).
- **Complete Primary-Key Set Comparison:** Compares the full ordered primary-key set (`s_all_keys == t_all_keys`) across every row of every table (not merely the 10-key display sample stored in `source_sample_keys` / `target_sample_keys`). A mismatch at the 11th or any subsequent record immediately triggers `ERR_KEY_MISMATCH_<TABLE>`.
- **Lossless Binary-Framed SHA-256 Content Comparison:** `compute_table_content_hash()` computes a deterministic, type-preserving, length-prefixed SHA-256 digest over the expected columns and rows in caller-supplied order. Each value is framed as a 1-byte ASCII tag, an 8-byte big-endian length, and the payload: `NULL` `N`, `INTEGER` `I` (decimal ASCII; `bool` encoded as 0/1), `REAL` `R` (IEEE-754 big-endian binary64 via `struct.pack(">d")`, no rounding), `TEXT` `T` (UTF-8; values that cannot be encoded fail closed with `ContentHashError`), `BLOB` `B` (raw bytes). No `-0.0` canonicalization is performed and `NaN`/`Infinity` are not rejected; they are hashed by their binary64 bit patterns. Unsupported value types and missing columns fail closed. Unlike string/JSON normalization, this preserves exact distinctions between `None` vs `"__NULL__"`, integer `6` vs string `"6"`, nearby floats (`0.1234561` vs `0.1234562`), and whitespace/JSON formatting differences.
- **Schema Migration Metadata Parity Requirements:** Because `schema_migrations` (`version`, `applied_at`, `description`) is reconciled as one of the 15 schema-v6 tables via full key and lossless content hashing, both `source_repo` and `target_repo` must have identical migration history rows (including `version`, `applied_at` timestamp, and `description`) in addition to `MAX(version) >= 6`. Any divergence in migration metadata produces `ERR_CONTENT_MISMATCH_SCHEMA_MIGRATIONS`.
- **Current Full-Result-Set Memory Limitation:** `StagingCutoverReconciler.reconcile()` currently fetches the full primary-key list (`SELECT <pk> FROM <table> ORDER BY <pk> ASC`) and full table row set (`SELECT <cols> FROM <table> ORDER BY <pk> ASC`) into memory via `execute_read_query()` to compute complete key parity and `compute_table_content_hash()`. The preflight recovery-state JSON integrity check also currently loads all checkpoint JSON values into memory so it does not silently ignore entries beyond a sample window. This in-memory design is intended for bounded offline staging fixtures and small-to-moderate datasets; large production datasets would require streaming/chunked cursor pagination in a future phase.

### 4.2. Migration Abort Conditions
Cutover readiness is immediately ABORTED if:
- `ERR_PREFLIGHT_FAILED`: Target database fails preflight checks.
- `ERR_SOURCE_VERSION` / `ERR_TARGET_VERSION` / `ERR_VERSION_CHECK_FAILED`: Source or target schema version `< 6` or version query fails.
- `ERR_COUNT_MISMATCH_<TABLE>`: Record counts differ between source and target for any of the 15 tables.
- `ERR_KEY_MISMATCH_<TABLE>`: Complete primary-key sets differ between source and target for any of the 15 tables (even when row counts and the first 10 sample keys match).
- `ERR_CONTENT_MISMATCH_<TABLE>`: Lossless SHA-256 content hashes differ between source and target for any of the 15 tables.
- `ERR_TABLE_INSPECT_<TABLE>`: Any query or content-hashing exception occurs during table reconciliation (failing closed with only the exception class name).
- `ERR_RECONCILIATION_NOT_SAFE`: Composite reconciliation safety check (`is_safe_to_cutover`) is false.
- `ERR_NO_VERIFIED_BACKUP`: Verified encrypted backup is not present prior to cutover.
- `ERR_NO_ROLLBACK_PLAN`: Rollback restore point is unavailable.
- `ERR_COST_INVARIANT_VIOLATION`: Any non-zero cost is detected (\$0.00 / 0 TRY violation).
- `ERR_NO_PO_SIGNOFF`: Explicit Product Owner sign-off is absent.

### 4.3. Multi-Factor Go/No-Go Gate (Offline Planning Only)
The `GoNoGoGate.evaluate()` function requires affirmative verification on all 7 criteria (`all_preflights_passed`, `schema_compatible`, `data_reconciliation_passed` via `cutover_report.is_safe_to_cutover`, `backup_verified`, `rollback_ready`, `zero_cost_verified`, `product_owner_signed_off`, and zero `abort_reasons`) before emitting a `GO` decision (`can_proceed=True`). Any reconciliation failure (count, complete key set, lossless content hash, version, or table inspection error) is propagated into `decision.abort_reasons` and forces an immediate `NO-GO`. Emitting `can_proceed=True` is strictly an offline planning readiness artifact and **never** authorizes live cloud cutover.

---

## 5. Schedule and Budget Constraints (Europe/Istanbul)
The approved scheduling architecture remains inactive in this phase, but is codified as the future operating model:
- **Timezone:** Europe/Istanbul (`UTC+03:00`).
- **RSS Discovery:** Every 3 hours starting at 00:17 (e.g., 00:17, 03:17, 06:17, 09:17, 12:17, 15:17, 18:17, 21:17).
- **Primary Editor Digest Windows:** 09:30, 15:30, and 21:30.
- **Admin Operational Health Report:** Nightly around 22:00.
- **Quiet Hours:** 00:00–08:30 (breaking news with score >= 95 allowed if configured; normal alerts deferred or held).
- **Execution Policy:** No catch-up execution upon runner restart.
- **Budget Invariant:** 0 TRY / \$0.00 operational cost.
