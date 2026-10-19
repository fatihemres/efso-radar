# Efso Radar — Phase 5B.3 Cloud Automation Safety Architecture & Persistent State Contracts

**Document Version:** 1.0.0  
**Phase:** 5B.3B  
**Author:** Senior Software Architect, Security Engineer, Database Reliability Engineer, and QA Lead  
**Status:** IMPLEMENTED & VERIFIED (OFFLINE TESTABLE)  
**Baseline main SHA:** `1efaa1b83921a1dcf31d9311608493c8aabb00f6`  

---

## 1. Executive Summary & Purpose

Efso Radar is transitioning from manual, workstation-bound execution toward unattended zero-cost cloud automation on GitHub Actions while preserving strict zero-cost operation (**0 TRY / $0.00 ongoing cloud spending**).

Ephemeral cloud runners possess no persistent local disks; each scheduled run starts in a fresh container and terminates immediately upon completion. To run reliably without duplicate notifications, state corruption, or silent failures, Efso Radar requires:
1. **Zero Unsafe Fallbacks:** Modular execution failures must fail closed with nonzero exit codes; automatic fallbacks to legacy unisolated code or silent fallbacks from cloud to SQLite are completely eliminated.
2. **Persistent State Contracts (Schema v6):** Database-enforced persistence for recipient outboxes, in-flight recovery states, daily admin reports, experiment token budgets, and scheduled execution audit logs across both local SQLite and remote Turso (libSQL).
3. **Database-Enforced Atomic Concurrency:** A single atomic SQL conditional statement replacing vulnerable multi-step TOCTOU checks, paired with worker ID fencing.
4. **Durable Delivery Protocol & Idempotency:** Role-segregated delivery (`PRIMARY_EDITOR` as sole editorial recipient, `ADMIN` as technical recipient), with `DELIVERY_UNCERTAIN` safety gates and unique outbox constraints.
5. **Timezone & Window Safety:** Canonical Europe/Istanbul (UTC+3) in-application date/time validation preventing stale or out-of-window message replays.
6. **Disaster Recovery Isolation:** Strict preservation of the offline SQLite backup boundary established in Phase 5B.2B with `execute_production_cutover` unconditionally locked out.

---

## 2. Elimination of Unsafe Legacy Fallbacks & Fail-Closed Policy

### 2.1 Removal of Broad Exception Fallback in `src/radar.py`
In legacy releases, a broad exception handler caught modular orchestrator errors and silently attempted to fall back to the monolithic legacy runner:
```python
# PREVIOUS UNGUARDED BEHAVIOR (ELIMINATED):
except Exception as exc:
    LOG.warning("Modular orchestrator encountered error (%s); falling back to legacy runner", exc)
```
**Remediation:** This block has been completely eliminated. Any error encountered by `RadarOrchestrator` now immediately raises an exception and exits with code 1. No exception can silently trigger legacy delivery or mutate unisolated state.

### 2.2 Live Execution Ban on `--legacy`
The monolithic legacy runner (`--legacy`) was designed for single-file development and lacks multi-recipient routing, claim leases, and SQLite/Turso parity.
- Programmatic invocations (`radar.run(use_legacy=True, dry_run=False)`) unconditionally raise `RuntimeError("--legacy mode cannot be run live; it is strictly restricted to --dry-run")`.
- CLI invocations (`python src/radar.py --legacy`) without `--dry-run` are rejected during argument parsing (`parser.error(...)`) with exit code 2.
- `--legacy` is only permissible when combined with `--dry-run`.
- `--demo` and `--dry-run` continue working identically with ephemeral database isolation.

### 2.3 Explicit Cloud Mode Configuration Enforcement
When intended cloud mode is requested via `EFSO_STORAGE_MODE=turso` (or `storage_mode="turso"`):
- Both `TURSO_DATABASE_URL` and `TURSO_AUTH_TOKEN` must be present and valid.
- If either or both credentials are missing or empty, configuration validation immediately raises `ValueError` and execution fails closed.
- It is architecturally prohibited from silently falling back to local SQLite when cloud persistence was requested.

---

## 3. Persistent State Contracts (Schema v6 Specification)

Database migration step 6 (`CURRENT_SCHEMA_VERSION = 6`) defines five portable tables supported identically on SQLite and Turso:

### 3.1 `telegram_delivery_records` (Durable Outbox & Delivery Ledger)
Tracks every message scheduled or dispatched per recipient to enforce at-most-once delivery.
```sql
CREATE TABLE IF NOT EXISTS telegram_delivery_records (
    delivery_id TEXT PRIMARY KEY,
    message_key TEXT NOT NULL,
    recipient_role TEXT NOT NULL,
    recipient_chat_id TEXT NOT NULL,
    category TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    error_message TEXT,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    sent_at TEXT,
    CONSTRAINT uq_recipient_message UNIQUE (recipient_chat_id, message_key)
);
CREATE INDEX IF NOT EXISTS idx_telegram_delivery_records_status
    ON telegram_delivery_records (status, updated_at);
CREATE INDEX IF NOT EXISTS idx_telegram_delivery_records_role
    ON telegram_delivery_records (recipient_role, created_at);
```
- `uq_recipient_message UNIQUE (recipient_chat_id, message_key)` guarantees that duplicate attempts to broadcast the same message or digest to the same recipient are rejected at the database level.
- `status`: Supports `PENDING`, `DISPATCHING`, `SENT`, `FAILED`, and `DELIVERY_UNCERTAIN`.

### 3.2 `durable_recovery_state` (Checkpoint Recovery)
Allows in-flight batch discovery, evaluation, or outbox dispatching to survive runner interruptions or unexpected crashes.
```sql
CREATE TABLE IF NOT EXISTS durable_recovery_state (
    state_key TEXT PRIMARY KEY,
    stage TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    batch_id TEXT,
    last_processed_id TEXT,
    state_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_durable_recovery_stage
    ON durable_recovery_state (stage, updated_at);
```

### 3.3 `admin_operational_reports` (Daily Technical Health Reports)
Enforces at-most-once dispatch of the 22:00 Europe/Istanbul administrative operational report.
```sql
CREATE TABLE IF NOT EXISTS admin_operational_reports (
    report_date TEXT PRIMARY KEY,
    dispatched_at TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    status TEXT NOT NULL,
    recipient_chat_id TEXT NOT NULL,
    summary_json TEXT NOT NULL
);
```
- `report_date` is the calendar date (`YYYY-MM-DD`) in Europe/Istanbul timezone.
- Being the primary key, any second dispatch attempt on the same calendar day fails or is ignored without duplicate Telegram messages.

### 3.4 `experiment_budget_history` (Durable AI Token & Cost Accounting)
Preserves AI consumption across ephemeral runner invocations to enforce the $1.20 monthly ceiling.
```sql
CREATE TABLE IF NOT EXISTS experiment_budget_history (
    record_id TEXT PRIMARY KEY,
    period_month TEXT NOT NULL,
    run_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    calls_count INTEGER NOT NULL DEFAULT 0,
    prompt_tokens INTEGER NOT NULL DEFAULT 0,
    completion_tokens INTEGER NOT NULL DEFAULT 0,
    total_tokens INTEGER NOT NULL DEFAULT 0,
    estimated_cost_usd REAL NOT NULL DEFAULT 0.0
);
CREATE INDEX IF NOT EXISTS idx_budget_history_period
    ON experiment_budget_history (period_month, timestamp);
```

### 3.5 `scheduled_execution_history` (Runner Audit Log)
Maintains audit trail of all automated executions.
```sql
CREATE TABLE IF NOT EXISTS scheduled_execution_history (
    run_id TEXT PRIMARY KEY,
    run_type TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    articles_collected INTEGER NOT NULL DEFAULT 0,
    candidates_evaluated INTEGER NOT NULL DEFAULT 0,
    notifications_dispatched INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);
CREATE INDEX IF NOT EXISTS idx_scheduled_execution_started
    ON scheduled_execution_history (started_at);
```

---

## 4. Database-Enforced Atomic Concurrency Control (Run Locks)

### 4.1 Vulnerability of Legacy Concurrency Pattern
Previously, run locking used a two-step `SELECT` followed by conditional `UPDATE` or `INSERT`. In concurrent cloud environments, two runner jobs triggered simultaneously could both read that no lock existed and both execute `INSERT`/`UPDATE`, resulting in a race condition (TOCTOU).

### 4.2 Single-Statement Atomic Conditional Execution
Both `SqliteStateRepository` and `TursoStateRepository` now execute a single atomic SQL statement:
```sql
INSERT INTO run_locks (lock_key, worker_id, acquired_at, expires_at, heartbeat_at)
VALUES ('radar_cron', ?, ?, ?, ?)
ON CONFLICT(lock_key) DO UPDATE SET
    worker_id = excluded.worker_id,
    acquired_at = excluded.acquired_at,
    expires_at = excluded.expires_at,
    heartbeat_at = excluded.heartbeat_at
WHERE run_locks.expires_at < ? OR run_locks.worker_id = ?;
```
**Mechanism:**
1. **Fresh lock:** If no row with `lock_key='radar_cron'` exists, `INSERT` succeeds (`affected_row_count == 1`).
2. **Expired lock:** If an existing lock has expired (`run_locks.expires_at < now`), the `WHERE` clause evaluates to true, atomically transferring lock ownership (`affected_row_count == 1`).
3. **Re-entrant lock:** If the same worker re-acquires (`run_locks.worker_id = worker_id`), the `WHERE` clause evaluates to true (`affected_row_count == 1`).
4. **Collision with active lock:** If another worker owns an unexpired lock, the `WHERE` condition fails, no update occurs, and the statement affects 0 rows (`affected_row_count == 0`), returning `False` without race conditions.

### 4.3 Worker ID Fencing on Release and Renewal
To prevent stale workers whose lease expired from disrupting a new lock owner:
- **Fenced Release:**
  ```sql
  DELETE FROM run_locks WHERE lock_key = 'radar_cron' AND worker_id = ?;
  ```
  If another worker has taken over the expired lock, the stale worker's `DELETE` matches 0 rows and does NOT delete the new owner's lock.
- **Fenced Renewal:**
  ```sql
  UPDATE run_locks
  SET expires_at = ?, heartbeat_at = ?
  WHERE lock_key = 'radar_cron' AND worker_id = ? AND expires_at >= ?;
  ```
  Only the active, non-expired owner can extend the lock lease.

### 4.4 Downstream Write Fencing & Epoch Limitations
In distributed system theory, a database lock guarantees mutual exclusion on the lock resource itself. However, if a worker experiences a stop-the-world process pause (e.g., long garbage collection, VM freeze, or asynchronous network partition) exceeding its lease timeout:
1. Another worker will rightfully acquire the expired lock.
2. If the paused worker resumes execution without re-checking its lease, it could theoretically attempt downstream side-effects (such as dispatching an HTTP POST to Telegram Bot API).
3. **Efso Radar Architectural Mitigations:**
   - **Outbox Database Deduplication:** `telegram_delivery_records` enforces `UNIQUE(recipient_chat_id, message_key)`. Even if two workers attempt to record or dispatch, the database layer physically rejects duplicates.
   - **Pre-Dispatch Lease Heartbeat Verification:** Workers re-verify lock ownership immediately before external side-effects.
   - **Short-Lived Leases with Active Renewal:** Runner batches maintain conservative lease periods with active heartbeats.
   - **Fencing Epoch / Token Guidance:** For future multi-region orchestrators with external storage writes, monotonic fencing tokens (incrementing integer epochs passed to all downstream operations) are required if external systems support fencing tokens. For Telegram Bot API (which does not support custom fencing headers), outbox message key uniqueness serves as the ultimate idempotency barrier.

---

## 5. Durable Telegram Delivery Protocol & Safety Gates

### 5.1 Recipient Role Segregation
- **`PRIMARY_EDITOR` (Baş Editör):** The sole recipient of routine editorial notifications (`EDITORIAL_DIGEST`, `VERIFIED_BREAKING`, `SENSITIVE_EDITORIAL_REVIEW`, `HOLD_FOR_VERIFICATION`).
- **`ADMIN` (Yönetici / Teknik Sahip):** Receives administrative operational health reports (`ADMIN_HEALTH_REPORT` / `OPERATIONAL_HEALTH_REPORT`). ADMIN is strictly excluded from editorial broadcasts.
- **Fail-Closed Routing:** If `PRIMARY_EDITOR` is missing or not allowlisted, editorial delivery aborts cleanly without sending to ADMIN (unless `legacy_editorial_fallback` is explicitly configured).

### 5.2 `DELIVERY_UNCERTAIN` Safety Gate
If an HTTP request times out, disconnects, or drops while communicating with the Telegram Bot API:
- The delivery record is marked with `status='DELIVERY_UNCERTAIN'`.
- It is recorded with timestamp and error message.
- Automatic retry loops strictly exclude `DELIVERY_UNCERTAIN` records from blind re-dispatch.
- This prevents duplicate messages when the Telegram server received the message but the client did not receive the HTTP 200 OK acknowledgment.

### 5.3 Legal Delivery Transition State Machine & Non-Regression Invariant
To prevent state regressions where a previously sent or uncertain message is inadvertently downgraded to `PENDING` or `FAILED` via retries or upserts:
- **State Transition Graph:**
  - `PENDING -> {DISPATCHING, SENT, DELIVERY_UNCERTAIN, FAILED}`
  - `DISPATCHING -> {SENT, DELIVERY_UNCERTAIN, FAILED, PENDING}`
  - `FAILED -> {PENDING, DISPATCHING, SENT}`
  - `DELIVERY_UNCERTAIN -> {}` (Generic updates/upserts cannot transition to SENT; automated flows cannot transition uncertain deliveries)
  - `SENT -> {}` (Terminal immutable state; replays with identical payload are idempotent no-ops)
- **Illegal Regressions Raised as `DeliveryStatusRegressionError`:**
  - `SENT -> PENDING` / `SENT -> FAILED` / `SENT -> DISPATCHING`: Strictly forbidden.
  - `DELIVERY_UNCERTAIN -> PENDING` / `DELIVERY_UNCERTAIN -> FAILED`: Strictly forbidden.
  - Generic `DELIVERY_UNCERTAIN -> SENT`: Forbidden in `update_telegram_delivery_status` and `record_telegram_delivery`.
- **Restricted Operator Reconciliation Protocol (`reconcile_uncertain_delivery`):**
  - **Operator-Supplied Metadata Only:** Metadata strings (`reconciliation_method`, `verified_by`, `proof_reference`, `proof_notes`) represent operator-supplied assertions only, not independently verified cryptographic proof.
  - **Automated Flow Lockout:** Unattended cron runs, retry loops, and dispatch workers are strictly barred from calling reconciliation.
  - **Live Operation Lockout:** In live production (`live_mode=True`), automatic reconciliation remains permanently disabled during Phase 5B.3B because independent cryptographic verification is not implemented.
  - **Explicit Authorization Barrier:** Calling `reconcile_uncertain_delivery` requires explicit operator authorization (`operator_authorized=True`). Empty or fabricated metadata strings alone do not authorize delivery-state transitions.
  - **Audit Trail & Evidence Preservation:** When authorized, `original_uncertainty_evidence` (`error_message`, `marked_uncertain_at`) and `operator_reconciliation_metadata` are permanently recorded in `payload_json`.
  - **Future Procedure Requirements:** Future milestones enabling live reconciliation must define a formal multi-party operator approval ceremony, independent API verification checks (e.g. Bot API update offset query logs), and immutable audit logs.
- **Identity Immutability (`DeliveryIdentityConflictError`):**
  - Attempting to overwrite an existing `(recipient_chat_id, message_key)` record with a differing `recipient_role` or `category` raises `DeliveryIdentityConflictError`. Raw recipient chat IDs are masked (`87...78`) in exception text.
- **Database-Level Guarded UPSERT:**
  ```sql
  ON CONFLICT(recipient_chat_id, message_key) DO UPDATE SET ...
  WHERE telegram_delivery_records.status NOT IN ('SENT', 'DELIVERY_UNCERTAIN');
  ```
  Both SQLite and Turso databases physically enforce this invariant, guaranteeing that generic operations can never alter or overwrite either `SENT` or `DELIVERY_UNCERTAIN` records.

### 5.4 Immutable Budget History & Collision-Resistant Event IDs
The AI consumption ledger (`experiment_budget_history`) tracks Gemini token usage and estimated USD expenditure:
- **Full Run ID Preservation:** Event IDs use full collision-resistant keys `rec_id = f"bud_{period_month}_{clean_run}"` (replacing legacy 16-character truncation).
- **Strict INSERT Policy (No `INSERT OR REPLACE`):**
  Existing records are never overwritten silently.
- **Idempotent Exact Replays:** If a record with `rec_id` already exists and all metrics (`calls_count`, `prompt_tokens`, `completion_tokens`, `total_tokens`, `estimated_cost_usd`) match exactly, execution returns `True` as a safe idempotent no-op.
- **Conflict Fail-Closed (`BudgetHistoryConflictError`):** If a record with `rec_id` exists but contains differing metrics or cost, `BudgetHistoryConflictError` is raised immediately.
- **Original Timestamp Immutability:** Historical event timestamps are preserved exactly; `utc_now()` is never substituted for existing records.

---

## 6. Zero-Cost, Quiet-Hours & Timezone Safety

### 6.1 Canonical Europe/Istanbul Timezone (UTC+3)
Cloud runners run in UTC. All scheduling logic localizes explicitly to `Europe/Istanbul` (permanent UTC+3 without DST).
- `get_istanbul_timezone()` provides fallback to `timezone(timedelta(hours=3))` on platforms without tzdata.
- `to_istanbul_date()` formats dates as `YYYY-MM-DD` in Turkish standard time.

### 6.2 In-Application Window Validation & Non-Catch-Up Policy
- `is_admin_report_window(current_time, window_start="21:30", window_end="22:30")` validates that the daily admin operational report is only dispatched within its 22:00 TRT window.
- If a GitHub Actions workflow triggers outside this window (due to runner queues or late triggers), the application refuses to send the report.
- **Missed Run Policy:** Missed daily reports are never replayed or caught up during subsequent runs.

---

## 7. Disaster Recovery Boundaries & Production Isolation

### 7.1 Separation from Phase 5B.2B Offline SQLite Backups
- Phase 5B.2B established the offline encrypted backup and verified restore architecture (`.efbk` assets) for local SQLite and JSON ledgers.
- Remote Turso databases operate in cloud infrastructure. Remote Turso snapshots are distinct from local workstation backups.
- Phase 5B.3B updates the backup verification engine to dynamically support `CURRENT_SCHEMA_VERSION = 6` while preserving all offline safety guarantees.

### 7.2 Permanent Lockout of Production Cutover
In `src/efso_radar/backup/restore.py`:
`execute_production_cutover` remains permanently and unconditionally locked:
```python
raise RestoreVerificationError(
    "execute_production_cutover is permanently and unconditionally DISABLED during Phase 5B offline development."
)
```
Live database replacement or cutover is strictly prohibited.

### 7.3 Backwards-Compatible Verification Policy for Schema v5 & v6 Backups
To prevent historical encrypted backups created during Phase 5B.2B (which carry schema version 5) from failing validation under the schema version 6 codebase, the verification subsystem enforces:
- `SUPPORTED_BACKUP_SCHEMA_VERSIONS = {5, 6}`: `verify_restore_archive()`, `verify_staging_database()`, and `verify_inventory()` accept any backup whose authenticated manifest schema version is within the supported set.
- **Strict Internal Version Parity:** The database contained inside the archive must match the manifest's declared schema version (`db_version == manifest_version`).
- **Zero Ciphertext Rewrite:** Historical `.efbk` releases on GitHub remain immutable; no re-encryption or retroactive migration is performed on cold backup assets.

---

## 8. Lossless Synthetic Fixture Migration Guide

The ledger migration module (`src/efso_radar/repositories/migration_ledger.py`) provides `migrate_ledgers_to_repository()` with the following guarantees:
1. **Strict Two-Phase Architecture & Non-SQLite Write Lockout:**
   - **Phase 1 (Pre-Flight Validation):** Validates all files, records, timestamps, roles, categories, and conflict checks entirely in memory. Preflight read-only checks are safely supported across repository contracts (including simulated/cloud repos). If any record fails or conflict occurs, or in `dry_run=True`, execution halts with 0 database writes.
   - **Phase 2 (Transactional Execution on SQLite Only):** Live migration for non-SQLite repositories (such as production Turso) is **NOT IMPLEMENTED** in Phase 5B.3B and fails closed with 0 writes, requiring separately approved transactional cloud integration. On SQLite, writes are applied inside a single atomic transaction block (`with repo._connection() as conn:`).
2. **Strict INSERT-Only Application & Rollback on Collision:**
   - Preflight-approved records are applied using strict `INSERT INTO` statements without `ON CONFLICT DO UPDATE`.
   - Migration never overwrites an existing delivery identity or changes recipient roles, categories, payloads, or evidence.
   - If a concurrent or unexpected identity collision occurs during application, SQLite raises an integrity error, the entire transaction is rolled back, and 0 partial writes persist.
3. **Strict Timestamp Immutability:** Historical records must contain valid ISO timestamps. Timestamps are never invented or defaulted to `utc_now()`, and budget records never invent day-1 timestamps (`f"{period}-01T00:00:00+00:00"`). Records missing timestamps fail closed.
4. **Accurate Historical Status Mapping:**
   - `DELIVERY_SUCCESS`, `SENT`, `SUCCESS` -> `DeliveryStatus.SENT`
   - `DELIVERY_UNCERTAIN` -> `DeliveryStatus.DELIVERY_UNCERTAIN`
   - `FAILED` -> `DeliveryStatus.FAILED`
   - `HELD_QUIET_HOURS` -> `DeliveryStatus.HELD_QUIET_HOURS` (preserved accurately, never collapsed into FAILED)
   - `PENDING` -> `DeliveryStatus.PENDING`
   - Unknown statuses fail validation immediately and are never silently converted.
5. **Authoritative Evidence for Roles & Categories:**
   - Recipient roles and notification categories must be authoritatively present in the source record.
   - Missing roles are never defaulted to `PRIMARY_EDITOR`.
   - Missing categories are never defaulted to `EDITORIAL_DIGEST`.
   - Missing required fields immediately fail validation closed.
6. **Complete Conflict & Duplicate Detection:**
   - Compares identity, status, role, category, timestamp, error message, and payload evidence.
   - Identical status alone is insufficient to skip; any mismatch in timestamp, payload, role, or category is flagged as a conflict.
7. **Comprehensive Recipient Chat ID Privacy:**
   - All raw Telegram chat IDs in error messages, conflict reports, entry keys, and migration logs are sanitized and masked (`87...78`, `19...35`) via `mask_text_chat_ids()` and `mask_entry_reference()`.
   - Raw source entry keys embedding chat IDs (e.g. `8712345678:d_hash_001`) are masked before output.
8. **Pre-Flight Dry-Run Validation (`dry_run=True`):**
   Executes complete parsing, timestamp validation, alias deduplication, and database conflict checks without mutating the database, returning a structured `MigrationLedgerResult` report.

---

## 9. Verification Sign-Off & Test Matrix

Acceptance test scenarios implemented in `tests/test_phase5b3b_cloud_safety_contracts.py`:

| # | Test Scenario | Verified Safety Invariant | Result |
|---|---|---|---|
| 01 | `test_01_legacy_runner_live_execution_rejected` | Live `--legacy` fails closed with RuntimeError | PASSED |
| 02 | `test_02_legacy_runner_dry_run_zero_network_zero_ai` | `--legacy --dry-run` permitted with strictly 0 network and 0 Gemini calls | PASSED |
| 03 | `test_03_modular_runner_removes_silent_fallback` | Modular orchestrator error never falls back to legacy | PASSED |
| 04 | `test_04_cloud_mode_configuration_enforcement` | `EFSO_STORAGE_MODE=turso` fails closed without SQLite fallback | PASSED |
| 05 | `test_05_atomic_concurrency_lock_mutual_exclusion` | 2-worker race: Worker 1 succeeds, Worker 2 fails (0 affected rows) | PASSED |
| 06 | `test_06_atomic_concurrency_expired_lock_stealing` | Expired lock atomically acquired by new worker | PASSED |
| 07 | `test_07_atomic_concurrency_reentrancy` | Same worker can re-acquire and extend active lock | PASSED |
| 08 | `test_08_atomic_concurrency_stale_worker_release_fenced` | Stale worker release cannot delete new worker's lock | PASSED |
| 09 | `test_09_atomic_concurrency_stale_worker_renew_fenced` | Stale worker renew cannot extend new worker's lock | PASSED |
| 10 | `test_10_multithreaded_sqlite_concurrency_barrier` | Multi-threaded race (10 threads via Barrier): exactly 1 winner | PASSED |
| 11 | `test_11_telegram_outbox_uniqueness_and_idempotency` | `UNIQUE(recipient_chat_id, message_key)` prevents outbox duplicates | PASSED |
| 12 | `test_12_telegram_outbox_uncertain_state_safety` | `DELIVERY_UNCERTAIN` state persisted and excluded from blind retry | PASSED |
| 13 | `test_13_delivery_status_regression_protection` | `SENT` and `UNCERTAIN` cannot regress to `PENDING` or `FAILED` | PASSED |
| 14 | `test_14_admin_report_at_most_once_per_date` | Admin report dispatch permitted at most once per calendar date | PASSED |
| 15 | `test_15_admin_report_window_and_no_catchup` | Europe/Istanbul window validation; out-of-window rejected | PASSED |
| 16 | `test_16_durable_recovery_state_restart_resilience` | Checkpoint persists and recovers across runner restarts | PASSED |
| 17 | `test_17_lossless_ledger_migration_and_dryrun` | Lossless migration, strict timestamps, status mapping & dry-run | PASSED |
| 18 | `test_18_budget_ledger_immutability_and_conflict` | Full event IDs, idempotent replay, conflict rejection | PASSED |
| 19 | `test_19_backup_schema_v5_compatibility` | Historical v5 encrypted backups verify cleanly under v6 codebase | PASSED |
| 20 | `test_20_repository_parity_sqlite_and_turso` | Sqlite and Turso repositories satisfy identical atomic contracts | PASSED |

