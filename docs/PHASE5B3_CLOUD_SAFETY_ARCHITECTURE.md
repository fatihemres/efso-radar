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

---

## 8. Synthetic Fixture Migration Guide (JSON Ledgers to Database Tables)

The ledger migration module (`src/efso_radar/repositories/migration_ledger.py`) provides `migrate_ledgers_to_repository()`:
1. **Source Files:** Reads `telegram_digest_delivery_ledger.json` and `experiment_budget_ledger.json`.
2. **Idempotent Insertion:** Checks `repo.get_telegram_delivery(chat_id, message_key)` before inserting.
3. **Conflict Detection:** If an entry already exists with a conflicting delivery status, logs a conflict in `MigrationLedgerResult.conflicts_detected`.
4. **Zero State Mutation on Conflict:** Re-running migration produces 0 duplicates.

---

## 9. Verification Sign-Off & Test Matrix

All 16 acceptance test scenarios implemented in `tests/test_phase5b3b_cloud_safety_contracts.py` pass cleanly:

| # | Test Scenario | Verified Safety Invariant | Result |
|---|---|---|---|
| 01 | `test_01_legacy_runner_live_execution_rejected` | Live `--legacy` fails closed with RuntimeError | PASSED |
| 02 | `test_02_legacy_runner_dry_run_allowed` | `--legacy --dry-run` permitted | PASSED |
| 03 | `test_03_modular_runner_removes_silent_fallback` | Modular orchestrator error never falls back to legacy | PASSED |
| 04 | `test_04_cloud_mode_configuration_enforcement` | `EFSO_STORAGE_MODE=turso` fails closed without SQLite fallback | PASSED |
| 05 | `test_05_atomic_concurrency_lock_mutual_exclusion` | 2-worker race: Worker 1 succeeds, Worker 2 fails (0 affected rows) | PASSED |
| 06 | `test_06_atomic_concurrency_expired_lock_stealing` | Expired lock atomically acquired by new worker | PASSED |
| 07 | `test_07_atomic_concurrency_reentrancy` | Same worker can re-acquire and extend active lock | PASSED |
| 08 | `test_08_atomic_concurrency_stale_worker_release_fenced` | Stale worker release cannot delete new worker's lock | PASSED |
| 09 | `test_09_atomic_concurrency_stale_worker_renew_fenced` | Stale worker renew cannot extend new worker's lock | PASSED |
| 10 | `test_10_telegram_outbox_uniqueness_and_idempotency` | `UNIQUE(recipient_chat_id, message_key)` prevents outbox duplicates | PASSED |
| 11 | `test_11_telegram_outbox_uncertain_state_safety` | `DELIVERY_UNCERTAIN` state persisted and excluded from blind retry | PASSED |
| 12 | `test_12_admin_report_at_most_once_per_date` | Admin report dispatch permitted at most once per calendar date | PASSED |
| 13 | `test_13_admin_report_window_and_no_catchup` | Europe/Istanbul window validation; out-of-window rejected | PASSED |
| 14 | `test_14_durable_recovery_state_restart_resilience` | Checkpoint persists and recovers across runner restarts | PASSED |
| 15 | `test_15_ledger_migration_utility_idempotency_and_conflict` | JSON ledger migration idempotent with conflict detection | PASSED |
| 16 | `test_16_repository_parity_sqlite_and_turso` | Sqlite and Turso repositories satisfy identical atomic contracts | PASSED |
