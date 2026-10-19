# Efso Radar — Phase 5B.3C: Hermetic Cloud Persistence Integration Architecture

## 1. Executive Summary & Zero-Cost Mandate

Efso Radar Phase 5B.3C establishes the integration architecture between the pipeline orchestration layer and the cloud persistence backend (Turso libSQL over Hrana v2 protocol).

This milestone is executed under strict zero-cost and hermetic boundaries:
- **Cost**: Exactly 0.00 TRY / $0.00 USD.
- **Network Boundaries**: 100% offline testing. Zero live network calls to Turso, Telegram, Gemini, or RSS endpoints.
- **Operational Database Integrity**: The local operational database (`data/radar.db`) and local `.env` are strictly untouched and unreferenced during integration testing.
- **Fail-Closed Guarantees**: Any cloud storage configuration with missing or incomplete credentials fails closed immediately without silent fallback to SQLite or legacy runners.

---

## 2. State Mapping Matrix

Every production-relevant read and write operation is mapped to a strict persistent contract on `StateRepository`, implemented identically on both `SqliteStateRepository` and `TursoStateRepository`:

| Pipeline Stage / Entity | Primary Operation | Repository Contract Method | Database Table | Concurrency / Idempotency Rule |
| :--- | :--- | :--- | :--- | :--- |
| **Concurrency Fencing** | Run Lock Acquisition | `acquire_run_lock(worker_id, timeout_minutes)` | `run_locks` | Mutual exclusion; expired leases (>timeout) can be stolen with atomic conditional UPDATE. |
| **Concurrency Fencing** | Run Lock Release | `release_run_lock(worker_id)` | `run_locks` | Worker releases lease on completion or graceful exit. |
| **Audit Logging** | Scheduled Run Start | `record_scheduled_execution_start(run_id, run_type, worker_id, started_at)` | `scheduled_executions` | Logs ephemeral runner start with timestamp and worker metadata. |
| **Audit Logging** | Scheduled Run Finish | `record_scheduled_execution_finish(run_id, status, articles, candidates, notifs, error, finished_at)` | `scheduled_executions` | Logs execution terminal status (`SUCCESS`, `FAILED`) and pipeline metrics. |
| **Pipeline Checkpointing** | Stage Recovery State | `save_recovery_state(stage, worker_id, state_payload, batch_id)` | `durable_recovery_state` | Upsert checkpoint for pipeline stages (`INITIATED`, `DISCOVERY_COMPLETED`, `FAILED`). |
| **Pipeline Checkpointing** | Recovery State Clear | `clear_recovery_state(stage)` | `durable_recovery_state` | Cleared on successful stage/run completion. |
| **Backlog Persistence** | Save Fresh Candidates | `save_pending_candidates(candidates)` | `backlog_candidates` | Preserves un-evaluated candidates across runner restarts and feed rotation. |
| **Backlog Retrieval** | Fetch Active Backlog | `get_pending_candidates(max_age_hours, now)` | `backlog_candidates` | Loads unexpired candidates from backlog for evaluation. |
| **Article Deduplication** | Mark Articles Processed | `mark_articles_processed(articles)` | `processed_articles` | Prevents re-ingesting identical URLs/titles within 30-day window. |
| **Story Intelligence** | Query Recent Stories | `get_recent_stories(limit, days)` | `stories`, `editorial_assessments` | Supplies LLM with contextual memory of recent stories. |
| **Story Intelligence** | Record Story / Update | `record_new_story()`, `record_story_update()` | `stories`, `story_updates` | Atomic insertion of newly detected stories and developments. |
| **Notification Claims** | Claim Dispatch Lease | `claim_notification_for_dispatch(notif_id, worker_id, lease_seconds)` | `notifications` | Atomic lease acquisition prevents duplicate Telegram dispatches across concurrent workers. |
| **Notification Claims** | Claim Recovery | `recover_expired_dispatch_claims()` | `notifications` | Reclaims orphaned notification claims where worker crashed during network dispatch. |
| **Telegram Outbox** | Record Delivery Attempt | `record_telegram_delivery(delivery)` | `telegram_delivery_records` | Durable recipient-bound outbox. Protects terminal `SENT` and `DELIVERY_UNCERTAIN` statuses from regression. |
| **Telegram Outbox** | Duplicate Check | `get_telegram_delivery()`, `is_digest_delivered_to_recipient()` | `telegram_delivery_records` | Prevents duplicate sends across ephemeral runner restarts. |
| **Admin Reports** | Daily Report Window | `record_admin_report(report_date, ...)` | `admin_operational_reports` | Enforces at-most-once delivery per Europe/Istanbul calendar day (`YYYY-MM-DD`). |
| **AI Budget Guard** | Record Budget Usage | `record_budget_usage(...)` | `ai_budget_history` | Immutable audit log of API usage. Rejects conflicting replays with `BudgetHistoryConflictError`. |
| **AI Budget Guard** | Monthly Aggregation | `get_monthly_budget_summary(period_month)` | `ai_budget_history` | Aggregates tokens and cost for month across all ephemeral runners. |

---

## 3. Ephemeral Runner Lifecycle (GitHub Actions)

When executed inside ephemeral cloud runners (e.g. GitHub Actions cron), local storage is wiped on container destruction. The lifecycle preserves full state durability:

```
[GitHub Actions Runner Spins Up]
           │
           ▼
[Storage Mode Resolution] ──► EFSO_STORAGE_MODE=turso
           │                  (Fail closed if credentials missing; NO fallback to local SQLite)
           ▼
[Mutual Exclusion Lock] ────► acquire_run_lock(worker_id)
           │                  (If locked by another active runner, yield gracefully and exit 0)
           ▼
[Audit Start Log] ──────────► record_scheduled_execution_start(run_id)
           │                  save_recovery_state("RADAR_PIPELINE", stage="INITIATED")
           ▼
[Durable Claim Recovery] ───► recover_expired_dispatch_claims()
           │                  (Recovers orphaned notifications from previous crashed runners)
           ▼
[News Discovery & Backlog] ─► Fetch RSS + get_pending_candidates()
           │                  save_recovery_state("RADAR_PIPELINE", stage="DISCOVERY_COMPLETED")
           ▼
[Deduplication & Evaluation]► Check processed_articles & ai_usage_ledger batch cache
           │                  (Zero LLM tokens spent on identical batches)
           ▼
[Telegram Dispatch Gate] ───► Check telegram_delivery_records for recipient + digest hash
           │                  (Zero duplicate messages dispatched to Primary Editor or Admin)
           ▼
[Finalize Execution] ───────► clear_recovery_state("RADAR_PIPELINE")
           │                  record_scheduled_execution_finish(run_id, status="SUCCESS")
           ▼
[Release Lock & Teardown] ──► release_run_lock(worker_id)
                              (Container destroyed; Turso retains 100% durable state)
```

---

## 4. Recovery Semantics & Fault Tolerance

1. **Backlog Preservation**: Candidates discovered during scan-only runs or deprioritized in large batches are stored in `backlog_candidates`. Subsequent runners retrieve them with `get_pending_candidates()`, ensuring stories survive feed rotation.
2. **Crash During Dispatch**: If a worker claims a notification and dies mid-dispatch, `recover_expired_dispatch_claims()` resets the notification from `DISPATCHING` to `PENDING` once the lease expires (default 180s).
3. **Ambiguous Network Timeouts**: A 504 Gateway Timeout or HTTP client timeout sets the status to `DELIVERY_UNCERTAIN`. Subsequent runners or automatic retry loops are strictly barred from re-attempting delivery to prevent duplicate messages.
4. **Budget Defense**: Reconciled batches write immutable consumption records to `ai_budget_history`. Subsequent runners query `get_monthly_budget_summary()` to compute cumulative spend across all runner instances.

---

## 5. Concurrency Fencing & Mutual Exclusion

1. **Table**: `run_locks (lock_id PRIMARY KEY, worker_id, acquired_at, expires_at)`.
2. **Acquisition**:
   - `INSERT OR IGNORE` attempts to create `lock_id = 1`.
   - If row exists, conditional `UPDATE run_locks SET worker_id = ?, acquired_at = ?, expires_at = ? WHERE lock_id = 1 AND expires_at < ?`.
   - Returns `True` only if inserted or updated.
3. **Stale Lease Stealing**: If a previous runner crashed without releasing its lock, the lock becomes stealable once `utc_now() > expires_at`.
4. **Graceful Yielding**: If lock acquisition fails, the runner logs a warning and exits with status 0, preventing resource waste or conflicting database operations.

---

## 6. Pre-requisites for Future Live Cloud Activation

Before Phase 5C or live cloud deployment:
1. **Turso Database Provisioning**: Provision a dedicated Turso database in the EU region (e.g. `fra` Frankfurt).
2. **GitHub Repository Secrets**:
   - `TURSO_DATABASE_URL`: `libsql://<db-name>-<org>.turso.io`
   - `TURSO_AUTH_TOKEN`: Secure read/write bearer token.
   - `EFSO_STORAGE_MODE`: `turso`.
3. **Production Schema Verification**: Run pre-flight schema migration check (`schema_version = 6`) to confirm all 14 tables and indexes exist.
4. **Product Owner Explicit Authorization**: Production activation requires separate, written approval from the Product Owner.
