# Efso Radar — Phase 5B.3C: Hermetic Cloud Persistence Integration Architecture

## 1. Executive Summary & Zero-Cost Mandate

Efso Radar Phase 5B.3C establishes the integration architecture between the pipeline orchestration layer and the cloud persistence backend (Turso libSQL over Hrana v2 protocol).

This milestone is executed under strict zero-cost and hermetic boundaries:
- **Cost**: Exactly 0.00 TRY / $0.00 USD.
- **Network Boundaries**: 100% offline testing. Zero live network calls to Turso, Telegram, Gemini, or RSS endpoints.
- **Operational Database Integrity**: The local operational database (`data/radar.db`) and local `.env` are strictly untouched and unreferenced during integration testing.
- **Fail-Closed Guarantees**: Any cloud storage configuration with missing or incomplete credentials fails closed immediately without silent fallback to SQLite or legacy runners.

---

## 2. State Mapping Matrix (Schema v6)

Every production-relevant read and write operation is mapped to a strict persistent contract on `StateRepository`, implemented identically on both `SqliteStateRepository` and `TursoStateRepository`:

| Pipeline Stage / Entity | Primary Operation | Repository Contract Method | Database Table | Concurrency / Idempotency Rule |
| :--- | :--- | :--- | :--- | :--- |
| **Concurrency Fencing** | Run Lock Acquisition | `acquire_run_lock(worker_id, timeout_minutes, now)` | `run_locks` | Mutual exclusion; expired leases (`expires_at < now`) can be stolen with atomic conditional `INSERT ... ON CONFLICT(lock_key) DO UPDATE`. |
| **Concurrency Fencing** | Run Lock Release | `release_run_lock(worker_id)` | `run_locks` | Worker releases lease on completion or error in `finally` block. |
| **Audit Logging** | Scheduled Run Start | `record_scheduled_execution_start(run_id, run_type, worker_id, started_at)` | `scheduled_execution_history` | Logs ephemeral runner start with timestamp and worker metadata. |
| **Audit Logging** | Scheduled Run Finish | `record_scheduled_execution_finish(run_id, status, articles, candidates, notifs, error, finished_at)` | `scheduled_execution_history` | Logs execution terminal status (`SUCCESS`, `FAILED`) and pipeline metrics. |
| **Pipeline Checkpointing** | Stage Recovery State | `save_recovery_state(stage, worker_id, state_payload, batch_id)` | `durable_recovery_state` | Upsert checkpoint for pipeline stages (`INITIATED`, `DISCOVERY_COMPLETED`, `FAILED`). |
| **Pipeline Checkpointing** | Recovery State Clear | `clear_recovery_state(stage)` | `durable_recovery_state` | Cleared on successful stage/run completion. |
| **Backlog Persistence** | Save Fresh Candidates | `save_pending_candidates(candidates)` | `candidate_backlog` | Preserves un-evaluated candidates across runner restarts and feed rotation. |
| **Backlog Retrieval** | Fetch Active Backlog | `get_pending_candidates(max_age_hours, now)` | `candidate_backlog` | Loads unexpired candidates from backlog for evaluation. |
| **Article Deduplication** | Mark Articles Processed | `mark_articles_processed(articles)` | `processed_articles` | Prevents re-ingesting identical URLs/titles within 30-day window. |
| **Story Intelligence** | Query Recent Stories | `get_recent_stories(limit, days)` | `stories`, `editorial_assessments` | Supplies LLM with contextual memory of recent stories. |
| **Story Intelligence** | Record Story / Update | `record_new_story()`, `record_story_update()` | `stories`, `story_updates` | Atomic insertion of newly detected stories and developments. |
| **Notification Claims** | Claim Dispatch Lease | `claim_notification_for_dispatch(notif_id, worker_id, lease_seconds)` | `notifications` | Atomic lease acquisition prevents duplicate Telegram dispatches across concurrent workers. |
| **Notification Claims** | Claim Recovery | `recover_expired_dispatch_claims()` | `notifications` | Reclaims orphaned notification claims where worker crashed during network dispatch. |
| **Telegram Pre-Send Claim** | Atomic Outbox Claim | `claim_telegram_delivery(delivery_id, message_key, ...)` | `telegram_delivery_records` | Atomically claims delivery (`DISPATCHING`) before external HTTP dispatch. Rejects duplicates and competing workers. |
| **Telegram Outbox** | Record Delivery Terminal | `record_telegram_delivery(delivery)` | `telegram_delivery_records` | Durable recipient-bound outbox. Protects terminal `SENT` and `DELIVERY_UNCERTAIN` statuses from regression. |
| **Telegram Outbox** | Duplicate Check | `get_telegram_delivery()`, `is_digest_delivered_to_recipient()` | `telegram_delivery_records` | Prevents duplicate sends across ephemeral runner restarts. |
| **Admin Reports** | Daily Report Window | `record_admin_report(report_date, ...)` | `admin_operational_reports` | Enforces at-most-once delivery per Europe/Istanbul calendar day (`YYYY-MM-DD`). |
| **AI Budget Guard** | Record Budget Usage | `record_budget_usage(...)` | `experiment_budget_history` | Immutable audit log of API usage. Rejects conflicting replays with `BudgetHistoryConflictError`. |
| **AI Budget Guard** | Monthly Aggregation | `get_monthly_budget_summary(period_month)` | `experiment_budget_history` | Aggregates tokens and cost for month across all ephemeral runners. Fails closed on repository error. |

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
[Telegram Pre-Send Claim] ──► claim_telegram_delivery(delivery_id, message_key, ...)
           │                  (Atomic pre-send claim on telegram_delivery_records before HTTP call)
           ▼
[Telegram Dispatch Gate] ───► Post to Telegram; update telegram_delivery_records to SENT or DELIVERY_UNCERTAIN
           │                  (Zero duplicate messages dispatched to Primary Editor or Admin)
           ▼
[Finalize Execution] ───────► clear_recovery_state("RADAR_PIPELINE")
           │                  record_scheduled_execution_finish(run_id, status="SUCCESS")
           ▼
[Release Lock & Teardown] ──► release_run_lock(worker_id) [Guaranteed in finally block]
                              (Container destroyed; Turso retains 100% durable state)
```

---

## 4. Recovery Semantics & Fault Tolerance

1. **Backlog Preservation**: Candidates discovered during scan-only runs or deprioritized in large batches are stored in `candidate_backlog`. Subsequent runners retrieve them with `get_pending_candidates()`, ensuring stories survive feed rotation.
2. **Crash During Dispatch**: Before any HTTP dispatch to Telegram, `claim_telegram_delivery()` transitions the record in `telegram_delivery_records` to `DISPATCHING`. If the worker or container crashes before or during dispatch, subsequent workers observe the non-initial status and cannot claim the message, preventing duplicate external deliveries.
3. **Ambiguous Network Timeouts**: A 504 Gateway Timeout or HTTP client timeout sets the status to `DELIVERY_UNCERTAIN`. Subsequent runners or automatic retry loops are strictly barred from re-attempting delivery to prevent duplicate messages.
4. **Budget Defense**: Reconciled batches write immutable consumption records to `experiment_budget_history`. Subsequent runners query `get_monthly_budget_summary()` to compute cumulative spend across all runner instances. Read or write failures on the authoritative repository fail closed immediately.
5. **Orchestrator Lifecycle Safety**: Mutual exclusion locks acquired via `acquire_run_lock()` are strictly guarded within `try ... finally` blocks. Any failure during execution startup (`record_scheduled_execution_start`, `save_recovery_state`) releases the lock immediately, preventing 15-minute worker stalls.

---

## 5. Concurrency Fencing & Mutual Exclusion

1. **Table Schema**:
   ```sql
   CREATE TABLE IF NOT EXISTS run_locks (
       lock_key TEXT PRIMARY KEY,
       worker_id TEXT NOT NULL,
       acquired_at TEXT NOT NULL,
       expires_at TEXT NOT NULL,
       heartbeat_at TEXT NOT NULL
   );
   ```
2. **Acquisition Protocol**:
   - Primary key is `lock_key = 'radar_cron'`.
   - Uses atomic `INSERT INTO run_locks ... ON CONFLICT(lock_key) DO UPDATE SET ... WHERE run_locks.expires_at < ? OR run_locks.worker_id = ?`.
   - Returns `True` only if row was inserted or updated.
3. **Stale Lease Stealing**: If a previous runner crashed without releasing its lock, the lock becomes stealable once `utc_now() > expires_at`.
4. **Graceful Yielding**: If lock acquisition fails, the runner logs a warning and exits with status 0, preventing resource waste or conflicting database operations.

---

## 6. Implementation Scope Boundaries: Offline vs. Simulated vs. Production

To maintain strict architectural transparency and zero-cost safety:

### A. Implemented Offline Integration (Phase 5B.3C)
- Complete contract parity across `StateRepository`, `SqliteStateRepository`, and `TursoStateRepository`.
- Atomic pre-send delivery claims (`claim_telegram_delivery`) on `telegram_delivery_records`.
- Full audit lifecycle logging (`scheduled_execution_history`) and stage recovery (`durable_recovery_state`).
- Fail-closed storage mode configuration (`storage_mode="turso"` without credentials raises `ValueError`).
- Fail-closed AI budget ledger (`ExperimentBudgetGuard`) requiring authoritative repository availability.
- Strict recipient separation: `PRIMARY_EDITOR` for editorial news; `ADMIN` for operational health reporting only.

### B. Synthetic Emulation & Test Doubles
- Turso HTTP / Hrana v2 protocol client (`TursoHTTPClient`) tested using mock `httpx.BaseTransport`.
- Synthetic SQLite-backed Hrana v2 endpoints emulating transaction batons, statement pipelines, and rollbacks.
- Deterministic synthetic clock injection (`now` parameter) for simulated lease expiration and stealing tests.

### C. Known Limitations & Production Prerequisites
- **Distributed AI Budget Reservation Limitation**: Truly atomic distributed budget reservation across multi-tenant ephemeral runners is not implemented without a centralized reservation service or transaction coordinator. Therefore, **live AI execution remains strictly disabled (`AI_EVALUATION_ENABLED=False`)**, maintaining the strict 0 TRY / $0.00 expenditure mandate.
- **Production Activation Prerequisites**:
  1. Dedicated EU-hosted Turso database provisioning.
  2. Secret injection (`TURSO_DATABASE_URL`, `TURSO_AUTH_TOKEN`).
  3. Pre-flight schema v6 migration verification.
  4. Explicit Product Owner authorization before enabling live GitHub Actions schedules.
