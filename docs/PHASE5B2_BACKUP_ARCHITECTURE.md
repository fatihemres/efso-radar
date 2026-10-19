# Efso Radar — Phase 5B.2 Architecture: Encrypted Backup & Zero-Cost Retention

**Document Version:** 1.3.0  
**Status:** Approved Hardened Architecture Specification (Phase 5B.2B Final Offline Baseline)  
**Security Classification:** Public Documentation (Zero Secrets / Zero Private Identifiers)  
**Operating Budget:** STRICTLY 0 TRY / $0.00 ongoing spending  
**Anchor Release:** `v0.4.0`  

---

## 1. Executive Summary & Approved Decisions

Phase 5B.2 establishes an enterprise-grade, offline-testable, encrypted disaster recovery and retention subsystem for Efso Radar.

### Approved Design Decisions
1. **Target Storage Architecture:** Separate private GitHub backup repository (`fatihemres/efso-radar-backups` — to be provisioned later upon separate authorization).
2. **Release Structure:** **One stable rolling Release** (e.g., tag `rolling-backups`) hosting dated encrypted backup assets, rather than daily Git commits or thousands of individual release tags.
3. **Execution Schedule (Design Only):** Daily at **02:15 Europe/Istanbul (23:15 UTC)** during quiet hours, following daily digest consolidation.
4. **Retention Window (Design Only):** **14 days maximum** rolling retention.
5. **Recovery Safety Invariant:** Minimum verified recovery points rule: **Never delete the last valid backup**, and retain at least 2 verified recovery points regardless of age.
6. **Zero Monetary Expenditure:** Strict 0 TRY / $0.00 limit. No credit card requirement; no cloud billing activation.
7. **External Dependency Uncertainty:** GitHub Release asset storage quotas and provider deletion SLAs are external vendor dependencies, not immutable permanent zero-cost guarantees. Systems must monitor storage metrics and fail closed if limits change.

---

## 2. Multi-Store Consistency & Snapshot Coordination Barrier

Efso Radar maintains state across multiple stores:
1. **Relational Database (`radar.db`):** Stories, updates, candidate backlog, assessments, feedback, schema migrations, and processed articles.
2. **Outbox Delivery Ledger (`telegram_digest_delivery_ledger.json`):** Recipient-scoped delivery records (`{chat_id}:{digest_hash}`) and stable digest IDs.
3. **Audit Budget Ledger (`experiment_budget_ledger.json`):** AI usage experiment budgets, token accounting, and cost limits.

### Coordinated Snapshot Protocol & Quiescence Verification (Approach B)
Backing up stores independently without locking introduces split-brain skew:
- If `radar.db` is captured at $T_1$ and `telegram_digest_delivery_ledger.json` at $T_2$, a digest sent between $T_1$ and $T_2$ would be marked delivered in the ledger but missing from the database, or vice versa.
- **Approach B Quiescence Invariant & Writer Limitation:** In Phase 5B.2B, backups are treated strictly as an offline/quiescent operation. Prior to snapshotting, `create_backup()` explicitly executes `verify_quiescence(operational_data_dir)` and checks `TelegramDeliveryReconciliationGate.is_dispatch_blocked()`. **Critical Limitation:** The absence of active `run_locks` in SQLite is a necessary condition, but it does NOT prove that all database and JSON writers are paused (e.g. ad-hoc background scripts, unmanaged workers, or direct ledger modifications). Live unattended backups remain strictly blocked until cross-process writer coordination or externally verified quiescence is integrated into production cycles.
- To guarantee cross-store consistency, `create_backup()` executes inside an exclusive `SnapshotCoordinationBarrier`:
  - **Deadlock Avoidance & Timeout:** Uses an advisory cross-store lock with a strict 5.0s timeout. If writers cannot be paused, the backup aborts fail-closed with `BackupCoordinationTimeoutError`.
  - **Point-in-Time SQLite Snapshot:** Uses SQLite's online backup API (`sqlite3.Connection.backup()`), folds all active WAL frames into a temporary snapshot, switches `journal_mode = DELETE;` to eliminate `-wal`/`-shm` dependencies, and strips transient `run_locks`.
  - **Authoritative Schema Version:** Queries `SELECT MAX(version) FROM schema_migrations;` inside the snapshot to record the true schema version (not row counts).
  - **Multi-Store Bundle:** Bundles SQLite snapshot and validated JSON ledgers into an in-memory tarball, computes SHA-256, and encrypts the archive before releasing the lock.

```mermaid
sequenceDiagram
    participant Lock as SnapshotCoordinationBarrier
    participant SQLite as SQLite DB (radar.db)
    participant Ledgers as JSON Ledgers
    participant Backup as Backup Engine

    Backup->>Lock: Acquire exclusive coordination lock (5s timeout)
    Backup->>SQLite: Open sqlite3.Connection.backup() to temporary snapshot
    Note over SQLite: Checkpoints WAL frames atomically; copies pages
    Backup->>SQLite: Set journal_mode = DELETE & strip run_locks
    Backup->>SQLite: Query SELECT MAX(version) FROM schema_migrations
    Backup->>Ledgers: Validate and read telegram_digest_delivery_ledger.json
    Backup->>Ledgers: Validate and read experiment_budget_ledger.json
    Backup->>Backup: Package snapshot + ledgers into memory tarball
    Backup->>Backup: Compute SHA-256 of unencrypted tarball
    Backup->>Backup: Encrypt via AES-256-GCM with full header AAD binding
    Backup->>Lock: Release coordination lock
```

---

## 3. Cryptographic Envelope Specification (`.efbk`)

### 3.1 Algorithm & Standards
- **Cipher:** AES-256-GCM (Galois/Counter Mode) via Python's standard `cryptography` library (`AESGCM`).
- **Key Size:** 256 bits (32 bytes). Key rotation supported via `key_id` metadata.
- **Nonce (IV):** 96 bits (12 bytes) cryptographically random (`os.urandom(12)`), generated uniquely for each backup. **Never reused under any circumstance.**
- **Authentication Tag:** 128 bits (16 bytes) appended to ciphertext; guarantees tamper evidence.
- **Envelope Magic Bytes:** ASCII `EFBK` (`0x45 0x46 0x42 0x4B`).
- **Format Version:** `1` (uint16 big-endian).
- **Size Boundaries:** Maximum manifest size 64 KB; maximum envelope size 1 GB.

### 3.2 Envelope Binary Layout & Full Header Authentication
All binary header fields are cryptographically bound as Additional Authenticated Data (AAD) alongside the manifest JSON. Any independent tampering with magic, version, cipher ID, creation time, key ID, nonce, manifest length, manifest payload, ciphertext, or tag causes decryption to fail closed immediately:

```
+---------------+---------------+---------------------------------------+
| Offset (Bytes)| Length (Bytes)| Field Description                     |
+---------------+---------------+---------------------------------------+
| 0             | 4             | Magic Bytes: "EFBK"                   |
| 4             | 2             | Envelope Format Version (uint16 = 1)  |
| 6             | 2             | Cipher Algorithm ID (uint16 = 1: GCM) |
| 8             | 8             | Created Timestamp (int64 Unix epoch)  |
| 16            | 16            | Key Identifier (16 bytes ASCII/null)  |
| 32            | 12            | Nonce / IV (12 random bytes)          |
| 44            | 4             | AAD Manifest Length N (uint32)        |
| 48            | N             | Authenticated Manifest (UTF-8 JSON)   |
| 48 + N        | End           | Ciphertext + 16-byte GCM Auth Tag     |
+---------------+---------------+---------------------------------------+
```

$\text{Full AAD} = \text{Header Bytes (48 bytes)} + \text{Manifest Bytes (N bytes)}$

---

## 4. Post-Restore Telegram Delivery Reconciliation & Uncertainty Gate

A restored delivery ledger only contains outbox records up to the backup's creation timestamp. If a disaster recovery restores an older backup, any digest sent **after** that backup could be inadvertently resent.

To prevent duplicate deliveries:
1. **Surviving History Reconciliation:** `TelegramDeliveryReconciliationGate` inspects surviving operational outbox records. Any digests delivered after the backup timestamp are merged into the active ledger so historical hash blocks are preserved.
2. **Authoritative Continuity Verification:** The gate strictly verifies surviving records (`_verify_surviving_continuity()`). If records contain contradictory delivery outcomes (e.g. both `DELIVERY_SUCCESS` and `FAILED` recorded for the same digest hash) or unparseable timestamps, the gate immediately flags an anomaly and enters `DELIVERY_UNCERTAIN`.
3. **Durable On-Disk State:** When entering `DELIVERY_UNCERTAIN`, the gate writes durable state to disk (`.recovery_pending_reconciliation.json`). This ensures the blocked status survives worker process restarts. The gate exposes `TelegramDeliveryReconciliationGate.is_dispatch_blocked(data_dir)` for external processes to check whether dispatch is blocked before initiating any outbox actions.
4. **DELIVERY_UNCERTAIN Fail-Closed Gate:** If no surviving ledger exists to verify the gap between backup creation and recovery, the gate enters `DELIVERY_UNCERTAIN` and **blocks automated Telegram dispatch**.
5. **Explicit Operator Authorization:** Automated dispatch cannot resume until an operator audits the channel/chat state and issues an explicit authorization token via `authorize_resumption(operator_note, token)`.

---

## 5. Non-Destructive Staging & Transactional Cutover Mechanics

### 5.1 Safe Staging Path Invariants
- Default staging uses dedicated system temporary directories (`tempfile.mkdtemp(prefix="efso_restore_staging_")`).
- Arbitrary caller paths are checked: roots (`/`, `C:\`), user home directories, repository roots, and operational `data/` directories are rejected with `UnsafeStagingPathError`.
- Pre-existing non-empty caller directories are strictly refused to prevent deleting user data.
- Envelope decryption occurs **prior** to filesystem staging; wrong keys or corrupt archives never modify any filesystem directory.

### 5.2 Transactional Crash-Recovery Cutover & Simulation Isolation
Sequential file copying is not an atomic operation. Disaster recovery mechanics implement:
1. **Unconditional Production Fail-Closed:** `execute_production_cutover()` unconditionally raises `ProductionCutoverDisabledError` (even if `simulated=True`). Live operational restoration is forbidden in Phase 5B.2B.
2. **Dedicated Hermetic Offline Simulation:** Hermetic testing is routed through `simulate_offline_cutover_in_test_workspace()`. If no workspace is supplied, it creates and manages an isolated `tempfile.TemporaryDirectory` internally. If a workspace is supplied, it strictly checks workspace isolation via `validate_simulation_workspace()`, rejecting symlinks, filesystem roots, user homes, repository root, source tree, `data/` directories, any path outside system temporary storage (without relying on name substrings), and any directory containing non-fixture files.
3. **Quiescence Verification:** Ensures zero active worker run locks or active database connections before staging replacements.
4. **Pre-Cutover Backup via SQLite Backup API:** Live `radar.db` is backed up via `sqlite3.Connection.backup()` to `radar.db.pre_cutover.bak`, and JSON ledgers are copied to `.pre_cutover.bak`.
5. **Crash Journal (`.cutover_journal.json`):** Tracks each state transition (`INITIATED`, `PRE_BACKUP_COMPLETED`, `REPLACED_DB`, `REPLACED_DELIVERY_LEDGER`, `REPLACED_BUDGET_LEDGER`, `COMPLETED`, `ROLLED_BACK`).
6. **Atomic Replacement (`os.replace`):** Files are staged on the target directory and replaced atomically.
7. **Step-by-Step Rollback:** Any failure at any step immediately rolls back to pre-cutover backups.
8. **Startup Recovery:** `detect_and_recover_incomplete_cutover()` detects interrupted cutovers on startup and executes emergency rollback.

---

## 6. Verified Retention Lifecycle & Storage Budget Enforcement

1. **Full Restore Verification in Inventory:** `RetentionPolicyManager.verify_inventory()` does not rely solely on AES-GCM envelope authentication. It executes full restore verification (`verify_restore_archive()`) inside an ephemeral staging directory:
   - Validates tar extraction against Zip-Slip and bomb thresholds.
   - Validates database SQLite `PRAGMA integrity_check` and `foreign_key_check`.
   - Confirms true schema migrations version matches `expected_schema_version` (version 5).
   - Validates structure and status enumerations of both Telegram delivery and experiment budget ledgers.
   - Any asset with corrupt internal database or schema mismatch is quarantined into `issues` and **excluded** from `verified_points`.
   - Temporary staging directories are cleanly purged on both success and failure lifecycles.
2. **Inventory Fingerprinting:** Evaluates an inventory fingerprint (SHA-256 of sorted asset IDs, names, sizes, and SHA-256 digests). Before deleting any asset, the fingerprint is revalidated; if storage changed concurrently, deletion aborts with `StaleInventoryError`.
3. **Budget Conflict Resolution:** If total storage exceeds `max_storage_budget_bytes`, pruning is planned. However, if pruning would reduce verified recovery points below 2, **new uploads are halted (`upload_blocked=True`) rather than deleting backups**.
4. **Never Delete the Last Verified Backup:** Under no circumstances will the newest verified backup be deleted.

---

## 7. Privacy & Credential Isolation

- **Masked Chat IDs:** Unencrypted logs and verification reports mask all recipient chat IDs (`87...78`).
- **Zero Secrets in Repository or Backups:** `TELEGRAM_BOT_TOKEN`, `GEMINI_API_KEY`, and database credentials are never stored in backups or commits.
- **Git Exclusion:** `.gitignore` excludes `*.efbk`, `data/*.db*`, `data/*.json`, and `data/restore_staging/`.

---

## 8. Milestone Boundaries & Production Readiness Matrix

To prevent accidental operational risk, Phase 5B.2B strictly compartmentalizes capabilities into four categories:

### 8.1 Implemented & Test-Verified Offline Capabilities
- **AES-256-GCM Envelope Encryption (`.efbk`):** Full 48-byte binary header authenticated in AAD with 16-byte authentication tag, random nonces, and key rotation support.
- **Consistent SQLite Snapshotting:** Online backup API (`sqlite3.Connection.backup()`), atomic WAL frame folding, journal_mode switch to `DELETE`, and active `run_locks` purging.
- **Structural Ledger Integrity:** Strict schema validation and status enumeration checking for both `telegram_digest_delivery_ledger.json` and `experiment_budget_ledger.json`.
- **Zip-Slip & Bomb Protection:** Extraction defenses against directory traversal and tar decompression bombs (> 500 MB).
- **SQLite Engine Verification:** PRAGMA integrity check, foreign key check, migration version verification, and manifest row count comparison.
- **Non-Destructive Staging & Auto-Cleanup:** Caller path safety validation refusing root, home, repo, non-temp, and pre-existing non-empty directories. Operation-owned temporary staging is cleanly purged on both verification success and failure.
- **Full Restore Viability in Retention Inventory:** `verify_inventory()` executes full restore verification inside ephemeral staging; corrupt SQLite databases or schema mismatches are quarantined and cannot satisfy `min_recovery_points = 2`.
- **Fingerprinted Retention Lifecycle:** Enforces 14-day retention, atomic fingerprint checks against concurrent modifications, storage budget upload halting, and immutable protection for the latest verified backup.
- **Durable Telegram Uncertainty Gate:** Persists `.recovery_pending_reconciliation.json` to survive process crashes, checks surviving delivery history continuity, and blocks automated dispatch under `DELIVERY_UNCERTAIN`.
- **Zero Secrets & Chat ID Masking:** Complete redaction of credentials and recipient chat IDs (`87...78`).

### 8.2 Synthetic Test Simulations
- **`simulate_offline_cutover_in_test_workspace()`:** Transactional cutover simulation confined strictly to internally owned temporary directories or strictly validated fixtures in system temporary storage (without relying on name substring matching). Rejects symlinks, repository locations, user folders, and non-fixture files. Verifies quiescence, pre-cutover backups, `.cutover_journal.json`, atomic `os.replace`, and verified step-by-step rollback.
- **Startup Crash Recovery:** `detect_and_recover_incomplete_cutover()` simulating rollback of interrupted cutover states.
- **`MockGitHubReleaseStorageAdapter`:** In-memory hermetic simulation of GitHub Release asset operations, upload limits, and inventory fingerprinting.

### 8.3 Not-Yet-Integrated / Blocked Production Mechanisms
- **Production Cutover Entry Point:** `execute_production_cutover()` unconditionally raises `ProductionCutoverDisabledError`. Live production restoration is forbidden and blocked at runtime.
- **Unattended Live Multi-Store Backups:** Approach B is enforced; `create_backup()` verifies quiescent state and checks dispatch blocks before snapshotting. **Writer Limitation:** The absence of active `run_locks` does not prove all database and JSON writers are paused. Unattended backups during concurrent write traffic remain strictly blocked until cross-process writer coordination or externally verified quiescence is embedded into application run loops.
- **Telegram Dispatcher Recovery Wiring:** `TelegramDeliveryReconciliationGate.is_dispatch_blocked()` is provided, but runtime wiring into active Telegram notification channels is deferred to production deployment.

### 8.4 Future Production Prerequisites (Prior to Live Activation)
1. **GitHub Backup Repository Provisioning:** Create private repository `fatihemres/efso-radar-backups` with scoped Fine-Grained Personal Access Token (`contents:write` on release assets).
2. **Key Injection:** Inject high-entropy 32-byte key via `EFSO_BACKUP_ENCRYPTION_KEY` environment secret into deployment environment.
3. **Dispatcher Gating:** Embed `TelegramDeliveryReconciliationGate.is_dispatch_blocked()` check in `TelegramNotificationChannel.send()` and `send_digest_batch()`.
4. **Writer Coordination Wiring:** Coordinate backup execution with quiet hours worker lifecycle to guarantee quiescence before taking snapshots.
5. **PO Approval & Live Disaster Recovery Drill:** Supervised end-to-end drill in staging environment before activating daily automated cron.

