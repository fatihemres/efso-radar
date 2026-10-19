# Efso Radar — Phase 5B.2 Architecture: Encrypted Backup & Zero-Cost Retention

**Document Version:** 1.2.0  
**Status:** Approved Hardened Architecture Specification (Phase 5B.2B Implementation Baseline)  
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

### Coordinated Snapshot Protocol
Backing up stores independently without locking introduces split-brain skew:
- If `radar.db` is captured at $T_1$ and `telegram_digest_delivery_ledger.json` at $T_2$, a digest sent between $T_1$ and $T_2$ would be marked delivered in the ledger but missing from the database, or vice versa.
- To prevent skew, `create_backup()` executes inside an exclusive `SnapshotCoordinationBarrier`:
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
2. **DELIVERY_UNCERTAIN State:** If no surviving ledger exists to verify the gap between backup creation and recovery, the gate enters `DELIVERY_UNCERTAIN` and **blocks automated Telegram dispatch**.
3. **Explicit Operator Authorization:** Automated dispatch cannot resume until an operator audits the channel/chat state and issues an explicit authorization token.

---

## 5. Non-Destructive Staging & Transactional Cutover Mechanics

### 5.1 Safe Staging Path Invariants
- Default staging uses dedicated system temporary directories (`tempfile.mkdtemp(prefix="efso_restore_staging_")`).
- Arbitrary caller paths are checked: roots (`/`, `C:\`), user home directories, repository roots, and operational `data/` directories are rejected with `UnsafeStagingPathError`.
- Pre-existing non-empty caller directories are strictly refused to prevent deleting user data.
- Envelope decryption occurs **prior** to filesystem staging; wrong keys or corrupt archives never modify any filesystem directory.

### 5.2 Transactional Crash-Recovery Cutover
Sequential file copying is not an atomic operation. Production cutover implements:
1. **Quiescence Verification:** Ensures zero active worker run locks or active database connections.
2. **Pre-Cutover Backup via SQLite Backup API:** Live `radar.db` is backed up via `sqlite3.Connection.backup()` to `radar.db.pre_cutover.bak`, and JSON ledgers are copied to `.pre_cutover.bak`.
3. **Crash Journal (`.cutover_journal.json`):** Tracks each state transition (`INITIATED`, `PRE_BACKUP_COMPLETED`, `REPLACED_DB`, `REPLACED_DELIVERY_LEDGER`, `REPLACED_BUDGET_LEDGER`, `COMPLETED`, `ROLLED_BACK`).
4. **Atomic Replacement (`os.replace`):** Files are staged on the target directory and replaced atomically.
5. **Startup Recovery:** `detect_and_recover_incomplete_cutover` detects interrupted cutovers on startup and executes emergency rollback.
6. **Phase 5B.2B Offline Policy:** Live production cutover against real operational environments is disabled fail-closed (`ProductionCutoverDisabledError`). Offline disaster-recovery tests require `simulated=True`.

---

## 6. Verified Retention Lifecycle & Storage Budget Enforcement

1. **Content-Verified Recovery Points:** `RetentionPolicyManager` does not treat remote filenames as trusted recovery points. Assets must be downloaded and cryptographically authenticated before being counted towards `min_recovery_points = 2`.
2. **Inventory Fingerprinting:** Evaluates an inventory fingerprint (SHA-256 of sorted asset IDs and sizes). Before deleting any asset, the fingerprint is revalidated; if storage changed concurrently, deletion aborts with `StaleInventoryError`.
3. **Budget Conflict Resolution:** If total storage exceeds `max_storage_budget_bytes`, pruning is planned. However, if pruning would reduce verified recovery points below 2, **new uploads are halted (`upload_blocked=True`) rather than deleting backups**.
4. **Never Delete the Last Verified Backup:** Under no circumstances will the newest verified backup be deleted.

---

## 7. Privacy & Credential Isolation

- **Masked Chat IDs:** Unencrypted logs and verification reports mask all recipient chat IDs (`87...43`).
- **Zero Secrets in Repository or Backups:** `TELEGRAM_BOT_TOKEN`, `GEMINI_API_KEY`, and database credentials are never stored in backups or commits.
- **Git Exclusion:** `.gitignore` excludes `*.efbk`, `data/*.db*`, `data/*.json`, and `data/restore_staging/`.
