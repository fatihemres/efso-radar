# Efso Radar — Phase 5B.2 Architecture: Encrypted Backup & Zero-Cost Retention

**Document Version:** 1.1.0  
**Status:** Approved Architecture Specification (Phase 5B.2B Implementation Baseline)  
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
7. **External Dependency Uncertainty:** GitHub Release asset storage quotas and policies are external vendor dependencies, not immutable permanent zero-cost guarantees. Systems must monitor storage metrics and fail closed if limits change.

---

## 2. Multi-Store Consistency Coordination

Efso Radar maintains state across multiple stores:
1. **Relational Database (`radar.db`):** Stories, updates, candidate backlog, assessments, feedback, schema migrations, and processed articles.
2. **Outbox Delivery Ledger (`telegram_digest_delivery_ledger.json`):** Recipient-scoped delivery records (`{chat_id}:{digest_hash}`) and stable digest IDs.
3. **Audit Budget Ledger (`experiment_budget_ledger.json`):** AI usage experiment budgets, token accounting, and cost limits.

### Atomic Consistency Coordination Protocol
Backing up stores independently introduces split-brain skew:
- If `radar.db` is captured at $T_1$ and `telegram_digest_delivery_ledger.json` at $T_2$, a digest sent between $T_1$ and $T_2$ would be marked delivered in the ledger but missing from the database, or vice versa.
- If restored without the delivery ledger, Efso Radar would re-deliver historical breaking news and digests to `PRIMARY_EDITOR`.

To guarantee multi-store point-in-time atomicity:
```mermaid
sequenceDiagram
    participant Orch as Orchestrator / Scheduler
    participant Lock as Lock Coordinator
    participant SQLite as SQLite DB (radar.db)
    participant Ledgers as JSON Ledgers
    participant Backup as Backup Engine

    Backup->>Lock: Acquire exclusive backup lock (or verify quiet hours idle)
    Backup->>SQLite: Open sqlite3.Connection.backup() to in-memory staging DB
    Note over SQLite: Checkpoints WAL frames atomically; copies pages
    Backup->>Ledgers: Read telegram_digest_delivery_ledger.json
    Backup->>Ledgers: Read experiment_budget_ledger.json
    Backup->>SQLite: Strip transient run_locks from in-memory snapshot
    Backup->>Backup: Package SQLite snapshot + ledgers into memory tarball
    Backup->>Backup: Compute SHA-256 of unencrypted tarball
    Backup->>Backup: Encrypt via AES-256-GCM with fresh 96-bit nonce
    Backup->>Lock: Release backup lock
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

### 3.2 Envelope Binary Layout
```
+---------------+---------------+---------------------------------------+
| Offset (Bytes)| Length (Bytes)| Field Description                     |
+---------------+---------------+---------------------------------------+
| 0             | 4             | Magic Bytes: "EFBK"                   |
| 4             | 2             | Envelope Format Version (uint16 = 1)  |
| 6             | 2             | Cipher Algorithm ID (uint16 = 1: GCM) |
| 8             | 8             | Created Timestamp (int64 Unix epoch)  |
| 16            | 12            | Nonce / IV (12 random bytes)          |
| 28            | 4             | AAD Manifest Length N (uint32)        |
| 32            | N             | Authenticated Manifest (UTF-8 JSON)   |
| 32 + N        | End           | Ciphertext + 16-byte GCM Auth Tag     |
+---------------+---------------+---------------------------------------+
```

### 3.3 Authenticated Additional Data (AAD) Manifest
The manifest is passed into `AESGCM` as Additional Authenticated Data. Any modification to the manifest causes decryption to fail authentication immediately:
- `manifest_version`: 1
- `created_at_utc`: ISO-8601 UTC timestamp
- `key_id`: Cryptographic identifier (e.g. `efso-2026-v1`) supporting key rotation
- `source_engine`: `sqlite` (or `turso`)
- `schema_version`: Current SQLite schema migration version (5)
- `plaintext_sha256`: SHA-256 hash of unencrypted tar archive
- `plaintext_byte_size`: Uncompressed byte count
- `table_row_counts`: Snapshot of durable table row counts
- `ledger_summary`: Digest counts and experiment budget batch count
- `git_commit_sha`: Git commit under which backup was generated

---

## 4. Privacy & Credential Isolation

### 4.1 Plaintext vs Encrypted Data Isolation
- **Git Repository Exclusion:** Neither unencrypted operational databases nor encrypted `.efbk` backup files are ever committed to the public Git repository. `.gitignore` strictly excludes `data/*.db*`, `data/*.json`, and `*.efbk`.
- **Recipient Chat ID Protection:**
  - Encrypted archives contain authentic delivery ledgers which reference recipient chat IDs (e.g., `PRIMARY_EDITOR` ID).
  - Unencrypted logs, CLI outputs, and restore verification reports **must never expose raw chat IDs**.
  - All chat IDs in logs or reports are formatted with `Settings.get_masked_chat_id()` (e.g., `87...43`).
- **Secret Zero-Exposure Rule:**
  - API keys (`TELEGRAM_BOT_TOKEN`, `GEMINI_API_KEY`, `TURSO_AUTH_TOKEN`) are strictly prohibited from being archived into backups. Backups contain application data only.
  - Recovery requires injecting credentials from secure environment secrets, never from backup archives.

---

## 5. Storage Architecture & Retention Lifecycle

### 5.1 The Git Storage Fallacy vs GitHub Releases
- In Git, running `git rm old_backup.efbk` creates a new commit but **does not remove the object from the `.git` packfile**. History remains immutable. Daily 15 MB commits expand storage indefinitely (~5.5 GB/year), forcing destructive force-pushes or repository recreation.
- **Solution — Single Rolling Release with Assets:**
  - In a dedicated private backup repository (`efso-radar-backups`), backups are uploaded as attached assets to a single permanent Release tagged `rolling-backups`.
  - When an asset older than 14 days is deleted via the GitHub REST API (`DELETE /repos/{owner}/{repo}/releases/assets/{asset_id}`), GitHub physically removes the asset from object storage and frees space immediately.
  - Zero commits are added to Git history; zero force-pushes; zero Git packfile bloat.

### 5.2 Storage Limits & External Policy Uncertainty
- **Asset Size Limit:** GitHub allows individual Release assets up to 2 GiB (Efso Radar backups are ~15–30 MB, well within 2% of limit).
- **Storage Policy Considerations:** While GitHub Free currently allows unlimited Release assets for legitimate project use, vendor terms can change. The retention manager actively audits total asset count and byte usage, enforcing a strict 14-day limit (~210 MB total).
- **Recovery Invariants:**
  - **Never Delete the Last Backup:** If network errors or failed runs prevent fresh backups, old backups must NOT be purged simply because they exceed 14 days.
  - **Minimum Recovery Points ($N \ge 2$):** Retention deletion only executes if at least two verified recovery points will remain available after the purge.

---

## 6. Fail-Closed Restore & Verification Protocol

Restoring from backup must be 100% fail-closed and must never silently overwrite live production data.

### 6.1 Restore Verification Workflow
1. **Isolated Staging:** Extraction occurs exclusively in an isolated staging directory (`data/restore_staging/`). Live files are completely untouched.
2. **Cryptographic Authentication:** Decryption with `AES-256-GCM`. If the key is wrong, nonce is corrupted, or ciphertext is tampered with by even 1 bit, execution halts immediately with `CryptographicIntegrityError`.
3. **Digest Verification:** Plaintext SHA-256 is computed and verified against `manifest.plaintext_sha256`.
4. **Archive Security:**
   - **Path Traversal Protection:** Every member path in the tar archive is validated. Any path containing `..`, absolute paths (`/` or `C:\`), or symbolic links outside the staging directory is rejected with `PathTraversalSecurityError`.
   - **Decompression Bomb Protection:** Cumulative extracted bytes are bounded (maximum 500 MB).
5. **Database Consistency Checks:**
   - `PRAGMA integrity_check;` $\rightarrow$ Must return `["ok"]`.
   - `PRAGMA foreign_key_check;` $\rightarrow$ Must return zero violations.
   - `schema_migrations` $\rightarrow$ Must match version `5`.
6. **Ledger Identity & Anti-Duplicate Verification:**
   - `telegram_digest_delivery_ledger.json` is checked for valid JSON structure and verified recipient delivery records.
   - Ensures that after restore, previously delivered digest hashes remain blocked from re-sending.
7. **Discrepancy Reporting & Product Owner Cutover Gate:**
   - Emits a structured Markdown report detailing restored row counts, timestamp ranges, and ledger status.
   - **Automatic production overwrite is forbidden.** Production cutover requires explicit Product Owner authorization.
8. **Rollback Safety:**
   - During cutover, live files are renamed to `.pre_restore_bak`. If post-cutover smoke checks fail, live files are immediately restored from the safety backup.
