# Efso Radar — Roadmap and Acceptance Gates

**Snapshot 2026-10-26** — roadmap records intent, not permission for live execution. Status meanings and quality gates: [Engineering Governance](docs/ENGINEERING_GOVERNANCE.md). PO alone closes milestones.

| Milestone | Scope & acceptance target | Evidence | Current classification |
| --- | --- | --- | --- |
| Phase 1–4 | Local editorial radar, AI assessment, RSS and Telegram pilot; manual acceptance | [Phase 4 closure](docs/PHASE4_CLOSURE.md) | **CLOSED / ACCEPTED** (historical pilot) |
| Phase 5A | Zero-cost unattended architecture and approved safety design | [PR #2](https://github.com/fatihemres/efso-radar/pull/2), [ADR](docs/PHASE5A_ARCHITECTURE_DECISION.md) | **INTEGRATED** |
| Phase 5B.1 | Persistent state and scan-only RSS groundwork | [PR #3](https://github.com/fatihemres/efso-radar/pull/3) | **INTEGRATED** |
| Phase 5B.2 | AES-256-GCM backup and restoration primitives | [PR #5](https://github.com/fatihemres/efso-radar/pull/5), [backup design](docs/PHASE5B2_BACKUP_ARCHITECTURE.md) | **INTEGRATED** — external backup activation not approved |
| Phase 5B.3B / C-1 / C-2 | Cloud safety contracts, Turso persistence parity, read-only preflight/recovery | [PRs #6](https://github.com/fatihemres/efso-radar/pull/6), [#7](https://github.com/fatihemres/efso-radar/pull/7), [#8](https://github.com/fatihemres/efso-radar/pull/8) | **INTEGRATED** |
| Phase 5B.3D | Offline orchestration, scheduling contracts, disabled workflow templates | [PR #9](https://github.com/fatihemres/efso-radar/pull/9) | **INTEGRATED** — cron inactive |
| Phase 5B.3E | Secure Turso client and read-only readiness | [PR #10](https://github.com/fatihemres/efso-radar/pull/10) | **INTEGRATED** |
| Phase 5B.3F | Hermetic reliability/concurrency and recovery validation | [PR #12](https://github.com/fatihemres/efso-radar/pull/12), [Issue #11](https://github.com/fatihemres/efso-radar/issues/11) | **INTEGRATED / OFFLINE VERIFIED** |
| Phase 5B.3G-A | Controlled live staging *specification*, without live execution | [PR #14](https://github.com/fatihemres/efso-radar/pull/14) | **INTEGRATED (DESIGN)** |
| Phase 5B.3G-B | Hermetic eight-step mock staging runner, strict safety barriers | [PR #16](https://github.com/fatihemres/efso-radar/pull/16) | **INTEGRATED / OFFLINE VERIFIED** |
| Phase 5B.3G-C | Conditional real Turso staging reliability session, exactly scoped and audited | [Issue #13](https://github.com/fatihemres/efso-radar/issues/13), [LIVE-01 evidence](https://github.com/fatihemres/efso-radar/issues/13#issuecomment-6421424925), [plan](docs/PHASE5B3G_LIVE_STAGING_RELIABILITY_PLAN.md) | **READ-ONLY LIVE-01 OPERATOR PASS / WRITE PHASES NOT AUTHORIZED** |
| Follow-on cloud cutover & unattended operations | Cloud parity, backups, zero-cost controls, human editorial recipient tests, PO release decision | Existing Phase 5 architecture; future issue required | **DISCOVERY / NOT AUTHORIZED** |
| Rights and distribution | Review third-party origins and decide visibility/distribution | [Issue #17](https://github.com/fatihemres/efso-radar/issues/17) | **IN PROGRESS (AUDIT)** |

## 2026-10-28 Phase 5B.3G-C Live-Ready Operator Preparation Checkpoint (Candidate / Unmerged PR #35)

Operator candidate `LiveReadyStagingOperator` and `HranaOutboundSecurityBoundary` prepared as an unmerged candidate (Draft PR #35, Review Round 3 remediation, pending independent review).
- Reuses existing hermetic runner, preflight, manifest sealing, and session gate components.
- Enforces strict default-deny for live execution; fails closed immediately on non-hermetic transports (no duck-typing bypasses).
- Outbound boundary wired into transport chain, enforcing wire-level stage sequence and human approval gating: direct client mutations outside active stages or prior to preflight/human approval fail closed immediately. Transitions require source-backed evidence (verified wire preflight requests and operator attestation); public SHA-256 cannot be self-issued to escalate permissions.
- Read-only least privilege at wire edge: broad operational table queries (`telegram_delivery_records`, etc.) and full table scans strictly rejected (`READ_NOT_ALLOWLISTED`); exact SQL template inventory (`_SYNTHETIC_READ_TEMPLATES`) with exact parameter count (1) and manifest PK match enforced. Preflight queries strictly stage-scoped to LIVE-01 only.
- Strict cleanup reserve protection: `set_cleanup_phase()` strictly restricted to `LIVE-08`; attempts to escalate cleanup reserve before `LIVE-08` are permanently denied with `CLEANUP_RESERVE_ESCALATION`.
- Full gate workflow: verified LIVE-01 preflight, mandatory human window attestation before write stages, deadline (<120s), budget (<100 requests strictly enforced at wire edge, 20 cleanup reserve), ambiguous COMMIT = UNKNOWN, conditional DELETE (affected == 1), post-run lockdown assertion, and 3-state audit (CLEAN/RESIDUAL/UNKNOWN).
- Transport lifecycle closure: client and security boundary permanently closed/invalidated across all exit paths (nominal run, early exit without human approval, exceptions).
- Unlinking of manifest and PASSED status strictly blocked if human approval, any of the 8 stages, or lockdown assertion is missing or incomplete.
- MANIFEST DURABILITY LIMITATION: Best-effort process-crash retention within OS tempdir. Machine reboot / temp purging is NOT resilient and remains an open prerequisite for unattended live cloud execution.
- LOCKDOWN ASSERTION NOTICE: Post-run lockdown recording is an offline simulated assertion only, NOT real cloud dashboard state or token revocation.
- All live staging operations remain **UNAUTHORIZED**; Block Writes remains ON. 57 focused offline operator tests passing.

## 2026-10-26 LIVE-01 milestone checkpoint

Read-only schema/integrity preflight passed on the owner-operated staging connection, with **15 expected tables, 129 expected columns and five ephemeral synthetic-key collision checks**. This is a **verified read-only substep**, not closure of G-C and not an approval to unblock writes. Afterward the owner invalidated all database tokens; any future authenticated session needs freshly scoped credentials and a new approval for its exact operation.

**Remaining dependency:** [Draft PR #26](https://github.com/fatihemres/efso-radar/pull/26) has an offline transport deadline guard, but the G-B eight-step runner is **not connected to real Turso**. A future live-capable implementation needs separate code review and hard gates for manifest pre-registration/retention, write-unblock window, bounded requests/session time, ambiguous COMMIT = UNKNOWN, ownership-checked cleanup and manual Block Writes restoration. The earlier LIVE-01 check's ephemeral run IDs/keys **must not** be reused or mistaken for a persisted run manifest.

## Current dependency gate: Phase 5B.3G-C
1. Reconcile [Phase 5B.3G plan v2.2.0](docs/PHASE5B3G_LIVE_STAGING_RELIABILITY_PLAN.md) with actual [offline runner](src/efso_radar/staging/runner.py), including any divergences introduced by later PR #16 fixes. This is a *review task*, not an instruction to run it live.
2. Verify free-tier limits/zero-charge exposure before any future activation, plus exact destination and isolated credential scope. No access to remote DB is authorized merely to prepare this gate.
3. Obtain explicit staged PO approvals **separately** for any read-only connection, token creation, disabling Block Writes, synthetic writes, cleanup and re-lock. If authorization is withheld, remain offline.
4. Verify ownership manifest retention, exact conditional cleanup, UNKNOWN/no-retry behavior and post-run proof of zero residual records; require PO acceptance for closure.
5. Production cutover, delivery enablement, scheduled actions, backup service and release are **independent later approvals**, never implied by staging success.

## Backlog discipline
GitHub Issues track actionable scope; an approved issue may move PLANNED → IN PROGRESS → IMPLEMENTED/CANDIDATE → VERIFIED → INTEGRATED → CLOSED/ACCEPTED. An integrated PR does not automatically close its parent milestone or enable live service. New ideas: [Idea Register](docs/IDEA_REGISTER.md).
