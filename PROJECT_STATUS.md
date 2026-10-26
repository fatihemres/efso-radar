# Efso Radar — Project Checkpoint

**Verified snapshot date:** 2026-10-26 (GitHub inspection; NOT a live status feed)
**Canonical repository:** [fatihemres/efso-radar](https://github.com/fatihemres/efso-radar)
**Baseline inspected:** `main` at `d1660653e9c6438660386120d8b9f3ae5357aded` **before this governance PR**. This checkpoint remains anchored to that implementation baseline until integrated and reverified.

## Product & implementation
Efso Radar supplies selective Turkish/global entertainment-news discovery and editorial decision support for Efso Magazin; it does not automatically publish Instagram posts. Python 3.11/3.12 project with modular RSS discovery, multi-source parsing, deduplication/story memory, Gemini integration, Telegram formatting/delivery, SQLite and Turso Hrana v2 repositories, orchestration, encrypted backup primitives and offline staging test runner. Full architectural detail: [Architecture](docs/ARCHITECTURE.md).

**Version ambiguity:** Historical Phase 4 closure and ADR call `v0.4.0` an anchor; actual `src/efso_radar/__init__.py` declares `__version__ = "2.0.0"`. No GitHub Releases were returned on inspection. Treat public stable release identity as **unreconciled**, not 2.0.0 or v0.4.0 by assumption. See [Release Policy](docs/RELEASE_POLICY.md).

## Latest operational checkpoint — LIVE-01 READ-ONLY (2026-10-26)

This dated addendum supersedes the *planning-only* LIVE-01 assessment in the earlier snapshot below; it does not rewrite that historical baseline.

- **Main verified:** `6dc438547417384f73c0358022130f93a8320145` ([PR #28](https://github.com/fatihemres/efso-radar/pull/28) merged); [CI 40358925571](https://github.com/fatihemres/efso-radar/actions/runs/40358925571) Linux 3.11/3.12 and hosted Windows 3.12 all PASS, Ruff PASS.
- **Owner-operated real Turso READ-ONLY LIVE-01: PASS**, twice (before/after PR #28) against only the exact staging allowlist. Latest 47/60 HTTP requests, schema version 6, **15 tables, 129 expected columns, five ephemeral synthetic-key collision reads (none found)**. The latest local Python 3.12.10 focused suite: **10 passed, 10 subtests passed**. These are owner-supplied terminal logs, not ChatGPT-initiated remote calls. [Evidence, Issue #13](https://github.com/fatihemres/efso-radar/issues/13#issuecomment-6421424925).
- The owner reported performing **Invalidate All Tokens** after the read-only PASS. Official Turso documentation states that database tokens and related group tokens are invalidated; **no new credential is authorized or available** to this reviewer. Database records are not deleted by token invalidation. Dashboard **Block Writes ON still needs a fresh human confirmation**; no remote write/change was authorized.
- **Phase 5B.3G-C remains OPEN and NOT LIVE FOR WRITES**: the integrated `live01_readonly.py` is an isolated *read-only* tool; the eight-step `StagingSessionRunner` remains mock/hermetic-only. LIVE-01 did **not** persist an ownership manifest or reserve keys for any later write session; new per-session collision checks and crash-recoverable manifest are prerequisites to future mutations. [PR #26](https://github.com/fatihemres/efso-radar/pull/26) remains a separate draft offline deadline candidate (its branch is behind `main`); no merge authority was inferred. No billing/overage guarantee from PASS queries.

## Delivery status
- **CLOSED/ACCEPTED (historical):** Phase 4 pilot; [closure record](docs/PHASE4_CLOSURE.md). Historical live trials do not authorize present operation.
- **INTEGRATED:** Phase 5A design PR [#2](https://github.com/fatihemres/efso-radar/pull/2); Phase 5B.1 [#3](https://github.com/fatihemres/efso-radar/pull/3), 5B.2 [#5](https://github.com/fatihemres/efso-radar/pull/5), 5B.3B [#6](https://github.com/fatihemres/efso-radar/pull/6), 5B.3C-1/-2 [#7](https://github.com/fatihemres/efso-radar/pull/7)/[#8](https://github.com/fatihemres/efso-radar/pull/8), 5B.3D [#9](https://github.com/fatihemres/efso-radar/pull/9), 5B.3E [#10](https://github.com/fatihemres/efso-radar/pull/10), 5B.3F [#12](https://github.com/fatihemres/efso-radar/pull/12), 5B.3G-A specification [#14](https://github.com/fatihemres/efso-radar/pull/14), 5B.3G-B offline runner [#16](https://github.com/fatihemres/efso-radar/pull/16).
- **Rights documentation INTEGRATED:** [#18](https://github.com/fatihemres/efso-radar/pull/18) and [#19](https://github.com/fatihemres/efso-radar/pull/19); rights audit [#17](https://github.com/fatihemres/efso-radar/issues/17) is OPEN.
- **ACTIVE PLANNING / NOT AUTHORIZED LIVE:** [#13](https://github.com/fatihemres/efso-radar/issues/13) Phase 5B.3G controlled Turso staging validation. Next logical work: review staging readiness and safety gates, then obtain explicit per-operation PO authority before any remote action. 5B.3G-B is offline-only; it is **not** evidence that real Turso transaction tests have run.
- No new product code or live workflow activated by this governance PR.

## Verified quality evidence
GitHub Actions run [40211637125](https://github.com/fatihemres/efso-radar/actions/runs/40211637125) for merged PR #16 succeeded on Python 3.11 and 3.12. Antigravity **reported** 568 local passing tests and 154 subtests; independent historical code review and CI status were checked, but the local command was not rerun in this environment. See [VERIFICATION](VERIFICATION.md).

## Immediate risks / decision gates
1. **Live staging:** Block Writes ON, revoked credentials; real reads/writes and connection attempts prohibited without new PO authorization; $0 spend ceiling. Remote availability/state not independently rechecked now.
2. **Historical document drift:** Phase 4 dual-editorial-recipient wording and old $1.20 budget contradict subsequent PO policy. [DECISIONS](DECISIONS.md) establishes current priority; do not rewrite historical reports.
3. **Runtime budget drift:** Despite the PO-approved $0.00 / 0 TRY spending policy, the current `.env.example` and `config.py` retain positive historical monetary defaults (`1.20` monthly / `0.05` per-run and shadow-evaluation) with positive lower validators. AI evaluation is disabled by default; this is a **configuration/policy discrepancy, not evidence of charges**. Follow-up [Issue #21](https://github.com/fatihemres/efso-radar/issues/21) is OPEN and requires a separately authorized implementation PR before any live activation.
4. **Rights:** Public repo is proprietary for original material, but third-party audit [#17](https://github.com/fatihemres/efso-radar/issues/17) unfinished.
5. **Release numbering:** 2.0.0 in Python metadata vs v0.4.0 historical document anchor; no stable release assumed.
6. **Local state:** User/agent reports indicate local `main` clean after branch housekeeping. This was not directly inspected from the user's Windows filesystem; any new untracked worktrees must be checked at task start.

Other records: [ROADMAP](ROADMAP.md), [DECISIONS](DECISIONS.md), [engineering workflow](docs/ENGINEERING_GOVERNANCE.md).

## Governance documentation pending integration
- [Draft PR #20](https://github.com/fatihemres/efso-radar/pull/20) establishes canonical management documents. As of this snapshot, it is a **documentation candidate** rather than merged `main` content; independent reviewer and PO merge gate remain open.
- Existing `main` ruleset [Protect main](https://github.com/fatihemres/efso-radar/rules/25393440) is active and requires a PR, linear history and strict Python 3.11/3.12 checks (reviewer approval count set to 0). This is observed GitHub configuration, not approval to merge.
