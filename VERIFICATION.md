# Efso Radar — Verification Ledger

**Checkpoint:** 2026-10-26. This ledger separates direct GitHub CI evidence, agent-reported local runs, historical pilot acceptance and missing validations. **No tests were run by ChatGPT against a local clone for this documentation task.** Evidence is not a release certificate.

## Current independently checked evidence

| What | Evidence / environment | Result and precise scope |
| --- | --- | --- |
| Governance documentation PR #20 | [CI 40302843823](https://github.com/fatihemres/efso-radar/actions/runs/40302843823), head `c3cc34aab00d848b8ea79cdafbb96993701a1835` | **Python 3.11 and 3.12 PASS** on Linux; independently inspected both CI job logs: **554 passed, 14 skipped, 154 subtests passed per matrix job**. This validates that exact prior SHA, not subsequent amendments, local Windows, Markdown rendering, or live services. |
| Phase 5B.3G-B offline runner / Turso hardening | [PR #16](https://github.com/fatihemres/efso-radar/pull/16), final feature HEAD `674894bd4550452f5e452fcd07b9023906a2e2de`, [Actions run 40211637125](https://github.com/fatihemres/efso-radar/actions/runs/40211637125) | **CI successful** for Python **3.11** and **3.12** on Linux. GitHub PR merged via rebase; `main` integration commit `9a9ee4a9af35b2a027238da3fca6ffc0e445c9cd`. |
| Offline test suite | Antigravity completion report for PR #16; reported `python -m pytest tests` | **568 passed / 154 subtests reported** by coding agent; not reproduced on Windows by this reviewer. Run link is the independently checked CI confirmation, not independent reproduction of local output. |
| Previous reliability foundation | [PR #12](https://github.com/fatihemres/efso-radar/pull/12), [Phase 5B.3F record](docs/PHASE5B3F_HERMETIC_RELIABILITY_VALIDATION.md) | Integrated hermetic transaction, concurrency, parity and crash recovery tests; historical test totals are per milestone and must not be added together. |
| Prior manual editorial pilot | [Phase 4 closure](docs/PHASE4_CLOSURE.md) | **Historical claimed acceptance**, including narrowly scoped RSS/Gemini/Telegram tests. Not authorization for present live operations or proof of current unattended reliability. |
| Repository governance PR | This documentation-only branch | Prior reviewed HEAD `c3cc34a` passed both CI jobs as above; any new documentation commit must be rechecked. Markdown links/rendering and explicit PO merge approval are separate review gates. No runtime test or live activation is claimed here. |

## Issue #21 — Zero-spend implementation candidate (unmerged)

- Candidate branch `fix/issue-21-zero-spend-gates-20261026` attempts hard-blocking of real Gemini calls in the editorial engine, shadow evaluator and historical Phase 4D helper until separately approved provider billing safeguards exist. Existing offline cached results and synthetic mocks are deliberately distinct.
- Operational AI monetary defaults are zero; `Settings.from_env` rejects positive historical budget overrides, while explicit positive values remain available only to legacy hermetic price-estimation tests. This is not provider Free Tier verification or permission to activate services.
- Added negative tests for defaults, zero/negative validation, positive environment rejection, production engine no-dispatch and real shadow client no-dispatch. Historical mock-only calibration budget tests explicitly use old numeric ceilings so they still test the cost calculator without permitting real dispatch. **Tests have not been locally run by ChatGPT**.
- Initial CI at `fbaf7a7` ([run 40311808779](https://github.com/fatihemres/efso-radar/actions/runs/40311808779)) **FAILED**: 4 pre-existing tests assumed positive defaults or the missing-key diagnostic. The candidate patch addresses those specific regressions; rerun CI and inspect exact logs before any VERIFIED classification.
- After isolated regression corrections at `474fd344d08c3544f3d7cf29271b33f44152ae9d`, GitHub Actions [run 40312122720](https://github.com/fatihemres/efso-radar/actions/runs/40312122720) **PASSED** on Linux Python **3.11 and 3.12**: **558 passed, 14 skipped, 154 subtests passed per Python job**, confirmed from both raw job logs.
- **Independent review correction (2026-10-26):** Review of all 63 non-test Python files identified four Gemini SDK dispatch locations, including previously unguarded `src/radar.py:judge` and a bypass via injected `mock_client` in the shadow evaluator. Scoped commits [`462b9956`](https://github.com/fatihemres/efso-radar/commit/462b9956e3be6ea81e8ec1583274c7d5f5b8465f) and [`27723b0a`](https://github.com/fatihemres/efso-radar/commit/27723b0a72599aa2b63ba5c273b990a23e98ec9d) close both routes, move historical pilot live-denial before local file/ledger access, reject NaN/infinite monetary budgets, and explicitly scope mocks to hermetic tests. They do not authorize live operations.
- **Post-review code CI:** [run 40321496127](https://github.com/fatihemres/efso-radar/actions/runs/40321496127) on commit `27723b0a72599aa2b63ba5c273b990a23e98ec9d` **PASSED** on Linux Python **3.11 and 3.12**: **561 passed, 14 skipped, 163 subtests passed per job**, confirmed independently from raw job logs. No independent local Windows or provider test. Any subsequent doc/test commit needs its own CI evidence. The PR remains DRAFT / UNMERGED pending the Product Owner.

## Phase 5B.3G-C Live-Ready Operator Candidate Evidence — 2026-10-28

| What | Exact evidence / environment | Result and boundary |
| --- | --- | --- |
| Live-Ready Operator candidate tests (post-review 3) | Windows Python 3.12.10, `venv\Scripts\python.exe -m pytest tests/test_phase5b3g_c_live_ready_operator_offline.py -v` | **57 passed in 8.35s**; zero real network calls. Covered: rejection of caller-self-issued attestations (wire preflight queries and 5 synthetic collision probes verified before preflight completion; operator instance verification before human window), monotonic stage sequence (`LIVE-01` -> preflight -> human window -> `LIVE-02` -> ... -> `LIVE-08`; out-of-order stage transitions rejected with `OUT_OF_ORDER_STAGE`), cleanup reserve escalation prevention (`set_cleanup_phase()` strictly restricted to `LIVE-08`), read-only least privilege via exact SQL template inventory (`_SYNTHETIC_READ_TEMPLATES`) with exact parameter count (1) and manifest PK match, rejection of `OR 1=1` injection, join cardinality expansion, subqueries, and extra parameters; stage-scoped preflight queries strictly limited to `LIVE-01` and rejected outside `LIVE-01`. Concrete anti-spoofing (`_conn`, `is_hermetic`, custom subclasses rejected), transport lifecycle closure across all exits (nominal, early halt without human approval, exception), strict request budget ceiling (<100 requests; request #100 blocked), constructor budget invariants rejecting booleans and non-finite floats, manifest tampering, unregistered identities, preflight gate, collision abortion, human approval window, false-acceptance prevention, deadline exhaustion (>120s), cleanup reserve (80 cutoff), ambiguous COMMIT = UNKNOWN, ownership mismatch halt, affected row count != 1, 3-state audit (CLEAN/RESIDUAL), calendar-valid ISO 8601 UTC validation, wire boundary integration, and CLI default-deny. |
| Phase 5B.3G full staging suite | Windows Python 3.12.10, `venv\Scripts\python.exe -m pytest (Get-ChildItem tests/test_phase5b3g*.py) -q` | **221 passed, 1 skipped, 118 subtests passed in 18.95s** across all Phase 5B.3G test modules. |
| Full hermetic test suite (post-review 3) | Windows Python 3.12.10, `venv\Scripts\python.exe -m pytest tests -k "not test_phase3c2b_notification_quality_on_calibration_dataset and not test_budget_ledger_reporting_matches_ledger_file and not test_phase4d_final_editorial_review_override" -q` | **722 passed, 1 skipped, 3 deselected, 276 subtests passed in 157.91s**; zero real network calls. |
| GitHub CI Suite (PR #35 post-review 3) | [Actions run 40646755517](https://github.com/fatihemres/efso-radar/actions/runs/40646755517), head `90fea43` | **PASS** across all matrix jobs: `static-safety` (Ruff) clean (10s); Linux Python 3.11 (`712 passed, 14 skipped, 276 subtests passed in 25.56s`), Linux Python 3.12 (`712 passed, 14 skipped, 276 subtests passed in 36.04s`), and hosted Windows Python 3.12 (`712 passed, 14 skipped, 276 subtests passed in 3m24s`). |
| Outbound Hrana security boundary | `HranaOutboundSecurityBoundary` in `src/efso_radar/staging/hrana_outbound_guard.py` | Deny-by-default on real HTTP transports; rejects escape hatches (`live_authorized=True` denied); rejects duck-typing and custom transport subclasses; enforces destination URL, POST method, payload size <= 16KB, non-duplicate JSON keys, transaction baton state machine, request budget (<100 requests, 20 reserve), deadline (<120s), calendar-accurate ISO timestamps, exact read template allowlist, and error sanitization. Integrated into operator transport pipeline. |

## Phase 5B.3G-C Live-Ready Operator Candidate Evidence — 2026-10-27

| What | Exact evidence / environment | Result and boundary |
| --- | --- | --- |
| Live-Ready Operator candidate tests (post-review 2) | Windows Python 3.12.10, `venv\Scripts\python.exe -m pytest tests/test_phase5b3g_c_live_ready_operator_offline.py -v` | **50 passed in 10.73s**; zero real network calls. Covered: default deny on real HTTP transports, concrete anti-spoofing (`_conn`, `is_hermetic`, custom subclasses rejected), wire-level stage sequence and human approval gating (direct client mutations before preflight, before human window, without active stage, or on stage mismatch rejected with `MockHranaDenied`), single-shot mutation replay protection, read-only least privilege (operational tables `telegram_delivery_records`, `owner_feedback`, etc. and full table scans strictly rejected with `READ_NOT_ALLOWLISTED`), transport lifecycle closure across all exits (nominal, early halt without human approval, exception), strict request budget ceiling (<100 requests; request #100 blocked), constructor budget invariants rejecting booleans and non-finite floats, manifest tampering, unregistered identities, preflight gate, collision abortion, human approval window, false-acceptance prevention, deadline exhaustion (>120s), cleanup reserve (80 cutoff), ambiguous COMMIT = UNKNOWN, ownership mismatch halt, affected row count != 1, 3-state audit (CLEAN/RESIDUAL), calendar-valid ISO 8601 UTC validation, wire boundary integration, and CLI default-deny. |
| Phase 5B.3G full staging suite | Windows Python 3.12.10, `venv\Scripts\python.exe -m pytest tests/test_phase5b3g* -v` | **200 passed, 1 skipped, 118 subtests passed in 16.45s** across all Phase 5B.3G test modules. |
| GitHub CI Suite (PR #35) | [Actions run 40513016694](https://github.com/fatihemres/efso-radar/actions/runs/40513016694), head `5006c1d` | **PASS** across all matrix jobs: `static-safety` (Ruff) clean (8s); Linux Python 3.11 (`691 passed, 14 skipped, 276 subtests passed in 57s`), Linux Python 3.12 (`691 passed, 14 skipped, 276 subtests passed in 56s`), and hosted Windows Python 3.12 (`691 passed, 14 skipped, 276 subtests passed in 4m4s`). Prior runs [40502836649](https://github.com/fatihemres/efso-radar/actions/runs/40502836649), [40502270723](https://github.com/fatihemres/efso-radar/actions/runs/40502270723), and [40494992373](https://github.com/fatihemres/efso-radar/actions/runs/40494992373) also passed. |
| Outbound Hrana security boundary | `HranaOutboundSecurityBoundary` in `src/efso_radar/staging/hrana_outbound_guard.py` | Deny-by-default on real HTTP transports; rejects escape hatches (`live_authorized=True` denied); rejects duck-typing and custom transport subclasses; enforces destination URL, POST method, payload size <= 16KB, non-duplicate JSON keys, transaction baton state machine, request budget (<100 requests, 20 reserve), deadline (<120s), calendar-accurate ISO timestamps, and error sanitization. Integrated into operator transport pipeline. |

## Latest checked implementation and live read-only evidence — 2026-10-26

| What | Exact evidence / environment | Result and boundary |
| --- | --- | --- |
| PR #27 initial LIVE-01 read-only operator | Merged `32c0e3bfdf10ffa4f267b51440bbdc9e455d3d16`; [CI #40337613519](https://github.com/fatihemres/efso-radar/actions/runs/40337613519) | Hosted Linux 3.11/3.12 + Windows 3.12 PASS; mock-only tests. |
| PR #28 one-query sanitized diagnostic | Merged `6dc438547417384f73c0358022130f93a8320145`; [main CI #40358925571](https://github.com/fatihemres/efso-radar/actions/runs/40358925571) | Each matrix job: **571 passed / 14 skipped / 176 subtests passed**; Ruff PASS, Linux pip check/pip-audit clean. |
| PO Windows Python 3.12 focused tests | Owner-provided PowerShell at `C:\Dev\EfsoRadar`, `PYTHONPATH=src`, `venv\Scripts\python.exe -m pytest tests/test_phase5b3g_c_live01_readonly.py -q` | At integrated `main` `6dc4385`: **10 passed, 10 subtests passed in 0.88s**. Owner-provided log, not assistant local execution. |
| First authorized remote staging LIVE-01 | Owner-provided console output, read-only Turso token; old CLI then new CLI at `6dc4385`. [Issue #13 comment](https://github.com/fatihemres/efso-radar/issues/13#issuecomment-6421424925) | **PASS** (both); latest **47/60 reads**, schema v6, **15 expected tables**, **129 expected columns**, **five synthetic-key collisions absent**; **no write operations** by read-only client/transport design. Not a claim that the complete remote database content was audited. |
| Credential invalidation | Owner's direct statement after PASS: clicked **Invalidate All Tokens** | Treat as reported revocation of database/group credentials, **not independently verified**; no SQL data deletion implied, other legacy token consumers may require reissuing. Do not request tokens/chat secrets. |

**Remaining gaps:** Block Writes ON not freshly observed after LIVE-01; there is no saved write-session ownership manifest from LIVE-01; no LIVE-02 through LIVE-08 remote mutations, ownership-checked cleanup or remote recovery proof. The original 8-step staging runner remains **hermetic-only**. Existing PR #26, an offline deadline candidate, is **draft/unmerged** and was last CI-verified against an older `main` base. These gaps cannot be waived by a remote read-only PASS. No provider-specific absolute $0 billing guarantee was verified.

## Reproducible offline commands

Windows PowerShell from `C:\Dev\EfsoRadar`:

```powershell
git status --short
$env:PYTHONPATH = 'src'
python -m pytest tests -v
python -W error -m efso_radar.staging.runner --help
```

CI source: [.github/workflows/ci.yml](.github/workflows/ci.yml); currently `ubuntu-latest`, Python 3.11 and 3.12, requirements from `requirements-dev.txt`, `python -m pytest tests -v`. No separate lint, typecheck, coverage, CodeQL, Windows Actions matrix or release pipeline was found in the inspected repository tree. Do not claim these checks exist or pass.

## Release and validation gates

- **IMPLEMENTED:** Code committed to a task branch. **VERIFIED:** tests/review with identifiable SHA and environment. **INTEGRATED:** merged on `main`. **ACCEPTED:** explicit PO acceptance. **LIVE:** separate operational authorization and observed evidence. Different gates.
- For implementation PRs: record focused tests, full suite, OS/versions, outcomes, CI links, security gates, rollback and reviewer decision.
- For runtime-critical changes: negative/adversarial cases (ambiguous COMMIT, ownership mismatch, no blind retry, cleanup budget, token masking, readonly firewall, deterministic idempotency). Count synthetic rows separately from mutating SQL operations.
- Before any future live staging: independently reconcile observed remote schema, permissions, cleanup and final locks **only after specific PO authorization**.
- For public-distribution/release: third-party provenance [#17](https://github.com/fatihemres/efso-radar/issues/17), version reconciliation and separate release approval.

## Open verification gaps

1. Real Turso transaction/write/cleanup session not run under Phase 5B.3G; Block Writes remains ON. No cloud call authorized.
2. Local Windows run of latest `main` test matrix not independently captured in this task.
3. **Branch ruleset verified:** active ruleset [Protect main](https://github.com/fatihemres/efso-radar/rules/25393440) enforces PR workflow, linear history, no deletion/force updates and strict checks `test (3.11)` / `test (3.12)`. It requires **0 approving reviews** at GitHub policy level; PO permission remains mandatory by governance. Standalone CodeQL/secret scan results are **not** established. The older branch-protection endpoint (different from rulesets) returned 403.
4. Python package `__version__=2.0.0` versus historical `v0.4.0` requires PO release/version decision.
5. Full end-to-end autonomous system operation, safety under provider quotas, and user/editor live acceptance are NOT proven by offline CI.

Append significant verified evidence with date, commit, command, environment, outcome and evidence URL. Avoid per-PR report file sprawl.
