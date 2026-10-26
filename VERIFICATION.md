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
- Added negative tests for defaults, zero/negative validation, positive environment rejection, production engine no-dispatch and real shadow client no-dispatch. **Tests have not been locally run by ChatGPT**; CI outcome at final PR HEAD must be independently checked before a VERIFIED label.

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
