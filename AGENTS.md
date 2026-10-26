# Efso Radar — Agent Working Contract

Applies to Codex, Google Antigravity and other software agents. Owner: Product Owner (PO). This document governs agent conduct; it does **not** authorize implementing the roadmap.

## Before touching the repository
1. Read [README](README.md), [PROJECT_STATUS](PROJECT_STATUS.md), [DECISIONS](DECISIONS.md), [ROADMAP](ROADMAP.md), [VERIFICATION](VERIFICATION.md), [engineering workflow](docs/ENGINEERING_GOVERNANCE.md) and task-relevant files under `docs/`. Use live Git/PR/CI evidence rather than stale document snapshots.
2. Confirm repository `fatihemres/efso-radar`, target branch, `git status`, HEAD, worktrees, existing issues/PRs and scope. Never overwrite another agent's work.
3. Clearly distinguish PO-approved decisions, plans, agent proposals, implemented code, CI-verified results, PO acceptance and production activation.

## Authority and scope
- PO owns product scope, architectural trade-offs, budget, milestone acceptance, merges and releases. Agents may research/review, propose alternatives and implement only an explicitly assigned scope.
- Use short-lived task branches from current `main` or isolated worktrees when parallel work is authorized. Never commit directly to `main` or change protected refs, force push, erase history or merge without explicit PO approval.
- Prefer smallest patch respecting existing ports-and-adapters architecture. No unrelated refactor, dependencies, scheduled automation, deployments or new providers. Every change needs an explicit task goal and acceptance criteria.
- Preserve operational artifacts: `.env`, all credentials, `data/` (especially `data/radar.db`, `data/state.json`), encrypted backups, recovery manifests, agent worktrees and other uncommitted work. Never inspect real operational records merely for convenience.
- PUBLIC repository; proprietary original material under [LICENSE](LICENSE), third-party rights audit [#17](https://github.com/fatihemres/efso-radar/issues/17) remains open. Never publish tokens, personal chat IDs, third-party restricted material or private user data.

## Hard runtime restrictions (until separately lifted by PO)
- Absolutely **$0.00 / 0 TRY**. Do not enable paid services, billing, automatic plan upgrades or paid fallbacks.
- No production, scheduled cron, RSS collection, Gemini evaluation, Telegram sending, backup upload, Instagram posting, staging connections or Turso API calls — **including read-only** — without an explicit per-operation PO authorization. Documentation describing live actions is not permission.
- Staging `efso-radar-staging`: Block Writes ON; previous tokens revoked. Never change it, create credentials or attempt live operations. Approved future destination is exact allowlisted host in [Phase 5B.3G plan](docs/PHASE5B3G_LIVE_STAGING_RELIABILITY_PLAN.md), NOT a live-run authorization.
- For present engineering work, use in-memory/temp SQLite fixtures, mocked HTTP transport and hermetic tests only.

## Implementation quality and reporting
- Confirm Python 3.11 / 3.12 compatibility, current package dependencies in `requirements*.txt`, existing Schema v6, and repository contracts. Preserve idempotent notification handling, ownership-checked cleanup, ambiguous COMMIT = UNKNOWN, at-most-once delivery and fail-closed budget behavior.
- Standard offline validation: `PYTHONPATH=src python -m pytest tests -v` (PowerShell: `$env:PYTHONPATH='src'; python -m pytest tests -v`). Run focused tests first. Distinguish commands actually executed locally from GitHub CI outcomes. Do not run live scripts or tools as tests.
- Submit a PR with issue/milestone, exact files, scope, safety checks, evidence (command, OS/Python version, commit, test count, CI run), risks and rollback approach. Keep PR unmerged for independent review and PO go/no-go.
- `IMPLEMENTED` = code exists; `VERIFIED` = traceable tests/review; `INTEGRATED` = merged to `main`; `CLOSED/ACCEPTED` = explicit PO decision. Never collapse these states.
- At significant milestones, update existing checkpoint/decision/roadmap/verification documents within the relevant PR. Do not create a new handoff for every conversation.
- End each task with branch/commit/PR, work performed, validations and missing tests, unchanged operational boundaries, remaining decisions and explicit STOP state. Never claim background work was completed unless evidenced.

Authoritative references and release gate: [engineering governance](docs/ENGINEERING_GOVERNANCE.md), [release policy](docs/RELEASE_POLICY.md).
