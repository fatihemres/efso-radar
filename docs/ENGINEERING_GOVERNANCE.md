# Efso Radar — Engineering Governance & Git Workflow

**Effective documentation baseline:** 2026-10-26. The PO owns integration, operational and release decisions. Common agent execution contract: [AGENTS.md](../AGENTS.md). This document covers process, not authorization to enable production.

## Branch and agent isolation
1. Confirm repository, `git status --short`, branch, HEAD, `git worktree list`, linked Issues/PRs and all remote branch heads before editing.
2. Start one named branch per scoped task from current `main`; use distinct worktrees when multiple agents are active, avoid shared dirty working directories. No force push, reset, rebase of shared branches, or deletion of other agents' work without explicit approval.
3. Deliver smallest relevant diff and traceable commits. Reference the Issue and document substantial decisions with [DECISIONS.md](../DECISIONS.md).
4. PR must show scope, changed paths, test evidence, risk, security impact, rollback, known gaps and **STOP for independent review**. No unreviewed auto-merge. When PO approves, prefer rebase/linear integration where permitted, verify CI, HEAD/merge SHA and local `git pull --ff-only`.
5. After integration, clean only individually verified merged branches that are unused by worktrees/open PRs. Rebase merges can make ancestry checks misleading; compare PR head SHA and working branch before deletion.
6. Never treat a drafted Issue, branch, Markdown plan, passing CI or coding-agent report as permission to run live services or close a milestone.

## Verification policy
- Default: Python 3.11 and 3.12 on CI/Linux, hermetic mocks and isolated test DB, focused + full `pytest`; run additional Windows checks when change affects local OS behavior.
- No live-network unit tests, no secret/env inspection, no tracked operational state. Real Turso, RSS, Telegram, Gemini and backup operations require independent PO authorization.
- CI green is necessary but not sufficient for high-risk behavior; independent diff review and PO acceptance are separate gates. Log tests/commands/evidence in [VERIFICATION.md](../VERIFICATION.md).
- Prompt injection, third-party news content, SQL injection and untrusted HTTP error responses are threat surfaces; use defensive parsing, parameterized SQL, masking, least privilege and fail-closed unknown-state handling.

## GitHub infrastructure baseline and low-bureaucracy policy
- Existing: protected `main` (GitHub branch API shows protected=true), PR integration, Issues, and [CI workflow](../.github/workflows/ci.yml) for Python 3.11/3.12. There are separate **manual, offline-preview-only** workflows for scan, digest, admin health and backup; cron blocks are commented out.
- Cannot verify exact required reviewers, required status checks, bypass rights or ruleset details because branch protection API returned 403 to integration. **PO manual audit needed**; do not change protection during documentation setup.
- CI presently lacks standalone lint/typecheck/security CodeQL/coverage/Windows matrix. Propose a low-cost security/code scanning assessment in a future scoped Issue after checking whether GitHub Advanced Security and third-party tooling incur charges; no new CI workflow authorized here.
- No need for another project board yet: use existing Issues and labels intentionally. Suggested **future** minimal label taxonomy (not asserted created): `type:bug`, `type:feature`, `type:docs`, `area:security`, `status:blocked`, `needs:po-decision`. Only create labels when a concrete triage need arises.
- Never expose credentials or private data in public README, PR, CI logs, issue text, screenshots or artifacts. `.env.example` may document variable names, never values.

## Reconciliation and maintenance
- Authority order: recent explicit PO instruction > observed GitHub code/PR/CI for implementation facts > canonical docs/approved decisions > dated handoff > agent reports and hypotheses. Treat product preference differently from code reality. Log conflicts rather than silently override.
- During relevant PRs update `PROJECT_STATUS.md` after material integration/phase movement, `DECISIONS.md` for decisions, `ROADMAP.md` for milestone changes, `VERIFICATION.md` for quality evidence and `docs/IDEA_REGISTER.md` for product ideas. Avoid duplicates and one-file-per-PR handoffs.
- Revisit governance when external risk changes, especially billing/free tiers, GitHub public rights, notification recipient policy and staging authorization.

## Release gate
Follow [Release Policy](RELEASE_POLICY.md). No release, tag, protected-ref change, rollout or rollback command without separate PO consent.
