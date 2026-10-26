# Efso Radar — Versioning and Release Governance

**Policy checkpoint:** 2026-10-26. **Release operations not authorized** by this file.

## Current evidence
- Historical Phase 4 closure and later ADRs reference stable anchor `v0.4.0` (see [Phase 4 closure](PHASE4_CLOSURE.md)).
- The actual Python package at `src/efso_radar/__init__.py` currently reports `__version__ = "2.0.0"`.
- GitHub Releases API returned an empty list during this review. Tag history was not independently enumerated. These facts **do not establish a new stable release**.
- A public repository containing owner-owned proprietary code is not automatically open-source. [LICENSE](../LICENSE); third-party audit [#17](https://github.com/fatihemres/efso-radar/issues/17).

## Working policy until PO resolves numbering
1. Track accepted work by **milestone + PR + immutable SHA**. Do not infer a release version from package metadata.
2. **PO decision pending:** reconcile `v0.4.0` versus `2.0.0`, select SemVer or another scheme (including whether to keep historical tags), decide public distributions.
3. `main` is integration branch; task branches are ephemeral. Only create a release candidate branch/tag after separate PO authorization. Never move or delete an existing published tag.
4. Candidate release requires: scope/freeze, clean integration tree, code/review/CI confirmation, packaging test on supported platform(s), license/provenance checks, operational cost risk approval, migration/backup/restore and rollback rehearsal where relevant, then **explicit PO acceptance**.
5. Stable release/tag/change log must include version, date, SHA, changes, migration impact, known limitations and support/rollback actions. Public release must not expose operational data.
6. SQL schema v6 migrations and external services require compatible fail-closed downgrade/rollback design; do not assume backups alone guarantee safe rollback.
7. Production activation is a distinct decision from publishing a release; currently **none is authorized**.

**No active release automation or public version announcement is implied.** Record eventual decisions in [DECISIONS.md](../DECISIONS.md).
