"""Build canonical synthetic G-C SQL intents without ever dispatching SQL.

This is reusable *data preparation* for a future separately authorized
staging runner, not an executable writer, connection layer, or permission
grant. No network, filesystem mutations, tokens or database client imports.
"""
from __future__ import annotations

from dataclasses import dataclass
import json

from efso_radar.staging.manifest_seal import OfflineManifestSeal
from efso_radar.staging.offline_operator import _INSERT_ARTICLE, _INSERT_STORY
from efso_radar.staging.runner import create_default_manifest_entities


@dataclass(frozen=True)
class PlannedSql:
    """Immutable SQL text/parameter tuple; never call a remote client here."""

    sql: str
    args: tuple[str | int, ...]


class OfflineWriteIntentBuilder:
    """Derive only registered-key parameterized SQL from a sealed manifest.

    The caller MUST enforce all downstream operator gates separately. The
    returned statements are NOT a permit, authorization or completed audit.
    """

    def __init__(self, seal: OfflineManifestSeal) -> None:
        if type(seal) is not OfflineManifestSeal:
            raise TypeError("Expected an offline, sealed per-run manifest.")
        seal.assert_unchanged()
        self._seal = seal
        self._run_id = seal.run_id

    def _registered(self, table: str, key: str):
        self._seal.assert_unchanged()
        for entity in create_default_manifest_entities(self._run_id):
            if entity.table == table and entity.pk_value == key:
                self._seal.require_registered_identity(
                    entity.table, entity.pk_column, entity.pk_value
                )
                return entity
        raise PermissionError("OFFLINE_SQL_UNREGISTERED_TARGET")

    def live02_atomic_pair(self) -> tuple[PlannedSql, PlannedSql]:
        """Prepare two exact Stage-02 inserts; no BEGIN/COMMIT, no dispatch."""
        run = self._run_id
        article = self._registered("processed_articles", f"synth_art_{run}_01")
        story = self._registered("stories", f"synth_story_{run}_01")
        stamp = "2026-10-25T15:00:00Z"  # canonical hermetic fixture epoch
        return (
            PlannedSql(
                _INSERT_ARTICLE,
                (
                    article.pk_value,
                    f"https://example.com/synth/{run}/01",
                    article.expected_attributes["title"],
                    article.expected_attributes["source"],
                    stamp,
                    stamp,
                ),
            ),
            PlannedSql(
                _INSERT_STORY,
                (
                    story.pk_value,
                    story.expected_attributes["headline_tr"],
                    story.expected_attributes["category"],
                    stamp,
                    stamp,
                    json.dumps([article.pk_value]),
                    1,
                    1,
                ),
            ),
        )

    def conditional_cleanup(self) -> tuple[PlannedSql, ...]:
        """Construct owner-scoped DELETE intents; never dispatch or unlink.

        Row payload verification, affected row counts, ordering and final
        CLEAN/RESIDUAL/UNKNOWN audit are mandatory *later* caller duties.
        """
        run = self._run_id
        entities = create_default_manifest_entities(run)
        planned = []
        for item in sorted(
            entities, key=lambda e: e.cleanup_dependency_order, reverse=True
        ):
            ent = self._registered(item.table, item.pk_value)
            marker = ent.expected_attributes
            if ent.table == "processed_articles":
                sql = (
                    "DELETE FROM processed_articles "
                    "WHERE article_id = ? AND source = ? AND title = ?"
                )
                args = (
                    ent.pk_value, marker["source"], marker["title"]
                )
            elif ent.table == "stories":
                sql = (
                    "DELETE FROM stories "
                    "WHERE story_id = ? AND category = ? AND headline_tr = ?"
                )
                args = (
                    ent.pk_value, marker["category"], marker["headline_tr"]
                )
            elif ent.table == "run_locks":
                sql = (
                    "DELETE FROM run_locks "
                    "WHERE lock_key = ? AND (worker_id = ? OR worker_id = ?)"
                )
                args = (ent.pk_value, *marker["worker_ids"])
            elif ent.table == "durable_recovery_state":
                sql = (
                    "DELETE FROM durable_recovery_state "
                    "WHERE state_key = ? AND stage = ? AND worker_id = ?"
                )
                args = (ent.pk_value, marker["stage"], marker["worker_id"])
            else:
                raise PermissionError("OFFLINE_SQL_UNSUPPORTED_TABLE")
            planned.append(PlannedSql(sql=sql, args=tuple(args)))
        return tuple(planned)
