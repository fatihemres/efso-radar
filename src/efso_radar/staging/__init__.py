"""Staging preflight, recovery inspection, and cutover readiness for Efso Radar."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from efso_radar.staging.preflight import (
    CloudPreflightValidator,
    PreflightReport,
)
from efso_radar.staging.recovery_inspection import (
    DeliveryRecoveryInspector,
    RecoveryInspectionReport,
)
from efso_radar.staging.cutover import (
    StagingCutoverReconciler,
    StagingCutoverReport,
    GoNoGoGate,
    GoNoGoDecision,
)

if TYPE_CHECKING:
    from efso_radar.staging.connection import (
        StagingConnectionReport,
        check_staging_connection,
    )
    from efso_radar.staging.runner import (
        AuditOutcomeState,
        SessionStatus,
        StagingSessionRunner,
        StagingRunnerReport,
        PerRunOwnershipManifest,
        RegisteredEntity,
        create_default_manifest_entities,
        is_hermetic_transport,
        verify_hermetic_session_transport,
        is_ambiguous_transaction_error,
        reconcile_ambiguous_outcome,
        validate_staging_destination,
        EXACT_STAGING_DESTINATION,
        EXACT_STAGING_HOSTNAME,
        recover_manifest_offline,
        execute_controlled_staging,
    )
    from efso_radar.staging.manifest_seal import (
        OfflineManifestSeal,
        ManifestSealViolation,
    )
    from efso_radar.staging.persistent_manifest_store import (
        PersistentManifestStore,
        HermeticTestManifestStore,
        HermeticTestFixtureScope,
        ManifestStoreError,
        ManifestStoreSecurityError,
        ManifestStoreFileExistsError,
        ManifestStoreCorruptionError,
        ManifestStoreConcurrencyError,
        ManifestStorePostPublicationError,
        StoragePlatformGuarantees,
        get_storage_platform_guarantees,
    )
    from efso_radar.staging.hrana_outbound_guard import (
        HranaOutboundSecurityBoundary,
        GuardedHermeticHranaTransport,
        AuthorizedMockIntent,
        MockHranaDenied,
    )
    from efso_radar.staging.live_ready_operator import (
        LiveReadyStagingOperator,
        LiveReadyOperatorReport,
        LiveReadyOperatorStatus,
    )
    from efso_radar.staging.journal import (
        SCHEMA_V7_STAGING_JOURNAL_DDL,
        apply_candidate_schema_v7,
        JournalStatus,
        ReadProvenance,
        JournalExecutionPattern,
        StagingJournalEntry,
        StagingOwnershipJournal,
        JournalError,
        InvalidRunIdError,
        InvalidJournalTransitionError,
        FencingConflictError,
        StaleOwnerError,
        ManifestTamperedError,
        AmbiguousCommitError,
        ActiveWorkloadCollisionError,
        TargetInventoryViolationError,
        UnauthorizedTakeoverError,
        validate_status_transition,
        canonical_inventory_json,
    )

__all__ = [
    "CloudPreflightValidator",
    "PreflightReport",
    "DeliveryRecoveryInspector",
    "RecoveryInspectionReport",
    "StagingCutoverReconciler",
    "StagingCutoverReport",
    "GoNoGoGate",
    "GoNoGoDecision",
    "AuditOutcomeState",
    "SessionStatus",
    "StagingSessionRunner",
    "StagingRunnerReport",
    "PerRunOwnershipManifest",
    "RegisteredEntity",
    "create_default_manifest_entities",
    "is_hermetic_transport",
    "verify_hermetic_session_transport",
    "is_ambiguous_transaction_error",
    "reconcile_ambiguous_outcome",
    "validate_staging_destination",
    "EXACT_STAGING_DESTINATION",
    "EXACT_STAGING_HOSTNAME",
    "recover_manifest_offline",
    "execute_controlled_staging",
    "OfflineManifestSeal",
    "ManifestSealViolation",
    "PersistentManifestStore",
    "HermeticTestManifestStore",
    "HermeticTestFixtureScope",
    "ManifestStoreError",
    "ManifestStoreSecurityError",
    "ManifestStoreFileExistsError",
    "ManifestStoreCorruptionError",
    "ManifestStoreConcurrencyError",
    "ManifestStorePostPublicationError",
    "StoragePlatformGuarantees",
    "get_storage_platform_guarantees",
    "StagingConnectionReport",
    "check_staging_connection",
    "HranaOutboundSecurityBoundary",
    "GuardedHermeticHranaTransport",
    "AuthorizedMockIntent",
    "MockHranaDenied",
    "LiveReadyStagingOperator",
    "LiveReadyOperatorReport",
    "LiveReadyOperatorStatus",
    "SCHEMA_V7_STAGING_JOURNAL_DDL",
    "apply_candidate_schema_v7",
    "JournalStatus",
    "ReadProvenance",
    "JournalExecutionPattern",
    "StagingJournalEntry",
    "StagingOwnershipJournal",
    "JournalError",
    "InvalidRunIdError",
    "InvalidJournalTransitionError",
    "FencingConflictError",
    "StaleOwnerError",
    "ManifestTamperedError",
    "AmbiguousCommitError",
    "ActiveWorkloadCollisionError",
    "TargetInventoryViolationError",
    "UnauthorizedTakeoverError",
    "validate_status_transition",
    "canonical_inventory_json",
]

_RUNNER_EXPORTS = frozenset({
    "AuditOutcomeState",
    "SessionStatus",
    "StagingSessionRunner",
    "StagingRunnerReport",
    "PerRunOwnershipManifest",
    "RegisteredEntity",
    "create_default_manifest_entities",
    "is_hermetic_transport",
    "verify_hermetic_session_transport",
    "is_ambiguous_transaction_error",
    "reconcile_ambiguous_outcome",
    "validate_staging_destination",
    "EXACT_STAGING_DESTINATION",
    "EXACT_STAGING_HOSTNAME",
    "recover_manifest_offline",
    "execute_controlled_staging",
})

_SEAL_EXPORTS = frozenset({
    "OfflineManifestSeal",
    "ManifestSealViolation",
})

_PERSISTENT_STORE_EXPORTS = frozenset({
    "PersistentManifestStore",
    "HermeticTestManifestStore",
    "HermeticTestFixtureScope",
    "ManifestStoreError",
    "ManifestStoreSecurityError",
    "ManifestStoreFileExistsError",
    "ManifestStoreCorruptionError",
    "ManifestStoreConcurrencyError",
    "ManifestStorePostPublicationError",
    "StoragePlatformGuarantees",
    "get_storage_platform_guarantees",
})

_CONNECTION_EXPORTS = frozenset({
    "StagingConnectionReport",
    "check_staging_connection",
})

_GUARD_EXPORTS = frozenset({
    "HranaOutboundSecurityBoundary",
    "GuardedHermeticHranaTransport",
    "AuthorizedMockIntent",
    "MockHranaDenied",
})

_OPERATOR_EXPORTS = frozenset({
    "LiveReadyStagingOperator",
    "LiveReadyOperatorReport",
    "LiveReadyOperatorStatus",
})

_JOURNAL_EXPORTS = frozenset({
    "SCHEMA_V7_STAGING_JOURNAL_DDL",
    "apply_candidate_schema_v7",
    "JournalStatus",
    "ReadProvenance",
    "JournalExecutionPattern",
    "StagingJournalEntry",
    "StagingOwnershipJournal",
    "JournalError",
    "InvalidRunIdError",
    "InvalidJournalTransitionError",
    "FencingConflictError",
    "StaleOwnerError",
    "ManifestTamperedError",
    "AmbiguousCommitError",
    "ActiveWorkloadCollisionError",
    "TargetInventoryViolationError",
    "UnauthorizedTakeoverError",
    "validate_status_transition",
    "canonical_inventory_json",
})


def __getattr__(name: str) -> Any:
    if name in _CONNECTION_EXPORTS:
        from efso_radar.staging.connection import (
            StagingConnectionReport,
            check_staging_connection,
        )
        mapping = {
            "StagingConnectionReport": StagingConnectionReport,
            "check_staging_connection": check_staging_connection,
        }
        return mapping[name]
    if name in _RUNNER_EXPORTS:
        import efso_radar.staging.runner as runner_mod
        return getattr(runner_mod, name)
    if name in _SEAL_EXPORTS:
        import efso_radar.staging.manifest_seal as seal_mod
        return getattr(seal_mod, name)
    if name in _PERSISTENT_STORE_EXPORTS:
        import efso_radar.staging.persistent_manifest_store as pstore_mod
        return getattr(pstore_mod, name)
    if name in _GUARD_EXPORTS:
        import efso_radar.staging.hrana_outbound_guard as guard_mod
        return getattr(guard_mod, name)
    if name in _OPERATOR_EXPORTS:
        import efso_radar.staging.live_ready_operator as op_mod
        return getattr(op_mod, name)
    if name in _JOURNAL_EXPORTS:
        import efso_radar.staging.journal as journal_mod
        return getattr(journal_mod, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
