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
from efso_radar.staging.runner import (
    AuditOutcomeState,
    SessionStatus,
    StagingSessionRunner,
    StagingRunnerReport,
    PerRunOwnershipManifest,
    validate_staging_destination,
    EXACT_STAGING_DESTINATION,
    EXACT_STAGING_HOSTNAME,
    recover_manifest_offline,
)

if TYPE_CHECKING:
    from efso_radar.staging.connection import (
        StagingConnectionReport,
        check_staging_connection,
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
    "validate_staging_destination",
    "EXACT_STAGING_DESTINATION",
    "EXACT_STAGING_HOSTNAME",
    "recover_manifest_offline",
    "StagingConnectionReport",
    "check_staging_connection",
]


def __getattr__(name: str) -> Any:
    if name in ("StagingConnectionReport", "check_staging_connection"):
        from efso_radar.staging.connection import (
            StagingConnectionReport,
            check_staging_connection,
        )
        mapping = {
            "StagingConnectionReport": StagingConnectionReport,
            "check_staging_connection": check_staging_connection,
        }
        return mapping[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
