"""Offline DACL pattern review; no ACL writes, store capability or live attestation.

The only native entry point inspects an active pytest fixture directory. Pure
snapshots are synthetic evidence, not authentication of a filesystem or caller.
No report contains paths, SIDs, raw ACEs, descriptors or exception text.
"""

from __future__ import annotations

from enum import Enum
import sys
from typing import Annotated, Literal

from pydantic import BaseModel, Field, StringConstraints, ValidationError, model_validator

from efso_radar.staging.persistent_manifest_store import (
    HermeticTestFixtureScope,
    HermeticTestManifestStore,
)

FILE_ALL_ACCESS = 0x001F01FF
MAX_ACES = 64


def _sid(authority: int, *subauthorities: int) -> bytes:
    return (
        bytes((1, len(subauthorities)))
        + authority.to_bytes(6, "big")
        + b"".join(value.to_bytes(4, "little") for value in subauthorities)
    )


_PRIVILEGED = frozenset((_sid(5, 18), _sid(5, 32, 544)))


class EvidenceStatus(str, Enum):
    POLICY_MATCH = "POLICY_MATCH"
    INSECURE = "INSECURE"
    UNVERIFIABLE = "UNVERIFIABLE"


class Reason(str, Enum):
    SNAPSHOT_MATCH = "SNAPSHOT_MATCH"
    UNSUPPORTED_PLATFORM = "UNSUPPORTED_PLATFORM"
    FIXTURE_SCOPE_REJECTED = "FIXTURE_SCOPE_REJECTED"
    INSPECTION_FAILED = "INSPECTION_FAILED"
    INVALID_EVIDENCE = "INVALID_EVIDENCE"
    UNSUPPORTED_FILESYSTEM = "UNSUPPORTED_FILESYSTEM"
    REPARSE_POINT = "REPARSE_POINT"
    TIME_OF_CHECK_DRIFT = "TIME_OF_CHECK_DRIFT"
    TOKEN_CONTEXT_UNSUPPORTED = "TOKEN_CONTEXT_UNSUPPORTED"
    OWNER_UNAVAILABLE = "OWNER_UNAVAILABLE"
    OWNER_MISMATCH = "OWNER_MISMATCH"
    MISSING_DACL = "MISSING_DACL"
    NULL_DACL = "NULL_DACL"
    EMPTY_DACL = "EMPTY_DACL"
    INHERITANCE_UNVERIFIED = "INHERITANCE_UNVERIFIED"
    FOREIGN_ALLOW = "FOREIGN_ALLOW"
    PRIVILEGED_POLICY_UNDECIDED = "PRIVILEGED_POLICY_UNDECIDED"
    DENY_SEMANTICS_UNSUPPORTED = "DENY_SEMANTICS_UNSUPPORTED"
    UNSUPPORTED_ACE = "UNSUPPORTED_ACE"
    UNSUPPORTED_MASK = "UNSUPPORTED_MASK"
    OWNER_ACCESS_UNPROVEN = "OWNER_ACCESS_UNPROVEN"


class _Frozen(BaseModel):
    model_config = {
        "extra": "forbid",
        "frozen": True,
        "strict": True,
        "revalidate_instances": "always",
    }


class AceEvidence(_Frozen):
    """Private bounded input; only basic ALLOW=0 / DENY=1 layouts are understood."""

    ace_type: int = Field(ge=0, le=255)
    flags: int = Field(ge=0, le=255)
    mask: int = Field(ge=0, le=0xFFFFFFFF)
    sid: bytes | None = Field(max_length=68, repr=False, exclude=True)


_Identity = tuple[
    Annotated[int, Field(ge=0, le=0xFFFFFFFF)],
    Annotated[int, Field(ge=0, le=0xFFFFFFFF)],
    Annotated[int, Field(ge=0, le=0xFFFFFFFF)],
]
_Digest = Annotated[bytes, Field(min_length=32, max_length=32)]


class DaclSnapshot(_Frozen):
    """Private evidence; never serialize this input as a public inspection report."""

    current_user_sid: bytes = Field(max_length=68, repr=False, exclude=True)
    owner_sid: bytes | None = Field(max_length=68, repr=False, exclude=True)
    dacl_present: bool
    dacl_null: bool
    dacl_protected: bool
    aces: tuple[AceEvidence, ...] = Field(max_length=MAX_ACES, repr=False, exclude=True)
    filesystem: Annotated[str, StringConstraints(max_length=16)]
    local_fixed_volume: bool
    reparse_point: bool
    token_elevated: bool
    impersonating: bool
    context_stable: bool
    identity_before: _Identity = Field(repr=False, exclude=True)
    identity_after: _Identity = Field(repr=False, exclude=True)
    descriptor_before: _Digest = Field(repr=False, exclude=True)
    descriptor_after: _Digest = Field(repr=False, exclude=True)


class DaclReview(_Frozen):
    status: EvidenceStatus
    reasons: tuple[Reason, ...]
    provenance: Literal[
        "NOT_COLLECTED", "HERMETIC_SIMULATION", "WINDOWS_TEST_FIXTURE"
    ] = "HERMETIC_SIMULATION"
    scope: Literal["FIXTURE_DIRECTORY_ONLY"] = "FIXTURE_DIRECTORY_ONLY"
    action: Literal["REVIEW_ONLY"] = "REVIEW_ONLY"
    live_storage_authorized: Literal[False] = False
    durability_verified: Literal[False] = False

    @model_validator(mode="after")
    def _validate_review_coherence(self) -> DaclReview:
        if not self.reasons:
            raise ValueError("Review must contain at least one reason code.")
        if self.status == EvidenceStatus.POLICY_MATCH:
            if self.reasons != (Reason.SNAPSHOT_MATCH,):
                raise ValueError(
                    "POLICY_MATCH review must contain exactly (Reason.SNAPSHOT_MATCH,)."
                )
            if self.provenance == "NOT_COLLECTED":
                raise ValueError("POLICY_MATCH cannot have NOT_COLLECTED provenance.")
        else:
            if Reason.SNAPSHOT_MATCH in self.reasons:
                raise ValueError("Non-matching review cannot contain SNAPSHOT_MATCH.")
            insecure_reasons = {
                Reason.OWNER_MISMATCH,
                Reason.MISSING_DACL,
                Reason.NULL_DACL,
                Reason.FOREIGN_ALLOW,
            }
            has_insecure = bool(insecure_reasons.intersection(self.reasons))
            if self.status == EvidenceStatus.INSECURE and not has_insecure:
                raise ValueError(
                    "INSECURE review must contain at least one insecure reason code."
                )
            if self.status == EvidenceStatus.UNVERIFIABLE and has_insecure:
                raise ValueError(
                    "UNVERIFIABLE review cannot contain insecure reason codes."
                )
        return self


def _unverifiable(reason: Reason) -> DaclReview:
    return DaclReview(status=EvidenceStatus.UNVERIFIABLE, reasons=(reason,))


class _InspectionFailure(Exception):
    def __init__(self, reason: Reason):
        super().__init__(reason.value)
        self.reason = reason


def _valid_sid(value: bytes | None) -> bool:
    return (
        value is not None
        and len(value) >= 12
        and value[0] == 1
        and 1 <= value[1] <= 15
        and len(value) == 8 + 4 * value[1]
    )


def _supported_individual_user_sid(sid: bytes | None) -> bool:
    """Check if SID matches supported individual local, domain, or AzureAD user profile.

    Separates parser validity from strict policy support. Rejects service accounts
    (SYSTEM, LocalService, NetworkService, NT SERVICE/*), virtual accounts, built-in
    groups (Administrators, Users, Guests, Backup Operators), domain groups (Domain
    Admins/Users/Guests), placeholder SIDs (CREATOR OWNER, OWNER RIGHTS, NULL SID),
    and well-known non-individual RIDs (501: Guest, 502: KRBTGT).
    """
    if not _valid_sid(sid):
        return False
    assert sid is not None
    sub_count = sid[1]
    authority = int.from_bytes(sid[2:8], "big")
    subauthorities = [
        int.from_bytes(sid[8 + 4 * i : 12 + 4 * i], "little")
        for i in range(sub_count)
    ]
    # Standard Windows Local or Domain SAM/AD Account: S-1-5-21-<d1>-<d2>-<d3>-<rid>
    if authority == 5 and sub_count == 5 and subauthorities[0] == 21:
        rid = subauthorities[4]
        # Reject well-known non-individual domain RIDs
        # 501: Guest, 502: KRBTGT, 512-527: Domain Groups, 553: RAS/IAS Servers
        if rid in {501, 502, 553} or (512 <= rid <= 527):
            return False
        return True
    # Azure AD / Entra ID User Account: S-1-12-1-<sub1>-<sub2>-<sub3>-<sub4>
    if authority == 12 and sub_count == 5 and subauthorities[0] == 1:
        return True
    return False


def review_dacl_snapshot(snapshot: DaclSnapshot) -> DaclReview:
    """Conservative pattern recognition, NOT Windows AccessCheck emulation.

    No deny ACE cancels a foreign allow for purposes of this stricter policy.
    All deny/object/callback/unknown ACE semantics remain unverified. Neither
    SYSTEM nor Administrators gets an implicit owner-private exception.
    """
    try:
        if type(snapshot) is not DaclSnapshot:
            return _unverifiable(Reason.INVALID_EVIDENCE)
        snapshot = DaclSnapshot.model_validate(snapshot)
    except (ValidationError, TypeError):
        return _unverifiable(Reason.INVALID_EVIDENCE)
    if snapshot.filesystem != "NTFS" or not snapshot.local_fixed_volume:
        return _unverifiable(Reason.UNSUPPORTED_FILESYSTEM)
    if snapshot.reparse_point:
        return _unverifiable(Reason.REPARSE_POINT)
    if (
        snapshot.identity_before != snapshot.identity_after
        or not snapshot.identity_before[0]
        or not any(snapshot.identity_before[1:])
        or snapshot.descriptor_before != snapshot.descriptor_after
        or not snapshot.context_stable
    ):
        return _unverifiable(Reason.TIME_OF_CHECK_DRIFT)
    if (
        snapshot.token_elevated
        or snapshot.impersonating
        or not _supported_individual_user_sid(snapshot.current_user_sid)
    ):
        return _unverifiable(Reason.TOKEN_CONTEXT_UNSUPPORTED)
    if not _valid_sid(snapshot.current_user_sid) or not _valid_sid(snapshot.owner_sid):
        return _unverifiable(Reason.OWNER_UNAVAILABLE)
    if (not snapshot.dacl_present and (snapshot.dacl_null or snapshot.aces)) or (
        snapshot.dacl_null and snapshot.aces
    ):
        return _unverifiable(Reason.INVALID_EVIDENCE)
    insecure: list[Reason] = []
    unknown: list[Reason] = []
    if snapshot.owner_sid != snapshot.current_user_sid:
        insecure.append(Reason.OWNER_MISMATCH)
    if not snapshot.dacl_present:
        insecure.append(Reason.MISSING_DACL)
    if snapshot.dacl_null:
        insecure.append(Reason.NULL_DACL)
    if not snapshot.aces:
        unknown.append(Reason.EMPTY_DACL)
    if not snapshot.dacl_protected:
        unknown.append(Reason.INHERITANCE_UNVERIFIED)
    owner_mask = 0
    for ace in snapshot.aces:
        if ace.ace_type not in (0, 1):
            unknown.append(Reason.UNSUPPORTED_ACE)
            continue
        if not _valid_sid(ace.sid):
            unknown.append(Reason.UNSUPPORTED_ACE)
            continue
        if ace.flags & ~0x1F:
            unknown.append(Reason.UNSUPPORTED_ACE)
        if ace.flags & 0x1C:  # inherited, inherit-only, no-propagate
            unknown.append(Reason.INHERITANCE_UNVERIFIED)
        if ace.ace_type == 1:
            unknown.append(Reason.DENY_SEMANTICS_UNSUPPORTED)
        else:
            if ace.sid != snapshot.current_user_sid:
                if ace.sid in _PRIVILEGED:
                    unknown.append(Reason.PRIVILEGED_POLICY_UNDECIDED)
                else:
                    insecure.append(Reason.FOREIGN_ALLOW)
            elif not (ace.flags & 0x08):
                owner_mask |= ace.mask

            if not ace.mask or (ace.mask & ~FILE_ALL_ACCESS):
                unknown.append(Reason.UNSUPPORTED_MASK)
    if owner_mask != FILE_ALL_ACCESS:
        unknown.append(Reason.OWNER_ACCESS_UNPROVEN)
    reasons = tuple(dict.fromkeys(insecure + unknown))
    if insecure:
        return DaclReview(status=EvidenceStatus.INSECURE, reasons=reasons)
    if unknown:
        return DaclReview(status=EvidenceStatus.UNVERIFIABLE, reasons=reasons)
    return DaclReview(
        status=EvidenceStatus.POLICY_MATCH, reasons=(Reason.SNAPSHOT_MATCH,)
    )


def inspect_test_fixture_dacl(scope: HermeticTestFixtureScope) -> DaclReview:
    """Explicit read-only inspection of exactly one active pytest fixture root.

    No arbitrary path argument, directory creation, content read, ACL setter or
    store integration. Scope provenance is a misuse guardrail, not a privilege
    boundary against hostile code in the same process. Any error fails closed.
    """

    def unavailable(reason: Reason) -> DaclReview:
        return DaclReview(
            status=EvidenceStatus.UNVERIFIABLE,
            reasons=(reason,),
            provenance="NOT_COLLECTED",
        )

    def native_failure(reason: Reason) -> DaclReview:
        return DaclReview(
            status=EvidenceStatus.UNVERIFIABLE,
            reasons=(reason,),
            provenance="WINDOWS_TEST_FIXTURE",
        )

    if (
        type(scope) is not HermeticTestFixtureScope
        or getattr(scope, "_entered", False) is not True
    ):
        return unavailable(Reason.FIXTURE_SCOPE_REJECTED)
    if sys.platform != "win32":
        return unavailable(Reason.UNSUPPORTED_PLATFORM)
    try:
        root = scope.fixture_root
        if root not in HermeticTestManifestStore._AUTHORIZED_FIXTURE_ROOTS:
            return unavailable(Reason.FIXTURE_SCOPE_REJECTED)
        from efso_radar.staging._windows_dacl_reader import require_local_fixed_drive

        require_local_fixed_drive(root)
        if HermeticTestFixtureScope._validate_fixture_root(root) != root:
            return unavailable(Reason.FIXTURE_SCOPE_REJECTED)
        st_before = root.stat(follow_symlinks=False)
        expected_dev = st_before.st_dev & 0xFFFFFFFF
        expected_inode = st_before.st_ino
        if not expected_dev or not expected_inode:
            return unavailable(Reason.TIME_OF_CHECK_DRIFT)
    except _InspectionFailure as exc:
        return unavailable(exc.reason)
    except Exception:
        return unavailable(Reason.FIXTURE_SCOPE_REJECTED)
    try:
        from efso_radar.staging._windows_dacl_reader import read_fixture_snapshot

        snapshot = read_fixture_snapshot(
            root, expected_dev=expected_dev, expected_inode=expected_inode
        )
        if HermeticTestFixtureScope._validate_fixture_root(root) != root:
            return native_failure(Reason.TIME_OF_CHECK_DRIFT)
        st_after = root.stat(follow_symlinks=False)
        if (st_after.st_dev & 0xFFFFFFFF) != expected_dev or st_after.st_ino != expected_inode:
            return native_failure(Reason.TIME_OF_CHECK_DRIFT)
        if (
            not scope._entered
            or root not in HermeticTestManifestStore._AUTHORIZED_FIXTURE_ROOTS
        ):
            return native_failure(Reason.TIME_OF_CHECK_DRIFT)
        return review_dacl_snapshot(snapshot).model_copy(
            update={"provenance": "WINDOWS_TEST_FIXTURE"}
        )
    except _InspectionFailure as exc:
        return native_failure(exc.reason)
    except Exception:
        # This native boundary must never expose API errors, paths, SIDs or ACEs.
        return native_failure(Reason.INSPECTION_FAILED)
