"""Issue #41: synthetic security metadata and one read-only pytest fixture probe."""

from __future__ import annotations

from contextlib import ExitStack
import json
from pathlib import Path, PureWindowsPath
import struct
import sys
from unittest.mock import patch
from unittest.mock import Mock
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from efso_radar.staging import windows_dacl_evidence as policy
from efso_radar.staging._windows_dacl_reader import _copy_bounded, _copy_sid, _parse_acl
from efso_radar.staging import _windows_dacl_reader as reader
from efso_radar.staging.persistent_manifest_store import (
    HermeticTestFixtureScope,
    HermeticTestManifestStore,
    ManifestStoreSecurityError,
    PersistentManifestStore,
    get_storage_platform_guarantees,
)
from efso_radar.staging.runner import AuditOutcomeState

OWNER = policy._sid(5, 21, 111, 222, 333, 1001)
OTHER = policy._sid(5, 21, 111, 222, 333, 1002)
SECRET = "Bearer private-fixture-marker https://private.invalid/ S-1-5-private"


def ace(sid=OWNER, mask=policy.FILE_ALL_ACCESS, kind=0, flags=0):
    return policy.AceEvidence(ace_type=kind, flags=flags, mask=mask, sid=sid)


def snapshot(**updates):
    values = dict(
        current_user_sid=OWNER,
        owner_sid=OWNER,
        dacl_present=True,
        dacl_null=False,
        dacl_protected=True,
        aces=(ace(),),
        filesystem="NTFS",
        local_fixed_volume=True,
        reparse_point=False,
        token_elevated=False,
        impersonating=False,
        context_stable=True,
        identity_before=(123, 0, 456),
        identity_after=(123, 0, 456),
        descriptor_before=b"a" * 32,
        descriptor_after=b"a" * 32,
    )
    values.update(updates)
    return policy.DaclSnapshot(**values)


def check_report(report):
    assert not report.live_storage_authorized
    assert not report.durability_verified
    assert report.action == "REVIEW_ONLY"
    rendered = report.model_dump_json()
    assert SECRET not in rendered
    assert "S-1-" not in rendered
    assert "https://" not in rendered
    assert "owner_sid" not in rendered and "aces" not in rendered
    assert "CLEAN" not in rendered and "LIVE_READY" not in rendered
    return report


def review(value):
    with ExitStack() as stack:
        for name in (
            "builtins.open",
            "io.open",
            "pathlib.Path.stat",
            "os.chmod",
            "os.mkdir",
            "socket.socket",
            "sqlite3.connect",
            "httpx.Client.send",
        ):
            stack.enter_context(
                patch(name, side_effect=AssertionError("Pure policy attempted I/O"))
            )
        report = policy.review_dacl_snapshot(value)
    return check_report(report)


@pytest.fixture
def mocked_native_volume(monkeypatch):
    """Only synthetic adapter tests bypass drive probing; real probe never uses it."""
    monkeypatch.setattr(reader, "require_local_fixed_drive", lambda path: None)


def test_minimum_owner_only_pattern_is_inert_and_deterministic():
    result = review(snapshot())
    assert result.status == policy.EvidenceStatus.POLICY_MATCH
    assert result.reasons == (policy.Reason.SNAPSHOT_MATCH,)
    assert result == review(snapshot())
    with pytest.raises(ValidationError):
        policy.DaclReview.model_validate(
            {**result.model_dump(), "live_storage_authorized": True}
        )


@pytest.mark.parametrize(
    "changes,reason,status",
    [
        ({"owner_sid": None}, policy.Reason.OWNER_UNAVAILABLE, "UNVERIFIABLE"),
        ({"owner_sid": OTHER}, policy.Reason.OWNER_MISMATCH, "INSECURE"),
        ({"dacl_present": False, "aces": ()}, policy.Reason.MISSING_DACL, "INSECURE"),
        ({"dacl_null": True, "aces": ()}, policy.Reason.NULL_DACL, "INSECURE"),
        ({"aces": ()}, policy.Reason.EMPTY_DACL, "UNVERIFIABLE"),
        (
            {"dacl_protected": False},
            policy.Reason.INHERITANCE_UNVERIFIED,
            "UNVERIFIABLE",
        ),
        ({"filesystem": "FAT32"}, policy.Reason.UNSUPPORTED_FILESYSTEM, "UNVERIFIABLE"),
        ({"filesystem": "ReFS"}, policy.Reason.UNSUPPORTED_FILESYSTEM, "UNVERIFIABLE"),
        (
            {"local_fixed_volume": False},
            policy.Reason.UNSUPPORTED_FILESYSTEM,
            "UNVERIFIABLE",
        ),
        ({"reparse_point": True}, policy.Reason.REPARSE_POINT, "UNVERIFIABLE"),
        (
            {"identity_after": (123, 0, 999)},
            policy.Reason.TIME_OF_CHECK_DRIFT,
            "UNVERIFIABLE",
        ),
        (
            {"identity_after": (999, 0, 456)},
            policy.Reason.TIME_OF_CHECK_DRIFT,
            "UNVERIFIABLE",
        ),
        (
            {"descriptor_after": b"b" * 32},
            policy.Reason.TIME_OF_CHECK_DRIFT,
            "UNVERIFIABLE",
        ),
        ({"context_stable": False}, policy.Reason.TIME_OF_CHECK_DRIFT, "UNVERIFIABLE"),
        (
            {"token_elevated": True},
            policy.Reason.TOKEN_CONTEXT_UNSUPPORTED,
            "UNVERIFIABLE",
        ),
        (
            {"impersonating": True},
            policy.Reason.TOKEN_CONTEXT_UNSUPPORTED,
            "UNVERIFIABLE",
        ),
    ],
)
def test_missing_unsafe_or_unverifiable_evidence(changes, reason, status):
    result = review(snapshot(**changes))
    assert result.status.value == status
    assert reason in result.reasons


@pytest.mark.parametrize(
    "sid",
    [
        policy._sid(1, 0),
        policy._sid(5, 11),
        policy._sid(5, 32, 545),
        policy._sid(5, 7),
        OTHER,
    ],
)
@pytest.mark.parametrize("flags", [0, 0x10, 0x03, 0x0B])
def test_explicit_inherited_and_child_only_foreign_allow_is_rejected(sid, flags):
    result = review(snapshot(aces=(ace(), ace(sid, mask=1, flags=flags))))
    assert result.status == policy.EvidenceStatus.INSECURE
    assert policy.Reason.FOREIGN_ALLOW in result.reasons


@pytest.mark.parametrize("sid", [policy._sid(5, 18), policy._sid(5, 32, 544)])
def test_no_implicit_system_or_administrators_exception(sid):
    result = review(snapshot(aces=(ace(), ace(sid))))
    assert result.status == policy.EvidenceStatus.UNVERIFIABLE
    assert policy.Reason.PRIVILEGED_POLICY_UNDECIDED in result.reasons


@pytest.mark.parametrize("deny_first", [True, False])
def test_deny_does_not_cancel_foreign_allow_in_either_order(deny_first):
    denied, allowed = ace(OTHER, kind=1), ace(OTHER)
    pair = (denied, allowed) if deny_first else (allowed, denied)
    result = review(snapshot(aces=(ace(), *pair)))
    assert result.status == policy.EvidenceStatus.INSECURE
    assert policy.Reason.FOREIGN_ALLOW in result.reasons
    assert policy.Reason.DENY_SEMANTICS_UNSUPPORTED in result.reasons


@pytest.mark.parametrize(
    "extra,reason",
    [
        (ace(kind=1), policy.Reason.DENY_SEMANTICS_UNSUPPORTED),
        (ace(kind=5), policy.Reason.UNSUPPORTED_ACE),
        (ace(kind=9), policy.Reason.UNSUPPORTED_ACE),
        (ace(kind=255), policy.Reason.UNSUPPORTED_ACE),
        (ace(flags=0x80), policy.Reason.UNSUPPORTED_ACE),
        (ace(flags=0x10), policy.Reason.INHERITANCE_UNVERIFIED),
        (ace(mask=0x10000000), policy.Reason.UNSUPPORTED_MASK),
        (ace(mask=0x80000000), policy.Reason.UNSUPPORTED_MASK),
    ],
)
def test_unsupported_semantics_never_match(extra, reason):
    result = review(snapshot(aces=(ace(), extra)))
    assert result.status == policy.EvidenceStatus.UNVERIFIABLE
    assert reason in result.reasons


def test_ownership_alone_and_partial_owner_grant_are_not_proof():
    for aces in ((), (ace(mask=0),), (ace(mask=1),), (ace(flags=8),)):
        result = review(snapshot(aces=aces))
        assert result.status == policy.EvidenceStatus.UNVERIFIABLE
        assert policy.Reason.OWNER_ACCESS_UNPROVEN in result.reasons


@pytest.mark.parametrize(
    "updates",
    [
        {"aces": (ace(),) * 65},
        {"current_user_sid": b"malformed"},
        {"owner_sid": b"\x01\x0f"},
        {"token_elevated": "false"},
        {"filesystem": SECRET},
        {"descriptor_before": b"short"},
    ],
)
def test_malformed_constructed_evidence_is_sanitized(updates, caplog):
    result = review(snapshot().model_copy(update=updates))
    assert result.status == policy.EvidenceStatus.UNVERIFIABLE
    assert SECRET not in caplog.text


def test_private_inputs_do_not_serialize_sids_or_raw_aces():
    dumped = snapshot().model_dump()
    assert "owner_sid" not in dumped and "current_user_sid" not in dumped
    assert "aces" not in dumped and "descriptor_before" not in dumped


@pytest.mark.parametrize("changes", [{"dacl_present": False}, {"dacl_null": True}])
def test_contradictory_descriptor_evidence_is_unverifiable(changes):
    assert review(snapshot(**changes)).reasons == (policy.Reason.INVALID_EVIDENCE,)


@pytest.mark.parametrize(
    "sid", [policy._sid(5, 18), policy._sid(5, 19), policy._sid(5, 20)]
)
def test_service_identity_cannot_claim_owner_only_match(sid):
    result = review(snapshot(current_user_sid=sid, owner_sid=sid, aces=(ace(sid),)))
    assert result.reasons == (policy.Reason.TOKEN_CONTEXT_UNSUPPORTED,)


def test_scope_required_before_any_native_inspection():
    for invalid in (
        None,
        Path(".env"),
        object(),
        object.__new__(HermeticTestFixtureScope),
    ):
        with patch(
            "pathlib.Path.stat", side_effect=AssertionError("No arbitrary stat")
        ):
            result = policy.inspect_test_fixture_dacl(invalid)
        assert result.reasons == (policy.Reason.FIXTURE_SCOPE_REJECTED,)


def test_internal_reader_also_rejects_unscoped_paths_before_api_or_metadata():
    with patch.object(
        reader.ctypes,
        "WinDLL",
        create=True,
        side_effect=AssertionError("No native API"),
    ):
        with patch("pathlib.Path.stat", side_effect=AssertionError("No metadata")):
            with pytest.raises(policy._InspectionFailure) as rejected:
                reader.read_fixture_snapshot(Path(".env"), expected_inode=0)
    assert rejected.value.reason == policy.Reason.FIXTURE_SCOPE_REJECTED


def test_unsupported_platform_does_not_load_native_reader(tmp_path):
    with (
        HermeticTestFixtureScope(tmp_path) as scope,
        patch.object(policy.sys, "platform", "linux"),
    ):
        with patch(
            "efso_radar.staging._windows_dacl_reader.read_fixture_snapshot",
            side_effect=AssertionError("No native read on Linux"),
        ):
            result = check_report(policy.inspect_test_fixture_dacl(scope))
        assert result.reasons == (policy.Reason.UNSUPPORTED_PLATFORM,)
        assert result.provenance == "NOT_COLLECTED"


@pytest.mark.parametrize(
    "failure", [PermissionError(SECRET), OSError(SECRET), RuntimeError(SECRET)]
)
def test_native_errors_never_leak_paths_sids_or_exceptions(
    tmp_path, failure, caplog, mocked_native_volume
):
    with (
        HermeticTestFixtureScope(tmp_path) as scope,
        patch.object(policy.sys, "platform", "win32"),
    ):
        with patch(
            "efso_radar.staging._windows_dacl_reader.read_fixture_snapshot",
            side_effect=failure,
        ):
            result = check_report(policy.inspect_test_fixture_dacl(scope))
        assert result.reasons == (policy.Reason.INSPECTION_FAILED,)
        assert str(tmp_path) not in result.model_dump_json()
        assert SECRET not in caplog.text


def test_scope_revalidation_reparse_and_drift_fail_closed(
    tmp_path, mocked_native_volume
):
    with (
        HermeticTestFixtureScope(tmp_path) as scope,
        patch.object(policy.sys, "platform", "win32"),
    ):
        with patch.object(
            HermeticTestFixtureScope,
            "_validate_fixture_root",
            side_effect=OSError(SECRET),
        ):
            assert policy.inspect_test_fixture_dacl(scope).reasons == (
                policy.Reason.FIXTURE_SCOPE_REJECTED,
            )
        with patch(
            "efso_radar.staging._windows_dacl_reader.read_fixture_snapshot",
            side_effect=policy._InspectionFailure(policy.Reason.REPARSE_POINT),
        ):
            assert policy.inspect_test_fixture_dacl(scope).reasons == (
                policy.Reason.REPARSE_POINT,
            )
        with patch.object(
            HermeticTestFixtureScope,
            "_validate_fixture_root",
            side_effect=[tmp_path, tmp_path / "changed"],
        ):
            with patch(
                "efso_radar.staging._windows_dacl_reader.read_fixture_snapshot",
                return_value=snapshot(),
            ):
                assert policy.inspect_test_fixture_dacl(scope).reasons == (
                    policy.Reason.TIME_OF_CHECK_DRIFT,
                )
    assert policy.inspect_test_fixture_dacl(scope).reasons == (
        policy.Reason.FIXTURE_SCOPE_REJECTED,
    )


def test_match_never_promotes_existing_windows_gates(tmp_path, mocked_native_volume):
    assert review(snapshot()).status == policy.EvidenceStatus.POLICY_MATCH
    with (
        HermeticTestFixtureScope(tmp_path) as scope,
        patch.object(policy.sys, "platform", "win32"),
    ):
        with patch(
            "efso_radar.staging._windows_dacl_reader.read_fixture_snapshot",
            return_value=snapshot(),
        ):
            result = check_report(policy.inspect_test_fixture_dacl(scope))
            assert result.status == policy.EvidenceStatus.POLICY_MATCH
            assert result.provenance == "WINDOWS_TEST_FIXTURE"
        with pytest.raises(ManifestStoreSecurityError, match="DACL/ACL"):
            get_storage_platform_guarantees().verify_platform_policy()
        candidate = tmp_path / "candidate"
        candidate.mkdir()
        # Test-owned paths only; make the temp exclusion neutral so the independent
        # Windows constructor DACL gate itself is exercised, without ACL changes.
        real_exists = Path.exists

        def without_temp_fallbacks(path):
            return (
                False if path in (Path("/tmp"), Path("/var/tmp")) else real_exists(path)
            )

        with (
            patch(
                "efso_radar.staging.persistent_manifest_store.tempfile.gettempdir",
                return_value=str(tmp_path / "other"),
            ),
            patch.object(Path, "exists", without_temp_fallbacks),
        ):
            with pytest.raises(
                ManifestStoreSecurityError, match="Windows NTFS DACL/ACL"
            ):
                PersistentManifestStore(candidate, live_authorized=True)
        disabled = object.__new__(PersistentManifestStore)
        disabled._is_hermetic_test = False
        with pytest.raises(
            ManifestStoreSecurityError, match="writes are strictly disabled"
        ):
            disabled.save_manifest(None)
        store = HermeticTestManifestStore(tmp_path / "store")
        with pytest.raises(
            ManifestStoreSecurityError, match="deletion is strictly disabled"
        ):
            store.delete_manifest_if_clean(
                "run_20261029T120000Z_a1b2c3d4e5f6", AuditOutcomeState.CLEAN
            )


def acl_blob(aces):
    body = b"".join(
        struct.pack("<BBHI", a.ace_type, a.flags, 8 + len(a.sid), a.mask) + a.sid
        for a in aces
    )
    return struct.pack("<BBHHH", 2, 0, 8 + len(body), len(aces), 0) + body


def test_bounded_native_acl_decoder_preserves_order_flags_and_masks():
    aces = (ace(OTHER, kind=1), ace(flags=3))
    assert _parse_acl(acl_blob(aces)) == aces
    unknown = struct.pack("<BBHHHBBH", 4, 0, 12, 1, 0, 9, 0, 4)
    assert _parse_acl(unknown)[0].ace_type == 9


@pytest.mark.parametrize(
    "raw", [r"\\uncontacted.invalid\share\fixture", r"\\?\C:\fixture", "relative"]
)
def test_network_device_and_relative_paths_rejected_without_native_or_path_access(raw):
    with patch.object(
        reader.ctypes, "WinDLL", create=True, side_effect=AssertionError("No DLL call")
    ):
        with patch("pathlib.Path.stat", side_effect=AssertionError("No path metadata")):
            with pytest.raises(policy._InspectionFailure) as rejected:
                reader.require_local_fixed_drive(PureWindowsPath(raw))
    assert rejected.value.reason == policy.Reason.UNSUPPORTED_FILESYSTEM


@pytest.mark.parametrize("drive_type", [0, 1, 2, 4, 5, 6])
def test_nonfixed_including_mapped_network_drive_rejected_before_path_access(
    drive_type,
):
    probe = Mock(return_value=drive_type)
    kernel = SimpleNamespace(GetDriveTypeW=probe)
    with patch.object(reader.ctypes, "WinDLL", create=True, return_value=kernel):
        with patch("pathlib.Path.stat", side_effect=AssertionError("No path metadata")):
            with pytest.raises(policy._InspectionFailure) as rejected:
                reader.require_local_fixed_drive(PureWindowsPath(r"Z:\fixture"))
    probe.assert_called_once_with("Z:\\")
    assert rejected.value.reason == policy.Reason.UNSUPPORTED_FILESYSTEM


@pytest.mark.parametrize(
    "raw",
    [
        b"",
        b"x" * 7,
        b"x" * 65537,
        struct.pack("<BBHHH", 2, 0, 8, 65, 0),
        struct.pack("<BBHHHBBH", 2, 0, 12, 1, 0, 0, 0, 4),
    ],
    ids=["empty", "short", "oversized", "too-many-aces", "short-basic-ace"],
)
def test_native_decoder_rejects_malformed_and_unbounded_data(raw):
    with pytest.raises(ValueError):
        _parse_acl(raw)


@pytest.mark.skipif(
    sys.platform != "win32", reason="Actual Win32 fixture probe requires Windows"
)
def test_real_windows_fixture_probe_is_read_only_and_sanitized(tmp_path):
    sentinel = tmp_path / "fixture-sentinel.txt"
    sentinel.write_bytes(b"owned-test-fixture")
    before = sentinel.read_bytes()
    names = set(tmp_path.iterdir())
    captured = []
    native = reader.read_fixture_snapshot

    def capture(*args, **kwargs):
        sample = native(*args, **kwargs)
        captured.append(sample)
        return sample

    with HermeticTestFixtureScope(tmp_path) as scope:
        with patch.object(reader, "read_fixture_snapshot", side_effect=capture):
            result = check_report(policy.inspect_test_fixture_dacl(scope))
        with pytest.raises(policy._InspectionFailure) as changed:
            native(tmp_path, expected_inode=tmp_path.stat().st_ino + 1)
        assert changed.value.reason == policy.Reason.TIME_OF_CHECK_DRIFT
    assert len(captured) == 1  # Actual native descriptor collection succeeded.
    assert result.provenance == "WINDOWS_TEST_FIXTURE"
    assert policy.Reason.INSPECTION_FAILED not in result.reasons
    assert policy.Reason.FIXTURE_SCOPE_REJECTED not in result.reasons
    assert sentinel.read_bytes() == before and set(tmp_path.iterdir()) == names
    assert str(tmp_path) not in result.model_dump_json()
    # A default inherited fixture ACL may be UNVERIFIABLE/INSECURE. Never mutate
    # it to manufacture POLICY_MATCH, or infer operator-host isolation from it.
    assert json.loads(result.model_dump_json())["live_storage_authorized"] is False


def test_zero_mask_allow_ace_never_produces_policy_match():
    # Owner with mask == 0 alone
    res = review(snapshot(aces=(ace(OWNER, mask=0),)))
    assert res.status == policy.EvidenceStatus.UNVERIFIABLE
    assert policy.Reason.UNSUPPORTED_MASK in res.reasons
    assert policy.Reason.OWNER_ACCESS_UNPROVEN in res.reasons

    # Owner with valid mask + additional owner ACE with mask == 0
    res2 = review(
        snapshot(
            aces=(
                ace(OWNER, mask=policy.FILE_ALL_ACCESS),
                ace(OWNER, mask=0),
            )
        )
    )
    assert res2.status == policy.EvidenceStatus.UNVERIFIABLE
    assert policy.Reason.UNSUPPORTED_MASK in res2.reasons

    # Foreign with mask == 0: MUST be classified INSECURE with FOREIGN_ALLOW and UNSUPPORTED_MASK
    res3 = review(
        snapshot(
            aces=(
                ace(OWNER, mask=policy.FILE_ALL_ACCESS),
                ace(OTHER, mask=0),
            )
        )
    )
    assert res3.status == policy.EvidenceStatus.INSECURE
    assert policy.Reason.FOREIGN_ALLOW in res3.reasons
    assert policy.Reason.UNSUPPORTED_MASK in res3.reasons

    # Privileged with mask == 0: UNVERIFIABLE with PRIVILEGED_POLICY_UNDECIDED and UNSUPPORTED_MASK
    res4 = review(
        snapshot(
            aces=(
                ace(OWNER, mask=policy.FILE_ALL_ACCESS),
                ace(policy._sid(5, 18), mask=0),
            )
        )
    )
    assert res4.status == policy.EvidenceStatus.UNVERIFIABLE
    assert policy.Reason.PRIVILEGED_POLICY_UNDECIDED in res4.reasons
    assert policy.Reason.UNSUPPORTED_MASK in res4.reasons


@pytest.mark.parametrize(
    "sid",
    [
        policy._sid(5, 18),  # Local System
        policy._sid(5, 19),  # Local Service
        policy._sid(5, 20),  # Network Service
        policy._sid(5, 32, 544),  # Builtin Administrators
        policy._sid(5, 32, 545),  # Builtin Users
        policy._sid(5, 32, 546),  # Builtin Guests
        policy._sid(5, 80, 12345, 67890),  # NT SERVICE\* (Virtual service account)
        policy._sid(5, 82, 12345, 67890),  # IIS AppPool\*
        policy._sid(1, 0),  # NULL SID
        policy._sid(1, 1, 0),  # Everyone
        policy._sid(3, 0),  # Creator Owner
        policy._sid(3, 4),  # Owner Rights
        policy._sid(5, 21, 100, 200, 300, 501),  # Domain Guest (RID 501)
        policy._sid(5, 21, 100, 200, 300, 502),  # KRBTGT (RID 502)
        policy._sid(5, 21, 100, 200, 300, 512),  # Domain Admins (RID 512)
        policy._sid(5, 21, 100, 200, 300, 513),  # Domain Users (RID 513)
        policy._sid(5, 21, 100, 200, 300, 514),  # Domain Guests (RID 514)
        policy._sid(5, 21, 100, 200, 300, 553),  # RAS and IAS Servers (RID 553)
        bytes([1, 0, 0, 0, 0, 0, 0, 5]),  # Zero subauthority SID S-1-5
    ],
    ids=[
        "system",
        "local-service",
        "network-service",
        "builtin-admins",
        "builtin-users",
        "builtin-guests",
        "nt-service",
        "iis-apppool",
        "null-sid",
        "everyone",
        "creator-owner",
        "owner-rights",
        "rid-501-guest",
        "rid-502-krbtgt",
        "rid-512-domain-admins",
        "rid-513-domain-users",
        "rid-514-domain-guests",
        "rid-553-ras-servers",
        "zero-subauthorities",
    ],
)
def test_unsupported_current_user_and_token_contexts(sid):
    assert policy._supported_individual_user_sid(sid) is False
    res = review(snapshot(current_user_sid=sid, owner_sid=sid, aces=(ace(sid),)))
    assert res.status == policy.EvidenceStatus.UNVERIFIABLE
    assert res.reasons == (policy.Reason.TOKEN_CONTEXT_UNSUPPORTED,)


@pytest.mark.parametrize(
    "sid",
    [
        policy._sid(5, 21, 100, 200, 300, 1001),  # Local/Domain SAM user RID 1001
        policy._sid(5, 21, 100, 200, 300, 1000),  # First custom RID 1000
        policy._sid(5, 21, 100, 200, 300, 500),  # Builtin Administrator user account RID 500
        policy._sid(12, 1, 12345, 67890, 11111, 22222),  # Azure AD / Entra ID user
    ],
    ids=["local-sam-1001", "local-sam-1000", "admin-user-500", "azure-ad-user"],
)
def test_supported_individual_user_sids(sid):
    assert policy._supported_individual_user_sid(sid) is True
    res = review(snapshot(current_user_sid=sid, owner_sid=sid, aces=(ace(sid),)))
    assert res.status == policy.EvidenceStatus.POLICY_MATCH
    assert res.reasons == (policy.Reason.SNAPSHOT_MATCH,)


@pytest.mark.parametrize("flag", [0x20, 0x40, 0x80, 0x20 | 0x10])
def test_foreign_allow_preserved_even_with_unsupported_ace_flags(flag):
    # Foreign ACE with CRITICAL_ACE_FLAG (0x20) or other unknown flags
    foreign_ace = ace(OTHER, mask=1, flags=flag)
    res = review(snapshot(aces=(ace(), foreign_ace)))
    assert res.status == policy.EvidenceStatus.INSECURE
    assert policy.Reason.FOREIGN_ALLOW in res.reasons
    assert policy.Reason.UNSUPPORTED_ACE in res.reasons


def test_dacl_review_coherence_validator():
    with pytest.raises(ValidationError, match="at least one reason code"):
        policy.DaclReview(status=policy.EvidenceStatus.UNVERIFIABLE, reasons=())

    with pytest.raises(
        ValidationError, match="POLICY_MATCH review must contain exactly"
    ):
        policy.DaclReview(
            status=policy.EvidenceStatus.POLICY_MATCH,
            reasons=(policy.Reason.SNAPSHOT_MATCH, policy.Reason.UNSUPPORTED_MASK),
            provenance="WINDOWS_TEST_FIXTURE",
        )

    with pytest.raises(ValidationError, match="cannot have NOT_COLLECTED provenance"):
        policy.DaclReview(
            status=policy.EvidenceStatus.POLICY_MATCH,
            reasons=(policy.Reason.SNAPSHOT_MATCH,),
            provenance="NOT_COLLECTED",
        )

    with pytest.raises(
        ValidationError, match="Non-matching review cannot contain SNAPSHOT_MATCH"
    ):
        policy.DaclReview(
            status=policy.EvidenceStatus.UNVERIFIABLE,
            reasons=(policy.Reason.SNAPSHOT_MATCH,),
        )

    with pytest.raises(
        ValidationError,
        match="INSECURE review must contain at least one insecure reason code",
    ):
        policy.DaclReview(
            status=policy.EvidenceStatus.INSECURE,
            reasons=(policy.Reason.UNSUPPORTED_MASK,),
        )

    with pytest.raises(
        ValidationError, match="UNVERIFIABLE review cannot contain insecure reason codes"
    ):
        policy.DaclReview(
            status=policy.EvidenceStatus.UNVERIFIABLE,
            reasons=(policy.Reason.FOREIGN_ALLOW,),
        )


@pytest.mark.parametrize(
    "raw_path",
    [
        "C:relative",
        "C:dir/file",
        r"C:dirile",
        r"C:	est\..\escape",
        r"C:\..\escape",
        r"D:\path\.\..\other",
        r"\.\C:\device",
    ],
    ids=[
        "drive-relative",
        "drive-relative-slash",
        "drive-relative-backslash",
        "dotdot-traversal",
        "dotdot-root",
        "dotdot-nested",
        "device-path",
    ],
)
def test_rooted_path_validation_rejects_relative_drive_and_traversal(raw_path):
    with pytest.raises(policy._InspectionFailure) as exc:
        reader.require_local_fixed_drive(PureWindowsPath(raw_path))
    assert exc.value.reason == policy.Reason.UNSUPPORTED_FILESYSTEM


def test_zero_volume_rejected():
    res = review(snapshot(identity_before=(0, 1, 2), identity_after=(0, 1, 2)))
    assert res.status == policy.EvidenceStatus.UNVERIFIABLE
    assert policy.Reason.TIME_OF_CHECK_DRIFT in res.reasons


def test_parse_acl_with_padded_basic_ace():
    sub_count = OWNER[1]
    sid_len = 8 + 4 * sub_count
    normal_ace_size = 8 + sid_len
    padded_ace_size = normal_ace_size + 4  # 4-byte padding at end of basic ACE
    ace_bytes = (
        struct.pack("<BBHI", 0, 0, padded_ace_size, policy.FILE_ALL_ACCESS)
        + OWNER
        + b"\x00\x00\x00\x00"
    )
    acl = struct.pack("<BBHHH", 2, 0, 8 + len(ace_bytes), 1, 0) + ace_bytes
    parsed = _parse_acl(acl)
    assert len(parsed) == 1
    assert parsed[0].sid == OWNER
    assert parsed[0].mask == policy.FILE_ALL_ACCESS
    assert parsed[0].ace_type == 0
    assert parsed[0].flags == 0


def test_parse_acl_adversarial_malformed_sids():
    # Basic ACE where sub_count indicates SID length exceeding ACE length
    # ACE length = 20, but sub_count = 5 (needs 28 bytes for SID -> 8 + 28 = 36 bytes)
    malformed_ace = struct.pack("<BBHI", 0, 0, 20, policy.FILE_ALL_ACCESS) + bytes([1, 5, 0, 0, 0, 0, 0, 5, 21, 0, 0, 0])
    acl = struct.pack("<BBHHH", 2, 0, 8 + len(malformed_ace), 1, 0) + malformed_ace
    with pytest.raises(ValueError, match="Truncated basic ACE SID"):
        _parse_acl(acl)

    # Basic ACE with 0 subauthorities
    zero_sub_sid = bytes([1, 0, 0, 0, 0, 0, 0, 5])
    zero_sub_ace = struct.pack("<BBHI", 0, 0, 16, policy.FILE_ALL_ACCESS) + zero_sub_sid
    acl2 = struct.pack("<BBHHH", 2, 0, 8 + len(zero_sub_ace), 1, 0) + zero_sub_ace
    with pytest.raises(ValueError, match="Invalid basic ACE SID"):
        _parse_acl(acl2)


def test_copy_bounded_and_copy_sid_memory_bounds_safety():
    buffer = b"\x01\x05\x00\x00\x00\x00\x00\x05\x15\x00\x00\x00" + b"\x01" * 16
    # Use ctypes buffer for actual safe memory reading
    import ctypes
    c_buf = ctypes.create_string_buffer(buffer)
    addr = ctypes.addressof(c_buf)
    size = len(buffer)

    # Valid bounded copy
    assert _copy_bounded(addr, 8, addr, size) == buffer[:8]

    # Null address
    with pytest.raises(ValueError, match="Metadata pointer out of bounds"):
        _copy_bounded(0, 4, addr, size)

    # Negative length
    with pytest.raises(ValueError, match="Metadata pointer out of bounds"):
        _copy_bounded(addr, -1, addr, size)

    # Address < base
    with pytest.raises(ValueError, match="Metadata pointer out of bounds"):
        _copy_bounded(addr - 1, 4, addr, size)

    # Address + length > base + size
    with pytest.raises(ValueError, match="Metadata pointer out of bounds"):
        _copy_bounded(addr, size + 1, addr, size)

    # Valid copy_sid
    assert _copy_sid(addr, addr, size) == buffer

    # copy_sid with invalid revision (revision = 2)
    bad_rev_buf = ctypes.create_string_buffer(b"\x02\x01\x00\x00\x00\x00\x00\x05\x01\x00\x00\x00")
    with pytest.raises(ValueError, match="Invalid SID layout"):
        _copy_sid(ctypes.addressof(bad_rev_buf), ctypes.addressof(bad_rev_buf), len(bad_rev_buf))

    # copy_sid with 0 subauthorities
    zero_sub_buf = ctypes.create_string_buffer(b"\x01\x00\x00\x00\x00\x00\x00\x05")
    with pytest.raises(ValueError, match="Invalid SID layout"):
        _copy_sid(ctypes.addressof(zero_sub_buf), ctypes.addressof(zero_sub_buf), len(zero_sub_buf))

    # copy_sid with > 15 subauthorities (e.g. 16)
    sixteen_sub_buf = ctypes.create_string_buffer(b"\x01\x10\x00\x00\x00\x00\x00\x05")
    with pytest.raises(ValueError, match="Invalid SID layout"):
        _copy_sid(ctypes.addressof(sixteen_sub_buf), ctypes.addressof(sixteen_sub_buf), len(sixteen_sub_buf))


@pytest.mark.skipif(
    sys.platform != "win32", reason="Actual Win32 fixture probe requires Windows"
)
def test_real_windows_fixture_probe_detects_volume_and_inode_drift(tmp_path):
    with HermeticTestFixtureScope(tmp_path):
        st = tmp_path.stat()
        real_dev = st.st_dev & 0xFFFFFFFF
        real_ino = st.st_ino
        # Inode drift
        with pytest.raises(policy._InspectionFailure) as ino_drift:
            reader.read_fixture_snapshot(
                tmp_path, expected_inode=real_ino + 1, expected_dev=real_dev
            )
        assert ino_drift.value.reason == policy.Reason.TIME_OF_CHECK_DRIFT

        # Volume drift
        with pytest.raises(policy._InspectionFailure) as dev_drift:
            reader.read_fixture_snapshot(
                tmp_path,
                expected_inode=real_ino,
                expected_dev=(real_dev + 1) & 0xFFFFFFFF,
            )
        assert dev_drift.value.reason == policy.Reason.TIME_OF_CHECK_DRIFT
