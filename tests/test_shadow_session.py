"""Hermetic unit tests for Issue #65 / Stage B shadow session claim and replay denial contract.

Independent Review Hardening:
1. Gated behind ShadowSessionFixtureScope attestation; un-attested or arbitrary directories fail before os.open.
2. Routine dry_run(), execute_one_shot(), and evaluate_precomputed_response_json() are strictly side-effect-free (0 I/O).
3. Pre-dispatch claim exercised only at deliberate simulated dispatch seam (simulate_hermetic_dispatched_session).
4. Data/model validation occurs before claim I/O; invalid candidates/model halt with 0 files created.
5. Non-empty roots with foreign/stray entries are rejected fail-closed (HALT/UNKNOWN).
6. Constant sanitized error messages; no raw payload reflections or unredacted sensitive paths.
7. Reparse point check fails closed on OSError.
8. Race-resistant atomic no-overwrite claim between two distinct test subprocesses.
9. Zero live SDK client calls or network dispatches.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from efso_radar.ai.client import AIEvaluationError
from efso_radar.ai.shadow_operator import (
    ALLOWED_SHADOW_MODEL,
    SHADOW_SAMPLE_ID,
    OneShotShadowOperatorRunner,
)
from efso_radar.ai.shadow_session import (
    DEFAULT_PURPOSE_VERSION,
    MAX_CLAIM_FILE_BYTES,
    SHADOW_SESSION_DISCLAIMER,
    ShadowSessionClaimRecord,
    ShadowSessionCorruptedError,
    ShadowSessionError,
    ShadowSessionFixtureScope,
    ShadowSessionReplayError,
    ShadowSessionSecurityError,
    ShadowSessionStorageError,
    _find_repo_root,
    _is_reparse_point_or_symlink,
    claim_shadow_session,
    compute_canonical_session_identity,
    get_canonical_claim_filename,
    inspect_existing_session_claim,
    validate_test_fixture_root,
)
from efso_radar.config import Settings
from efso_radar.domain import Article


class TestShadowSessionClaimContract(unittest.TestCase):
    """Hermetic test suite for Issue #65 shadow session claim & replay denial contract."""

    def setUp(self) -> None:
        self.tmp_dir = tempfile.TemporaryDirectory(prefix="pytest-session-test-")
        self.fixture_root = Path(self.tmp_dir.name).resolve()
        self.scope = ShadowSessionFixtureScope(self.fixture_root)
        self.scope.__enter__()

    def tearDown(self) -> None:
        try:
            self.scope.__exit__(None, None, None)
        finally:
            self.tmp_dir.cleanup()

    def test_canonical_session_identity_and_filename_derivation(self) -> None:
        """Identity and filename are deterministically derived from fixed immutable inputs."""
        ident = compute_canonical_session_identity(
            cohort_id=SHADOW_SAMPLE_ID,
            model_name=ALLOWED_SHADOW_MODEL,
            purpose_version=DEFAULT_PURPOSE_VERSION,
        )
        self.assertTrue(ident.startswith("efso_shadow_efso_shadow_20261030_historical_public_v1_gemini_3_5_flash_lite_"))

        fname = get_canonical_claim_filename(
            cohort_id=SHADOW_SAMPLE_ID,
            model_name=ALLOWED_SHADOW_MODEL,
            purpose_version=DEFAULT_PURPOSE_VERSION,
        )
        self.assertEqual(fname, f"claim_{ident}.json")

        # Evading replay via arbitrary cohort, model, or purpose is strictly rejected
        with self.assertRaises(ShadowSessionSecurityError):
            compute_canonical_session_identity(cohort_id="arbitrary-cohort")

        with self.assertRaises(ShadowSessionSecurityError):
            compute_canonical_session_identity(model_name="gemini-2.0-flash")

        with self.assertRaises(ShadowSessionSecurityError):
            compute_canonical_session_identity(purpose_version="custom-uuid-v2")

    def test_fixture_scope_attestation_required_before_write(self) -> None:
        """Claim operations are gated behind explicit ShadowSessionFixtureScope attestation."""
        # Create an un-attested valid temporary directory
        with tempfile.TemporaryDirectory(prefix="pytest-unattested-") as td:
            unattested_path = Path(td).resolve()
            # Fails before attempting any write or os.open
            with self.assertRaises(ShadowSessionSecurityError) as ctx:
                claim_shadow_session(unattested_path)

            self.assertIn("has not been attested as an authorized test-owned fixture scope", str(ctx.exception))
            # No files created
            self.assertEqual(list(unattested_path.iterdir()), [])

            # Entering the scope authorizes it
            with ShadowSessionFixtureScope(unattested_path):
                rec = claim_shadow_session(unattested_path)
                self.assertEqual(rec.state, "CONSUMED")

    def test_untrusted_arbitrary_directories_rejected_before_os_open(self) -> None:
        """Arbitrary project, desktop, non-temp, or non-pytest directories are rejected fail-closed."""
        repo_root = _find_repo_root()
        self.assertIsNotNone(repo_root)

        # 1. Repository root rejected
        with self.assertRaises(ShadowSessionSecurityError):
            validate_test_fixture_root(repo_root)

        # 2. Inside repository working copy rejected
        with self.assertRaises(ShadowSessionSecurityError):
            validate_test_fixture_root(repo_root / "src")

        # 3. Path traversal (..) rejected
        with self.assertRaises(ShadowSessionSecurityError):
            validate_test_fixture_root(f"{self.fixture_root}/../other")

        # 4. Null byte rejected
        with self.assertRaises(ShadowSessionSecurityError):
            validate_test_fixture_root(f"{self.fixture_root}\x00/bad")

        # 5. Non-temp directory rejected
        with self.assertRaises(ShadowSessionSecurityError):
            validate_test_fixture_root(Path.home())

        # 6. Temp root itself rejected
        with self.assertRaises(ShadowSessionSecurityError):
            validate_test_fixture_root(tempfile.gettempdir())

        # 7. Non-pytest prefix temporary directory rejected
        with tempfile.TemporaryDirectory(prefix="ordinary-temp-") as ordinary_td:
            with self.assertRaises(ShadowSessionSecurityError) as ctx_ord:
                validate_test_fixture_root(ordinary_td)
            self.assertIn("does not belong to a valid pytest fixture hierarchy", str(ctx_ord.exception))

    def test_verified_empty_fixture_claims_consumed_pre_dispatch(self) -> None:
        """Verified empty fixture transitions to CONSUMED pre-dispatch with complete evidence."""
        existing = inspect_existing_session_claim(self.fixture_root)
        self.assertIsNone(existing)

        record = claim_shadow_session(self.fixture_root)
        self.assertIsInstance(record, ShadowSessionClaimRecord)
        self.assertEqual(record.state, "CONSUMED")
        self.assertEqual(record.cohort_id, SHADOW_SAMPLE_ID)
        self.assertEqual(record.model_name, ALLOWED_SHADOW_MODEL)
        self.assertEqual(record.purpose_version, DEFAULT_PURPOSE_VERSION)
        self.assertEqual(record.process_pid, os.getpid())
        self.assertIn("STAGE B SAME-HOST ONE-USE SESSION CLAIM CONTRACT", record.disclaimer)

        claim_file = self.fixture_root / get_canonical_claim_filename()
        self.assertTrue(claim_file.exists())
        self.assertTrue(claim_file.is_file())

        inspected = inspect_existing_session_claim(self.fixture_root)
        self.assertIsNotNone(inspected)
        self.assertEqual(inspected.session_identity, record.session_identity)
        self.assertEqual(inspected.state, "CONSUMED")

    def test_second_run_after_claim_halts_with_replay_denied(self) -> None:
        """Attempting a second session claim on the same fixture halts with ReplayDenied."""
        claim_shadow_session(self.fixture_root)

        with self.assertRaises(ShadowSessionReplayError) as ctx:
            claim_shadow_session(self.fixture_root)

        self.assertIn("Replay denied: session", str(ctx.exception))
        self.assertIn("has already been claimed and consumed", str(ctx.exception))

        claim_file = self.fixture_root / get_canonical_claim_filename()
        self.assertTrue(claim_file.exists())

    def test_nonempty_fixture_with_foreign_entries_rejected_fail_closed(self) -> None:
        """A fixture directory containing foreign entries or unknown files is rejected as UNKNOWN/HALT."""
        foreign_file = self.fixture_root / "stray_foreign_file.txt"
        foreign_file.write_text("unverified content", encoding="utf-8")

        # 1. Unclaimed directory with foreign file fails before os.open
        with self.assertRaises(ShadowSessionSecurityError) as ctx1:
            claim_shadow_session(self.fixture_root)
        self.assertIn("Fixture root is not clean/empty", str(ctx1.exception))
        self.assertIn("foreign or unverified entries", str(ctx1.exception))

        # Stray file is NOT deleted
        self.assertTrue(foreign_file.exists())

        # 2. Existing valid claim file alongside foreign file fails as corrupted/extraneous
        foreign_file.unlink()
        claim_shadow_session(self.fixture_root)

        foreign_file.write_text("another stray", encoding="utf-8")
        with self.assertRaises(ShadowSessionCorruptedError) as ctx2:
            claim_shadow_session(self.fixture_root)
        self.assertIn("extraneous foreign entries alongside claim file", str(ctx2.exception))

    def test_race_resistant_atomic_claim_between_two_distinct_subprocesses(self) -> None:
        """Two distinct Python subprocesses concurrently racing to claim the same attested fixture root.

        Guarantees:
        - Exactly one subprocess succeeds (exit code 0).
        - The other subprocess is rejected with ReplayDenied (exit code 1).
        - Exactly one claim file is created.
        - Subsequent sequential execution is also rejected.
        """
        subproc_script = (
            "import sys\n"
            "from pathlib import Path\n"
            "from efso_radar.ai.shadow_session import claim_shadow_session, ShadowSessionFixtureScope, ShadowSessionError\n"
            "root = Path(sys.argv[1])\n"
            "ShadowSessionFixtureScope.register_authorized_fixture_root(root)\n"
            "try:\n"
            "    claim = claim_shadow_session(root)\n"
            "    sys.exit(0)\n"
            "except ShadowSessionError:\n"
            "    sys.exit(1)\n"
            "except Exception:\n"
            "    sys.exit(2)\n"
        )

        repo_src = str(Path(__file__).resolve().parent.parent / "src")
        env = os.environ.copy()
        env["PYTHONPATH"] = repo_src

        p1 = subprocess.Popen(
            [sys.executable, "-c", subproc_script, str(self.fixture_root)],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        p2 = subprocess.Popen(
            [sys.executable, "-c", subproc_script, str(self.fixture_root)],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        out1, err1 = p1.communicate(timeout=15)
        out2, err2 = p2.communicate(timeout=15)

        exit_codes = sorted([p1.returncode, p2.returncode])
        self.assertEqual(
            exit_codes,
            [0, 1],
            msg=f"Expected [0, 1] exit codes for concurrent racers. Got {exit_codes}. p1: {err1}, p2: {err2}",
        )

        claim_files = list(self.fixture_root.glob("claim_*.json"))
        self.assertEqual(len(claim_files), 1)

        p3 = subprocess.run(
            [sys.executable, "-c", subproc_script, str(self.fixture_root)],
            env=env,
            capture_output=True,
            timeout=15,
        )
        self.assertEqual(p3.returncode, 1)

    def test_denied_live_calls_and_dry_run_produce_zero_filesystem_io(self) -> None:
        """Routine execute_one_shot(), dry_run(), and invalid evaluation produce 0 claim files / I/O."""
        runner = OneShotShadowOperatorRunner()

        # 1. Denied execute_one_shot() produces zero I/O
        with patch("builtins.open") as mock_open:
            with self.assertRaises(AIEvaluationError):
                runner.execute_one_shot()
            mock_open.assert_not_called()

        self.assertEqual(list(self.fixture_root.iterdir()), [])

        # 2. Routine dry_run() produces zero I/O
        fresh_runner = OneShotShadowOperatorRunner()
        res = fresh_runner.dry_run()
        self.assertEqual(res.status, "DRY_RUN_COMPLETED_OFFLINE")
        self.assertEqual(res.session_lock_status, "CROSS_PROCESS_SESSION_LOCK_PENDING_LIVE_AUTHORIZATION")
        self.assertEqual(list(self.fixture_root.iterdir()), [])

        # 3. Invalid precomputed JSON produces zero I/O
        third_runner = OneShotShadowOperatorRunner()
        with self.assertRaises(AIEvaluationError):
            third_runner.evaluate_precomputed_response_json("{malformed json")
        self.assertEqual(list(self.fixture_root.iterdir()), [])

    def test_simulated_dispatch_seam_validates_data_before_claim_io(self) -> None:
        """Simulation seam validates candidates and model BEFORE any claim file I/O."""
        # 1. Invalid model halts before claim creation
        bad_settings = Settings(gemini_model="gemini-1.5-pro")
        runner_bad_model = OneShotShadowOperatorRunner(settings=bad_settings)
        with self.assertRaises(AIEvaluationError) as ctx1:
            runner_bad_model.simulate_hermetic_dispatched_session(self.fixture_root)
        self.assertIn("Shadow operator only permits model", str(ctx1.exception))
        # Zero claim files created
        self.assertEqual(list(self.fixture_root.iterdir()), [])

        # 2. Tampered candidate content halts before claim creation
        runner_ok = OneShotShadowOperatorRunner()
        from efso_radar.ai.shadow_preflight import get_historical_public_shadow_sample
        sample = get_historical_public_shadow_sample()
        tampered_sample = [
            Article(
                id=sample[0].id,
                title="TAMPERED TITLE INJECTION",
                snippet=sample[0].snippet,
                url=sample[0].url,
                source=sample[0].source,
                published_at=sample[0].published_at,
                market=sample[0].market,
                canonical_url=sample[0].canonical_url,
            )
        ] + sample[1:]

        with self.assertRaises(AIEvaluationError) as ctx2:
            runner_ok.simulate_hermetic_dispatched_session(self.fixture_root, candidates=tampered_sample)
        self.assertIn("Candidate content modification rejected", str(ctx2.exception))
        # Zero claim files created
        self.assertEqual(list(self.fixture_root.iterdir()), [])

    def test_simulated_dispatch_interruption_retains_claim_and_blocks_replay(self) -> None:
        """Simulated dispatch interruption retains pre-dispatch claim on disk and blocks second run."""
        runner = OneShotShadowOperatorRunner()

        def fail_dispatch_callback() -> None:
            raise RuntimeError("Simulated dispatch crash / transport timeout")

        with self.assertRaises(RuntimeError):
            runner.simulate_hermetic_dispatched_session(
                self.fixture_root, dispatch_callback=fail_dispatch_callback
            )

        # Claim was created pre-dispatch and retained
        claim_file = self.fixture_root / get_canonical_claim_filename()
        self.assertTrue(claim_file.exists())
        record = inspect_existing_session_claim(self.fixture_root)
        self.assertIsNotNone(record)
        self.assertEqual(record.state, "CONSUMED")

        # Second attempt with fresh runner is blocked by ReplayDenied
        runner2 = OneShotShadowOperatorRunner()
        with self.assertRaises(ShadowSessionReplayError):
            runner2.simulate_hermetic_dispatched_session(self.fixture_root)

    def test_simulated_dispatch_success_updates_session_lock_status(self) -> None:
        """Successful simulated dispatch updates session_lock_status to SAME_HOST_FIXTURE_CLAIM_CONSUMED."""
        runner = OneShotShadowOperatorRunner()
        result = runner.simulate_hermetic_dispatched_session(self.fixture_root)

        expected_ident = compute_canonical_session_identity()
        self.assertEqual(result.status, "HERMETIC_SIMULATED_DISPATCH_SUCCESS")
        self.assertIn("SAME_HOST_FIXTURE_CLAIM_CONSUMED", result.session_lock_status)
        self.assertIn(expected_ident, result.session_lock_status)

        # Second run on same root is denied
        runner2 = OneShotShadowOperatorRunner()
        with self.assertRaises(ShadowSessionReplayError):
            runner2.simulate_hermetic_dispatched_session(self.fixture_root)

    def test_reparse_point_check_fails_closed_on_oserror(self) -> None:
        """_is_reparse_point_or_symlink fails closed with ShadowSessionSecurityError on OSError."""
        with patch("os.lstat", side_effect=OSError("Access denied during lstat")):
            with patch("sys.platform", "win32"):
                with self.assertRaises(ShadowSessionSecurityError) as ctx:
                    _is_reparse_point_or_symlink(self.fixture_root)
                self.assertIn("Filesystem attribute inspection failed", str(ctx.exception))

    def test_short_write_and_fsync_error_retains_partial_file_and_blocks_replay(self) -> None:
        """Fsync error during claim creation retains partial file on disk without deletion and blocks replay."""
        with patch("os.fsync", side_effect=OSError("Simulated physical flush error")):
            with self.assertRaises(ShadowSessionStorageError) as ctx:
                claim_shadow_session(self.fixture_root)

            self.assertIn("Failed to fsync session claim to disk", str(ctx.exception))
            self.assertIn("State retained on disk to deny replay", str(ctx.exception))

        claim_file = self.fixture_root / get_canonical_claim_filename()
        self.assertTrue(claim_file.exists())

        with self.assertRaises((ShadowSessionReplayError, ShadowSessionCorruptedError)):
            claim_shadow_session(self.fixture_root)

    def test_stale_and_malformed_evidence_halts_without_cleanup(self) -> None:
        """Stale, empty, oversized, and corrupted claim files halt fail-closed without deletion."""
        claim_file = self.fixture_root / get_canonical_claim_filename()

        # 1. Zero-byte empty file
        claim_file.write_text("", encoding="utf-8")
        with self.assertRaises(ShadowSessionCorruptedError) as ctx1:
            claim_shadow_session(self.fixture_root)
        self.assertIn("Existing claim file is empty (0 bytes)", str(ctx1.exception))
        self.assertTrue(claim_file.exists())

        # 2. Corrupted / partial JSON
        claim_file.write_text('{"session_identity": "partial_json', encoding="utf-8")
        with self.assertRaises(ShadowSessionCorruptedError) as ctx2:
            claim_shadow_session(self.fixture_root)
        self.assertIn("Existing claim file is malformed", str(ctx2.exception))
        self.assertTrue(claim_file.exists())

        # 3. Oversized file (> 64 KB)
        claim_file.write_text("x" * (MAX_CLAIM_FILE_BYTES + 100), encoding="utf-8")
        with self.assertRaises(ShadowSessionCorruptedError) as ctx3:
            claim_shadow_session(self.fixture_root)
        self.assertIn("exceeds maximum size", str(ctx3.exception))
        self.assertTrue(claim_file.exists())

        # 4. Foreign session identity mismatch
        foreign_record = {
            "session_identity": "foreign_identity_injection_attack",
            "cohort_id": SHADOW_SAMPLE_ID,
            "model_name": ALLOWED_SHADOW_MODEL,
            "purpose_version": DEFAULT_PURPOSE_VERSION,
            "claimed_at_utc": "2026-11-02T00:00:00Z",
            "process_pid": 1234,
            "hostname": "foreign-host",
            "state": "CONSUMED",
            "disclaimer": "test",
        }
        claim_file.write_text(json.dumps(foreign_record), encoding="utf-8")
        with self.assertRaises(ShadowSessionCorruptedError) as ctx4:
            claim_shadow_session(self.fixture_root)
        self.assertIn("Claim record identity mismatch", str(ctx4.exception))
        self.assertTrue(claim_file.exists())

    def test_error_messages_contain_no_raw_payloads_or_unredacted_paths(self) -> None:
        """Error messages are constant sanitized codes and do not leak raw payload data or full paths."""
        claim_file = self.fixture_root / get_canonical_claim_filename()
        sensitive_payload = '{"private_leak": "SECRET_USER_DATA_12345"}'
        claim_file.write_text(sensitive_payload, encoding="utf-8")

        with self.assertRaises(ShadowSessionCorruptedError) as ctx:
            claim_shadow_session(self.fixture_root)

        err_msg = str(ctx.exception)
        self.assertNotIn("SECRET_USER_DATA_12345", err_msg)
        self.assertNotIn("private_leak", err_msg)

    def test_documented_limitations_integrity(self) -> None:
        """Session contract disclaimers strictly state platform limits and non-power-loss boundaries."""
        self.assertIn("SAME-HOST ONE-USE SESSION CLAIM CONTRACT", SHADOW_SESSION_DISCLAIMER)
        self.assertIn("NOT cross-runner, NOT distributed, and NOT an exactly-once", SHADOW_SESSION_DISCLAIMER)
        self.assertIn("NOT a physical power-loss or host-reboot durability guarantee", SHADOW_SESSION_DISCLAIMER)
        self.assertIn("Missing state on an untrusted or previously used root is UNKNOWN", SHADOW_SESSION_DISCLAIMER)
        self.assertIn("Windows directory durability and owner-host ACL remain blocked", SHADOW_SESSION_DISCLAIMER)
        self.assertIn("Contains zero live network or Gemini SDK capability", SHADOW_SESSION_DISCLAIMER)

    def test_negative_safety_zero_sdk_client_instantiations(self) -> None:
        """All session operations, runner dry runs, and fail-closed dispatches instantiate zero SDK clients."""
        with patch("google.genai.Client") as mock_client:
            claim_shadow_session(self.fixture_root)

            r2 = OneShotShadowOperatorRunner()
            r2.dry_run()

            r3 = OneShotShadowOperatorRunner()
            with self.assertRaises(AIEvaluationError):
                r3.execute_one_shot()

            mock_client.assert_not_called()


if __name__ == "__main__":
    unittest.main()
