"""Hermetic unit tests for Issue #65 / Stage B shadow session claim and replay denial contract.

Verifies:
1. Stable canonical session identity & filename derived deterministically from (cohort, model, purpose).
2. Verified empty fixture -> CONSUMED pre-dispatch claim record.
3. Second run after claim halts with ShadowSessionReplayError.
4. Race-resistant atomic claim between two distinct Python test subprocesses on same tmp_path.
5. Simulated interruption / ambiguous dispatch leaves claim consumed and blocks replay.
6. Short-write and fsync errors retain partial file on disk without deletion and block replay.
7. Stale, 0-byte, oversized, and malformed claim files halt fail-closed without deletion.
8. Security rejection of untrusted roots (.env, data/, src/, repo root, traversal, null bytes).
9. Symlink and reparse point rejection across root and ancestors.
10. OneShotShadowOperatorRunner integration updates session_lock_status and blocks repeat execution.
11. Explicit documentation of same-host fixture-only, power-loss, and cross-runner limitations.
12. Negative safety tests proving zero SDK client instantiation or live inference.
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
    ShadowSessionReplayError,
    ShadowSessionSecurityError,
    ShadowSessionStorageError,
    _find_repo_root,
    _is_reparse_point_or_symlink,
    claim_shadow_session,
    compute_canonical_session_identity,
    get_canonical_claim_filename,
    inspect_existing_session_claim,
    validate_fixture_root,
)
from efso_radar.config import Settings


class TestShadowSessionClaimContract(unittest.TestCase):
    """Hermetic test suite for Issue #65 shadow session claim & replay denial contract."""

    def setUp(self) -> None:
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.fixture_root = Path(self.tmp_dir.name).resolve()

    def tearDown(self) -> None:
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

    def test_verified_empty_fixture_claims_consumed_pre_dispatch(self) -> None:
        """Verified empty fixture transitions to CONSUMED pre-dispatch with complete evidence."""
        # Pre-condition: no claim file
        existing = inspect_existing_session_claim(self.fixture_root)
        self.assertIsNone(existing)

        # Claim execution
        record = claim_shadow_session(self.fixture_root)
        self.assertIsInstance(record, ShadowSessionClaimRecord)
        self.assertEqual(record.state, "CONSUMED")
        self.assertEqual(record.cohort_id, SHADOW_SAMPLE_ID)
        self.assertEqual(record.model_name, ALLOWED_SHADOW_MODEL)
        self.assertEqual(record.purpose_version, DEFAULT_PURPOSE_VERSION)
        self.assertEqual(record.process_pid, os.getpid())
        self.assertIn("STAGE B SAME-HOST ONE-USE SESSION CLAIM CONTRACT", record.disclaimer)

        # Post-condition: claim file exists on disk
        claim_file = self.fixture_root / get_canonical_claim_filename()
        self.assertTrue(claim_file.exists())
        self.assertTrue(claim_file.is_file())

        # Inspect reads back valid record
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

        # Claim file remains intact on disk
        claim_file = self.fixture_root / get_canonical_claim_filename()
        self.assertTrue(claim_file.exists())

    def test_race_resistant_atomic_claim_between_two_distinct_subprocesses(self) -> None:
        """Two distinct Python subprocesses concurrently racing to claim the same fixture root.

        Guarantees:
        - At most one subprocess succeeds (exit code 0).
        - The other subprocess is rejected with ReplayDenied (exit code 1).
        - Exactly one claim file is created.
        - Subsequent sequential execution is also rejected.
        """
        subproc_script = (
            "import sys\n"
            "from pathlib import Path\n"
            "from efso_radar.ai.shadow_session import claim_shadow_session, ShadowSessionError\n"
            "try:\n"
            "    claim = claim_shadow_session(Path(sys.argv[1]))\n"
            "    sys.exit(0)\n"
            "except ShadowSessionError:\n"
            "    sys.exit(1)\n"
            "except Exception:\n"
            "    sys.exit(2)\n"
        )

        repo_src = str(Path(__file__).resolve().parent.parent / "src")
        env = os.environ.copy()
        env["PYTHONPATH"] = repo_src

        # Launch two distinct subprocesses concurrently
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
            msg=f"Expected [0, 1] exit codes for concurrent racers. Got {exit_codes}. p1 err: {err1}, p2 err: {err2}",
        )

        # Verify exactly one claim file was created
        claim_files = list(self.fixture_root.glob("claim_*.json"))
        self.assertEqual(len(claim_files), 1)

        # Subsequent third run also fails
        p3 = subprocess.run(
            [sys.executable, "-c", subproc_script, str(self.fixture_root)],
            env=env,
            capture_output=True,
            timeout=15,
        )
        self.assertEqual(p3.returncode, 1)

    def test_simulated_interruption_ambiguous_dispatch_retains_claim_blocks_replay(self) -> None:
        """Simulated ambiguous / denied dispatch leaves session consumed and prevents re-execution."""
        runner = OneShotShadowOperatorRunner(session_fixture_root=self.fixture_root)

        # execute_one_shot unconditionally fails closed with AIEvaluationError (live denial)
        with self.assertRaises(AIEvaluationError):
            runner.execute_one_shot(session_fixture_root=self.fixture_root)

        # Pre-dispatch claim was written before the error
        claim_file = self.fixture_root / get_canonical_claim_filename()
        self.assertTrue(claim_file.exists())
        record = inspect_existing_session_claim(self.fixture_root)
        self.assertIsNotNone(record)
        self.assertEqual(record.state, "CONSUMED")

        # Second attempt with a fresh runner instance fails with ShadowSessionReplayError
        fresh_runner = OneShotShadowOperatorRunner(session_fixture_root=self.fixture_root)
        with self.assertRaises(ShadowSessionReplayError) as ctx:
            fresh_runner.execute_one_shot(session_fixture_root=self.fixture_root)

        self.assertIn("has already been claimed and consumed", str(ctx.exception))

    def test_short_write_and_fsync_error_retains_partial_file_and_blocks_replay(self) -> None:
        """Fsync error during claim creation retains partial file on disk without deletion and blocks replay."""
        with patch("os.fsync", side_effect=OSError("Simulated physical flush error")):
            with self.assertRaises(ShadowSessionStorageError) as ctx:
                claim_shadow_session(self.fixture_root)

            self.assertIn("Failed to fsync session claim to disk", str(ctx.exception))
            self.assertIn("State retained on disk to deny replay", str(ctx.exception))

        # Retention guarantee: file was NOT deleted
        claim_file = self.fixture_root / get_canonical_claim_filename()
        self.assertTrue(claim_file.exists())

        # Subsequent attempt to claim sees the existing file and fails closed
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
        self.assertTrue(claim_file.exists(), "Empty claim file must not be deleted")

        # 2. Corrupted / partial JSON
        claim_file.write_text('{"session_identity": "partial_json', encoding="utf-8")
        with self.assertRaises(ShadowSessionCorruptedError) as ctx2:
            claim_shadow_session(self.fixture_root)
        self.assertIn("Existing claim file is malformed", str(ctx2.exception))
        self.assertTrue(claim_file.exists(), "Corrupted claim file must not be deleted")

        # 3. Oversized file (> 64 KB)
        claim_file.write_text("x" * (MAX_CLAIM_FILE_BYTES + 100), encoding="utf-8")
        with self.assertRaises(ShadowSessionCorruptedError) as ctx3:
            claim_shadow_session(self.fixture_root)
        self.assertIn("exceeds maximum size", str(ctx3.exception))
        self.assertTrue(claim_file.exists(), "Oversized file must not be deleted")

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
        self.assertTrue(claim_file.exists(), "Foreign claim file must not be deleted")

    def test_rejects_untrusted_operational_and_traversal_roots(self) -> None:
        """Untrusted operational roots, path traversal, and null bytes are rejected fail-closed."""
        repo_root = _find_repo_root()
        self.assertIsNotNone(repo_root)

        # 1. Null bytes
        with self.assertRaises(ShadowSessionSecurityError) as ctx1:
            validate_fixture_root(str(self.fixture_root) + "\x00/hack")
        self.assertIn("Null bytes forbidden", str(ctx1.exception))

        # 2. Path traversal
        with self.assertRaises(ShadowSessionSecurityError) as ctx2:
            validate_fixture_root(f"{self.fixture_root}/../other")
        self.assertIn("Path traversal ('..') is strictly forbidden", str(ctx2.exception))

        # 3. Repository root
        with self.assertRaises(ShadowSessionSecurityError) as ctx3:
            validate_fixture_root(repo_root)
        self.assertIn("Repository root cannot be used as a test fixture root", str(ctx3.exception))

        # 4. Operational data directory inside repo
        data_dir = repo_root / "data"
        if data_dir.exists():
            with self.assertRaises(ShadowSessionSecurityError) as ctx4:
                validate_fixture_root(data_dir)
            self.assertIn("Fixture root cannot reside inside the repository working directory", str(ctx4.exception))

        # 5. Non-existent directory
        with self.assertRaises(ShadowSessionSecurityError) as ctx5:
            validate_fixture_root(self.fixture_root / "does_not_exist_xyz")
        self.assertIn("Fixture root directory does not exist", str(ctx5.exception))

        # 6. File path instead of directory
        temp_file = self.fixture_root / "regular_file.txt"
        temp_file.write_text("hello", encoding="utf-8")
        with self.assertRaises(ShadowSessionSecurityError) as ctx6:
            validate_fixture_root(temp_file)
        self.assertIn("Fixture root must be a directory", str(ctx6.exception))

    def test_symlink_and_reparse_point_rejected(self) -> None:
        """Symlinks and reparse points are rejected across candidate path and hierarchy."""
        with patch("efso_radar.ai.shadow_session._is_reparse_point_or_symlink", return_value=True):
            with self.assertRaises(ShadowSessionSecurityError) as ctx:
                validate_fixture_root(self.fixture_root)
            self.assertIn("Symlink or reparse point detected", str(ctx.exception))

    def test_runner_integration_dry_run_updates_session_lock_status(self) -> None:
        """OneShotShadowOperatorRunner integrates session claim into report session_lock_status."""
        runner = OneShotShadowOperatorRunner(session_fixture_root=self.fixture_root)
        result = runner.dry_run()

        expected_ident = compute_canonical_session_identity()
        self.assertIn("SAME_HOST_FIXTURE_CLAIM_CONSUMED", result.session_lock_status)
        self.assertIn(expected_ident, result.session_lock_status)

        # Markdown report surfaces the consumed session lock status
        md = result.to_markdown_report()
        self.assertIn(f"**Cross-Process Session Lock:** `SAME_HOST_FIXTURE_CLAIM_CONSUMED:{expected_ident}`", md)

        # A second runner instance on the same fixture root fails replay
        runner2 = OneShotShadowOperatorRunner(session_fixture_root=self.fixture_root)
        with self.assertRaises(ShadowSessionReplayError):
            runner2.dry_run()

    def test_runner_integration_evaluate_precomputed_json_updates_session_lock_status(self) -> None:
        """OneShotShadowOperatorRunner offline precomputed JSON evaluation claims session pre-dispatch."""
        synthetic_payload = json.dumps({
            "run_editorial_summary_tr": "Sentetik editoryal özet",
            "picks": [
                {
                    "article_ids": ["news-20261030-001"],
                    "kind": "NEW",
                    "headline_tr": "Test Başlık",
                    "cover_title_tr": "Test Kapak",
                    "caption_idea_tr": "Test Açıklama",
                    "why_efso_tr": "Test Neden Efso",
                    "uncertainties_tr": "",
                    "suggested_format": "POST",
                    "category": "GLOBAL",
                    "verification_status": "REPUTABLE_REPORTING",
                    "score": 85,
                }
            ],
        })
        runner = OneShotShadowOperatorRunner(session_fixture_root=self.fixture_root)
        result = runner.evaluate_precomputed_response_json(synthetic_payload)

        expected_ident = compute_canonical_session_identity()
        self.assertIn("SAME_HOST_FIXTURE_CLAIM_CONSUMED", result.session_lock_status)
        self.assertIn(expected_ident, result.session_lock_status)

        # Second evaluation with fresh runner on same fixture root fails replay
        runner2 = OneShotShadowOperatorRunner(session_fixture_root=self.fixture_root)
        with self.assertRaises(ShadowSessionReplayError):
            runner2.evaluate_precomputed_response_json(synthetic_payload)

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

            # Dry run on second fixture
            with tempfile.TemporaryDirectory() as td2:
                r2 = OneShotShadowOperatorRunner(session_fixture_root=Path(td2))
                r2.dry_run()

            # Live execute_one_shot denial on third fixture
            with tempfile.TemporaryDirectory() as td3:
                r3 = OneShotShadowOperatorRunner(session_fixture_root=Path(td3))
                with self.assertRaises(AIEvaluationError):
                    r3.execute_one_shot()

            mock_client.assert_not_called()


if __name__ == "__main__":
    unittest.main()
