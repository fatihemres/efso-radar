"""No-network regression tests for the prospective mock session gates."""
from __future__ import annotations

import math
import unittest

from efso_radar.staging.offline_session_gate import (
    MockAudit,
    MockGateDenied,
    MockPhase,
    OfflineSessionGate,
)


DIGEST = "a" * 64
STAGES = (
    ("LIVE-02", 4),
    ("LIVE-03", 3),
    ("LIVE-04", 3),
    ("LIVE-05", 2),
    ("LIVE-06", 4),
    ("LIVE-07", 2),
)


class TestOfflineSessionGate(unittest.TestCase):
    def setUp(self):
        self.ticks = [100.0]
        self.gate = OfflineSessionGate(
            expected_manifest_sha256=DIGEST, clock=lambda: self.ticks[0]
        )

    def preflight(self, **changes):
        params = dict(
            manifest_sha256=DIGEST,
            schema_version=6,
            tables_checked=15,
            columns_checked=129,
            collision_checks=5,
            simulated_block_writes_on=True,
            request_count=47,
        )
        params.update(changes)
        self.gate.record_simulated_preflight(**params)

    def start(self):
        self.preflight()
        self.gate.record_simulated_human_window(marker="MOCK_ONLY")

    def stages(self):
        for stage, count in STAGES:
            self.gate.record_simulated_stage(
                stage=stage, manifest_sha256=DIGEST, request_count=count
            )

    def assert_halt(self, fn, reason):
        with self.assertRaisesRegex(MockGateDenied, reason):
            fn()
        snap = self.gate.snapshot()
        self.assertEqual(snap.phase, MockPhase.HALTED)
        self.assertEqual(snap.audit, MockAudit.UNKNOWN)
        self.assertTrue(snap.manifest_preserved)

    def test_full_mock_sequence_budget_and_clean_result(self):
        self.start()
        self.assertEqual(self.gate.snapshot().next_stage, "LIVE-02")
        self.stages()
        self.assertEqual(self.gate.snapshot().phase, MockPhase.STAGES_COMPLETE)
        self.gate.record_simulated_audit(
            manifest_sha256=DIGEST, ownership_verified=True,
            outcome=MockAudit.CLEAN, request_count=14
        )
        self.gate.record_simulated_lockdown(
            marker="MOCK_ONLY", simulated_block_writes_on=True,
            simulated_token_revoked=True
        )
        snap = self.gate.snapshot()
        self.assertEqual(snap.requests, 79)
        self.assertEqual(snap.audit, MockAudit.CLEAN)
        self.assertEqual(snap.phase, MockPhase.AUDITED)
        self.assertFalse(snap.manifest_preserved)
        self.assertTrue(snap.lockdown_recorded)

    def test_preflight_fails_closed_if_any_check_is_missing_or_invalid(self):
        cases = (
            {"manifest_sha256": "b" * 64},
            {"schema_version": 0},
            {"tables_checked": 14},
            {"columns_checked": 128},
            {"collision_checks": 4},
            {"simulated_block_writes_on": False},
            {"simulated_block_writes_on": 1},
            {"request_count": 0},
            {"request_count": True},
        )
        for params in cases:
            with self.subTest(params=params):
                gate = OfflineSessionGate(
                    expected_manifest_sha256=DIGEST, clock=lambda: 0.0
                )
                with self.assertRaises(MockGateDenied):
                    gate.record_simulated_preflight(
                        **dict(
                            manifest_sha256=DIGEST, schema_version=6,
                            tables_checked=15, columns_checked=129,
                            collision_checks=5, simulated_block_writes_on=True,
                            request_count=47, **{},
                        ) | params
                    )
                self.assertEqual(gate.snapshot().phase, MockPhase.HALTED)

    def test_write_requires_mock_window_and_correct_next_stage(self):
        self.preflight()
        self.assert_halt(
            lambda: self.gate.record_simulated_stage(
                stage="LIVE-02", manifest_sha256=DIGEST, request_count=4
            ),
            "STATE_SEQUENCE",
        )
        with self.assertRaisesRegex(MockGateDenied, "ALREADY_HALTED"):
            self.gate.record_simulated_human_window(marker="MOCK_ONLY")

    def test_human_window_marker_is_not_general_permission(self):
        self.preflight()
        self.assert_halt(
            lambda: self.gate.record_simulated_human_window(
                marker="REAL_PO_APPROVED"
            ),
            "WINDOW_ATTESTATION",
        )

    def test_stage_reorder_and_manifest_swap_halt(self):
        for wrong in (
            {"stage": "LIVE-04", "manifest_sha256": DIGEST},
            {"stage": "LIVE-02", "manifest_sha256": "b" * 64},
        ):
            with self.subTest(wrong=wrong):
                gate = OfflineSessionGate(
                    expected_manifest_sha256=DIGEST, clock=lambda: 0.0
                )
                gate.record_simulated_preflight(
                    manifest_sha256=DIGEST, schema_version=6,
                    tables_checked=15, columns_checked=129, collision_checks=5,
                    simulated_block_writes_on=True, request_count=47
                )
                gate.record_simulated_human_window(marker="MOCK_ONLY")
                with self.assertRaisesRegex(MockGateDenied, "STAGE_OR_MANIFEST"):
                    gate.record_simulated_stage(request_count=4, **wrong)
                self.assertEqual(gate.snapshot().phase, MockPhase.HALTED)

    def test_cannot_restart_preflight_or_reset_session(self):
        self.preflight()
        self.assert_halt(lambda: self.preflight(), "STATE_SEQUENCE")
        self.assertEqual(self.gate.snapshot().requests, 47)

    def test_request_cutoff_protects_cleanup_reserve(self):
        self.start()
        self.assert_halt(
            lambda: self.gate.record_simulated_stage(
                stage="LIVE-02", manifest_sha256=DIGEST, request_count=33
            ),
            "CLEANUP_RESERVE",
        )
        self.assertEqual(self.gate.snapshot().requests, 47)

    def test_request_count_and_hard_ceiling_fail_closed(self):
        self.start()
        self.stages()
        self.assert_halt(
            lambda: self.gate.record_simulated_audit(
                manifest_sha256=DIGEST, ownership_verified=True,
                outcome=MockAudit.CLEAN, request_count=35
            ),
            "REQUEST_CEILING",
        )
        self.assertEqual(self.gate.snapshot().requests, 65)

    def test_audit_requires_ownership_proof_and_exact_digest(self):
        for attr in (
            {"ownership_verified": False},
            {"manifest_sha256": "c" * 64},
            {"outcome": MockAudit.UNVERIFIED},
            {"outcome": "CLEAN"},
        ):
            with self.subTest(attr=attr):
                gate = OfflineSessionGate(
                    expected_manifest_sha256=DIGEST, clock=lambda: 0.0
                )
                gate.record_simulated_preflight(
                    manifest_sha256=DIGEST, schema_version=6,
                    tables_checked=15, columns_checked=129, collision_checks=5,
                    simulated_block_writes_on=True, request_count=47
                )
                gate.record_simulated_human_window(marker="MOCK_ONLY")
                for stage, count in STAGES:
                    gate.record_simulated_stage(
                        stage=stage, manifest_sha256=DIGEST, request_count=count
                    )
                values = dict(
                    manifest_sha256=DIGEST, ownership_verified=True,
                    outcome=MockAudit.CLEAN, request_count=14
                )
                values.update(attr)
                with self.assertRaises(MockGateDenied):
                    gate.record_simulated_audit(**values)
                self.assertEqual(gate.snapshot().audit, MockAudit.UNKNOWN)

    def test_unresolved_audit_never_promoted_to_clean_by_lockdown(self):
        self.start()
        self.stages()
        self.gate.record_simulated_audit(
            manifest_sha256=DIGEST, ownership_verified=True,
            outcome=MockAudit.RESIDUAL, request_count=14
        )
        self.gate.record_simulated_lockdown(
            marker="MOCK_ONLY", simulated_block_writes_on=True,
            simulated_token_revoked=True
        )
        snap = self.gate.snapshot()
        self.assertEqual(snap.audit, MockAudit.RESIDUAL)
        self.assertTrue(snap.manifest_preserved)

    def test_lockdown_after_halt_is_only_an_observation(self):
        self.start()
        self.assert_halt(
            lambda: self.gate.record_simulated_stage(
                stage="LIVE-02", manifest_sha256=DIGEST, request_count=80
            ), "CLEANUP_RESERVE"
        )
        self.gate.record_simulated_lockdown(
            marker="MOCK_ONLY", simulated_block_writes_on=True,
            simulated_token_revoked=True
        )
        snap = self.gate.snapshot()
        self.assertEqual(snap.audit, MockAudit.UNKNOWN)
        self.assertEqual(snap.phase, MockPhase.HALTED)
        self.assertTrue(snap.lockdown_recorded)

    def test_unverified_lockdown_does_not_mark_as_done(self):
        self.start()
        self.stages()
        self.gate.record_simulated_audit(
            manifest_sha256=DIGEST, ownership_verified=True,
            outcome=MockAudit.CLEAN, request_count=14
        )
        self.assert_halt(
            lambda: self.gate.record_simulated_lockdown(
                marker="MOCK_ONLY", simulated_block_writes_on=True,
                simulated_token_revoked=False
            ), "LOCKDOWN_EVIDENCE"
        )
        self.assertFalse(self.gate.snapshot().lockdown_recorded)

    def test_deadline_monotonic_and_malformed_time_fail_closed(self):
        self.start()
        self.ticks[0] = 220.0
        self.assert_halt(
            lambda: self.gate.record_simulated_stage(
                stage="LIVE-02", manifest_sha256=DIGEST, request_count=4
            ), "TIME_OR_CLOCK_INVALID"
        )

    def test_invalid_initial_digest_or_clock_never_start(self):
        for bad in ("", "not-a-hash", "A" * 64):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                OfflineSessionGate(expected_manifest_sha256=bad)
        for invalid in (math.nan, math.inf, True, "not-a-clock"):
            with self.subTest(invalid=invalid), self.assertRaises(MockGateDenied):
                OfflineSessionGate(
                    expected_manifest_sha256=DIGEST, clock=lambda: invalid
                )

    def test_bad_request_type_does_not_increment_counter(self):
        self.start()
        for invalid in (0, True, -1, 1.0):
            with self.subTest(invalid=invalid):
                # New object per test: stopping a session is irreversible.
                gate = OfflineSessionGate(
                    expected_manifest_sha256=DIGEST, clock=lambda: 0.0
                )
                gate.record_simulated_preflight(
                    manifest_sha256=DIGEST, schema_version=6,
                    tables_checked=15, columns_checked=129,
                    collision_checks=5, simulated_block_writes_on=True,
                    request_count=47
                )
                gate.record_simulated_human_window(marker="MOCK_ONLY")
                with self.assertRaisesRegex(MockGateDenied, "REQUEST_COUNT_INVALID"):
                    gate.record_simulated_stage(
                        stage="LIVE-02", manifest_sha256=DIGEST,
                        request_count=invalid
                    )
                self.assertEqual(gate.snapshot().requests, 47)


if __name__ == "__main__":
    unittest.main()
