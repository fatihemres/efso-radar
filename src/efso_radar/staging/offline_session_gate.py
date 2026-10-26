"""Offline *simulation* of G-C human-gate, budget and stage ordering.

NOT an authorization token or a live Turso transport. This component:
- never imports repository/database/network clients, reads files, or sends SQL;
- accepts only MOCK_ONLY attestations, not genuine operator permissions;
- cannot establish real Block Writes, billing, or credential state;
- cannot be used as evidence that LIVE-02 through LIVE-08 is approved.

It is a small testable model for the future separately reviewed live design.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import math
import re
import time
from typing import Callable


_STAGES = ("LIVE-02", "LIVE-03", "LIVE-04", "LIVE-05", "LIVE-06", "LIVE-07")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
MAX_ELAPSED_SECONDS = 120.0
FIRST_CLEANUP_REQUEST = 80
REQUEST_CEILING = 100
CLEANUP_ALLOWANCE = 20


class MockGateDenied(PermissionError):
    """An invalid simulated transition stops this one-shot session."""


class MockPhase(str, Enum):
    NEW = "NEW"
    PREFLIGHT = "PREFLIGHT"
    WINDOW = "MOCK_WINDOW"
    STAGES_COMPLETE = "STAGES_COMPLETE"
    AUDITED = "AUDITED"
    HALTED = "HALTED"


class MockAudit(str, Enum):
    UNVERIFIED = "UNVERIFIED"
    CLEAN = "CLEAN"
    RESIDUAL = "RESIDUAL"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class MockGateSnapshot:
    phase: MockPhase
    audit: MockAudit
    requests: int
    next_stage: str | None
    manifest_preserved: bool
    lockdown_recorded: bool


class OfflineSessionGate:
    """One-shot mocked transitions; never use its state to permit actual SQL."""

    def __init__(
        self, *,
        expected_manifest_sha256: str,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if not isinstance(expected_manifest_sha256, str) or not _SHA256.fullmatch(
            expected_manifest_sha256
        ):
            raise ValueError("Expected lowercase SHA-256 digest for a mock manifest.")
        if not callable(clock):
            raise TypeError("Clock must be callable.")
        self._digest = expected_manifest_sha256
        self._clock = clock
        self._started = self._read_clock()
        self._last_clock = self._started
        self._next_index = 0
        self._phase = MockPhase.NEW
        self._audit = MockAudit.UNVERIFIED
        self._requests = 0
        self._lockdown_recorded = False

    def _read_clock(self) -> float:
        tick = self._clock()
        if type(tick) not in (int, float) or not math.isfinite(tick):
            raise MockGateDenied("MOCK_CLOCK_INVALID")
        return float(tick)

    def _deny(self, reason: str) -> None:
        self._phase = MockPhase.HALTED
        self._audit = MockAudit.UNKNOWN
        raise MockGateDenied("OFFLINE_GATE_" + reason)

    def _ensure_active(self, *allowed: MockPhase) -> None:
        if self._phase == MockPhase.HALTED:
            self._deny("ALREADY_HALTED")
        now = self._read_clock()
        if now < self._last_clock or now - self._started >= MAX_ELAPSED_SECONDS:
            self._deny("TIME_OR_CLOCK_INVALID")
        self._last_clock = now
        if self._phase not in allowed:
            self._deny("STATE_SEQUENCE")

    @staticmethod
    def _count(value: int) -> bool:
        return type(value) is int and value > 0

    def _consume(self, count: int, *, cleanup: bool) -> None:
        if not self._count(count):
            self._deny("REQUEST_COUNT_INVALID")
        new_count = self._requests + count
        if new_count >= REQUEST_CEILING:
            self._deny("REQUEST_CEILING")
        if cleanup:
            if count > CLEANUP_ALLOWANCE:
                self._deny("CLEANUP_ALLOWANCE")
        elif new_count >= FIRST_CLEANUP_REQUEST:
            self._deny("CLEANUP_RESERVE")
        self._requests = new_count

    def record_simulated_preflight(
        self, *,
        manifest_sha256: str,
        schema_version: int,
        tables_checked: int,
        columns_checked: int,
        collision_checks: int,
        simulated_block_writes_on: bool,
        request_count: int,
    ) -> None:
        """Record *mock* evidence, never validate remote database state."""
        self._ensure_active(MockPhase.NEW)
        if (
            manifest_sha256 != self._digest
            or schema_version != 6
            or tables_checked != 15
            or columns_checked != 129
            or collision_checks != 5
            or simulated_block_writes_on is not True
        ):
            self._deny("PREFLIGHT_EVIDENCE")
        self._consume(request_count, cleanup=False)
        self._phase = MockPhase.PREFLIGHT

    def record_simulated_human_window(self, *, marker: str) -> None:
        """Only literal MOCK_ONLY, NEVER a real human or Dashboard approval."""
        self._ensure_active(MockPhase.PREFLIGHT)
        if marker != "MOCK_ONLY":
            self._deny("WINDOW_ATTESTATION")
        self._phase = MockPhase.WINDOW

    def record_simulated_stage(
        self, *, stage: str, manifest_sha256: str, request_count: int
    ) -> None:
        self._ensure_active(MockPhase.WINDOW)
        if (
            self._next_index >= len(_STAGES)
            or stage != _STAGES[self._next_index]
            or manifest_sha256 != self._digest
        ):
            self._deny("STAGE_OR_MANIFEST")
        self._consume(request_count, cleanup=False)
        self._next_index += 1
        if self._next_index == len(_STAGES):
            self._phase = MockPhase.STAGES_COMPLETE

    def record_simulated_audit(
        self, *, manifest_sha256: str, ownership_verified: bool,
        outcome: MockAudit, request_count: int
    ) -> None:
        self._ensure_active(MockPhase.STAGES_COMPLETE)
        if manifest_sha256 != self._digest or ownership_verified is not True:
            self._deny("OWNERSHIP_NOT_VERIFIED")
        if type(outcome) is not MockAudit or outcome == MockAudit.UNVERIFIED:
            self._deny("AUDIT_OUTCOME")
        self._consume(request_count, cleanup=True)
        self._audit = outcome
        self._phase = MockPhase.AUDITED

    def record_simulated_lockdown(
        self, *, marker: str, simulated_block_writes_on: bool,
        simulated_token_revoked: bool
    ) -> None:
        """Can document simulated lockdown even if session was halted.

        Does NOT restore an UNKNOWN session to CLEAN, and does not change
        any real database Dashboard setting.
        """
        if self._lockdown_recorded:
            self._deny("LOCKDOWN_REENTRY")
        if (
            marker != "MOCK_ONLY"
            or simulated_block_writes_on is not True
            or simulated_token_revoked is not True
            or self._phase not in (MockPhase.AUDITED, MockPhase.HALTED)
        ):
            self._deny("LOCKDOWN_EVIDENCE")
        self._lockdown_recorded = True

    def snapshot(self) -> MockGateSnapshot:
        next_stage = (
            _STAGES[self._next_index]
            if self._phase == MockPhase.WINDOW
            and self._next_index < len(_STAGES)
            else None
        )
        return MockGateSnapshot(
            phase=self._phase,
            audit=self._audit,
            requests=self._requests,
            next_stage=next_stage,
            manifest_preserved=self._audit != MockAudit.CLEAN,
            lockdown_recorded=self._lockdown_recorded,
        )
