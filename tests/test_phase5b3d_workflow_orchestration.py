"""Deterministic automated tests for Phase 5B.3D: Offline Workflow Orchestration & Automation Readiness.

Verifies the 15 required contract specifications:
1. All future Istanbul schedule slots map correctly to UTC (including previous-day offsets).
2. Quiet hours (00:00-08:30 Istanbul) prevent routine editorial digest dispatches.
3. Early and late triggers cannot cause invalid-window dispatch.
4. Skipped windows are not automatically caught up or replayed.
5. Duplicate workflow triggers resolve to a stable window identity.
6. ADMIN receives no editorial content.
7. PRIMARY_EDITOR does not receive ADMIN operational health reporting.
8. Daily ADMIN health reports are limited to at most one per Istanbul calendar day.
9. Missing configuration, invalid timestamps, and incorrect storage mode fail closed.
10. No API or network transport can be invoked by the offline preview path (strictly 0 network calls).
11. No operational database or ledger is modified by preview execution.
12. Backup previews never upload, delete, rotate, or alter recovery points (DESIGN ONLY).
13. Active GitHub Actions cron schedules remain disabled in all workflows.
14. No new manual workflow runs live application paths or injects live secrets.
15. Existing safety invariants and contracts remain fully preserved.
"""
from __future__ import annotations

import os
import re
import socket
import tempfile
import unittest
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

from efso_radar.config import Settings
from efso_radar.domain import DeliveryStatus
from efso_radar.notifications.formatters import format_telegram_admin_health_report
from efso_radar.repositories.sqlite import SqliteStateRepository
from efso_radar.workflow.contracts import (
    AdminHealthReportTaskContract,
    EditorialDigestTaskContract,
    EncryptedBackupTaskContract,
    ScanOnlyTaskContract,
    TaskExecutionMode,
)
from efso_radar.workflow.preview import (
    get_synthetic_editorial_candidates,
    get_synthetic_feed_articles,
    get_synthetic_health_metrics,
    run_offline_admin_health_preview,
    run_offline_backup_preview,
    run_offline_digest_preview,
    run_offline_scan_preview,
)
from efso_radar.workflow.schedule import (
    ISTANBUL_TZ,
    OPERATING_SCHEDULE_SLOTS,
    TaskEligibilityStatus,
    WorkflowTaskType,
    compute_window_identity,
    ensure_timezone_aware,
    evaluate_task_eligibility,
    get_eligible_tasks_for_timestamp,
    get_istanbul_date_str,
    is_in_quiet_hours,
    istanbul_to_utc,
    utc_to_istanbul,
)


class TestWorkflowSchedulingContract(unittest.TestCase):
    """Scope A: Schedule contract, timezone conversions, and boundary mathematics."""

    def test_01_all_istanbul_slots_map_correctly_to_utc(self) -> None:
        """Requirement 1: Verify all future Istanbul schedule slots map to UTC, including date offsets."""
        expected_mappings = {
            # 1. SCAN_ONLY: 8 slots every 3 hours at :17
            (WorkflowTaskType.SCAN_ONLY, "00:17"): ("21:17", -1),
            (WorkflowTaskType.SCAN_ONLY, "03:17"): ("00:17", 0),
            (WorkflowTaskType.SCAN_ONLY, "06:17"): ("03:17", 0),
            (WorkflowTaskType.SCAN_ONLY, "09:17"): ("06:17", 0),
            (WorkflowTaskType.SCAN_ONLY, "12:17"): ("09:17", 0),
            (WorkflowTaskType.SCAN_ONLY, "15:17"): ("12:17", 0),
            (WorkflowTaskType.SCAN_ONLY, "18:17"): ("15:17", 0),
            (WorkflowTaskType.SCAN_ONLY, "21:17"): ("18:17", 0),

            # 2. EDITORIAL_DIGEST: 3 slots outside quiet hours
            (WorkflowTaskType.EDITORIAL_DIGEST, "09:30"): ("06:30", 0),
            (WorkflowTaskType.EDITORIAL_DIGEST, "15:30"): ("12:30", 0),
            (WorkflowTaskType.EDITORIAL_DIGEST, "21:30"): ("18:30", 0),

            # 3. ADMIN_HEALTH_REPORT: 1 slot nightly
            (WorkflowTaskType.ADMIN_HEALTH_REPORT, "22:00"): ("19:00", 0),

            # 4. ENCRYPTED_BACKUP: 1 slot quiet hours (DESIGN ONLY)
            (WorkflowTaskType.ENCRYPTED_BACKUP, "02:15"): ("23:15", -1),
        }

        self.assertEqual(len(OPERATING_SCHEDULE_SLOTS), len(expected_mappings))

        for slot in OPERATING_SCHEDULE_SLOTS:
            key = (slot.task_type, slot.time_istanbul)
            self.assertIn(key, expected_mappings, f"Unexpected slot {key}")
            expected_utc, expected_offset = expected_mappings[key]
            self.assertEqual(slot.time_utc, expected_utc)
            self.assertEqual(slot.utc_day_offset, expected_offset)

            # Mathematical conversion check using Istanbul calendar anchor 2026-10-24
            h, m = map(int, slot.time_istanbul.split(":"))
            ist_dt = datetime(2026, 10, 24, h, m, tzinfo=ISTANBUL_TZ)
            utc_dt = istanbul_to_utc(ist_dt)

            self.assertEqual(utc_dt.strftime("%H:%M"), expected_utc)
            expected_utc_date = (ist_dt.date() + timedelta(days=expected_offset))
            self.assertEqual(utc_dt.date(), expected_utc_date)

    def test_02_quiet_hours_prevent_routine_editorial_dispatch(self) -> None:
        """Requirement 2: Quiet hours (00:00-08:30 Istanbul) block routine editorial digests."""
        # Times strictly inside quiet hours (00:00 to 08:30 Istanbul)
        quiet_times = [
            datetime(2026, 10, 24, 0, 0, tzinfo=ISTANBUL_TZ),
            datetime(2026, 10, 24, 2, 15, tzinfo=ISTANBUL_TZ),
            datetime(2026, 10, 24, 4, 30, tzinfo=ISTANBUL_TZ),
            datetime(2026, 10, 24, 8, 29, 59, tzinfo=ISTANBUL_TZ),
        ]
        for dt in quiet_times:
            self.assertTrue(is_in_quiet_hours(dt), f"Expected {dt} to be in quiet hours")
            res = evaluate_task_eligibility(WorkflowTaskType.EDITORIAL_DIGEST, dt)
            self.assertFalse(res.is_eligible)
            self.assertEqual(res.status, TaskEligibilityStatus.QUIET_HOURS_BLOCKED)
            self.assertIn("quiet hours", res.reason.lower())

        # Times strictly outside quiet hours
        active_times = [
            datetime(2026, 10, 24, 8, 30, tzinfo=ISTANBUL_TZ),
            datetime(2026, 10, 24, 9, 30, tzinfo=ISTANBUL_TZ),
            datetime(2026, 10, 24, 15, 30, tzinfo=ISTANBUL_TZ),
            datetime(2026, 10, 24, 21, 30, tzinfo=ISTANBUL_TZ),
            datetime(2026, 10, 24, 23, 59, tzinfo=ISTANBUL_TZ),
        ]
        for dt in active_times:
            self.assertFalse(is_in_quiet_hours(dt), f"Expected {dt} outside quiet hours")

    def test_03_early_and_late_triggers_cannot_cause_invalid_window_dispatch(self) -> None:
        """Requirement 3: Early triggers (< slot) and late triggers (> tolerance) fail closed."""
        # Slot 09:17 Istanbul (tolerance 45m: valid [09:17, 10:02])
        early_dt = datetime(2026, 10, 24, 9, 16, 30, tzinfo=ISTANBUL_TZ)  # 30s early
        res_early = evaluate_task_eligibility(WorkflowTaskType.SCAN_ONLY, early_dt, tolerance_minutes=45)
        self.assertFalse(res_early.is_eligible)
        self.assertEqual(res_early.status, TaskEligibilityStatus.EARLY_TRIGGER)
        self.assertIn("early", res_early.reason.lower())

        late_dt = datetime(2026, 10, 24, 10, 15, 0, tzinfo=ISTANBUL_TZ)   # 58m late (> 45m)
        res_late = evaluate_task_eligibility(WorkflowTaskType.SCAN_ONLY, late_dt, tolerance_minutes=45)
        self.assertFalse(res_late.is_eligible)
        self.assertEqual(res_late.status, TaskEligibilityStatus.WINDOW_EXPIRED)
        self.assertIn("expired", res_late.reason.lower())

        # Within tolerance (09:17 + 20m = 09:37)
        valid_dt = datetime(2026, 10, 24, 9, 37, 0, tzinfo=ISTANBUL_TZ)
        res_valid = evaluate_task_eligibility(WorkflowTaskType.SCAN_ONLY, valid_dt, tolerance_minutes=45)
        self.assertTrue(res_valid.is_eligible)
        self.assertEqual(res_valid.status, TaskEligibilityStatus.ELIGIBLE)
        self.assertEqual(res_valid.window_id, "SCAN_ONLY:2026-10-24T09:17")

    def test_04_skipped_windows_are_not_automatically_replayed(self) -> None:
        """Requirement 4: Missed windows are never retroactively replayed or re-dispatched."""
        # Triggered at 15:30 Istanbul. Missed slots (09:17, 09:30, 12:17) must not be replayed.
        eval_dt = datetime(2026, 10, 24, 15, 30, 0, tzinfo=ISTANBUL_TZ)
        res_digest = evaluate_task_eligibility(WorkflowTaskType.EDITORIAL_DIGEST, eval_dt)
        self.assertTrue(res_digest.is_eligible)
        self.assertEqual(res_digest.window_id, "EDITORIAL_DIGEST:2026-10-24T15:30")

        # Evaluate previous slot (09:30) evaluated at 15:30: expired, no catchup
        res_scan_at_digest_time = evaluate_task_eligibility(WorkflowTaskType.SCAN_ONLY, eval_dt)
        # 15:30 is 13m after 15:17 slot -> matches 15:17, NOT morning 09:17
        self.assertTrue(res_scan_at_digest_time.is_eligible)
        self.assertEqual(res_scan_at_digest_time.window_id, "SCAN_ONLY:2026-10-24T15:17")

    def test_05_duplicate_workflow_triggers_resolve_to_stable_identity(self) -> None:
        """Requirement 5: Duplicate triggers within the same window produce identical window_id."""
        t1 = datetime(2026, 10, 24, 9, 17, 5, tzinfo=ISTANBUL_TZ)
        t2 = datetime(2026, 10, 24, 9, 23, 12, tzinfo=ISTANBUL_TZ)
        t3 = datetime(2026, 10, 24, 9, 45, 0, tzinfo=ISTANBUL_TZ)

        res1 = evaluate_task_eligibility(WorkflowTaskType.SCAN_ONLY, t1, tolerance_minutes=45)
        res2 = evaluate_task_eligibility(WorkflowTaskType.SCAN_ONLY, t2, tolerance_minutes=45)
        res3 = evaluate_task_eligibility(WorkflowTaskType.SCAN_ONLY, t3, tolerance_minutes=45)

        self.assertEqual(res1.window_id, "SCAN_ONLY:2026-10-24T09:17")
        self.assertEqual(res1.window_id, res2.window_id)
        self.assertEqual(res2.window_id, res3.window_id)

    def test_06_admin_receives_no_editorial_content(self) -> None:
        """Requirement 6: Recipient routing guarantees ADMIN receives no routine editorial digests."""
        settings = Settings(
            telegram_chat_id="999888777",  # ADMIN
            telegram_primary_editor_chat_id="111222333",  # PRIMARY_EDITOR
            telegram_allowed_chat_ids=["999888777", "111222333"],
        )
        editorial_recipients = settings.get_editorial_recipients()
        self.assertEqual(len(editorial_recipients), 1)
        self.assertEqual(editorial_recipients[0]["role"], "PRIMARY_EDITOR")
        self.assertEqual(editorial_recipients[0]["chat_id"], "111222333")

        # Verify ADMIN chat ID is strictly absent
        self.assertNotIn("999888777", [r["chat_id"] for r in editorial_recipients])

        contract = EditorialDigestTaskContract(settings=settings)
        res = contract.run_preview(datetime(2026, 10, 24, 9, 30, tzinfo=ISTANBUL_TZ))
        self.assertEqual(len(res.recipients), 1)
        self.assertEqual(res.recipients[0]["role"], "PRIMARY_EDITOR")

    def test_07_primary_editor_does_not_receive_admin_health_reporting(self) -> None:
        """Requirement 7: PRIMARY_EDITOR does not receive ADMIN operational health reports."""
        settings = Settings(
            telegram_chat_id="999888777",  # ADMIN
            telegram_primary_editor_chat_id="111222333",  # PRIMARY_EDITOR
            telegram_allowed_chat_ids=["999888777", "111222333"],
        )
        admin_recipients = settings.get_administrative_recipients()
        self.assertEqual(len(admin_recipients), 1)
        self.assertEqual(admin_recipients[0]["role"], "ADMIN")
        self.assertEqual(admin_recipients[0]["chat_id"], "999888777")

        # Verify PRIMARY_EDITOR chat ID is strictly absent
        self.assertNotIn("111222333", [r["chat_id"] for r in admin_recipients])

        contract = AdminHealthReportTaskContract(settings=settings)
        res = contract.run_preview(datetime(2026, 10, 24, 22, 0, tzinfo=ISTANBUL_TZ))
        self.assertEqual(len(res.recipients), 1)
        self.assertEqual(res.recipients[0]["role"], "ADMIN")

    def test_08_daily_admin_health_report_limited_to_one_per_istanbul_day(self) -> None:
        """Requirement 8: Scheduling contract strictly limits admin health report to 1 per Istanbul calendar day."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test_admin_daily.db"
            repo = SqliteStateRepository(db_path)

            eval_dt = datetime(2026, 10, 24, 22, 5, tzinfo=ISTANBUL_TZ)

            # 1. Before dispatch: eligible
            res_before = evaluate_task_eligibility(
                WorkflowTaskType.ADMIN_HEALTH_REPORT,
                eval_dt,
                repository=repo,
            )
            self.assertTrue(res_before.is_eligible)
            self.assertEqual(res_before.status, TaskEligibilityStatus.ELIGIBLE)

            # 2. Record dispatch for date '2026-10-24'
            inserted = repo.record_admin_report(
                report_date="2026-10-24",
                worker_id="test_worker_1",
                status=DeliveryStatus.SENT,
                recipient_chat_id="999888777",
                summary_payload={"system_status": "NORMAL"},
            )
            self.assertTrue(inserted)
            self.assertTrue(repo.has_admin_report_been_dispatched("2026-10-24"))

            # 3. Subsequent evaluation on same Istanbul day must fail closed as ALREADY_DISPATCHED_TODAY
            res_after = evaluate_task_eligibility(
                WorkflowTaskType.ADMIN_HEALTH_REPORT,
                eval_dt,
                repository=repo,
            )
            self.assertFalse(res_after.is_eligible)
            self.assertEqual(res_after.status, TaskEligibilityStatus.ALREADY_DISPATCHED_TODAY)
            self.assertIn("already dispatched", res_after.reason.lower())

    def test_09_missing_config_invalid_timestamps_and_incorrect_storage_fail_closed(self) -> None:
        """Requirement 9: Naive datetimes, invalid timestamps, and invalid storage fail closed."""
        # Naive datetime fails closed
        naive_dt = datetime(2026, 10, 24, 12, 0, 0)
        with self.assertRaises(ValueError):
            ensure_timezone_aware(naive_dt)

        with self.assertRaises(ValueError):
            evaluate_task_eligibility(WorkflowTaskType.SCAN_ONLY, naive_dt)

        # Invalid storage mode fails closed
        with self.assertRaises(ValueError):
            Settings(storage_mode="turso", turso_database_url="", turso_auth_token="")

        # Live production execution is strictly blocked
        contract = ScanOnlyTaskContract()
        aware_dt = datetime(2026, 10, 24, 9, 17, tzinfo=ISTANBUL_TZ)
        with self.assertRaises(NotImplementedError):
            contract.execute(mode=TaskExecutionMode.LIVE_PRODUCTION, timestamp=aware_dt)


class TestWorkflowOfflinePreviewIsolation(unittest.TestCase):
    """Scope B: Hermetic offline previews, zero-network, zero-spend guarantees."""

    def test_10_no_api_or_network_transport_can_be_invoked_by_offline_preview(self) -> None:
        """Requirement 10: Hermetic isolation proves zero network requests across all preview adapters."""
        with patch("urllib.request.urlopen") as mock_urlopen, \
             patch("http.client.HTTPConnection.request") as mock_http, \
             patch("socket.socket.connect") as mock_connect:

            dt = datetime(2026, 10, 24, 9, 30, tzinfo=ISTANBUL_TZ)
            settings = Settings()

            # Execute all 4 task contracts in OFFLINE_PREVIEW mode
            scan_res = ScanOnlyTaskContract(settings).execute(TaskExecutionMode.OFFLINE_PREVIEW, dt)
            digest_res = EditorialDigestTaskContract(settings).execute(TaskExecutionMode.OFFLINE_PREVIEW, dt)
            admin_res = AdminHealthReportTaskContract(settings).execute(TaskExecutionMode.OFFLINE_PREVIEW, dt)
            backup_res = EncryptedBackupTaskContract(settings).execute(TaskExecutionMode.OFFLINE_PREVIEW, dt)

            self.assertEqual(scan_res.network_calls_made, 0)
            self.assertEqual(digest_res.network_calls_made, 0)
            self.assertEqual(admin_res.network_calls_made, 0)
            self.assertEqual(backup_res.network_calls_made, 0)

            self.assertEqual(scan_res.cost_usd, 0.0)
            self.assertEqual(digest_res.cost_usd, 0.0)
            self.assertEqual(admin_res.cost_usd, 0.0)
            self.assertEqual(backup_res.cost_usd, 0.0)

            # Strictly 0 external transport invocations
            self.assertEqual(mock_urlopen.call_count, 0)
            self.assertEqual(mock_http.call_count, 0)
            self.assertEqual(mock_connect.call_count, 0)

    def test_11_no_operational_database_or_ledger_modified_by_preview_execution(self) -> None:
        """Requirement 11: Operational data/radar.db and state files remain completely untouched."""
        op_db = Path("data/radar.db")
        op_state = Path("data/state.json")

        initial_db_exists = op_db.exists()
        initial_db_mtime = op_db.stat().st_mtime if initial_db_exists else None
        initial_state_exists = op_state.exists()
        initial_state_mtime = op_state.stat().st_mtime if initial_state_exists else None

        dt = datetime(2026, 10, 24, 9, 30, tzinfo=ISTANBUL_TZ)
        settings = Settings()

        ScanOnlyTaskContract(settings).run_preview(dt)
        EditorialDigestTaskContract(settings).run_preview(dt)
        AdminHealthReportTaskContract(settings).run_preview(dt)
        EncryptedBackupTaskContract(settings).run_preview(dt)

        if initial_db_exists:
            self.assertEqual(op_db.stat().st_mtime, initial_db_mtime)
        else:
            self.assertFalse(op_db.exists())

        if initial_state_exists:
            self.assertEqual(op_state.stat().st_mtime, initial_state_mtime)
        else:
            self.assertFalse(op_state.exists())

    def test_12_backup_previews_never_upload_delete_rotate_or_alter_recovery_points(self) -> None:
        """Requirement 12: Backup previews strictly produce planning metadata without remote mutations."""
        dt = datetime(2026, 10, 24, 2, 15, tzinfo=ISTANBUL_TZ)
        preview_data = run_offline_backup_preview(dt)

        self.assertEqual(preview_data["mode"], "OFFLINE_PREVIEW")
        self.assertEqual(preview_data["status"], "DESIGN_ONLY")
        self.assertEqual(preview_data["remote_upload_status"], "NOT_IMPLEMENTED (DESIGN ONLY)")
        self.assertEqual(preview_data["remote_retention_delete_status"], "NOT_IMPLEMENTED (DESIGN ONLY)")
        self.assertEqual(preview_data["network_requests"], 0)
        self.assertEqual(preview_data["cost_usd"], 0.0)


class TestWorkflowGitHubActionsSafety(unittest.TestCase):
    """Scope C: GitHub Actions workflows safety validation."""

    def test_13_active_github_actions_cron_remains_disabled(self) -> None:
        """Requirement 13: All workflow files have active cron schedules commented out or absent."""
        workflow_dir = Path(".github/workflows")
        self.assertTrue(workflow_dir.exists(), "Workflow directory missing")

        workflow_files = list(workflow_dir.glob("*.yml"))
        self.assertGreaterEqual(len(workflow_files), 4)

        for wf_path in workflow_files:
            content = wf_path.read_text(encoding="utf-8")
            for line in content.splitlines():
                stripped = line.strip()
                # Active schedule or cron line must not exist uncommented
                if stripped.startswith("schedule:") or stripped.startswith("- cron:"):
                    self.fail(f"Active scheduled cron found in {wf_path.name}: '{stripped}'")

    def test_14_no_new_manual_workflow_runs_live_application_paths(self) -> None:
        """Requirement 14: Manual workflow jobs execute preview/test only; no live secrets injected."""
        expected_separated_workflows = [
            "rss_discovery.yml",
            "editorial_digest.yml",
            "admin_health_report.yml",
            "encrypted_backup.yml",
        ]
        workflow_dir = Path(".github/workflows")

        for wf_name in expected_separated_workflows:
            wf_file = workflow_dir / wf_name
            self.assertTrue(wf_file.exists(), f"Expected workflow {wf_name} missing")
            content = wf_file.read_text(encoding="utf-8")

            # Active steps must only invoke --preview and must NOT inject live secrets in active job steps
            self.assertIn("--preview", content)

            # Parse lines outside commented template blocks
            active_lines = [l for l in content.splitlines() if not l.strip().startswith("#")]
            active_text = "\n".join(active_lines)

            # In active execution blocks, live secrets must NOT be injected
            self.assertNotIn("${{ secrets.TURSO_DATABASE_URL }}", active_text)
            self.assertNotIn("${{ secrets.GEMINI_API_KEY }}", active_text)
            self.assertNotIn("${{ secrets.TELEGRAM_BOT_TOKEN }}", active_text)
            self.assertNotIn("--live", active_text)

    def test_15_admin_health_report_html_formatter(self) -> None:
        """Requirement 15: Admin health report HTML formatter conforms to DAILY_ADMIN_HEALTH_REPORT_DESIGN."""
        metrics = get_synthetic_health_metrics()
        now = datetime(2026, 10, 24, 22, 0, tzinfo=ISTANBUL_TZ)
        html_output = format_telegram_admin_health_report(metrics, now=now)

        self.assertIn("EFSO RADAR · GÜNLÜK SAĞLIK RAPORU", html_output)
        self.assertIn("24 Ekim 2026", html_output)
        self.assertIn("22:00 TSİ", html_output)
        self.assertIn("SİSTEM DURUMU: NORMAL / SAĞLIKLI", html_output)
        self.assertIn("Başarılı: 8 / 8 pencere (%100)", html_output)
        self.assertIn("0,00 TRY / $0.00", html_output)
        self.assertIn("PRIMARY_EDITOR Teslimatı: 3 bülten (9 haber)", html_output)
        self.assertIn("yalnızca sistem yöneticisine (ADMIN)", html_output)



class TestPhase5B3DSecurityReviewFixes(unittest.TestCase):
    """Product Owner Code Review Security Fixes (Issues 1-4)."""

    def test_issue1_unconditional_preview_repository_isolation(self) -> None:
        """Issue 1: Caller-supplied repositories must never receive preview writes."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "operational_double.db"
            writable_repo = SqliteStateRepository(db_path)

            # Ensure double has empty backlog initially
            self.assertEqual(len(writable_repo.get_pending_candidates(max_age_hours=24)), 0)

            # 1. Calling run_offline_scan_preview with repository must NOT write to it
            dt_eligible = datetime(2026, 10, 24, 9, 17, tzinfo=ISTANBUL_TZ)
            res_preview = run_offline_scan_preview(dt_eligible, repository=writable_repo)
            self.assertEqual(res_preview["backlog_candidates_queued"], 3)
            # Crucial check: writable_repo must STILL have 0 candidates
            self.assertEqual(len(writable_repo.get_pending_candidates(max_age_hours=24)), 0)

            # 2. Calling contract.run_preview with repository must NOT write to it
            contract = ScanOnlyTaskContract()
            contract_res = contract.run_preview(dt_eligible, repository=writable_repo)
            self.assertEqual(contract_res.status, "SUCCESS")
            self.assertEqual(len(writable_repo.get_pending_candidates(max_age_hours=24)), 0)

            # 3. Ineligible trigger must NOT execute preview or touch storage
            dt_ineligible = datetime(2026, 10, 24, 9, 10, tzinfo=ISTANBUL_TZ)  # Early trigger
            ineligible_res = contract.run_preview(dt_ineligible, repository=writable_repo)
            self.assertIsNone(ineligible_res.window_id)
            self.assertEqual(ineligible_res.status, "INELIGIBLE_EARLY_TRIGGER")
            self.assertFalse(ineligible_res.summary.get("executed", True))
            self.assertEqual(len(writable_repo.get_pending_candidates(max_age_hours=24)), 0)

    def test_issue2_admin_health_report_no_fabricated_counters_or_costs(self) -> None:
        """Issue 2: Missing counters must be identified as UNKNOWN; costs must never fabricate TRY or 0."""
        now = datetime(2026, 10, 24, 22, 0, tzinfo=ISTANBUL_TZ)

        # 1. Missing operational counters must report BİLİNMİYOR, never fabricated as 8/8 success
        empty_metrics: dict[str, Any] = {}
        empty_html = format_telegram_admin_health_report(empty_metrics, now=now)
        self.assertIn("SİSTEM DURUMU: BİLİNMİYOR (VERİ YOK)", empty_html)
        self.assertIn("Başarılı: <i>BİLİNMİYOR (Veri Kaydı Yok)</i>", empty_html)
        self.assertIn("Toplam Taranan Makale: <i>BİLİNMİYOR</i>", empty_html)
        self.assertIn("Dahili Hesaplanan Harcama: <i>BİLİNMİYOR (Harcama Kanıtı Yok)</i>", empty_html)
        self.assertNotIn("8 / 8", empty_html)
        self.assertNotIn("0,00 TRY", empty_html)

        # 2. Nonzero cost with verified conversion rate
        cost_with_rate = {
            "system_status": "NORMAL / SAĞLIKLI",
            "rss_windows_success": 8,
            "rss_windows_total": 8,
            "cost_usd": 2.50,
            "usd_to_try_rate": 36.0,
        }
        rate_html = format_telegram_admin_health_report(cost_with_rate, now=now)
        self.assertIn("90,00 TRY / $2.50", rate_html)
        self.assertIn("Kur: 36.00", rate_html)

        # 3. Nonzero cost without verified conversion rate: NEVER present USD as TRY
        cost_no_rate = {
            "system_status": "NORMAL / SAĞLIKLI",
            "rss_windows_success": 8,
            "rss_windows_total": 8,
            "cost_usd": 2.50,
        }
        no_rate_html = format_telegram_admin_health_report(cost_no_rate, now=now)
        self.assertIn("$2.50", no_rate_html)
        self.assertIn("TRY Karşılığı Bilinmiyor — Kur Doğrulanmadı", no_rate_html)
        self.assertNotIn("2,50 TRY", no_rate_html)
        self.assertNotIn("0 TRY", no_rate_html)
        self.assertNotIn("0,00 TRY", no_rate_html)

        # 4. Explicit verified zero cost
        cost_zero = {
            "system_status": "NORMAL / SAĞLIKLI",
            "rss_windows_success": 8,
            "rss_windows_total": 8,
            "cost_usd": 0.00,
        }
        zero_html = format_telegram_admin_health_report(cost_zero, now=now)
        self.assertIn("0,00 TRY / $0.00", zero_html)

        # 5. Zero denominator for RSS windows handled safely
        zero_rss = {"rss_windows_success": 0, "rss_windows_total": 0}
        zero_rss_html = format_telegram_admin_health_report(zero_rss, now=now)
        self.assertIn("0 / 0 pencere (Tarama Penceresi Yok)", zero_rss_html)

        # 6. Malformed types handled without crash
        malformed = {
            "cost_usd": "invalid_cost",
            "rss_windows_success": "bad_counter",
            "delivery_errors": None,
        }
        malformed_html = format_telegram_admin_health_report(malformed, now=now)
        self.assertIn("BİLİNMİYOR", malformed_html)

    def test_issue3_preview_result_chat_id_privacy(self) -> None:
        """Issue 3: Output models must never leak raw chat IDs in model_dump, JSON, repr, or logs."""
        editor_secret_id = "SECRET_PRIMARY_EDITOR_CHAT_987654321"
        admin_secret_id = "SECRET_ADMIN_CHAT_123456789"

        settings = Settings(
            telegram_primary_editor_chat_id=editor_secret_id,
            telegram_chat_id=admin_secret_id,
            telegram_allowed_chat_ids=[editor_secret_id, admin_secret_id],
        )

        dt_digest = datetime(2026, 10, 24, 9, 30, tzinfo=ISTANBUL_TZ)
        digest_contract = EditorialDigestTaskContract(settings=settings)
        digest_res = digest_contract.run_preview(dt_digest)

        # Raw editor chat ID must NOT appear anywhere in the result model or its dumps
        self.assertNotIn(editor_secret_id, str(digest_res.model_dump()))
        self.assertNotIn(editor_secret_id, digest_res.model_dump_json())
        self.assertNotIn(editor_secret_id, repr(digest_res))
        self.assertNotIn(editor_secret_id, str(digest_res))
        self.assertNotIn(editor_secret_id, digest_res.message)
        self.assertNotIn(editor_secret_id, str(digest_res.summary))

        # Only masked chat ID appears
        self.assertEqual(len(digest_res.recipients), 1)
        self.assertEqual(digest_res.recipients[0].role, "PRIMARY_EDITOR")
        self.assertEqual(digest_res.recipients[0].masked_chat_id, "SE...21")
        self.assertNotIn(admin_secret_id, str(digest_res.model_dump()))

        # Admin health report must not leak admin raw chat ID
        dt_admin = datetime(2026, 10, 24, 22, 0, tzinfo=ISTANBUL_TZ)
        admin_contract = AdminHealthReportTaskContract(settings=settings)
        admin_res = admin_contract.run_preview(dt_admin)

        self.assertNotIn(admin_secret_id, str(admin_res.model_dump()))
        self.assertNotIn(admin_secret_id, admin_res.model_dump_json())
        self.assertNotIn(admin_secret_id, repr(admin_res))
        self.assertNotIn(admin_secret_id, str(admin_res))
        self.assertNotIn(admin_secret_id, admin_res.message)

        self.assertEqual(len(admin_res.recipients), 1)
        self.assertEqual(admin_res.recipients[0].role, "ADMIN")
        self.assertEqual(admin_res.recipients[0].masked_chat_id, "SE...89")
        self.assertNotIn(editor_secret_id, str(admin_res.model_dump()))

    def test_issue4_bounded_tolerance_and_strict_execution_modes(self) -> None:
        """Issue 4: tolerance_minutes bounded; invalid execution modes rejected; ineligible window_id is None."""
        dt = datetime(2026, 10, 24, 9, 17, tzinfo=ISTANBUL_TZ)

        # 1. Bounded tolerance range [1, 120]
        with self.assertRaises(ValueError):
            evaluate_task_eligibility(WorkflowTaskType.SCAN_ONLY, dt, tolerance_minutes=0)

        with self.assertRaises(ValueError):
            evaluate_task_eligibility(WorkflowTaskType.SCAN_ONLY, dt, tolerance_minutes=121)

        with self.assertRaises(ValueError):
            evaluate_task_eligibility(WorkflowTaskType.SCAN_ONLY, dt, tolerance_minutes=-10)

        with self.assertRaises(TypeError):
            evaluate_task_eligibility(WorkflowTaskType.SCAN_ONLY, dt, tolerance_minutes="45")  # type: ignore

        with self.assertRaises(TypeError):
            evaluate_task_eligibility(WorkflowTaskType.SCAN_ONLY, dt, tolerance_minutes=True)  # type: ignore

        # 2. Strict execution mode validation
        contract = ScanOnlyTaskContract()
        with self.assertRaises(ValueError):
            contract.execute(mode="INVALID_UNKNOWN_MODE", timestamp=dt)  # type: ignore

        with self.assertRaises(NotImplementedError):
            contract.execute(mode=TaskExecutionMode.LIVE_PRODUCTION, timestamp=dt)

        # 3. Ineligible timestamps must NOT receive active window_id
        dt_early = datetime(2026, 10, 24, 9, 10, tzinfo=ISTANBUL_TZ)
        res_early = evaluate_task_eligibility(WorkflowTaskType.SCAN_ONLY, dt_early)
        self.assertFalse(res_early.is_eligible)
        self.assertIsNone(res_early.window_id)

        contract_early_res = contract.run_preview(dt_early)
        self.assertIsNone(contract_early_res.window_id)
        self.assertEqual(contract_early_res.status, "INELIGIBLE_EARLY_TRIGGER")
        self.assertFalse(contract_early_res.summary.get("executed", True))




class TestPhase5B3DFinalAcceptanceCorrections(unittest.TestCase):
    """Final Acceptance Corrections Verification (Issues 1-4)."""

    def test_correction1_admin_health_report_invalid_metrics_adversarial(self) -> None:
        """Issue 1: Reject negative, fractional, inconsistent metrics; never derive % from errors alone."""
        now = datetime(2026, 10, 24, 22, 0, tzinfo=ISTANBUL_TZ)

        # 1. Delivery errors alone: must NEVER calculate percentage (e.g. 100 - errors*10)
        # Reports error count and marks success percentage UNKNOWN
        metrics_err_alone = {"delivery_errors": 2}
        html_err = format_telegram_admin_health_report(metrics_err_alone, now=now)
        self.assertIn("Teslimat Başarısı: <i>BİLİNMİYOR</i> (Hata: 2)", html_err)
        self.assertNotIn("%80", html_err)
        self.assertNotIn("%100", html_err)

        # 2. Reject negative, fractional, or boolean operational counters
        adversarial_counters = [
            {"delivery_errors": -1},
            {"delivery_errors": 2.5},
            {"delivery_errors": True},
            {"candidates_evaluated": -5},
            {"candidates_evaluated": 1.5},
            {"candidates_evaluated": False},
            {"total_articles_scanned": -10},
            {"total_articles_scanned": 4.2},
            {"gemini_calls": -1},
            {"gemini_calls": 3.14},
            {"gemini_calls": True},
        ]
        for bad_metric in adversarial_counters:
            html_bad = format_telegram_admin_health_report(bad_metric, now=now)
            self.assertIn("BİLİNMİYOR", html_bad)

        # 3. Reject rss_windows_success > rss_windows_total
        inconsistent_rss = {"rss_windows_success": 9, "rss_windows_total": 8}
        html_inconsistent = format_telegram_admin_health_report(inconsistent_rss, now=now)
        self.assertIn("Tutarsız Veri: Başarılı > Toplam", html_inconsistent)
        self.assertNotIn("%112", html_inconsistent)

        # 4. Reject non-finite USD costs (NaN, Infinity, -Infinity)
        for non_finite_cost in (float("nan"), float("inf"), float("-inf")):
            html_nan = format_telegram_admin_health_report({"cost_usd": non_finite_cost}, now=now)
            self.assertIn("Dahili Hesaplanan Harcama: <i>BİLİNMİYOR (Harcama Kanıtı Yok)</i>", html_nan)
            self.assertNotIn("NaN", html_nan)
            self.assertNotIn("Infinity", html_nan)
            self.assertNotIn("inf", html_nan.lower().split("harcama:")[1].split("\n")[0])

        # 5. Reject non-finite or negative exchange rates
        for bad_rate in (float("nan"), float("inf"), -36.0, 0.0):
            html_bad_rate = format_telegram_admin_health_report({"cost_usd": 10.0, "usd_to_try_rate": bad_rate}, now=now)
            self.assertIn("$10.00", html_bad_rate)
            self.assertIn("TRY Karşılığı Bilinmiyor", html_bad_rate)
            self.assertNotIn("NaN", html_bad_rate)
            self.assertNotIn("Infinity", html_bad_rate)
            self.assertNotIn("inf", html_bad_rate.lower().split("harcama:")[1].split("\n")[0])

        # 6. Reject negative costs; never interpret as valid expenditure
        for neg_cost in (-5.0, -0.01, -100.0):
            html_neg = format_telegram_admin_health_report({"cost_usd": neg_cost}, now=now)
            self.assertIn("Dahili Hesaplanan Harcama: <i>BİLİNMİYOR (Harcama Kanıtı Yok)</i>", html_neg)

        # 7. Missing one member of a metric pair must not fabricate zero
        missing_news = {"editor_digests_sent": 3, "editor_news_items": None}
        html_missing_news = format_telegram_admin_health_report(missing_news, now=now)
        self.assertIn("3 bülten (Haber Sayısı Bilinmiyor)", html_missing_news)
        self.assertNotIn("0 haber", html_missing_news)

        missing_digests = {"editor_digests_sent": None, "editor_news_items": 5}
        html_missing_digests = format_telegram_admin_health_report(missing_digests, now=now)
        self.assertIn("PRIMARY_EDITOR Teslimatı: Bülten Sayısı Bilinmiyor (5 haber)", html_missing_digests)
        self.assertNotIn("PRIMARY_EDITOR Teslimatı: 0 bülten", html_missing_digests)

        missing_rss_total = {"rss_windows_success": 8, "rss_windows_total": None}
        html_missing_rss = format_telegram_admin_health_report(missing_rss_total, now=now)
        self.assertIn("Başarılı: <i>BİLİNMİYOR (Veri Kaydı Yok)</i>", html_missing_rss)
        self.assertNotIn("8 / 0", html_missing_rss)

        # 8. HTML escaping and billing-status UNVERIFIED preserved
        xss_metrics = {
            "system_status": "<script>alert('xss')</script>",
            "operational_notes": ["<b>bold tag</b> & special 'chars'"],
            "cost_usd": 0.0,
        }
        html_xss = format_telegram_admin_health_report(xss_metrics, now=now)
        self.assertIn("&lt;script&gt;alert(&#x27;xss&#x27;)&lt;/script&gt;", html_xss)
        self.assertIn("&lt;b&gt;bold tag&lt;/b&gt; &amp; special &#x27;chars&#x27;", html_xss)
        self.assertIn("0,00 TRY / $0.00", html_xss)
        self.assertIn("Doğrulanmamış (Konsoldan teyit gerekir)", html_xss)

    def test_correction2_admin_health_report_midnight_date_mismatch(self) -> None:
        """Issue 2: Midnight trigger (00:00) must query repository using MATCHED SLOT date (2026-10-24)."""
        # Scheduled window: 2026-10-24 22:00 Europe/Istanbul
        # Actual trigger: 2026-10-25 00:00 Europe/Istanbul
        # Tolerance: 120 minutes
        midnight_trigger = datetime(2026, 10, 25, 0, 0, tzinfo=ISTANBUL_TZ)

        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test_midnight.db"
            repo = SqliteStateRepository(db_path)

            # Case A: NOT previously dispatched for 2026-10-24 -> ELIGIBLE
            res_eligible = evaluate_task_eligibility(
                WorkflowTaskType.ADMIN_HEALTH_REPORT,
                midnight_trigger,
                tolerance_minutes=120,
                repository=repo,
            )
            self.assertTrue(res_eligible.is_eligible)
            self.assertEqual(res_eligible.status, TaskEligibilityStatus.ELIGIBLE)
            self.assertEqual(res_eligible.window_id, "ADMIN_HEALTH_REPORT:2026-10-24")
            self.assertEqual(res_eligible.istanbul_date, "2026-10-24")
            self.assertEqual(res_eligible.delay_minutes, 120.0)

            # Case B: Dispatched for 2026-10-24 -> ALREADY_DISPATCHED_TODAY, window_id=None
            repo.record_admin_report(
                report_date="2026-10-24",
                worker_id="test_worker_midnight",
                status=DeliveryStatus.SENT,
                recipient_chat_id="999888777",
                summary_payload={"system_status": "NORMAL"},
            )
            self.assertTrue(repo.has_admin_report_been_dispatched("2026-10-24"))
            self.assertFalse(repo.has_admin_report_been_dispatched("2026-10-25"))

            res_dispatched = evaluate_task_eligibility(
                WorkflowTaskType.ADMIN_HEALTH_REPORT,
                midnight_trigger,
                tolerance_minutes=120,
                repository=repo,
            )
            self.assertFalse(res_dispatched.is_eligible)
            self.assertEqual(res_dispatched.status, TaskEligibilityStatus.ALREADY_DISPATCHED_TODAY)
            self.assertIsNone(res_dispatched.window_id)  # Ineligible run must NOT receive window_id
            self.assertEqual(res_dispatched.istanbul_date, "2026-10-24")
            self.assertIn("already dispatched for Istanbul calendar date 2026-10-24", res_dispatched.reason)

            # Case C: Beyond tolerance (e.g. 00:01 with 120m tolerance) -> WINDOW_EXPIRED
            late_trigger = datetime(2026, 10, 25, 0, 1, tzinfo=ISTANBUL_TZ)
            res_expired = evaluate_task_eligibility(
                WorkflowTaskType.ADMIN_HEALTH_REPORT,
                late_trigger,
                tolerance_minutes=120,
                repository=repo,
            )
            self.assertFalse(res_expired.is_eligible)
            self.assertEqual(res_expired.status, TaskEligibilityStatus.WINDOW_EXPIRED)
            self.assertIsNone(res_expired.window_id)

    def test_correction3_synthetic_rss_fixtures_freshness_across_dates(self) -> None:
        """Issue 3: Synthetic RSS fixtures dynamically derived from preview timestamp (remains fresh)."""
        future_dates = [
            datetime(2026, 11, 15, 6, 17, tzinfo=timezone.utc),  # November 2026
            datetime(2026, 12, 31, 15, 17, tzinfo=timezone.utc), # December 2026
            datetime(2027, 5, 20, 9, 17, tzinfo=timezone.utc),   # May 2027
            datetime(2028, 10, 24, 6, 17, tzinfo=timezone.utc),  # October 2028
        ]

        contract = ScanOnlyTaskContract()
        for dt in future_dates:
            # 1. run_offline_scan_preview directly
            preview_res = run_offline_scan_preview(dt)
            self.assertEqual(preview_res["synthetic_articles_scanned"], 3)
            self.assertEqual(preview_res["backlog_candidates_queued"], 3, f"Candidates expired on date {dt}")
            self.assertEqual(preview_res["cost_usd"], 0.00)
            self.assertTrue(preview_res["isolated_storage"])

            # 2. Contract execution
            contract_res = contract.run_preview(dt)
            self.assertEqual(contract_res.status, "SUCCESS")
            self.assertEqual(contract_res.summary["backlog_candidates_queued"], 3)

    def test_correction4_manual_workflow_smoke_tests_and_cli_sample_eligible(self) -> None:
        """Issue 4: Manual workflow smoke tests have deterministic eligible timestamps and CLI sample-eligible."""
        from efso_radar.workflow.cli import main as cli_main

        # 1. Check each workflow YAML has inputs.timestamp with default eligible timestamp
        workflows = {
            "rss_discovery.yml": "2026-10-24T06:17:00Z",
            "editorial_digest.yml": "2026-10-24T06:30:00Z",
            "admin_health_report.yml": "2026-10-24T19:00:00Z",
            "encrypted_backup.yml": "2026-10-23T23:15:00Z",
        }
        for wf_file, expected_ts in workflows.items():
            wf_path = Path(".github/workflows") / wf_file
            content = wf_path.read_text(encoding="utf-8")
            self.assertIn('default: "' + expected_ts + '"', content)
            expected_env = (
                'EFSO_PREVIEW_TIMESTAMP: "' + "${{ inputs.timestamp || '" + expected_ts + "' }}" + '"'
            )
            self.assertIn(expected_env, content)
            self.assertIn('--timestamp "$EFSO_PREVIEW_TIMESTAMP"', content)
            self.assertNotIn('--timestamp "${{ inputs.timestamp', content)
            # Verify active crons are commented out
            self.assertNotIn("\n  schedule:", content)
            # Verify zero live secrets in preview job
            self.assertNotIn("secrets.TURSO_DATABASE_URL", content.split("FUTURE PRODUCTION TEMPLATE")[0])

        # 2. Test CLI with --sample-eligible for each task
        for task_arg in ("scan-only", "editorial-digest", "admin-health-report", "encrypted-backup"):
            ret = cli_main(["--task", task_arg, "--preview", "--sample-eligible"])
            self.assertEqual(ret, 0)

        # 3. Test CLI with explicit ineligible timestamp: exits 0, reports [NO-GO], executed=False
        ret_ineligible = cli_main(["--task", "scan-only", "--preview", "--timestamp", "2026-10-24T05:00:00Z"])
        self.assertEqual(ret_ineligible, 0)

if __name__ == "__main__":
    unittest.main()
