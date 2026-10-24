"""Command-line interface for offline orchestration preview and schedule verification.

Supports safe offline inspection without making network requests or touching production state:
- View operating schedule and UTC/Istanbul time conversions.
- Check window eligibility for explicit injected UTC timestamps.
- Run hermetic offline previews for SCAN_ONLY, EDITORIAL_DIGEST, ADMIN_HEALTH_REPORT, and ENCRYPTED_BACKUP.
- Fail closed if live execution is attempted.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys

# Ensure UTF-8 output on Windows console (avoids cp1254 emoji charmap errors)
if sys.platform == "win32":
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from datetime import datetime, timezone
from typing import Optional

from efso_radar.config import Settings
from efso_radar.workflow.contracts import (
    AdminHealthReportTaskContract,
    EditorialDigestTaskContract,
    EncryptedBackupTaskContract,
    ScanOnlyTaskContract,
    TaskExecutionMode,
)
from efso_radar.workflow.schedule import (
    OPERATING_SCHEDULE_SLOTS,
    WorkflowTaskType,
    ensure_timezone_aware,
    evaluate_task_eligibility,
    get_eligible_tasks_for_timestamp,
    utc_to_istanbul,
)

logger = logging.getLogger("efso-radar.workflow.cli")


REPRESENTATIVE_ELIGIBLE_TIMESTAMPS: dict[WorkflowTaskType, str] = {
    WorkflowTaskType.SCAN_ONLY: "2026-10-24T06:17:00Z",
    WorkflowTaskType.EDITORIAL_DIGEST: "2026-10-24T06:30:00Z",
    WorkflowTaskType.ADMIN_HEALTH_REPORT: "2026-10-24T19:00:00Z",
    WorkflowTaskType.ENCRYPTED_BACKUP: "2026-10-23T23:15:00Z",
}

TASK_MAP = {
    "scan-only": WorkflowTaskType.SCAN_ONLY,
    "editorial-digest": WorkflowTaskType.EDITORIAL_DIGEST,
    "admin-health-report": WorkflowTaskType.ADMIN_HEALTH_REPORT,
    "encrypted-backup": WorkflowTaskType.ENCRYPTED_BACKUP,
}

CONTRACT_MAP = {
    WorkflowTaskType.SCAN_ONLY: ScanOnlyTaskContract,
    WorkflowTaskType.EDITORIAL_DIGEST: EditorialDigestTaskContract,
    WorkflowTaskType.ADMIN_HEALTH_REPORT: AdminHealthReportTaskContract,
    WorkflowTaskType.ENCRYPTED_BACKUP: EncryptedBackupTaskContract,
}


def parse_timestamp(ts_str: Optional[str]) -> datetime:
    """Parse timestamp string and ensure timezone-awareness."""
    if not ts_str:
        return datetime.now(timezone.utc)
    ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    return ensure_timezone_aware(ts)


def print_schedule_table() -> None:
    """Print the authoritative operational schedule matrix with UTC and quiet-hours mappings."""
    print("\n" + "=" * 90)
    print("EFSO RADAR — OPERATIONAL SCHEDULE MATRIX (FUTURE-READY / CURRENTLY DISABLED)")
    print("=" * 90)
    print(f"{'Task Type':<22} | {'Istanbul (UTC+3)':<16} | {'UTC Schedule':<14} | {'Target Recipient':<16} | {'Quiet Hours'}")
    print("-" * 90)
    for slot in OPERATING_SCHEDULE_SLOTS:
        recipient = (
            "PRIMARY_EDITOR" if slot.task_type == WorkflowTaskType.EDITORIAL_DIGEST
            else ("ADMIN" if slot.task_type == WorkflowTaskType.ADMIN_HEALTH_REPORT
                  else ("Archive Asset" if slot.task_type == WorkflowTaskType.ENCRYPTED_BACKUP else "Internal"))
        )
        h, m = map(int, slot.time_istanbul.split(":"))
        is_quiet = (0 <= h < 8) or (h == 8 and m < 30)
        quiet_str = "BLOCKED" if (is_quiet and slot.task_type == WorkflowTaskType.EDITORIAL_DIGEST) else ("QUIET_HOURS" if is_quiet else "ACTIVE")
        day_note = " (prev day)" if slot.utc_day_offset == -1 else ""
        print(f"{slot.task_type.value:<22} | {slot.time_istanbul:<16} | {slot.time_utc + day_note:<14} | {recipient:<16} | {quiet_str}")
    print("=" * 90)
    print("Quiet Hours: 00:00 - 08:30 Europe/Istanbul (21:00 - 05:30 UTC)")
    print("Recipients Policy: PRIMARY_EDITOR receives routine digests; ADMIN receives daily health report.")
    print("All live automation and GitHub Actions schedules are currently strictly DISABLED.")
    print("=" * 90 + "\n")


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Efso Radar — Offline Workflow Orchestration & Automation Readiness CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--task",
        choices=["scan-only", "editorial-digest", "admin-health-report", "encrypted-backup", "all"],
        default="all",
        help="Specific workflow task to preview (default: all)",
    )
    parser.add_argument(
        "--timestamp",
        type=str,
        default=None,
        help="Injected ISO-8601 UTC timestamp (e.g. 2026-10-24T06:30:00Z; defaults to current UTC time)",
    )
    parser.add_argument(
        "--sample-eligible",
        action="store_true",
        help="Use deterministic representative eligible slot timestamp for the task (ensures preview exercises synthetic logic)",
    )
    parser.add_argument(
        "--tolerance-minutes",
        type=int,
        default=45,
        help="Execution window tolerance in minutes (default: 45)",
    )
    parser.add_argument(
        "--verify-schedule",
        action="store_true",
        help="Display the schedule conversion matrix and check eligibility for the timestamp",
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        default=True,
        help="Run hermetic offline preview with synthetic data (default: True)",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Execute in live production mode (PROHIBITED in Phase 5B.3D)",
    )

    args = parser.parse_args(argv)

    if args.live:
        print(
            "ERROR: Live production execution is strictly NOT IMPLEMENTED and prohibited in Phase 5B.3D.\n"
            "All real API requests, external Telegram notifications, live Turso writes, and cloud schedules "
            "remain disabled pending explicit Product Owner approval.",
            file=sys.stderr,
        )
        return 1

    if not (1 <= args.tolerance_minutes <= 120):
        print(
            f"ERROR: --tolerance-minutes must be between 1 and 120 (got {args.tolerance_minutes}). "
            "Arbitrary wide tolerances are prohibited to prevent replaying missed slots.",
            file=sys.stderr,
        )
        return 1

    if args.sample_eligible and not args.timestamp:
        if args.task != "all":
            args.timestamp = REPRESENTATIVE_ELIGIBLE_TIMESTAMPS[TASK_MAP[args.task]]
        else:
            args.timestamp = "2026-10-24T06:17:00Z"

    try:
        ref_dt = parse_timestamp(args.timestamp)
    except Exception as exc:
        print(f"ERROR: Invalid timestamp format: {exc}", file=sys.stderr)
        return 1

    ist_dt = utc_to_istanbul(ref_dt)
    print(f"\n[EFSO RADAR OFFLINE ORCHESTRATION PREVIEW]")
    print(f"Evaluating timestamp: {ref_dt.isoformat()} (UTC)")
    print(f"Europe/Istanbul time: {ist_dt.strftime('%Y-%m-%d %H:%M:%S %Z')}")

    if args.verify_schedule:
        print_schedule_table()

    settings = Settings()
    tasks_to_run = (
        list(TASK_MAP.values())
        if args.task == "all"
        else [TASK_MAP[args.task]]
    )

    print(f"\nEvaluating eligibility for {len(tasks_to_run)} task(s) with {args.tolerance_minutes}m tolerance:")
    for task_type in tasks_to_run:
        contract_cls = CONTRACT_MAP[task_type]
        contract = contract_cls(settings=settings)
        result = contract.execute(
            mode=TaskExecutionMode.OFFLINE_PREVIEW,
            timestamp=ref_dt,
            tolerance_minutes=args.tolerance_minutes,
        )

        status_icon = "[OK]" if result.status == "SUCCESS" else "[NO-GO]"
        window_display = result.window_id if result.window_id else "None (Ineligible - No Active Window)"
        print(f"\n{status_icon} [{result.task_type.value}] Window ID: {window_display}")
        print(f"   Status: {result.status}")
        print(f"   Scheduled Slot (Istanbul): {result.scheduled_slot_istanbul}")
        print(f"   Message: {result.message}")
        print(f"   Network Requests: {result.network_calls_made} | Cost: ${result.cost_usd:.2f}")
        if result.recipients:
            rec_roles = [f"{r['role']} ({r['masked_chat_id']})" for r in result.recipients]
            print(f"   Recipients: {', '.join(rec_roles)}")
        else:
            print("   Recipients: None (Internal or Archive Asset)")
        print(f"   Summary: {json.dumps(result.summary, ensure_ascii=False)}")

    print("\n[PREVIEW COMPLETE] 0 network calls made; 0 operational databases modified; $0.00 spent.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
