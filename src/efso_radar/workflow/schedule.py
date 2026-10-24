"""Scheduling contract, timezone conversions, and window eligibility rules for Efso Radar.

Defines the offline scheduling architecture for future unattended operations:
- Europe/Istanbul (UTC+3) and UTC timezone-aware conversions.
- Operating schedule slots for:
    1. SCAN_ONLY (RSS discovery): 00:17, 03:17, 06:17, 09:17, 12:17, 15:17, 18:17, 21:17 Istanbul
    2. EDITORIAL_DIGEST: 09:30, 15:30, 21:30 Istanbul
    3. ADMIN_HEALTH_REPORT: 22:00 Istanbul (maximum 1 per Istanbul calendar day)
    4. ENCRYPTED_BACKUP: 02:15 Istanbul (DESIGN ONLY)
- Quiet hours enforcement (00:00-08:30 Istanbul) preventing routine editorial dispatches.
- Deterministic handling of early, late, duplicate, and out-of-schedule triggers.
- Fail-closed behavior on naive or unparseable timestamps.
- Zero automatic catch-up or replay of missed dispatches.
- Strict bounded tolerance enforcement (1 to 120 minutes) preventing arbitrary wide windows.
"""
from __future__ import annotations

import logging
from datetime import datetime, time, timedelta, timezone
from enum import Enum
from typing import Any, NamedTuple, Optional
from pydantic import BaseModel, Field

from efso_radar.repositories.base import StateRepository
from efso_radar.config import Settings

logger = logging.getLogger("efso-radar.workflow.schedule")

# Turkey abolished DST in 2016; Europe/Istanbul is permanent UTC+3.
ISTANBUL_TZ = timezone(timedelta(hours=3), name="Europe/Istanbul")
UTC = timezone.utc

# Quiet hours: 00:00 - 08:30 Europe/Istanbul (21:00 - 05:30 UTC)
QUIET_HOURS_START_TIME = time(0, 0)
QUIET_HOURS_END_TIME = time(8, 30)

# Window tolerance constraints
MIN_TOLERANCE_MINUTES: int = 1
MAX_TOLERANCE_MINUTES: int = 120
DEFAULT_TOLERANCE_MINUTES: int = 45


class WorkflowTaskType(str, Enum):
    """Enumeration of independent workflow tasks in Efso Radar."""
    SCAN_ONLY = "SCAN_ONLY"
    EDITORIAL_DIGEST = "EDITORIAL_DIGEST"
    ADMIN_HEALTH_REPORT = "ADMIN_HEALTH_REPORT"
    ENCRYPTED_BACKUP = "ENCRYPTED_BACKUP"


class TaskEligibilityStatus(str, Enum):
    """Result status for task window eligibility evaluation."""
    ELIGIBLE = "ELIGIBLE"
    EARLY_TRIGGER = "EARLY_TRIGGER"
    WINDOW_EXPIRED = "WINDOW_EXPIRED"
    QUIET_HOURS_BLOCKED = "QUIET_HOURS_BLOCKED"
    ALREADY_DISPATCHED_TODAY = "ALREADY_DISPATCHED_TODAY"
    OUT_OF_SCHEDULE = "OUT_OF_SCHEDULE"
    INVALID_TIMESTAMP = "INVALID_TIMESTAMP"


class ScheduleSlotDefinition(NamedTuple):
    """Specification of an operational schedule slot."""
    task_type: WorkflowTaskType
    time_istanbul: str  # "HH:MM"
    time_utc: str       # "HH:MM"
    utc_day_offset: int # -1 if UTC is previous calendar day, 0 if same day
    description: str


# Authoritative Schedule Slots (expressed in Europe/Istanbul and UTC)
OPERATING_SCHEDULE_SLOTS: list[ScheduleSlotDefinition] = [
    # 1. RSS Discovery (8 runs per day: every 3 hours at :17 past the hour)
    ScheduleSlotDefinition(WorkflowTaskType.SCAN_ONLY, "00:17", "21:17", -1, "RSS discovery (midnight window)"),
    ScheduleSlotDefinition(WorkflowTaskType.SCAN_ONLY, "03:17", "00:17", 0, "RSS discovery (night window)"),
    ScheduleSlotDefinition(WorkflowTaskType.SCAN_ONLY, "06:17", "03:17", 0, "RSS discovery (early morning window)"),
    ScheduleSlotDefinition(WorkflowTaskType.SCAN_ONLY, "09:17", "06:17", 0, "RSS discovery (morning window)"),
    ScheduleSlotDefinition(WorkflowTaskType.SCAN_ONLY, "12:17", "09:17", 0, "RSS discovery (midday window)"),
    ScheduleSlotDefinition(WorkflowTaskType.SCAN_ONLY, "15:17", "12:17", 0, "RSS discovery (afternoon window)"),
    ScheduleSlotDefinition(WorkflowTaskType.SCAN_ONLY, "18:17", "15:17", 0, "RSS discovery (evening window)"),
    ScheduleSlotDefinition(WorkflowTaskType.SCAN_ONLY, "21:17", "18:17", 0, "RSS discovery (night window)"),

    # 2. PRIMARY_EDITOR Consolidated Digest (3 runs per day outside quiet hours)
    ScheduleSlotDefinition(WorkflowTaskType.EDITORIAL_DIGEST, "09:30", "06:30", 0, "PRIMARY_EDITOR morning digest"),
    ScheduleSlotDefinition(WorkflowTaskType.EDITORIAL_DIGEST, "15:30", "12:30", 0, "PRIMARY_EDITOR afternoon digest"),
    ScheduleSlotDefinition(WorkflowTaskType.EDITORIAL_DIGEST, "21:30", "18:30", 0, "PRIMARY_EDITOR evening digest"),

    # 3. ADMIN Operational Health Report (maximum 1 per Istanbul calendar day)
    ScheduleSlotDefinition(WorkflowTaskType.ADMIN_HEALTH_REPORT, "22:00", "19:00", 0, "ADMIN nightly operational health report"),

    # 4. Encrypted Backup (Daily quiet-hours quiescent maintenance; DESIGN ONLY)
    ScheduleSlotDefinition(WorkflowTaskType.ENCRYPTED_BACKUP, "02:15", "23:15", -1, "Encrypted backup snapshot (DESIGN ONLY)"),
]


class TaskEligibilityResult(BaseModel):
    """Evaluation result for whether a workflow task is eligible to run at a given timestamp."""
    task_type: WorkflowTaskType
    is_eligible: bool
    status: TaskEligibilityStatus
    window_id: Optional[str] = None
    slot_time_istanbul: Optional[str] = None
    slot_time_utc: Optional[str] = None
    istanbul_date: str = Field(description="Calendar date in Europe/Istanbul (YYYY-MM-DD)")
    utc_timestamp_iso: str
    delay_minutes: float = 0.0
    reason: str


def ensure_timezone_aware(dt: datetime) -> datetime:
    """Validate that datetime is timezone-aware; fail closed on naive datetimes."""
    if not isinstance(dt, datetime):
        raise TypeError(f"Expected datetime object, got {type(dt).__name__}")
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        raise ValueError("Timestamp must be timezone-aware (naive datetimes are rejected fail-closed)")
    return dt


def utc_to_istanbul(dt: datetime) -> datetime:
    """Convert timezone-aware datetime to Europe/Istanbul (UTC+3)."""
    return ensure_timezone_aware(dt).astimezone(ISTANBUL_TZ)


def istanbul_to_utc(dt: datetime) -> datetime:
    """Convert timezone-aware datetime to UTC."""
    return ensure_timezone_aware(dt).astimezone(UTC)


def is_in_quiet_hours(dt: datetime) -> bool:
    """Check whether timestamp falls within Istanbul quiet hours (00:00 - 08:30).

    Routine editorial notifications must not be dispatched during quiet hours.
    """
    ist = utc_to_istanbul(dt)
    t = ist.time()
    return QUIET_HOURS_START_TIME <= t < QUIET_HOURS_END_TIME


def get_istanbul_date_str(dt: datetime) -> str:
    """Return Istanbul calendar date string (YYYY-MM-DD), correctly resolving UTC boundaries."""
    return utc_to_istanbul(dt).strftime("%Y-%m-%d")


def compute_window_identity(task_type: WorkflowTaskType, slot_ist_dt: datetime) -> str:
    """Compute deterministic, stable window identifier for deduplication and idempotency.

    Format:
    - ADMIN_HEALTH_REPORT: ADMIN_HEALTH_REPORT:YYYY-MM-DD (calendar day scope)
    - Others: {TASK_TYPE}:YYYY-MM-DDTHH:MM (scheduled slot scope)
    """
    slot_ist = utc_to_istanbul(slot_ist_dt)
    if task_type == WorkflowTaskType.ADMIN_HEALTH_REPORT:
        return f"{task_type.value}:{slot_ist.strftime('%Y-%m-%d')}"
    return f"{task_type.value}:{slot_ist.strftime('%Y-%m-%dT%H:%M')}"


def get_schedule_slots_for_task(task_type: WorkflowTaskType) -> list[ScheduleSlotDefinition]:
    """Retrieve all defined operational slots for a given task type."""
    return [slot for slot in OPERATING_SCHEDULE_SLOTS if slot.task_type == task_type]


def evaluate_task_eligibility(
    task_type: WorkflowTaskType,
    timestamp: datetime,
    tolerance_minutes: int = DEFAULT_TOLERANCE_MINUTES,
    repository: Optional[StateRepository] = None,
    settings: Optional[Settings] = None,
) -> TaskEligibilityResult:
    """Determine whether task is eligible to execute at the given timestamp.

    Rules:
    - Timestamp must be timezone-aware.
    - tolerance_minutes must be an integer between MIN_TOLERANCE_MINUTES (1) and MAX_TOLERANCE_MINUTES (120).
    - Early triggers (before scheduled slot time) fail closed as EARLY_TRIGGER with window_id=None.
    - Late triggers beyond tolerance_minutes fail closed as WINDOW_EXPIRED with window_id=None.
    - Skipped slots are NEVER retroactively caught up or replayed.
    - Quiet hours (00:00-08:30 Istanbul) block EDITORIAL_DIGEST routine dispatches with window_id=None.
    - For ADMIN_HEALTH_REPORT: Resolves the matching schedule slot FIRST, then checks
      delivery history using the matched slot's Istanbul calendar date (handling midnight
      crossings cleanly). Note: this repository check is a read-only pre-flight filter,
      NOT an atomic production delivery claim. If dispatched, returns ALREADY_DISPATCHED_TODAY
      with window_id=None.
    - For eligible runs only, a stable window_id is generated from slot time.
    """
    # 0. Bounded Tolerance Validation
    if not isinstance(tolerance_minutes, int) or isinstance(tolerance_minutes, bool):
        raise TypeError(f"tolerance_minutes must be an integer, got {type(tolerance_minutes).__name__}")
    if not (MIN_TOLERANCE_MINUTES <= tolerance_minutes <= MAX_TOLERANCE_MINUTES):
        raise ValueError(
            f"tolerance_minutes must be between {MIN_TOLERANCE_MINUTES} and {MAX_TOLERANCE_MINUTES} "
            f"(got {tolerance_minutes}). Bounded range prevents arbitrary tolerances from replaying missed slots."
        )

    now_ist = utc_to_istanbul(timestamp)
    now_utc = istanbul_to_utc(timestamp)
    current_date_str = now_ist.strftime("%Y-%m-%d")

    # 1. Quiet Hours Enforcement for Editorial Routine Dispatches
    if task_type == WorkflowTaskType.EDITORIAL_DIGEST:
        if is_in_quiet_hours(now_ist):
            return TaskEligibilityResult(
                task_type=task_type,
                is_eligible=False,
                status=TaskEligibilityStatus.QUIET_HOURS_BLOCKED,
                window_id=None,
                istanbul_date=current_date_str,
                utc_timestamp_iso=now_utc.isoformat(),
                reason="Routine editorial digest blocked: current time falls within quiet hours (00:00-08:30 Istanbul)",
            )

    slots = get_schedule_slots_for_task(task_type)
    if not slots:
        return TaskEligibilityResult(
            task_type=task_type,
            is_eligible=False,
            status=TaskEligibilityStatus.OUT_OF_SCHEDULE,
            window_id=None,
            istanbul_date=current_date_str,
            utc_timestamp_iso=now_utc.isoformat(),
            reason=f"No operational schedule defined for task type {task_type.value}",
        )

    # Evaluate possible slot instances across adjacent calendar dates (yesterday, today, tomorrow)
    # to handle late-night slots and UTC day-boundary offsets cleanly.
    current_date = now_ist.date()
    candidate_slots: list[tuple[datetime, ScheduleSlotDefinition]] = []
    for day_delta in (-1, 0, 1):
        target_date = current_date + timedelta(days=day_delta)
        for s in slots:
            h, m = map(int, s.time_istanbul.split(":"))
            slot_dt = datetime(target_date.year, target_date.month, target_date.day, h, m, tzinfo=ISTANBUL_TZ)
            candidate_slots.append((slot_dt, s))

    # Match active window: slot_dt <= now_ist <= slot_dt + tolerance
    active_matches = [
        (slot_dt, s) for slot_dt, s in candidate_slots
        if slot_dt <= now_ist <= (slot_dt + timedelta(minutes=tolerance_minutes))
    ]

    if active_matches:
        # Choose the active match (closest slot if multiple within tolerance)
        slot_dt, matched_slot = min(active_matches, key=lambda pair: abs((now_ist - pair[0]).total_seconds()))
        delay_min = (now_ist - slot_dt).total_seconds() / 60.0
        slot_date_str = slot_dt.strftime("%Y-%m-%d")

        # 2. Daily Admin Health Report At-Most-Once Pre-flight Eligibility Check
        # Evaluated against the resolved MATCHED SLOT's Istanbul calendar date, NOT the trigger date.
        # NOTE: repository.has_admin_report_been_dispatched() is a read-only pre-flight eligibility check,
        # NOT an atomic distributed delivery claim lock. Atomic claims require database-level constraints.
        if task_type == WorkflowTaskType.ADMIN_HEALTH_REPORT and repository is not None:
            try:
                if repository.has_admin_report_been_dispatched(slot_date_str):
                    return TaskEligibilityResult(
                        task_type=task_type,
                        is_eligible=False,
                        status=TaskEligibilityStatus.ALREADY_DISPATCHED_TODAY,
                        window_id=None,  # Ineligible run must NOT receive an active window identity
                        slot_time_istanbul=matched_slot.time_istanbul,
                        slot_time_utc=matched_slot.time_utc,
                        istanbul_date=slot_date_str,
                        utc_timestamp_iso=now_utc.isoformat(),
                        delay_minutes=round(delay_min, 2),
                        reason=f"Admin health report already dispatched for Istanbul calendar date {slot_date_str}",
                    )
            except Exception as exc:
                logger.warning("Failed checking admin report dispatch status: %s", exc)
                return TaskEligibilityResult(
                    task_type=task_type,
                    is_eligible=False,
                    status=TaskEligibilityStatus.OUT_OF_SCHEDULE,
                    window_id=None,
                    istanbul_date=slot_date_str,
                    utc_timestamp_iso=now_utc.isoformat(),
                    reason=f"Repository check failed closed: {type(exc).__name__}",
                )

        window_id = compute_window_identity(task_type, slot_dt)
        return TaskEligibilityResult(
            task_type=task_type,
            is_eligible=True,
            status=TaskEligibilityStatus.ELIGIBLE,
            window_id=window_id,
            slot_time_istanbul=matched_slot.time_istanbul,
            slot_time_utc=matched_slot.time_utc,
            istanbul_date=slot_date_str,
            utc_timestamp_iso=now_utc.isoformat(),
            delay_minutes=round(delay_min, 2),
            reason=f"Trigger matched slot {matched_slot.time_istanbul} Istanbul with {delay_min:.1f}m delay (within {tolerance_minutes}m tolerance)",
        )

    # If not in active window, find closest slot to report actionable status
    closest_slot_dt, closest_slot = min(candidate_slots, key=lambda pair: abs((now_ist - pair[0]).total_seconds()))
    diff_minutes = (now_ist - closest_slot_dt).total_seconds() / 60.0
    closest_date_str = closest_slot_dt.strftime("%Y-%m-%d")

    if diff_minutes < 0:
        # Triggered before slot
        early_by = abs(diff_minutes)
        return TaskEligibilityResult(
            task_type=task_type,
            is_eligible=False,
            status=TaskEligibilityStatus.EARLY_TRIGGER,
            window_id=None,  # Ineligible run must NOT receive an active window identity
            slot_time_istanbul=closest_slot.time_istanbul,
            slot_time_utc=closest_slot.time_utc,
            istanbul_date=closest_date_str,
            utc_timestamp_iso=now_utc.isoformat(),
            delay_minutes=round(diff_minutes, 2),
            reason=f"Early trigger: execution occurred {early_by:.1f}m before scheduled slot {closest_slot.time_istanbul} Istanbul",
        )
    else:
        # Triggered after slot plus tolerance
        late_by = diff_minutes
        return TaskEligibilityResult(
            task_type=task_type,
            is_eligible=False,
            status=TaskEligibilityStatus.WINDOW_EXPIRED,
            window_id=None,  # Ineligible run must NOT receive an active window identity
            slot_time_istanbul=closest_slot.time_istanbul,
            slot_time_utc=closest_slot.time_utc,
            istanbul_date=closest_date_str,
            utc_timestamp_iso=now_utc.isoformat(),
            delay_minutes=round(late_by, 2),
            reason=f"Window expired: execution occurred {late_by:.1f}m after scheduled slot {closest_slot.time_istanbul} Istanbul (exceeds {tolerance_minutes}m tolerance; no automatic replay)",
        )


def get_eligible_tasks_for_timestamp(
    timestamp: datetime,
    tolerance_minutes: int = DEFAULT_TOLERANCE_MINUTES,
    repository: Optional[StateRepository] = None,
    settings: Optional[Settings] = None,
) -> list[TaskEligibilityResult]:
    """Evaluate all workflow task types for eligibility at the given timestamp."""
    results: list[TaskEligibilityResult] = []
    for task_type in WorkflowTaskType:
        res = evaluate_task_eligibility(
            task_type=task_type,
            timestamp=timestamp,
            tolerance_minutes=tolerance_minutes,
            repository=repository,
            settings=settings,
        )
        results.append(res)
    return results
