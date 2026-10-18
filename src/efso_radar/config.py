"""Configuration management for Efso Radar.

Uses type-safe validation, supports environment variables, .env file,
and provides explicit defaults for budgets, quiet hours, and timeouts.
"""
from __future__ import annotations

import os
from datetime import datetime, time, timezone
from pathlib import Path
from typing import Literal
from pydantic import BaseModel, Field, field_validator


class Settings(BaseModel):
    """Central configuration for Efso Radar."""
    # API credentials
    gemini_api_key: str = Field(default="")
    gemini_model: str = Field(default="gemini-3.5-flash-lite")
    telegram_bot_token: str = Field(default="")
    telegram_chat_id: str = Field(default="")  # ADMIN / Technical Owner Chat ID
    telegram_primary_editor_chat_id: str = Field(default="")  # PRIMARY_EDITOR Chat ID (Product Owner's girlfriend)
    telegram_allowed_chat_ids: list[str] = Field(default_factory=list)
    telegram_delivery_enabled: bool = Field(default=False)
    telegram_max_retries: int = Field(default=3, ge=1, le=10)
    telegram_retry_backoff_base: float = Field(default=0.5, ge=0.0, le=10.0)
    instagram_publishing_enabled: bool = Field(default=False)  # Automated Instagram posting is strictly disabled

    # Editorial thresholds
    min_score: int = Field(default=82, ge=0, le=100)
    max_per_run: int = Field(default=3, ge=1, le=10)
    max_age_hours: int = Field(default=16, ge=1, le=72)
    max_candidates: int = Field(default=55, ge=5, le=150)

    # Persistence (SQLite or Turso Cloud)
    sqlite_db_path: Path = Field(default=Path("data/radar.db"))
    legacy_state_json_path: Path = Field(default=Path("data/state.json"))
    lock_timeout_minutes: int = Field(default=15, ge=1, le=60)
    turso_database_url: str = Field(default="")
    turso_auth_token: str = Field(default="")

    # 24/7 Monitoring & Quiet Hours (Telegram delivery control)
    monitoring_enabled_24_7: bool = Field(default=True)
    quiet_hours_enabled: bool = Field(default=False)
    quiet_hours_start_utc: str = Field(default="21:00")  # 21:00 UTC = 00:00 Turkey (UTC+3)
    quiet_hours_end_utc: str = Field(default="05:30")    # 05:30 UTC = 08:30 Turkey (UTC+3)
    quiet_hours_behavior: Literal["HOLD", "DISCARD", "ALLOW_BREAKING_95"] = Field(default="HOLD")
    max_quiet_hours_hold_age_hours: int = Field(default=12, ge=1, le=48)

    # Hybrid Telegram Notification Strategy (Configurable frequency & urgency tiering)
    notification_strategy: Literal["IMMEDIATE", "HYBRID", "DIGEST_ONLY"] = Field(default="HYBRID")
    scan_interval_minutes: int = Field(default=180, ge=5, le=720)
    breaking_alert_min_score: int = Field(default=92, ge=0, le=100)
    digest_min_score: int = Field(default=78, ge=0, le=100)
    digest_interval_minutes: int = Field(default=360, ge=30, le=1440)
    target_daily_telegram_messages: int = Field(default=6, ge=1, le=30)
    max_items_per_digest: int = Field(default=5, ge=1, le=15)
    cap_overflow_behavior: Literal["DEFER_TO_NEXT_DIGEST", "HOLD_IN_QUEUE"] = Field(
        default="DEFER_TO_NEXT_DIGEST"
    )

    # Low-Cost Architecture, API Budgets & Safety Circuit-Breakers
    # Live AI processing is disabled by default until separately authorized by the Product Owner
    ai_evaluation_enabled: bool = Field(default=False)
    min_discovery_priority_for_ai: float = Field(default=5.5, ge=0.0, le=20.0)
    ai_batch_size: int = Field(default=10, ge=1, le=60)
    max_output_tokens: int = Field(default=3200, ge=500, le=8192)
    max_estimated_run_cost_usd: float = Field(default=0.05, ge=0.001)
    max_monthly_ai_cost_usd: float = Field(default=1.20, ge=0.01)
    max_monthly_ai_calls: int = Field(default=300, ge=1)
    max_shadow_eval_budget_usd: float = Field(default=0.05, ge=0.001, le=0.50)
    usd_to_try_rate: float = Field(default=36.0, ge=1.0)
    enable_search_grounding: bool = Field(default=False)  # Disabled to prevent extra costs
    http_timeout_seconds: float = Field(default=18.0, ge=2.0, le=120.0)

    def effective_max_age_hours(self) -> int:
        """Ensure longer scan intervals or a missed run never invalidate story freshness."""
        interval_floor = int((self.scan_interval_minutes / 60.0) * 4) + 4
        return max(self.max_age_hours, interval_floor)

    def effective_backlog_retention_hours(self) -> int:
        """Ensure deferred backlog and digest items survive across multi-hour scan/digest cycles."""
        digest_floor = int((self.digest_interval_minutes / 60.0) * 3) + 6
        return max(self.effective_max_age_hours(), digest_floor)

    @field_validator("quiet_hours_start_utc", "quiet_hours_end_utc")
    @classmethod
    def validate_time_format(cls, v: str) -> str:
        parts = v.strip().split(":")
        if len(parts) != 2:
            raise ValueError("Time must be in HH:MM format (24-hour)")
        hour, minute = int(parts[0]), int(parts[1])
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError("Hour must be 0-23 and minute 0-59")
        return f"{hour:02d}:{minute:02d}"

    def is_in_quiet_hours(self, current_time: datetime | None = None) -> bool:
        """Check if given UTC datetime falls within configured quiet hours."""
        if not self.quiet_hours_enabled:
            return False

        now = (current_time or datetime.now(timezone.utc)).astimezone(timezone.utc)
        curr_t = now.time()

        start_h, start_m = map(int, self.quiet_hours_start_utc.split(":"))
        end_h, end_m = map(int, self.quiet_hours_end_utc.split(":"))
        start_t = time(start_h, start_m)
        end_t = time(end_h, end_m)

        if start_t <= end_t:
            return start_t <= curr_t <= end_t
        # Crosses midnight (e.g. 21:00 to 05:30)
        return curr_t >= start_t or curr_t <= end_t

    def is_chat_id_allowed(self, chat_id: str | int) -> bool:
        """Check if a given Telegram chat ID is explicitly allowlisted for editorial updates.

        Strictly rejects group and channel IDs starting with '-'.
        """
        target = str(chat_id).strip()
        if not target or target.startswith("-"):
            return False
        allowed = {str(cid).strip() for cid in self.telegram_allowed_chat_ids if str(cid).strip()}
        if allowed:
            return target in allowed
        if self.telegram_chat_id:
            return target == str(self.telegram_chat_id).strip()
        return False

    def get_masked_telegram_token(self) -> str:
        """Return safely masked Telegram token for logging/auditing without leaking secrets."""
        if not self.telegram_bot_token:
            return "<NOT_SET>"
        tok = self.telegram_bot_token.strip()
        if len(tok) < 10:
            return "***REDACTED***"
        return f"{tok[:6]}...{tok[-4:]}"

    def get_masked_chat_id(self, chat_id: Optional[str | int] = None) -> str:
        """Return safely masked chat ID for logging/auditing without leaking private IDs."""
        cid = str(chat_id or self.telegram_chat_id).strip()
        if not cid:
            return "<NOT_SET>"
        if len(cid) <= 4:
            return "***"
        return f"{cid[:2]}...{cid[-2:]}"

    def get_masked_admin_chat_id(self) -> str:
        """Return safely masked ADMIN chat ID."""
        return self.get_masked_chat_id(self.telegram_chat_id)

    def get_masked_primary_editor_chat_id(self) -> str:
        """Return safely masked PRIMARY_EDITOR chat ID."""
        return self.get_masked_chat_id(self.telegram_primary_editor_chat_id)

    def get_recipient_role(self, chat_id: str | int) -> Optional[str]:
        """Return the authorized role ('PRIMARY_EDITOR' or 'ADMIN') for a given chat ID."""
        target = str(chat_id).strip()
        if not target or not self.is_chat_id_allowed(target):
            return None
        if self.telegram_primary_editor_chat_id and target == str(self.telegram_primary_editor_chat_id).strip():
            return "PRIMARY_EDITOR"
        if self.telegram_chat_id and target == str(self.telegram_chat_id).strip():
            return "ADMIN"
        return "AUTHORIZED_RECIPIENT"

    def get_authorized_recipients(self) -> list[dict[str, str]]:
        """Return all validated, allowlisted private recipients with their roles.

        Both PRIMARY_EDITOR (Product Owner's girlfriend) and ADMIN (Product Owner)
        receive the complete editorial experience across all categories:
        - EDITORIAL_DIGEST
        - VERIFIED_BREAKING
        - SENSITIVE_EDITORIAL_REVIEW
        - HOLD_FOR_VERIFICATION
        """
        recipients: list[dict[str, str]] = []

        # 1. PRIMARY_EDITOR (Product Owner's girlfriend — primary intended editorial user)
        editor_id = str(self.telegram_primary_editor_chat_id).strip() if self.telegram_primary_editor_chat_id else ""
        if editor_id and self.is_chat_id_allowed(editor_id):
            recipients.append({
                "chat_id": editor_id,
                "role": "PRIMARY_EDITOR",
                "label": "Baş Editör / Primary Editor",
                "masked_chat_id": self.get_masked_chat_id(editor_id),
            })

        # 2. ADMIN (Product Owner — technical owner and administrator)
        admin_id = str(self.telegram_chat_id).strip() if self.telegram_chat_id else ""
        if admin_id and self.is_chat_id_allowed(admin_id):
            # Avoid duplicate if same chat ID was configured for both
            if not any(r["chat_id"] == admin_id for r in recipients):
                recipients.append({
                    "chat_id": admin_id,
                    "role": "ADMIN",
                    "label": "Yönetici / Admin",
                    "masked_chat_id": self.get_masked_chat_id(admin_id),
                })

        return recipients

    def get_recipients_for_category(self, category: Optional[Any] = None) -> list[dict[str, str]]:
        """Return authorized recipient records permitted to receive this notification category.

        Per Product Owner directive, both PRIMARY_EDITOR and ADMIN receive the complete
        editorial notification experience without restriction.
        """
        return self.get_authorized_recipients()

    @classmethod
    def from_env(cls) -> Settings:
        """Load settings from environment variables."""
        # Simple optional .env parser for standalone zero-dependency execution
        env_file = Path(".env")
        if env_file.exists():
            for line in env_file.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    k = k.strip()
                    v = v.strip().strip("'\"")
                    if k not in os.environ:
                        os.environ[k] = v

        raw_allowed = os.getenv("TELEGRAM_ALLOWED_CHAT_IDS", "")
        allowed_list = [cid.strip() for cid in raw_allowed.split(",") if cid.strip()]

        primary_editor_id = (
            os.getenv("TELEGRAM_PRIMARY_EDITOR_CHAT_ID", "")
            or os.getenv("TELEGRAM_SECONDARY_CHAT_ID", "")
        ).strip()

        if primary_editor_id and not primary_editor_id.startswith("-") and primary_editor_id not in allowed_list:
            allowed_list.append(primary_editor_id)

        return cls(
            gemini_api_key=os.getenv("GEMINI_API_KEY", ""),
            gemini_model=os.getenv("GEMINI_MODEL", "gemini-3.5-flash-lite"),
            telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
            telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
            telegram_primary_editor_chat_id=primary_editor_id,
            telegram_allowed_chat_ids=allowed_list,
            telegram_delivery_enabled=os.getenv("TELEGRAM_DELIVERY_ENABLED", "false").lower() in ("true", "1", "yes"),
            telegram_max_retries=int(os.getenv("TELEGRAM_MAX_RETRIES", "3")),
            telegram_retry_backoff_base=float(os.getenv("TELEGRAM_RETRY_BACKOFF_BASE", "0.5")),
            instagram_publishing_enabled=os.getenv("INSTAGRAM_PUBLISHING_ENABLED", "false").lower() in ("true", "1", "yes"),
            min_score=int(os.getenv("MIN_SCORE", "82")),
            max_per_run=int(os.getenv("MAX_PER_RUN", "3")),
            max_age_hours=int(os.getenv("MAX_AGE_HOURS", "16")),
            max_candidates=int(os.getenv("MAX_CANDIDATES", "55")),
            sqlite_db_path=Path(os.getenv("SQLITE_DB_PATH", "data/radar.db")),
            legacy_state_json_path=Path(os.getenv("STATE_PATH", "data/state.json")),
            turso_database_url=os.getenv("TURSO_DATABASE_URL", ""),
            turso_auth_token=os.getenv("TURSO_AUTH_TOKEN", ""),
            quiet_hours_enabled=os.getenv("QUIET_HOURS_ENABLED", "false").lower() in ("true", "1", "yes"),
            quiet_hours_start_utc=os.getenv("QUIET_HOURS_START_UTC", "21:00"),
            quiet_hours_end_utc=os.getenv("QUIET_HOURS_END_UTC", "05:30"),
            quiet_hours_behavior=os.getenv("QUIET_HOURS_BEHAVIOR", "HOLD"),  # type: ignore
            max_quiet_hours_hold_age_hours=int(os.getenv("MAX_QUIET_HOURS_HOLD_AGE_HOURS", "12")),
            notification_strategy=os.getenv("NOTIFICATION_STRATEGY", "HYBRID"),  # type: ignore
            scan_interval_minutes=int(os.getenv("SCAN_INTERVAL_MINUTES", "180")),
            breaking_alert_min_score=int(os.getenv("BREAKING_ALERT_MIN_SCORE", "92")),
            digest_min_score=int(os.getenv("DIGEST_MIN_SCORE", "78")),
            digest_interval_minutes=int(os.getenv("DIGEST_INTERVAL_MINUTES", "360")),
            target_daily_telegram_messages=int(os.getenv("TARGET_DAILY_TELEGRAM_MESSAGES", "6")),
            max_items_per_digest=int(os.getenv("MAX_ITEMS_PER_DIGEST", "5")),
            cap_overflow_behavior=os.getenv("CAP_OVERFLOW_BEHAVIOR", "DEFER_TO_NEXT_DIGEST"),  # type: ignore
            ai_evaluation_enabled=os.getenv("AI_EVALUATION_ENABLED", "false").lower() in ("true", "1", "yes"),
            min_discovery_priority_for_ai=float(os.getenv("MIN_DISCOVERY_PRIORITY_FOR_AI", "5.5")),
            ai_batch_size=int(os.getenv("AI_BATCH_SIZE", "10")),
            max_output_tokens=int(os.getenv("MAX_OUTPUT_TOKENS", "3200")),
            max_estimated_run_cost_usd=float(os.getenv("MAX_ESTIMATED_RUN_COST_USD", "0.05")),
            max_monthly_ai_cost_usd=float(os.getenv("MAX_MONTHLY_AI_COST_USD", "1.20")),
            max_monthly_ai_calls=int(os.getenv("MAX_MONTHLY_AI_CALLS", "300")),
            max_shadow_eval_budget_usd=float(os.getenv("MAX_SHADOW_EVAL_BUDGET_USD", "0.05")),
            usd_to_try_rate=float(os.getenv("USD_TO_TRY_RATE", "36.0")),
            enable_search_grounding=os.getenv("ENABLE_SEARCH_GROUNDING", "false").lower() in ("true", "1"),
            http_timeout_seconds=float(os.getenv("HTTP_TIMEOUT_SECONDS", "18.0")),
        )

