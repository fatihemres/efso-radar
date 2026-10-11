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

    telegram_chat_id: str = Field(default="")

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
    max_quiet_hours_hold_age_hours: int = Field(default=8, ge=1, le=48)


    # API Budgets and Safety
    max_output_tokens: int = Field(default=3200, ge=500, le=8192)
    max_estimated_run_cost_usd: float = Field(default=0.05, ge=0.001)
    enable_search_grounding: bool = Field(default=False)  # Disabled to prevent extra costs
    http_timeout_seconds: float = Field(default=18.0, ge=2.0, le=120.0)

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

        return cls(
            gemini_api_key=os.getenv("GEMINI_API_KEY", ""),
            gemini_model=os.getenv("GEMINI_MODEL", "gemini-3.5-flash-lite"),
            telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),

            telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
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
            max_quiet_hours_hold_age_hours=int(os.getenv("MAX_QUIET_HOURS_HOLD_AGE_HOURS", "8")),
            max_output_tokens=int(os.getenv("MAX_OUTPUT_TOKENS", "3200")),
            enable_search_grounding=os.getenv("ENABLE_SEARCH_GROUNDING", "false").lower() in ("true", "1"),
            http_timeout_seconds=float(os.getenv("HTTP_TIMEOUT_SECONDS", "18.0")),
        )

