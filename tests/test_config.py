"""Tests for Settings and quiet hours logic."""
import os
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from pydantic import ValidationError
from efso_radar.config import Settings


class TestConfig(unittest.TestCase):
    def test_default_settings(self):
        s = Settings()
        self.assertEqual(s.min_score, 82)
        self.assertEqual(s.max_per_run, 3)
        self.assertEqual(s.max_age_hours, 16)
        self.assertFalse(s.quiet_hours_enabled)

    def test_zero_spend_defaults_and_zero_is_valid(self):
        s = Settings()
        self.assertEqual(s.max_estimated_run_cost_usd, 0.0)
        self.assertEqual(s.max_monthly_ai_cost_usd, 0.0)
        self.assertEqual(s.max_shadow_eval_budget_usd, 0.0)
        self.assertFalse(s.ai_evaluation_enabled)
        self.assertEqual(Settings(max_estimated_run_cost_usd=0.0).max_estimated_run_cost_usd, 0.0)
        with self.assertRaises(ValidationError):
            Settings(max_monthly_ai_cost_usd=-0.01)

    def test_nonfinite_monetary_config_is_rejected(self):
        for budget in ("max_estimated_run_cost_usd", "max_monthly_ai_cost_usd", "max_shadow_eval_budget_usd"):
            for nonfinite in (float("nan"), float("inf"), float("-inf")):
                with self.subTest(budget=budget, value=str(nonfinite)):
                    with self.assertRaises(ValidationError):
                        Settings(**{budget: nonfinite})

    def test_from_env_refuses_positive_budget_without_touching_dotenv(self):
        with patch.object(Path, "exists", return_value=False):
            for key, old_value in (
                ("MAX_ESTIMATED_RUN_COST_USD", "0.05"),
                ("MAX_MONTHLY_AI_COST_USD", "1.20"),
                ("MAX_SHADOW_EVAL_BUDGET_USD", "0.05"),
            ):
                with self.subTest(legacy_budget=key):
                    with patch.dict(os.environ, {key: old_value, "AI_EVALUATION_ENABLED": "true"}, clear=True):
                        with self.assertRaisesRegex(ValueError, "Positive AI monetary budgets"):
                            Settings.from_env()
            with patch.dict(os.environ, {}, clear=True):
                s = Settings.from_env()
                self.assertEqual(s.max_monthly_ai_cost_usd, 0.0)

    def test_quiet_hours_cross_midnight(self):
        s = Settings(
            quiet_hours_enabled=True,
            quiet_hours_start_utc="21:00",
            quiet_hours_end_utc="05:30",
        )
        # 22:30 UTC -> inside quiet hours
        t1 = datetime(2026, 10, 11, 22, 30, tzinfo=timezone.utc)
        self.assertTrue(s.is_in_quiet_hours(t1))

        # 02:00 UTC -> inside quiet hours
        t2 = datetime(2026, 10, 11, 2, 0, tzinfo=timezone.utc)
        self.assertTrue(s.is_in_quiet_hours(t2))

        # 14:00 UTC -> daytime, outside quiet hours
        t3 = datetime(2026, 10, 11, 14, 0, tzinfo=timezone.utc)
        self.assertFalse(s.is_in_quiet_hours(t3))

    def test_quiet_hours_same_day(self):
        s = Settings(
            quiet_hours_enabled=True,
            quiet_hours_start_utc="01:00",
            quiet_hours_end_utc="07:00",
        )
        t_inside = datetime(2026, 10, 11, 4, 0, tzinfo=timezone.utc)
        t_outside = datetime(2026, 10, 11, 8, 0, tzinfo=timezone.utc)
        self.assertTrue(s.is_in_quiet_hours(t_inside))
        self.assertFalse(s.is_in_quiet_hours(t_outside))

    def test_quiet_hours_disabled(self):
        s = Settings(quiet_hours_enabled=False)
        t = datetime(2026, 10, 11, 23, 0, tzinfo=timezone.utc)
        self.assertFalse(s.is_in_quiet_hours(t))


if __name__ == "__main__":
    unittest.main()
