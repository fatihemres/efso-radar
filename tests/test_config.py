"""Tests for Settings and quiet hours logic."""
import unittest
from datetime import datetime, timezone

from efso_radar.config import Settings


class TestConfig(unittest.TestCase):
    def test_default_settings(self):
        s = Settings()
        self.assertEqual(s.min_score, 82)
        self.assertEqual(s.max_per_run, 3)
        self.assertEqual(s.max_age_hours, 16)
        self.assertFalse(s.quiet_hours_enabled)

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
