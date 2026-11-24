"""Tests for Settings, non-polluting from_env(), and quiet hours logic."""
import os
import tempfile
import unittest
from contextlib import nullcontext
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
        with (
            patch.object(Path, "exists", side_effect=AssertionError("Must not probe .env exists()")),
            patch.object(Path, "stat", side_effect=AssertionError("Must not probe .env stat()")),
            patch.object(Path, "read_text", side_effect=AssertionError("Must not read .env")),
        ):
            for key, old_value in (
                ("MAX_ESTIMATED_RUN_COST_USD", "0.05"),
                ("MAX_MONTHLY_AI_COST_USD", "1.20"),
                ("MAX_SHADOW_EVAL_BUDGET_USD", "0.05"),
            ):
                with self.subTest(legacy_budget=key):
                    with patch.dict(os.environ, {key: old_value, "AI_EVALUATION_ENABLED": "true"}, clear=True):
                        with self.assertRaisesRegex(ValueError, "Positive AI monetary budgets"):
                            Settings.from_env(load_dotenv=False)
            with patch.dict(os.environ, {}, clear=True):
                s = Settings.from_env(load_dotenv=False)
                self.assertEqual(s.max_monthly_ai_cost_usd, 0.0)

    def test_from_env_no_dotenv_modes_never_probe_or_read_cwd_dotenv(self):
        with tempfile.TemporaryDirectory(prefix="efso_t1c_cfg_nodotenv_") as td:
            tmp_root = Path(td)
            cwd_env = tmp_root / ".env"
            cwd_env.write_text(
                "MIN_SCORE=99\nGEMINI_API_KEY=cwd-sentinel-key\nTELEGRAM_CHAT_ID=777777777\n",
                encoding="utf-8",
            )
            custom_env = tmp_root / "custom.env"
            custom_env.write_text("MIN_SCORE=98\n", encoding="utf-8")

            orig_cwd = os.getcwd()
            try:
                os.chdir(tmp_root)
                with patch.dict(os.environ, {}, clear=True):
                    original_env = dict(os.environ)
                    with (
                        patch.object(Path, "exists", side_effect=AssertionError("Unexpected Path.exists()")),
                        patch.object(Path, "stat", side_effect=AssertionError("Unexpected Path.stat()")),
                        patch.object(Path, "read_text", side_effect=AssertionError("Unexpected Path.read_text()")),
                    ):
                        s1 = Settings.from_env(load_dotenv=False)
                        s2 = Settings.from_env(env_file=None)
                        s3 = Settings.from_env(env_file=custom_env, load_dotenv=False)
                        s4 = Settings.from_env(environ={"MIN_SCORE": "91"})

                    self.assertEqual(s1.min_score, 82)
                    self.assertEqual(s1.gemini_api_key, "")
                    self.assertEqual(s2.min_score, 82)
                    self.assertEqual(s3.min_score, 82)
                    self.assertEqual(s4.min_score, 91)
                    self.assertEqual(s4.gemini_api_key, "")
                    self.assertEqual(dict(os.environ), original_env)
            finally:
                os.chdir(orig_cwd)

    def test_from_env_explicit_env_file_loads_without_os_environ_leakage(self):
        with tempfile.TemporaryDirectory(prefix="efso_t1c_cfg_explicit_") as td:
            synth_env = Path(td) / "synthetic.env"
            synth_env.write_text(
                "\n".join([
                    "# Comment line",
                    "",
                    "malformed_line_without_equals",
                    "=ignored_empty_key",
                    "GEMINI_API_KEY='quoted=value=123'",
                    'TELEGRAM_BOT_TOKEN="999:SyntheticToken"',
                    "TELEGRAM_CHAT_ID=111222333",
                    "TELEGRAM_PRIMARY_EDITOR_CHAT_ID=444555666",
                    "TELEGRAM_ALLOWED_CHAT_IDS=111222333",
                    "MIN_SCORE=87",
                    "MAX_ESTIMATED_RUN_COST_USD=0.00",
                    "MAX_MONTHLY_AI_COST_USD=0.00",
                    "MAX_SHADOW_EVAL_BUDGET_USD=0.00",
                ]),
                encoding="utf-8",
            )
            with patch.dict(os.environ, {}, clear=True):
                original_env = dict(os.environ)
                s = Settings.from_env(env_file=synth_env)
                self.assertEqual(s.gemini_api_key, "quoted=value=123")
                self.assertEqual(s.telegram_bot_token, "999:SyntheticToken")
                self.assertEqual(s.min_score, 87)
                self.assertEqual(s.telegram_allowed_chat_ids, ["111222333", "444555666"])
                self.assertFalse(s.ai_evaluation_enabled)
                self.assertFalse(s.telegram_delivery_enabled)
                self.assertFalse(s.instagram_publishing_enabled)
                self.assertFalse(s.enable_search_grounding)
                self.assertEqual(dict(os.environ), original_env)

    def test_from_env_default_zero_arg_preserves_os_precedence_without_leakage(self):
        with tempfile.TemporaryDirectory(prefix="efso_t1c_cfg_precedence_") as td:
            tmp_root = Path(td)
            (tmp_root / ".env").write_text(
                "\n".join([
                    "MIN_SCORE=75",
                    "MAX_PER_RUN=5",
                    "GEMINI_API_KEY=dotenv-only-key",
                    "TURSO_DATABASE_URL=libsql://dotenv-only.invalid",
                ]),
                encoding="utf-8",
            )
            orig_cwd = os.getcwd()
            try:
                os.chdir(tmp_root)
                seeded_env = {
                    "MIN_SCORE": "95",
                    "TURSO_DATABASE_URL": "",
                }
                with patch.dict(os.environ, seeded_env, clear=True):
                    original_env = dict(os.environ)
                    s = Settings.from_env()
                    self.assertEqual(s.min_score, 95)
                    self.assertEqual(s.max_per_run, 5)
                    self.assertEqual(s.gemini_api_key, "dotenv-only-key")
                    self.assertEqual(s.turso_database_url, "")
                    self.assertNotIn("MAX_PER_RUN", os.environ)
                    self.assertNotIn("GEMINI_API_KEY", os.environ)
                    self.assertEqual(dict(os.environ), original_env)
            finally:
                os.chdir(orig_cwd)

    def test_from_env_explicit_environ_and_env_file_precedence_ignores_process_env(self):
        with tempfile.TemporaryDirectory(prefix="efso_t1c_cfg_custom_env_") as td:
            synth_env = Path(td) / "test.env"
            synth_env.write_text(
                "\n".join([
                    "MIN_SCORE=77",
                    "MAX_CANDIDATES=40",
                    "GEMINI_API_KEY=from-dotenv",
                ]),
                encoding="utf-8",
            )
            with patch.dict(os.environ, {"PROCESS_LEAK_SENTINEL": "must-ignore", "MIN_SCORE": "12"}, clear=True):
                original_env = dict(os.environ)
                s = Settings.from_env(
                    env_file=synth_env,
                    environ={"MIN_SCORE": "90", "GEMINI_API_KEY": ""},
                )
                self.assertEqual(s.min_score, 90)
                self.assertEqual(s.max_candidates, 40)
                self.assertEqual(s.gemini_api_key, "")
                self.assertEqual(dict(os.environ), original_env)

    def test_from_env_rejects_positive_negative_and_nonfinite_budgets_from_dotenv_and_environ(self):
        with tempfile.TemporaryDirectory(prefix="efso_t1c_cfg_budget_") as td:
            with patch.dict(os.environ, {"UNCHANGED_KEY": "keep"}, clear=True):
                original_env = dict(os.environ)

                for key in ("MAX_ESTIMATED_RUN_COST_USD", "MAX_MONTHLY_AI_COST_USD", "MAX_SHADOW_EVAL_BUDGET_USD"):
                    env_path = Path(td) / f"{key.lower()}.env"
                    env_path.write_text(f"{key}=0.05\nAI_EVALUATION_ENABLED=true\n", encoding="utf-8")
                    with self.subTest(source="dotenv", key=key):
                        with self.assertRaisesRegex(ValueError, "Positive AI monetary budgets"):
                            Settings.from_env(env_file=env_path, environ={})
                    with self.subTest(source="environ", key=key):
                        with self.assertRaisesRegex(ValueError, "Positive AI monetary budgets"):
                            Settings.from_env(environ={key: "0.05"})

                for key in ("MAX_ESTIMATED_RUN_COST_USD", "MAX_MONTHLY_AI_COST_USD", "MAX_SHADOW_EVAL_BUDGET_USD"):
                    for bad in ("-0.01", "nan", "inf", "-inf"):
                        with self.subTest(key=key, value=bad):
                            with self.assertRaises(ValidationError):
                                Settings.from_env(environ={key: bad})

                self.assertEqual(dict(os.environ), original_env)

    def test_from_env_preserves_turso_fail_closed_validation(self):
        with tempfile.TemporaryDirectory(prefix="efso_t1c_cfg_turso_") as td:
            env_partial = Path(td) / "partial_turso.env"
            env_partial.write_text(
                "EFSO_STORAGE_MODE=turso\nTURSO_DATABASE_URL=libsql://synthetic-turso.example.invalid\n",
                encoding="utf-8",
            )
            env_full = Path(td) / "full_turso.env"
            env_full.write_text(
                "EFSO_STORAGE_MODE=turso\n"
                "TURSO_DATABASE_URL=libsql://synthetic-turso.example.invalid\n"
                "TURSO_AUTH_TOKEN=synthetic-token-value\n",
                encoding="utf-8",
            )

            with patch.dict(os.environ, {}, clear=True):
                original_env = dict(os.environ)

                with self.assertRaisesRegex(ValueError, "Cloud storage mode enabled"):
                    Settings.from_env(environ={"EFSO_STORAGE_MODE": "turso"})

                with self.assertRaisesRegex(ValueError, "Partial Turso configuration detected"):
                    Settings.from_env(environ={"TURSO_DATABASE_URL": "libsql://synthetic.example.invalid"})

                with self.assertRaisesRegex(ValueError, "Cloud storage mode enabled"):
                    Settings.from_env(env_file=env_partial, environ={})

                # Empty OS/environ override must still mask dotenv URL and fail closed
                with self.assertRaisesRegex(ValueError, "Cloud storage mode enabled"):
                    Settings.from_env(env_file=env_full, environ={"TURSO_DATABASE_URL": ""})

                with self.assertRaisesRegex(ValueError, "Partial Turso configuration detected"):
                    Settings.from_env(
                        env_file=env_full,
                        environ={"EFSO_STORAGE_MODE": "auto", "TURSO_DATABASE_URL": ""},
                    )

                s = Settings.from_env(env_file=env_full, environ={})
                self.assertEqual(s.storage_mode, "turso")
                self.assertEqual(s.turso_database_url, "libsql://synthetic-turso.example.invalid")
                self.assertEqual(s.turso_auth_token, "synthetic-token-value")
                self.assertEqual(dict(os.environ), original_env)

    def test_from_env_rejects_malformed_arguments_and_invalid_values(self):
        with tempfile.TemporaryDirectory(prefix="efso_t1c_cfg_badargs_") as td:
            dir_as_env = Path(td) / "dir_env"
            dir_as_env.mkdir()

            with patch.dict(os.environ, {"KEEP_ME": "1"}, clear=True):
                original_env = dict(os.environ)

                with self.assertRaises(TypeError):
                    Settings.from_env(load_dotenv="false")  # type: ignore[arg-type]
                with self.assertRaises(TypeError):
                    Settings.from_env(load_dotenv=1)  # type: ignore[arg-type]
                with self.assertRaises(ValueError):
                    Settings.from_env(env_file=None, load_dotenv=True)
                with self.assertRaises(ValueError):
                    Settings.from_env(env_file="   ")
                with self.assertRaises(TypeError):
                    Settings.from_env(env_file=123)  # type: ignore[arg-type]
                with self.assertRaisesRegex(ValueError, "not a regular file"):
                    Settings.from_env(env_file=dir_as_env, environ={})
                with self.assertRaises(TypeError):
                    Settings.from_env(environ=["MIN_SCORE=90"])  # type: ignore[arg-type]
                with self.assertRaises(TypeError):
                    Settings.from_env(environ={"MIN_SCORE": 90})  # type: ignore[dict-item]
                with self.assertRaises(ValueError):
                    Settings.from_env(environ={"MIN_SCORE": "not-an-int"})
                with self.assertRaises(ValidationError):
                    Settings.from_env(environ={"QUIET_HOURS_START_UTC": "25:99"})
                with self.assertRaises(ValidationError):
                    Settings.from_env(environ={"EFSO_STORAGE_MODE": "postgres"})

                self.assertEqual(dict(os.environ), original_env)

    def test_from_env_dotenv_duplicate_keys_preserve_first_occurrence_precedence(self):
        with tempfile.TemporaryDirectory(prefix="efso_t1c_cfg_dupkeys_") as td:
            tmp_path = Path(td)
            ordinary_env = tmp_path / "ordinary_dups.env"
            ordinary_env.write_text(
                "\n".join([
                    "MIN_SCORE=77",
                    "MIN_SCORE=91",
                    "GEMINI_MODEL=first-synth",
                    "GEMINI_MODEL=second-synth",
                ]),
                encoding="utf-8",
            )

            with patch.dict(os.environ, {"PREEXISTING": "keep"}, clear=True):
                original_env = dict(os.environ)

                s_ordinary = Settings.from_env(env_file=ordinary_env, environ={})
                self.assertEqual(s_ordinary.min_score, 77)
                self.assertEqual(s_ordinary.gemini_model, "first-synth")

                for key in (
                    "MAX_ESTIMATED_RUN_COST_USD",
                    "MAX_MONTHLY_AI_COST_USD",
                    "MAX_SHADOW_EVAL_BUDGET_USD",
                ):
                    # Order A: positive first, zero second -> must fail closed (first wins)
                    pos_first_env = tmp_path / f"{key.lower()}_pos_first.env"
                    pos_first_env.write_text(
                        f"{key}=0.05\n{key}=0.00\n",
                        encoding="utf-8",
                    )
                    with self.subTest(order="positive_first", key=key):
                        with self.assertRaisesRegex(ValueError, "Positive AI monetary budgets"):
                            Settings.from_env(env_file=pos_first_env)
                        with self.assertRaisesRegex(ValueError, "Positive AI monetary budgets"):
                            Settings.from_env(env_file=pos_first_env, environ={})

                    # Order B: zero first, positive second -> legacy first-wins accepts zero
                    zero_first_env = tmp_path / f"{key.lower()}_zero_first.env"
                    zero_first_env.write_text(
                        f"{key}=0.00\n{key}=0.05\n",
                        encoding="utf-8",
                    )
                    with self.subTest(order="zero_first", key=key):
                        s_zero = Settings.from_env(env_file=zero_first_env, environ={})
                        self.assertEqual(s_zero.max_monthly_ai_cost_usd, 0.0)
                        self.assertEqual(s_zero.max_estimated_run_cost_usd, 0.0)
                        self.assertEqual(s_zero.max_shadow_eval_budget_usd, 0.0)

                    # Explicit OS and injected overrides win over positive-first dotenv
                    s_override_env = Settings.from_env(env_file=pos_first_env, environ={key: "0.00"})
                    self.assertEqual(s_override_env.max_monthly_ai_cost_usd, 0.0)
                    with patch.dict(os.environ, {key: "0.00"}, clear=True):
                        s_override_os = Settings.from_env(env_file=pos_first_env)
                        self.assertEqual(s_override_os.max_monthly_ai_cost_usd, 0.0)

                # Empty-string OS and supplied overrides override duplicate dotenv values
                turso_dup_env = tmp_path / "turso_dup.env"
                turso_dup_env.write_text(
                    "\n".join([
                        "TURSO_DATABASE_URL=libsql://first.example.invalid",
                        "TURSO_DATABASE_URL=libsql://second.example.invalid",
                        "TURSO_AUTH_TOKEN=synth-token",
                    ]),
                    encoding="utf-8",
                )
                with self.assertRaisesRegex(ValueError, "Partial Turso configuration detected"):
                    Settings.from_env(env_file=turso_dup_env, environ={"TURSO_DATABASE_URL": ""})
                with patch.dict(os.environ, {"TURSO_DATABASE_URL": ""}, clear=True):
                    with self.assertRaisesRegex(ValueError, "Partial Turso configuration detected"):
                        Settings.from_env(env_file=turso_dup_env)

                self.assertEqual(dict(os.environ), original_env)

    def test_from_env_windows_case_insensitive_normalization_semantics(self):
        contexts = [patch("efso_radar.config._is_case_insensitive_env", return_value=True)]
        if os.name == "nt":
            contexts.append(nullcontext())

        with tempfile.TemporaryDirectory(prefix="efso_t1c_cfg_win_") as td:
            tmp_path = Path(td)
            lower_env = tmp_path / "lower.env"
            lower_env.write_text(
                "\n".join([
                    "min_score=79",
                    "MIN_SCORE=93",
                    "Gemini_Model=gemini-synth-win",
                    "ai_evaluation_enabled=false",
                    "telegram_chat_id=123456789",
                ]),
                encoding="utf-8",
            )
            turso_partial_lower = tmp_path / "turso_partial_lower.env"
            turso_partial_lower.write_text(
                "turso_database_url=libsql://synthetic-win.example.invalid\n",
                encoding="utf-8",
            )
            turso_mode_lower = tmp_path / "turso_mode_lower.env"
            turso_mode_lower.write_text(
                "efso_storage_mode=turso\n",
                encoding="utf-8",
            )
            turso_full_lower = tmp_path / "turso_full_lower.env"
            turso_full_lower.write_text(
                "turso_database_url=libsql://synthetic-win.example.invalid\n"
                "turso_auth_token=synthetic-win-token\n",
                encoding="utf-8",
            )
            upper_env = tmp_path / "upper.env"
            upper_env.write_text(
                "MIN_SCORE=70\nTURSO_DATABASE_URL=libsql://upper.example.invalid\nTURSO_AUTH_TOKEN=upper-tok\n",
                encoding="utf-8",
            )

            for ctx in contexts:
                with ctx:
                    with patch.dict(os.environ, {}, clear=True):
                        original_env = dict(os.environ)

                        # Lowercase/mixed-case dotenv keys and case-insensitive first-wins duplicate
                        s = Settings.from_env(env_file=lower_env)
                        self.assertEqual(s.min_score, 79)
                        self.assertEqual(s.gemini_model, "gemini-synth-win")
                        self.assertFalse(s.ai_evaluation_enabled)
                        self.assertEqual(s.telegram_chat_id, "123456789")

                        # Lowercase / mixed-case positive budgets rejected
                        for idx, bad_budget in enumerate((
                            "max_monthly_ai_cost_usd=0.05\n",
                            "Max_Estimated_Run_Cost_Usd=0.01\n",
                            "max_shadow_eval_budget_usd=0.02\n",
                        )):
                            bad_file = tmp_path / f"bad_{idx}.env"
                            bad_file.write_text(bad_budget, encoding="utf-8")
                            with self.assertRaisesRegex(ValueError, "Positive AI monetary budgets"):
                                Settings.from_env(env_file=bad_file)

                        # Case-variant duplicates in both orders
                        var_pos_first = tmp_path / "var_pos_first.env"
                        var_pos_first.write_text(
                            "max_monthly_ai_cost_usd=0.05\nMAX_MONTHLY_AI_COST_USD=0.00\n",
                            encoding="utf-8",
                        )
                        with self.assertRaisesRegex(ValueError, "Positive AI monetary budgets"):
                            Settings.from_env(env_file=var_pos_first)

                        var_zero_first = tmp_path / "var_zero_first.env"
                        var_zero_first.write_text(
                            "Max_Monthly_Ai_Cost_Usd=0.00\nmax_monthly_ai_cost_usd=0.05\n",
                            encoding="utf-8",
                        )
                        s_var_zero = Settings.from_env(env_file=var_zero_first)
                        self.assertEqual(s_var_zero.max_monthly_ai_cost_usd, 0.0)

                        # Lowercase partial & turso storage mode fail closed
                        with self.assertRaisesRegex(ValueError, "Partial Turso configuration detected"):
                            Settings.from_env(env_file=turso_partial_lower)
                        with self.assertRaisesRegex(ValueError, "Cloud storage mode enabled"):
                            Settings.from_env(env_file=turso_mode_lower)

                        # OS overrides (including empty string) override lowercase dotenv
                        with patch.dict(os.environ, {"MIN_SCORE": "94"}, clear=True):
                            s_os = Settings.from_env(env_file=lower_env)
                            self.assertEqual(s_os.min_score, 94)

                        with patch.dict(os.environ, {"TURSO_DATABASE_URL": ""}, clear=True):
                            with self.assertRaisesRegex(ValueError, "Partial Turso configuration detected"):
                                Settings.from_env(env_file=turso_full_lower)

                        # Injected mixed-case environ overrides uppercase dotenv
                        with self.assertRaisesRegex(ValueError, "Partial Turso configuration detected"):
                            Settings.from_env(
                                env_file=upper_env,
                                environ={"min_score": "91", "turso_database_url": ""},
                            )
                        s_inj = Settings.from_env(
                            env_file=upper_env,
                            environ={"min_score": "91"},
                        )
                        self.assertEqual(s_inj.min_score, 91)

                        # Conflicting case-variant keys inside injected environ fail closed
                        with self.assertRaisesRegex(ValueError, "Conflicting case-variant duplicate key"):
                            Settings.from_env(
                                environ={
                                    "MAX_MONTHLY_AI_COST_USD": "0.00",
                                    "max_monthly_ai_cost_usd": "0.05",
                                }
                            )
                        with self.assertRaisesRegex(ValueError, "Conflicting case-variant duplicate key"):
                            Settings.from_env(environ={"MIN_SCORE": "90", "min_score": "70"})

                        self.assertEqual(dict(os.environ), original_env)

    def test_from_env_posix_case_sensitive_semantics(self):
        contexts = [patch("efso_radar.config._is_case_insensitive_env", return_value=False)]
        if os.name != "nt":
            contexts.append(nullcontext())

        with tempfile.TemporaryDirectory(prefix="efso_t1c_cfg_posix_") as td:
            tmp_path = Path(td)
            lower_only = tmp_path / "lower_only.env"
            lower_only.write_text(
                "min_score=74\nmax_monthly_ai_cost_usd=0.05\n",
                encoding="utf-8",
            )
            mixed_with_upper = tmp_path / "mixed_upper.env"
            mixed_with_upper.write_text(
                "min_score=74\nMIN_SCORE=91\n",
                encoding="utf-8",
            )
            upper_dup_budget = tmp_path / "upper_dup_budget.env"
            upper_dup_budget.write_text(
                "MAX_MONTHLY_AI_COST_USD=0.05\nMAX_MONTHLY_AI_COST_USD=0.00\n",
                encoding="utf-8",
            )

            for ctx in contexts:
                with ctx:
                    with patch.dict(os.environ, {}, clear=True):
                        original_env = dict(os.environ)

                        # Case-sensitive POSIX ignores lowercase keys just like legacy os.getenv("UPPER")
                        s_lower = Settings.from_env(env_file=lower_only)
                        self.assertEqual(s_lower.min_score, 82)
                        self.assertEqual(s_lower.max_monthly_ai_cost_usd, 0.0)

                        s_mixed = Settings.from_env(env_file=mixed_with_upper)
                        self.assertEqual(s_mixed.min_score, 91)

                        # Exact-case duplicate budget still preserves first-occurrence rejection
                        with self.assertRaisesRegex(ValueError, "Positive AI monetary budgets"):
                            Settings.from_env(env_file=upper_dup_budget)

                        # Injected environ remains case-sensitive (distinct keys allowed)
                        s_inj = Settings.from_env(environ={"min_score": "70", "MIN_SCORE": "91"})
                        self.assertEqual(s_inj.min_score, 91)

                        self.assertEqual(dict(os.environ), original_env)

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
