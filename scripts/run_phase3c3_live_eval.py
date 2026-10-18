"""
Phase 3C-3: Live Gemini 3.5 Flash-Lite Evaluation of Remaining 42 Calibration Records.
Executes 7 sequential batches of 6 items each under strict safety gates and durable $0.05 budget ceiling.
"""
import hashlib
import json
import logging
import os
import sys
from pathlib import Path

# Configure clean logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("phase3c3-live-eval")

from efso_radar.ai.calibration import (
    DEFAULT_FILLED_CALIBRATION_CSV,
    load_and_verify_calibration_csv,
)
from efso_radar.ai.cost import ExperimentBudgetGuard
from efso_radar.ai.phase3c2b_eval import BlindShadowEvaluationRunner
from efso_radar.config import Settings
from efso_radar.repositories.sqlite import SqliteStateRepository

EXPECTED_CSV_SHA256 = "108799c19f513327cb04872cec5dbcfb97107f2bd8bf2353acf28e3111ab2305"


def main():
    logger.info("=== Phase 3C-3: Starting Live Gemini 42-Story Evaluation ===")

    # 1. Gate 1: Check CSV Integrity
    csv_path = Path(DEFAULT_FILLED_CALIBRATION_CSV)
    if not csv_path.exists():
        raise FileNotFoundError(f"Calibration CSV not found: {csv_path}")

    csv_sha = hashlib.sha256(csv_path.read_bytes()).hexdigest().lower()
    if csv_sha != EXPECTED_CSV_SHA256.lower():
        raise ValueError(f"CSV SHA-256 mismatch! Found: {csv_sha}, Expected: {EXPECTED_CSV_SHA256}")
    logger.info("Gate 1 Passed: Authoritative CSV SHA-256 verified (%s)", csv_sha)

    # 2. Gate 2: Load Items & Slice CALIB-09 to CALIB-50
    all_50_items = load_and_verify_calibration_csv(csv_path, expected_count=50)
    eval_42_items = all_50_items[8:]
    if len(eval_42_items) != 42:
        raise ValueError(f"Expected exactly 42 items for CALIB-09..50, found {len(eval_42_items)}")
    if eval_42_items[0].review_id != "CALIB-09" or eval_42_items[-1].review_id != "CALIB-50":
        raise ValueError(f"Boundary mismatch: First={eval_42_items[0].review_id}, Last={eval_42_items[-1].review_id}")
    logger.info("Gate 2 Passed: 42 evaluation records selected (CALIB-09 to CALIB-50)")

    # 3. Gate 3: Check GEMINI_API_KEY
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY environment variable is missing!")
    logger.info("Gate 3 Passed: GEMINI_API_KEY is available (length: %d)", len(api_key))

    # 4. Gate 4: Cumulative Budget Guard Initialization
    budget_guard = ExperimentBudgetGuard(
        ledger_path=Path("data/experiment_budget_ledger.json"),
        budget_ceiling_usd=0.050000,
        canary1_db_path=Path("data/canary_radar.db"),
        canary2_db_path=Path("data/canary2_radar.db"),
    )
    prior_cost = budget_guard.get_prior_canaries_cost_usd()
    total_consumed = budget_guard.get_total_consumed_usd()
    allowance = budget_guard.get_remaining_allowance_usd()
    logger.info(
        "Gate 4 Passed: Persistent Budget Ledger initialized. Prior canaries: $%.6f, "
        "Total consumed: $%.6f, Remaining allowance: $%.6f (Ceiling: $%.4f)",
        prior_cost, total_consumed, allowance, budget_guard.budget_ceiling_usd
    )

    if allowance < 0.025:
        raise RuntimeError(f"Insufficient budget allowance (${allowance:.6f}) for 7 batches!")

    # 5. Gate 5: Repository & Runner Initialization
    eval_db_path = Path("data/phase3c3_radar.db")
    repo = SqliteStateRepository(eval_db_path)
    settings = Settings(
        gemini_model="gemini-3.5-flash-lite",
        max_shadow_eval_budget_usd=0.050000,
        max_output_tokens=3200,
    )
    runner = BlindShadowEvaluationRunner(
        settings=settings,
        repository=repo,
        batch_size=6,
    )
    logger.info("Gate 5 Passed: Runner configured with batch_size=6 (Target: 7 sequential batches)")

    # 6. Execute Live Gemini Evaluation
    report_path = Path("data/phase3c3_gemini_eval_report.json")
    logger.info("Starting live sequential inference across 7 batches...")
    
    predictions, metrics, error_analysis = runner.run_live_gemini_evaluation(
        items=eval_42_items,
        api_key=api_key,
        dry_run=False,
        max_retries=0,
        max_batches=7,
        automatic_continuation=False,
        output_artifact_path=report_path,
        schema_mode="DIAGNOSTIC",
        budget_guard=budget_guard,
    )

    logger.info("=== Phase 3C-3 Live Evaluation Finished Successfully ===")
    logger.info("Total Evaluated: %d", metrics.total_evaluated)
    logger.info("Batches Executed: %d, Cached: %d", metrics.batches_executed, metrics.batches_reused_from_cache)
    logger.info("Tokens: In=%d, Out=%d", metrics.total_input_tokens, metrics.total_output_tokens)
    logger.info("This Run Cost: $%.6f USD", metrics.total_estimated_cost_usd)
    logger.info("Cumulative Experiment Cost: $%.6f USD", budget_guard.get_total_consumed_usd())
    logger.info("Remaining Budget Allowance: $%.6f USD", budget_guard.get_remaining_allowance_usd())
    logger.info("Report saved to: %s", report_path)


if __name__ == "__main__":
    main()
