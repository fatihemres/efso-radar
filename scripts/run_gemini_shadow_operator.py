"""Run bounded, manual one-shot Gemini shadow evaluation operator for Issue #63 / Stage B.

Strict operator safety rules:
- Live network dispatch is strictly disabled in merged Stage B software; fails closed.
- Evaluates the fixed Stage A public-only historical news sample (SHADOW_SAMPLE_ID).
- Enforces single-shot, gemini-3.5-flash-lite, no grounding, Free Tier ($0.00 / 0 TRY).
- Segregates raw structured model picks, approved suggestions, suppressed picks, and unreturned cohort articles.
- Protects operational files: forbids writing to .env, data/, or overwriting existing files.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Add src to sys.path if not present
REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from efso_radar.ai.client import AIEvaluationError
from efso_radar.ai.shadow_operator import OneShotShadowOperatorRunner


def validate_output_path(path_str: str) -> Path:
    """Validate that the output path is safe and does not target operational records."""
    target = Path(path_str).resolve()
    repo_root = REPO_ROOT.resolve()

    # Reject writing to .env or inside data/ directory
    if target == (repo_root / ".env").resolve():
        raise ValueError("Security violation: Writing report to operational .env file is strictly forbidden.")

    data_dir = (repo_root / "data").resolve()
    if target == data_dir or data_dir in target.parents:
        raise ValueError("Security violation: Writing report into operational data/ directory is strictly forbidden.")

    # Prevent accidental overwrite
    if target.exists():
        raise ValueError(f"Output file already exists: '{target}'. Overwriting is forbidden for safety.")

    return target


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Efso Radar — Stage B Manual One-Shot Gemini Editorial Shadow Operator"
    )
    parser.add_argument(
        "--confirm-single-shot-free-tier",
        action="store_true",
        default=False,
        help="Confirmation flag (strictly fails closed in Stage B software preparation; live calls blocked).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Execute strictly offline dry run without dispatching SDK call.",
    )
    parser.add_argument(
        "--evaluate-json",
        type=str,
        default=None,
        help="Path to pre-computed structured JSON payload for offline evaluation.",
    )
    parser.add_argument(
        "--output-file",
        type=str,
        default=None,
        help="Optional file path to write the markdown report (safe location only).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    runner = OneShotShadowOperatorRunner()

    # Validate output path upfront before running
    target_out_path = None
    if args.output_file:
        try:
            target_out_path = validate_output_path(args.output_file)
        except ValueError as err:
            print(f"[SECURITY ERROR] Invalid output file path: {err}", file=sys.stderr)
            return 1

    # P1 Live Deny Gate: Live execution is strictly denied in merged Stage B software
    if args.confirm_single_shot_free_tier:
        print(
            "[SECURITY GATE BLOCKED] Live Gemini execution is strictly disabled in Stage B software preparation.\n"
            "Real API inference requires an explicit, separate Product Owner operational GO\n"
            "and a dedicated live execution adapter.\n",
            file=sys.stderr,
        )
        try:
            runner.execute_one_shot(confirm_single_shot_free_tier=True)
        except AIEvaluationError as exc:
            print(f"[DENIED] {exc}", file=sys.stderr)
        return 1

    if args.evaluate_json:
        json_path = Path(args.evaluate_json)
        if not json_path.exists():
            print(f"[ERROR] JSON file not found: {json_path}", file=sys.stderr)
            return 1
        try:
            payload = json_path.read_text(encoding="utf-8")
            result = runner.evaluate_precomputed_response_json(payload)
        except (AIEvaluationError, OSError) as exc:
            print(f"[ERROR] Evaluation failed: {exc}", file=sys.stderr)
            return 1
    else:
        # Default or explicit --dry-run
        try:
            result = runner.dry_run()
        except AIEvaluationError as exc:
            print(f"[ERROR] Dry run failed: {exc}", file=sys.stderr)
            return 1

    report_md = result.to_markdown_report()
    print(report_md)

    if target_out_path:
        target_out_path.parent.mkdir(parents=True, exist_ok=True)
        target_out_path.write_text(report_md, encoding="utf-8")
        print(f"\n[INFO] Report safely written to: {target_out_path}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
