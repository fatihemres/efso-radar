"""Run bounded, manual one-shot Gemini shadow evaluation operator for Issue #63 / Stage B.

Strict operator safety rules:
- Live network dispatch is strictly disabled in merged Stage B software; fails closed.
- CLI operates exclusively offline on the fixed Stage A public-only historical news sample (SHADOW_SAMPLE_ID).
- Forbids all arbitrary file reads and writes; no private or operational files (.env, data/) can be read or written.
- Enforces single-shot, gemini-3.5-flash-lite, no grounding, Free Tier ($0.00 / 0 TRY).
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
        help="Execute strictly offline dry run on the fixed public historical sample without dispatching SDK call.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    runner = OneShotShadowOperatorRunner()

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

    # Default or explicit --dry-run on fixed public historical sample only
    try:
        result = runner.dry_run()
    except AIEvaluationError as exc:
        print(f"[ERROR] Dry run failed: {exc}", file=sys.stderr)
        return 1

    report_md = result.to_markdown_report()
    print(report_md)
    return 0


if __name__ == "__main__":
    sys.exit(main())
