"""Run bounded, manual one-shot Gemini shadow evaluation operator for Issue #63 / Stage B.

Strict operator safety rules:
- Requires explicit --confirm-single-shot-free-tier flag for live execution.
- Defaults to offline --dry-run if confirmation is not provided.
- Evaluates the fixed Stage A public-only historical news sample (SHADOW_SAMPLE_ID).
- Enforces single-shot, zero SDK retry, gemini-3.5-flash-lite, no grounding, Free Tier ($0.00 / 0 TRY).
- Segregates raw structured model picks from post-validation alertable suggestions.
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
        help="Explicit confirmation that Free Tier status, active rate limits (15 RPM / 250K TPM / 500 RPD), "
        "and explicit PO authorization have been verified.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Execute strictly offline dry run without dispatching SDK call.",
    )
    parser.add_argument(
        "--output-file",
        type=str,
        default=None,
        help="Optional file path to write the markdown report.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    runner = OneShotShadowOperatorRunner()

    if args.dry_run or not args.confirm_single_shot_free_tier:
        if not args.dry_run and not args.confirm_single_shot_free_tier:
            print(
                "[NOTICE] --confirm-single-shot-free-tier not provided. Falling back to offline --dry-run.\n"
                "To execute a genuine one-shot evaluation, explicitly supply --confirm-single-shot-free-tier\n"
                "along with GEMINI_API_KEY and separate PO operational GO.\n",
                file=sys.stderr,
            )
        try:
            result = runner.dry_run()
        except AIEvaluationError as exc:
            print(f"[ERROR] Dry run failed: {exc}", file=sys.stderr)
            return 1
    else:
        try:
            result = runner.execute_one_shot(confirm_single_shot_free_tier=True)
        except AIEvaluationError as exc:
            print(f"[ERROR] Operator execution failed: {exc}", file=sys.stderr)
            return 1

    report_md = result.to_markdown_report()
    print(report_md)

    if args.output_file:
        out_path = Path(args.output_file)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(report_md, encoding="utf-8")
        print(f"\n[INFO] Report written to: {out_path}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
