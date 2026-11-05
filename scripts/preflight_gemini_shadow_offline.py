"""Run bounded, fail-closed offline Gemini shadow preflight for Issue #61.

Strictly offline:
- Zero real Gemini API calls.
- Validates the fixed public-only historical news sample (from Issue #49).
- Evaluates token bounds, prompt generation, batch hash, and Free Tier gate status.
- Generates a redacted, human-readable report with AI_RESULT_NOT_AVAILABLE.
"""
from __future__ import annotations

import sys
from pathlib import Path

# Add src to sys.path if not present
REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from efso_radar.ai.shadow_preflight import OneShotShadowPreflightRunner


def main() -> int:
    runner = OneShotShadowPreflightRunner()
    result = runner.run_preflight()
    print(result.to_markdown_report())
    return 0


if __name__ == "__main__":
    sys.exit(main())
