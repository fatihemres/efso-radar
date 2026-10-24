"""Entrypoint for python -m efso_radar.workflow."""
import sys
from efso_radar.workflow.cli import main

if __name__ == "__main__":
    sys.exit(main())
