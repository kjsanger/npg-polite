"""CLI program testing shim."""

import sys

from npg_polite.cli.admin import update_pipeline_tasks_cli

if __name__ == "__main__":
    sys.exit(update_pipeline_tasks_cli())
