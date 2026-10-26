"""CLI program testing shim."""

import sys

from npg_polite.cli.admin import add_interval_tasks_cli

if __name__ == "__main__":
    sys.exit(add_interval_tasks_cli())
