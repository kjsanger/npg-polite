"""CLI program testing shim."""

import sys

from npg_polite.cli.admin import register_pipeline_cli

if __name__ == "__main__":
    sys.exit(register_pipeline_cli())
