"""CLI program testing shim."""

import sys

from npg_polite.cli.admin import new_pipeline_token_cli

if __name__ == "__main__":
    sys.exit(new_pipeline_token_cli())
