#
# Copyright © 2026 Genome Research Ltd. All rights reserved.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

import argparse
import re
import sys
from datetime import datetime, timedelta

from npg.cli import add_logging_arguments, parse_iso_date
from npg.log import configure_structlog
from structlog import get_logger

from npg_polite import version
from npg_polite.porch import Pipeline, load_pipeline_config
from npg_polite.task import DatetimeParam, ShellCommandTask, make_interval_tasks


def logger():
    return get_logger()


REGISTER_PIPELINE = """Register a new Porch pipeline with the server."""


def register_pipeline_cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=REGISTER_PIPELINE,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser = add_logging_arguments(parser)
    # parser = add_pipeline_arguments(parser)
    parser = add_version_argument(parser)
    parser.add_argument(
        "--config", type=str, required=True, help="The pipeline configuration file."
    )

    args = parser.parse_args()
    configure_structlog(
        config_file=args.log_config,
        debug=args.debug,
        verbose=args.verbose,
        colour=args.colour,
        json=args.log_json,
    )

    if argv is None:
        argv = sys.argv[1:]
    logger().debug("Register pipeline CLI", argv=" ".join(argv))

    try:
        config = load_pipeline_config(args.config)
        pipeline = Pipeline(config=config)

        pipeline = pipeline.register()
        logger().info(
            "Pipeline registered",
            name=pipeline.name,
            uri=pipeline.uri,
            version=pipeline.version,
            compatibility_policy=pipeline.compatibility_policy,
            compatibility_version=pipeline.compatibility_version,
        )

        return 0
    except Exception as e:
        logger().error("Pipeline registration failed", error=str(e))

        if args.debug:
            raise

        return 1


NEW_PIPELINE_TOKEN = "Create a new pipeline token and print it to STDOUT."


def new_pipeline_token_cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=NEW_PIPELINE_TOKEN,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser = add_logging_arguments(parser)
    parser = add_version_argument(parser)
    parser.add_argument(
        "--config", type=str, required=True, help="The pipeline configuration file."
    )

    args = parser.parse_args(argv)
    configure_structlog(
        config_file=args.log_config,
        debug=args.debug,
        verbose=args.verbose,
        colour=args.colour,
        json=args.log_json,
    )

    if argv is None:
        argv = sys.argv[1:]
    logger().debug("New pipeline token", argv=" ".join(argv))

    try:
        config = load_pipeline_config(args.config)
        pipeline = Pipeline(config=config)
        print(pipeline.new_token(token_desc=pipeline.config.name))

        return 0
    except Exception as e:
        logger().error("Pipeline token creation failed", error=str(e))

        if args.debug:
            raise

        return 1


ADD_INTERVAL_TASKS = "Add shell command tasks for consecutive UTC time intervals."


def add_interval_tasks_cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=ADD_INTERVAL_TASKS,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser = add_logging_arguments(parser)
    parser = add_version_argument(parser)
    parser.add_argument(
        "--config", type=str, required=True, help="The pipeline configuration file."
    )

    parser.add_argument("command", type=str, help="Shell command template.")
    parser.add_argument(
        "--begin",
        required=True,
        type=_parse_named_datetime,
        metavar="NAME=TIMESTAMP",
        help="First boundary and parameter name; timestamp must be UTC (Z or +00:00).",
    )
    parser.add_argument(
        "--end",
        required=True,
        type=_parse_named_datetime,
        metavar="NAME=TIMESTAMP",
        help="Final boundary and parameter name; timestamp must be UTC (Z or +00:00).",
    )
    parser.add_argument(
        "--interval",
        required=True,
        type=_parse_interval,
        metavar="DURATION",
        help="Positive integer duration with s, m, h or d suffix; a day is 24 hours.",
    )
    parser.add_argument(
        "--param",
        action="append",
        type=_parse_named_value,
        default=[],
        metavar="NAME=VALUE",
        help="Shared string parameter; repeat for additional parameters.",
    )

    args = parser.parse_args(argv)
    shared_params: dict[str, str] = {}
    for name, value in args.param:
        if name in shared_params:
            parser.error(f"duplicate --param name: {name!r}")
        shared_params[name] = value

    configure_structlog(
        config_file=args.log_config,
        debug=args.debug,
        verbose=args.verbose,
        colour=args.colour,
        json=args.log_json,
    )

    if argv is None:
        argv = sys.argv[1:]
    logger().debug("Add interval tasks", argv=" ".join(argv))

    added = 0
    existing = 0
    try:
        config = load_pipeline_config(args.config)
        pipeline = Pipeline[ShellCommandTask](config=config)
        if pipeline.cls is not ShellCommandTask:
            raise ValueError("Interval tasks require the ShellCommandTask task class")

        tasks = make_interval_tasks(
            args.command,
            DatetimeParam(*args.begin),
            DatetimeParam(*args.end),
            args.interval,
            shared_params or None,
        )
        for task in tasks:
            if pipeline.add(task):
                added += 1
            else:
                existing += 1
        logger().info("Interval tasks added", added=added, existing=existing)
        return 0
    except Exception as e:
        logger().error(
            "Interval task submission failed",
            error=str(e),
            added=added,
            existing=existing,
        )
        if args.debug:
            raise
        return 1


#
# UPDATE_TASK_STATE = "Update task state."
#
# def update_task_state_cli(argv: list[str] | None = None)-> int:
#     parser = argparse.ArgumentParser(
#         description=UPDATE_TASK_STATE,
#         formatter_class=argparse.RawDescriptionHelpFormatter,
#     )
#     parser = add_logging_arguments(parser)
#     parser = add_version_argument(parser)
#     parser.add_argument(
#         "--porch-config", type=str, required=True, help="The Porch configuration file."
#     )
#
#     args = parser.parse_args(argv)
#     configure_structlog(
#         config_file=args.log_config,
#         debug=args.debug,
#         verbose=args.verbose,
#         colour=args.colour,
#         json=args.log_json,
#     )
#
#     if argv is None:
#         argv = sys.argv[1:]
#     logger().debug("Update task state", argv=" ".join(argv))


# def add_pipeline_arguments(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
#     parser.add_argument(
#         "--pipeline-task",
#         type=str,
#         required=True,
#         help="The fully-qualified class name of the tasks that the pipeline will handle.",
#     )
#     parser.add_argument(
#         "--pipeline-name", type=str, required=True, help="The pipeline name."
#     )
#     parser.add_argument(
#         "--pipeline-uri", type=str, required=True, help="The pipeline URI."
#     )
#     parser.add_argument(
#         "--pipeline-version", type=str, required=True, help="The pipeline version."
#     )
#     parser.add_argument(
#         "--pipeline-compatibility",
#         type=CompatibilityPolicy,
#         required=False,
#         choices=CompatibilityPolicy,
#         default=CompatibilityPolicy.EXACT,
#         help="The pipeline version compatibility policy.",
#     )
#     return parser


def add_version_argument(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument(
        "--version",
        help="Print the version and exit.",
        action="version",
        version=version(),
    )
    return parser


def _parse_named_value(value: str) -> tuple[str, str]:
    """Split a named command-line value at its first equals sign."""
    name, separator, content = value.partition("=")
    if not separator:
        raise argparse.ArgumentTypeError(f"Invalid NAME=VALUE pair '{value}'")
    return name, content


def _parse_named_datetime(value: str) -> tuple[str, datetime]:
    """Parse a named ISO-format timestamp."""
    name, timestamp = _parse_named_value(value)
    try:
        return name, datetime.fromisoformat(timestamp)
    except ValueError as e:
        raise argparse.ArgumentTypeError(
            f"invalid ISO-format timestamp in '{value}'"
        ) from e


def _parse_interval(value: str) -> timedelta:
    """Parse an integer duration with a seconds, minutes, hours, or days suffix."""
    match = re.fullmatch(r"([+-]?[0-9]+)([smhd])", value)
    if match is None:
        raise argparse.ArgumentTypeError(
            f"Invalid time interval '{value}'; expected an integer followed by s, m, h or d (e.g. 3600s, 6h, 1d)"
        )
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    try:
        return timedelta(seconds=int(match[1]) * units[match[2]])
    except (ValueError, OverflowError) as e:
        raise argparse.ArgumentTypeError("duration is too large") from e
