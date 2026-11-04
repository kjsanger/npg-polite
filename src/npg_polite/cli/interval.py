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
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from npg.cli import add_logging_arguments
from npg.log import configure_structlog

from npg_polite.cli.admin import logger
from npg_polite.cli.util import (
    add_version_argument,
)
from npg_polite.porch import Pipeline, load_pipeline_config
from npg_polite.task import DatetimeParam, ShellCommandTask, make_interval_tasks

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
            parser.error(
                f"duplicate --param name '{name}'; previous was "
                f"{name}={shared_params[name]}"
            )
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


RUN_INTERVAL_TASK = "Run a shell command task for a UTC time interval."


def run_interval_task_cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=RUN_INTERVAL_TASK,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser = add_logging_arguments(parser)
    parser = add_version_argument(parser)
    parser.add_argument(
        "--config", type=str, required=True, help="The pipeline configuration file."
    )
    parser.add_argument(
        "--max-running",
        type=int,
        default=1,
        help="The maximum number of tasks that should be running at once. Once this "
        "number of running tasks in the current pipeline is reached (or exceeded),"
        "further attempts to start tasks will be skipped, until the number running "
        "falls below this threshold. Optional, defaults to 1.",
    )
    parser.add_argument(
        "--task-timeout",
        type=str,
        help="Maximum duration permitted for a task. Tasks that are still running at the "
        "end of this period will be killed automatically. Optional, defaults to 30m.",
        default="30m",
    )
    parser.add_argument(
        "--working-directory",
        type=str,
        help="The working directory in which task processes will be run. "
        "Optional, defaults to the current working directory.",
        default=os.getcwd(),
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
    logger().debug("Add interval tasks", argv=" ".join(argv))

    try:
        work_dir = Path(args.working_directory).resolve(strict=True).as_posix()
        max_running = _parse_max_running(args.max_running)
        timeout = _parse_interval(args.task_timeout).total_seconds()

        config = load_pipeline_config(args.config)
        pipeline = Pipeline[ShellCommandTask](config=config)
        if pipeline.cls is not ShellCommandTask:
            raise ValueError("Interval tasks require the ShellCommandTask task class")

        running_tasks = pipeline.running()
        num_running = len(running_tasks)
        if num_running >= max_running:
            logger().info(
                "The maximum number of tasks are already running",
                name=pipeline.name,
                version=pipeline.version,
                max_runnng=max_running,
                num_running=num_running,
                tasks=running_tasks,
            )
            return 0

        claimed = pipeline.claim(1)
        if not claimed:
            return 0

        command_task = claimed.pop()

        pipeline.run(command_task)

        try:
            completed = command_task.execute(cwd=work_dir, timeout=timeout)
            if completed.returncode == 0:
                pipeline.done(command_task)
            else:
                pipeline.fail(command_task)
        except subprocess.TimeoutExpired:
            logger().error("Command timed out", timeout=timeout, task=command_task)
            pipeline.fail(command_task)

        return 0
    except Exception as e:
        logger().error("Interval task runner failed", error=str(e))
        if args.debug:
            raise
        return 1


def _parse_max_running(value: str) -> int:
    try:
        n = int(value)
        if n < 1:
            raise ValueError("Max running must be a positive integer")
        return n
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"Invalid max running value '{value}'") from e


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
            f"Invalid ISO-format timestamp in '{value}'"
        ) from e


def _parse_interval(value: str) -> timedelta:
    """Parse an integer duration with a seconds, minutes, hours, or days suffix."""
    match = re.fullmatch(r"([+-]?[0-9]+)([smhd])", value)
    if match is None:
        raise argparse.ArgumentTypeError(
            f"Invalid time interval '{value}'; "
            f"expected an integer followed by s, m, h or d (e.g. 3600s, 6h, 1d)"
        )
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    try:
        return timedelta(seconds=int(match[1]) * units[match[2]])
    except (ValueError, OverflowError) as e:
        raise argparse.ArgumentTypeError("Duration is too large") from e
