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
import json
import sys

from npg.cli import add_logging_arguments, open_input
from npg.log import configure_structlog
from structlog import get_logger

from npg_polite.cli.util import add_config_argument, add_version_argument
from npg_polite.porch import Pipeline, Task, load_pipeline_config


def logger():
    return get_logger()


REGISTER_PIPELINE = """Register a new Porch pipeline with the server."""


def register_pipeline_cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=REGISTER_PIPELINE,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser = add_logging_arguments(parser)
    parser = add_version_argument(parser)
    parser = add_config_argument(parser)

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
    parser = add_config_argument(parser)

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


LIST_TASKS = "List a pipeline's tasks."


def list_pipeline_tasks_cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=NEW_PIPELINE_TOKEN,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser = add_logging_arguments(parser)
    parser = add_version_argument(parser)
    parser = add_config_argument(parser)
    parser.add_argument(
        "--state",
        type=str,
        action="extend",
        nargs=1,
        choices=[
            "all",
            "cancelled",
            "claimed",
            "failed",
            "pending",
            "running",
            "succeeded",
        ],
        help="The state(s) of tasks to list.",
        default=[],
    )

    args = parser.parse_args(argv)
    configure_structlog(
        config_file=args.log_config,
        debug=args.debug,
        verbose=args.verbose,
        colour=args.colour,
        json=args.log_json,
    )

    try:
        config = load_pipeline_config(args.config)
        pipeline = Pipeline(config=config)
        states = sorted(set(args.state))
        if not states:
            states = ["all"]

        for s in states:
            match s:
                case "all":
                    _print_tasks(pipeline.all())
                case "cancelled":
                    _print_tasks(pipeline.cancelled())
                case "claimed":
                    _print_tasks(pipeline.claimed())
                case "failed":
                    _print_tasks(pipeline.failed())
                case "running":
                    _print_tasks(pipeline.running())
                case "succeeded":
                    _print_tasks(pipeline.succeeded())

        return 0
    except Exception as e:
        logger().error("Pipeline token creation failed", error=str(e))

        if args.debug:
            raise

        return 1


def _print_tasks(tasks: list[Task]):
    for task in tasks:
        print(json.dumps(task.to_serializable(), sort_keys=True))


UPDATE_TASK_STATE = "Update task state."


def update_pipeline_tasks_cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=UPDATE_TASK_STATE,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser = add_logging_arguments(parser)
    parser = add_version_argument(parser)
    parser = add_config_argument(parser)
    parser.add_argument(
        "state",
        help="The new task state",
        type=str,
        choices=["cancel", "fail", "retry", "done"],
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
    logger().debug("Update task state", argv=" ".join(argv))

    try:
        config = load_pipeline_config(args.config)
        pipeline = Pipeline(config=config)

        def _update(fn):
            with open_input(path=None) as stdin:
                for line in stdin:
                    t = pipeline.cls.from_serializable(json.loads(line))
                    t = fn(t)
                    logger().info("Updated task", task=t)

        match args.state:
            case "cancel":
                _update(pipeline.cancel)
            case "fail":
                _update(pipeline.fail)
            case "retry":
                _update(pipeline.retry)
            case "done":
                _update(pipeline.done)

        return 0
    except Exception as e:
        logger().error("Task update failed", error=str(e))

        if args.debug:
            raise
        return 1
