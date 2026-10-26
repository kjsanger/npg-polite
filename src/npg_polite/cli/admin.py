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
import sys

from npg.cli import add_logging_arguments
from npg.log import configure_structlog
from structlog import get_logger

from npg_polite import version
from npg_polite.porch import (
    Pipeline,
    load_pipeline_config,
    load_server_config,
    resolve_task_class,
)


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
        server_config = load_server_config(args.config)
        config = load_pipeline_config(args.config)
        task_class = resolve_task_class(config.task)
        pipeline = Pipeline(task_class, config=config, server_config=server_config)

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
    # parser = add_pipeline_arguments(parser)
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
        server_config = load_server_config(args.config)
        config = load_pipeline_config(args.config)
        task_class = resolve_task_class(config.task)

        pipeline = Pipeline(
            task_class,
            config=config,
            server_config=server_config,
        )
        print(pipeline.new_token(token_desc=pipeline.config.name))

        return 0
    except Exception as e:
        logger().error("Pipeline token creation failed", error=str(e))

        if args.debug:
            raise

        return 1


# ADD_INTERVAL_TASKS = "Add interval tasks."
#
#
# def add_interval_tasks_cli(argv: list[str] | None) -> int:
#     parser = argparse.ArgumentParser(
#         description=ADD_INTERVAL_TASKS,
#         formatter_class=argparse.RawDescriptionHelpFormatter,
#     )
#     parser = add_logging_arguments(parser)
#     parser = add_pipeline_arguments(parser)
#     parser = add_version_argument(parser)
#     parser.add_argument(
#         "--config", type=str, required=True, help="The pipeline configuration file."
#     )
#
#     parser.add_argument("command", type=str, help="Shell command template.")
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
#     logger().debug("Add interval tasks", argv=" ".join(argv))
#
#     config = Pipeline.Config(
#         name=args.pipeline_name,
#         uri=args.pipeline_uri,
#         version=args.pipeline_version,
#         compatibility_policy=args.pipeline_compatibility,
#     )
#
#     pipeline = Pipeline(
#         _resolve_task(args.pipeline_task),
#         config=config,
#         server_config=_load_server_config(args.config),
#     )
#     tasks = make_interval_tasks()
#

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
