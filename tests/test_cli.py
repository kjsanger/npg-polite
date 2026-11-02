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

import subprocess
import sys

from pytest import mark as m

from npg_polite.porch import Pipeline, load_pipeline_config
from npg_polite.task import ShellCommandTask
from tests.conftest import make_pipeline_config_file


class TestPipelineAdmin:
    @m.context("When valid pipeline arguments and Porch configuration are supplied")
    @m.it("Resolves the task class and registers the configured pipeline")
    def test_register_pipeline_cli(
        self, cli_wrapper_path, shellcmd_pipeline_config_file, clean_porch_environment
    ):
        result = subprocess.run(
            [
                sys.executable,
                (cli_wrapper_path / "register_pipeline.py").as_posix(),
                "--config",
                shellcmd_pipeline_config_file.as_posix(),
                "--debug",
            ],
            env=clean_porch_environment,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert (
            result.returncode == 0
        ), f"STDOUT: '{result.stdout}', STDERR: '{result.stderr}'"

        # Connect to the pipeline created by the CLI without registering it again.
        config = load_pipeline_config(shellcmd_pipeline_config_file)
        pipeline = Pipeline[ShellCommandTask](config=config)
        config.pipeline_token = pipeline.new_token("CLI registration test")

        task = ShellCommandTask(
            "echo {:message:}", {"message": "CLI registration test"}
        )
        assert pipeline.add(task)
        assert pipeline.all() == [task]

    @m.context(
        "When valid pipeline arguments and Porch configuration are supplied a "
        "new token can be requested for a registered pipeline"
    )
    @m.it("Creates a new token and prints it")
    def test_new_pipeline_token_cli(
        self, cli_wrapper_path, shellcmd_pipeline_config_file, clean_porch_environment
    ):
        config = load_pipeline_config(shellcmd_pipeline_config_file.as_posix())
        pipeline = Pipeline[ShellCommandTask](config=config)
        pipeline.register(update_config=True)

        result = subprocess.run(
            [
                sys.executable,
                (cli_wrapper_path / "new_pipeline_token.py").as_posix(),
                "--config",
                shellcmd_pipeline_config_file.as_posix(),
                "--debug",
            ],
            env=clean_porch_environment,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert (
            result.returncode == 0
        ), f"STDOUT: '{result.stdout}', STDERR: '{result.stderr}'"

        token = result.stdout.strip()
        assert token
        assert len(token.split()) == 1
        assert token != pipeline.config.pipeline_token

        # Authenticate with the printed token instead of the registration token.
        pipeline.config.pipeline_token = token
        pipeline.config.admin_token = None

        task = ShellCommandTask("echo {:message:}", {"message": "CLI token test"})
        assert pipeline.add(task)
        assert pipeline.all() == [task]


@m.describe("Interval task command line")
class TestIntervalCLI:
    @m.context("When the requested intervals fill or partly fill the time range")
    @m.it("Stores the expected interval tasks and shared string values")
    @m.parametrize(
        "interval, boundaries",
        [
            ("1h", [0, 1, 2, 3]),
            ("2h", [0, 2, 3]),
        ],
    )
    def test_submission(
        self,
        registered_shellcmd_pipeline,
        interval_cli_process,
        interval_cli_arguments,
        interval,
        boundaries,
    ):
        pipeline, args = registered_shellcmd_pipeline, interval_cli_arguments
        command = "echo {:start:} {:stop:} {:dataset:} {:empty:}"
        result = interval_cli_process(
            args
            + [
                "--interval",
                interval,
                "--param",
                "dataset= run=42 ",
                "--param",
                "empty=",
                command,
            ]
        )
        assert (
            result.returncode == 0
        ), f"STDOUT: '{result.stdout}', STDERR: '{result.stderr}'"
        expected = [
            ShellCommandTask(
                command,
                {
                    "start": f"2026-01-21T{start:02d}:00:00+00:00",
                    "stop": f"2026-01-21T{stop:02d}:00:00+00:00",
                    "dataset": " run=42 ",
                    "empty": "",
                },
            )
            for start, stop in zip(boundaries, boundaries[1:])
        ]
        assert sorted(pipeline.all(), key=lambda task: task.params["start"]) == expected

    @m.context("When durations use seconds, minutes, hours or days")
    @m.it("Uses the supplied boundary names and creates the same one-day task")
    @m.parametrize("interval", ["86400s", "1440m", "24h", "1d"])
    def test_units_and_custom_names(
        self,
        registered_shellcmd_pipeline,
        interval_cli_process,
        interval_cli_arguments,
        interval,
    ):
        pipeline, args = registered_shellcmd_pipeline, interval_cli_arguments
        command = "echo {:first:} {:last:}"
        result = interval_cli_process(
            args
            + [
                "--begin",
                "first=2026-01-21T00:00:00+00:00",
                "--end",
                "last=2026-01-22T00:00:00Z",
                "--interval",
                interval,
                command,
            ],
        )
        assert (
            result.returncode == 0
        ), f"STDOUT: '{result.stdout}', STDERR: '{result.stderr}'"
        assert pipeline.all() == [
            ShellCommandTask(
                command,
                {
                    "first": "2026-01-21T00:00:00+00:00",
                    "last": "2026-01-22T00:00:00+00:00",
                },
            )
        ]

    @m.context("When command-line syntax is invalid")
    @m.it("Exits with a usage error")
    @m.parametrize(
        "options, message",
        [
            (["--begin", "2026-01-21T00:00:00Z"], "NAME=VALUE"),
            (["--end", "stop=not-a-date"], "ISO-format timestamp"),
            (["--param", "dataset"], "NAME=VALUE"),
            (["--param", "x=1", "--param", "x=2"], "duplicate --param"),
            (["--interval", "1.5h"], "integer followed by"),
            (["--interval", "1w"], "integer followed by"),
            (["--interval", "60"], "integer followed by"),
            (["--interval", "999999999999999999999d"], "too large"),
        ],
    )
    def test_syntax_errors(
        self, interval_cli_arguments, interval_cli_process, options, message
    ):
        result = interval_cli_process(interval_cli_arguments + options + ["echo"])
        assert (
            result.returncode == 2
        ), f"STDOUT: '{result.stdout}', STDERR: '{result.stderr}'"
        assert message in result.stderr

    @m.context("When task-layer validation rejects an input")
    @m.it("Exits with a failure and leaves the pipeline empty")
    @m.parametrize(
        "options, command",
        [
            (["--interval=0s"], "echo"),
            (["--interval=-1h"], "echo"),
            (["--begin", "start=2026-01-21T00:00:00"], "echo"),
            (["--begin", "start=2026-01-21T00:00:00+01:00"], "echo"),
            (["--end", "stop=2026-01-20T00:00:00Z"], "echo"),
            (["--end", "start=2026-01-21T03:00:00Z"], "echo"),
            (["--param", "start=override"], "echo"),
            (["--param", "bad-name=value"], "echo"),
            ([], "echo {:missing:}"),
            ([], "echo {:broken"),
        ],
    )
    def test_domain_errors(
        self,
        registered_shellcmd_pipeline,
        interval_cli_process,
        interval_cli_arguments,
        options,
        command,
    ):
        pipeline, args = registered_shellcmd_pipeline, interval_cli_arguments
        result = interval_cli_process(args + options + [command])
        assert (
            result.returncode == 1
        ), f"STDOUT: '{result.stdout}', STDERR: '{result.stderr}'"
        assert pipeline.all() == []

    @m.context("When the configured task class is incompatible")
    @m.it("Rejects the configuration without submitting tasks")
    def test_incompatible_task(
        self,
        registered_shellcmd_pipeline,
        interval_cli_process,
        interval_cli_arguments,
        tmp_path,
    ):
        pipeline, args = registered_shellcmd_pipeline, interval_cli_arguments
        config = pipeline.config
        config_file = make_pipeline_config_file(
            tmp_path / "incompatible-pipeline.ini",
            config.name,
            config.uri,
            config.version,
            "tests.test_porch.ExampleTask",
        )
        result = interval_cli_process(args + ["--config", str(config_file), "echo"])
        assert (
            result.returncode == 1
        ), f"STDOUT: '{result.stdout}', STDERR: '{result.stderr}'"
        assert pipeline.all() == []

    @m.context("When the time range is empty")
    @m.it("Succeeds without submitting tasks")
    def test_empty_range(
        self, registered_shellcmd_pipeline, interval_cli_process, interval_cli_arguments
    ):
        pipeline, args = registered_shellcmd_pipeline, interval_cli_arguments
        result = interval_cli_process(
            args + ["--end", "stop=2026-01-21T00:00:00Z", "echo"]
        )
        assert (
            result.returncode == 0
        ), f"STDOUT: '{result.stdout}', STDERR: '{result.stderr}'"
        assert pipeline.all() == []

    @m.context("When the same interval tasks are submitted twice")
    @m.it("Succeeds and leaves the stored task collection unchanged")
    def test_existing_tasks(
        self, registered_shellcmd_pipeline, interval_cli_arguments, interval_cli_process
    ):
        pipeline, args = registered_shellcmd_pipeline, interval_cli_arguments
        args += ["echo {:start:} {:stop:}"]
        result = interval_cli_process(args)
        assert result.returncode == 0, result.stdout + result.stderr
        before = sorted(pipeline.all(), key=lambda task: task.params["start"])
        assert len(before) == 3

        result = interval_cli_process(args)
        assert (
            result.returncode == 0
        ), f"STDOUT: '{result.stdout}', STDERR: '{result.stderr}'"
        assert sorted(pipeline.all(), key=lambda task: task.params["start"]) == before

    @m.context("When configuration cannot be read")
    @m.it("Exits with a failure")
    def test_missing_config(
        self, interval_cli_arguments, interval_cli_process, tmp_path
    ):
        result = interval_cli_process(
            interval_cli_arguments + ["--config", str(tmp_path / "missing.ini"), "echo"]
        )
        assert (
            result.returncode == 1
        ), f"STDOUT: '{result.stdout}', STDERR: '{result.stderr}'"
