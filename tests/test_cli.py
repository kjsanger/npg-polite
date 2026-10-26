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

import os
import subprocess
import sys
from uuid import uuid4

from pytest import mark as m

from npg_polite.porch import Pipeline, load_pipeline_config, resolve_task_class
from npg_polite.task import ShellCommandTask
from tests.conftest import make_pipeline_config_file


class TestPipelineAdmin:
    @m.context("When valid pipeline arguments and Porch configuration are supplied")
    @m.it("Resolves the task class and registers the configured pipeline")
    def test_register_pipeline_cli(
        self, porch_server_config, cli_wrapper_path, tmp_path
    ):
        env = os.environ.copy()
        for name in ("PORCH_URL", "PORCH_ADMIN_TOKEN", "PORCH_PIPELINE_TOKEN"):
            env.pop(name, None)

        config_file = make_pipeline_config_file(
            tmp_path / "pipeline_config.ini",
            f"test-register-cli-{uuid4().hex}",
            "https://example.com/pipelines/shell",
            "1.1.2",
            "npg_polite.task.ShellCommandTask",
        )
        result = subprocess.run(
            [
                sys.executable,
                (cli_wrapper_path / "register_pipeline.py").as_posix(),
                "--config",
                config_file.as_posix(),
                "--debug",
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert (
            result.returncode == 0
        ), f"STDOUT: '{result.stdout}', STDERR: '{result.stderr}'"

        config = load_pipeline_config(config_file.as_posix())

        # Connect to the pipeline created by the CLI without registering it again.
        task_class = resolve_task_class(config.task)
        pipeline = Pipeline(
            task_class, config=config, server_config=porch_server_config
        )
        porch_server_config.pipeline_token = pipeline.new_token("CLI registration test")

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
        self, porch_server_config, cli_wrapper_path, tmp_path
    ):
        env = os.environ.copy()
        for name in ("PORCH_URL", "PORCH_ADMIN_TOKEN", "PORCH_PIPELINE_TOKEN"):
            env.pop(name, None)

        config_file = make_pipeline_config_file(
            tmp_path / "pipeline_config.ini",
            f"test-register-cli-{uuid4().hex}",
            "https://example.com/pipelines/shell",
            "1.1.2",
            "npg_polite.task.ShellCommandTask",
        )

        config = load_pipeline_config(config_file.as_posix())
        task_class = resolve_task_class(config.task)
        pipeline = Pipeline(
            task_class, config=config, server_config=porch_server_config
        )
        pipeline.register(update_config=True)

        result = subprocess.run(
            [
                sys.executable,
                (cli_wrapper_path / "new_pipeline_token.py").as_posix(),
                "--config",
                config_file.as_posix(),
                "--debug",
            ],
            env=env,
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
        assert token != pipeline.server_config.pipeline_token

        # Authenticate with the printed token instead of the registration token.
        pipeline.server_config.pipeline_token = token
        pipeline.server_config.admin_token = None

        task = ShellCommandTask("echo {:message:}", {"message": "CLI token test"})
        assert pipeline.add(task)
        assert pipeline.all() == [task]
