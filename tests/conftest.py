# -*- coding: utf-8 -*-
#
# Copyright © 2025, 2026 Genome Research Ltd. All rights reserved.
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
import socket
import subprocess
import sys
from collections.abc import Iterator
from configparser import ConfigParser
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from textwrap import dedent
from uuid import uuid4

import pytest
import requests
from requests import HTTPError

from npg_polite.porch import (
    Pipeline,
    load_pipeline_config,
    write_pipeline_config,
)
from npg_polite.task import DatetimeParam, IntervalBounds, ShellCommandTask

EXAMPLE_PIPELINE_CONFIG_FILE = Path(__file__).parent / "example_config.ini"
"""Example pipeline used in tests."""


def porch_server_available() -> bool:
    """Check if the test Porch server is available."""
    config = load_pipeline_config(EXAMPLE_PIPELINE_CONFIG_FILE)
    try:
        response = requests.request("GET", config.server_url, timeout=5)
        return response.status_code == 200
    except (requests.ConnectionError, HTTPError):
        return False
    except Exception:
        raise


@pytest.fixture(scope="session")
def cli_wrapper_path() -> Path:
    return Path(__file__).resolve().parent / "bin"


@pytest.fixture
def clean_porch_environment():
    """Return an environment with any externally set PORCH_* environment variables unset."""
    env = os.environ.copy()
    for name in ("PORCH_URL", "PORCH_ADMIN_TOKEN", "PORCH_PIPELINE_TOKEN"):
        env.pop(name, None)
    return env


@pytest.fixture
def example_pipeline_config() -> Pipeline.Config:
    """Return the example pipeline config, read from the INI file in the test
    directory and updated with an unique name to avoid clashes."""
    config = load_pipeline_config(EXAMPLE_PIPELINE_CONFIG_FILE)
    return replace(config, name=f"example-{uuid4().hex}")


@pytest.fixture
def shellcmd_pipeline_config_file(tmp_path) -> Path:
    """Provide a unique configuration for shell-command pipeline tests. The task name
    contains a UUID to ensure each test pipeline is distinct."""
    config_file: Path = tmp_path / "pipeline.ini"
    config = Pipeline.Config(
        name=f"shell-task-test-{uuid4().hex}",
        uri="https://example.com/shell-task-tests",
        version="1.0.0",
        task="npg_polite.task.ShellCommandTask",
        server_url="http://localhost:8081",
        admin_token="0" * 32,
    )
    write_pipeline_config(config, config_file)

    return config_file


@pytest.fixture
def registered_shellcmd_pipeline(
    shellcmd_pipeline_config_file,
) -> Pipeline[ShellCommandTask]:
    config = load_pipeline_config(shellcmd_pipeline_config_file)

    pipeline = Pipeline[ShellCommandTask](config=config).register(update_config=True)
    # Write the pipeline token back into the config for the tests to use
    write_pipeline_config(pipeline.config, shellcmd_pipeline_config_file)

    return pipeline


@pytest.fixture
def interval_cli_arguments(shellcmd_pipeline_config_file):
    return [
        "--config",
        shellcmd_pipeline_config_file.as_posix(),
        "--begin",
        "start=2026-01-21T00:00:00Z",
        "--end",
        "stop=2026-01-21T03:00:00+00:00",
        "--interval",
        "1h",
    ]


@pytest.fixture
def interval_cli_process(cli_wrapper_path, clean_porch_environment):
    """Run the interval CLI in a separate process with captured output."""

    def run(arguments, env=None):
        return subprocess.run(
            [
                sys.executable,
                str(cli_wrapper_path / "add_interval_tasks.py"),
                *arguments,
            ],
            env=clean_porch_environment if env is None else env,
            capture_output=True,
            text=True,
            timeout=30,
        )

    return run


@pytest.fixture
def interval_bounds() -> IntervalBounds:
    """Provide named UTC bounds spanning three hours for interval tasks."""
    return (
        DatetimeParam("start", datetime(2026, 1, 21, tzinfo=UTC)),
        DatetimeParam("stop", datetime(2026, 1, 21, 3, tzinfo=UTC)),
    )


@pytest.fixture
def utc_period_tasks() -> list[ShellCommandTask]:
    return [
        ShellCommandTask(
            "test {:start:} != {:end:}",
            {
                "start": f"2026-01-{day:02d}T00:00:00Z",
                "end": f"2026-01-{day + 1:02d}T00:00:00Z",
            },
        )
        for day in range(21, 25)
    ]


@pytest.fixture
def timeout_child() -> Iterator[tuple[socket.socket, Path, Path]]:
    """Provide a socket and a child script that holds its connection until exit and
    ignores SIGTERM."""
    # Unix socket paths have a small length limit; avoid nested pytest paths.
    with TemporaryDirectory(prefix="polite-", dir="/tmp") as directory:
        socket_path = Path(directory) / "child.sock"
        script = Path(directory) / "child.py"
        script.write_text(dedent("""\
                import signal
                import socket
                import sys

                signal.signal(signal.SIGTERM, signal.SIG_IGN)
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
                    connection.connect(sys.argv[1])
                    connection.sendall(b"R") # Send 'R' for ready
                    
                    # Closing the test's connection also releases a surviving child.
                    while connection.recv(1):
                        pass
                """))

        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
            listener.bind(str(socket_path))
            listener.listen(1)
            listener.settimeout(2)
            yield listener, script, socket_path


def make_pipeline_config_file(
    path: Path, name: str, uri: str, version: str, task: str
) -> Path:
    """Write a pipeline INI configuration with the test server settings.

    Args:
        path: The destination file, typically inside pytest's tmp_path.
        name: The pipeline name.
        uri: The pipeline URI.
        version: The pipeline version.
        task: The fully qualified task class name.

    Returns:
        The path to the configuration file.
    """
    config = ConfigParser(interpolation=None)
    with EXAMPLE_PIPELINE_CONFIG_FILE.open(encoding="utf-8") as source:
        config.read_file(source)

    with path.open("w", encoding="utf-8") as destination:
        config.write(destination)
    return path
