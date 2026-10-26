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

import socket
from collections.abc import Iterator
from configparser import ConfigParser
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from textwrap import dedent
from uuid import uuid4

import pytest
import requests
from npg.conf import IniData
from requests import HTTPError

from npg_polite.porch import PIPELINE_CONFIG_SECTION, Pipeline, SERVER_CONFIG_SECTION
from npg_polite.task import DatetimeParam, IntervalBounds, ShellCommandTask

TEST_SERVER_CONFIG_FILE = Path("./tests/polite_config.ini")
TEST_PIPELINE_CONFIG_FILE = Path("./tests/example_config.ini")


def porch_server_available() -> bool:
    """Check if the test Porch server is available."""
    config = IniData(Pipeline.ServerConfig).from_file(
        TEST_SERVER_CONFIG_FILE, SERVER_CONFIG_SECTION
    )
    try:
        response = requests.request("GET", config.url, timeout=5)
        return response.status_code == 200
    except (requests.ConnectionError, HTTPError):
        return False
    except Exception:
        raise


@pytest.fixture(scope="session")
def cli_wrapper_path() -> Path:
    return Path(__file__).resolve().parent / "bin"


@pytest.fixture(scope="function")
def porch_server_config() -> Pipeline.ServerConfig:
    return IniData(Pipeline.ServerConfig).from_file(
        TEST_SERVER_CONFIG_FILE, SERVER_CONFIG_SECTION
    )


@pytest.fixture
def registered_shellcommand_pipeline(porch_server_config) -> Pipeline[ShellCommandTask]:
    config = Pipeline.Config(
        name=f"shell-task-test-{uuid4().hex}",
        uri="https://example.com/shell-task-tests",
        version="1.0.0",
        task="npg_polite.task.ShellCommandTask",
    )

    return Pipeline(
        ShellCommandTask, config=config, server_config=porch_server_config
    ).register(update_config=True)


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
    with TEST_SERVER_CONFIG_FILE.open(encoding="utf-8") as source:
        config.read_file(source)
    config[PIPELINE_CONFIG_SECTION] = {
        "name": name,
        "uri": uri,
        "version": version,
        "task": task,
    }
    with path.open("w", encoding="utf-8") as destination:
        config.write(destination)
    return path
