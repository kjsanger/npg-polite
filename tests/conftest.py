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
from pathlib import Path
from tempfile import TemporaryDirectory
from textwrap import dedent
from uuid import uuid4

import pytest
import requests
from npg.conf import IniData
from requests import HTTPError

from npg_polite.porch import Pipeline
from npg_polite.task import ShellCommandTask

TEST_CONFIG_FILE = Path("./tests/polite_config.ini")

TEST_CONFIG_SECTION = "PORCH"


def porch_server_available() -> bool:
    """Check if the test Porch server is available."""
    config = IniData(Pipeline.ServerConfig).from_file(
        TEST_CONFIG_FILE, TEST_CONFIG_SECTION
    )
    try:
        response = requests.request("GET", config.url, timeout=5)
        return response.status_code == 200
    except (requests.ConnectionError, HTTPError):
        return False
    except Exception:
        raise


@pytest.fixture(scope="function")
def porch_server_config() -> Pipeline.ServerConfig:
    return IniData(Pipeline.ServerConfig).from_file(
        TEST_CONFIG_FILE, TEST_CONFIG_SECTION
    )


@pytest.fixture
def pipeline(porch_server_config) -> Pipeline[ShellCommandTask]:
    return Pipeline(
        ShellCommandTask,
        name=f"shell-task-test-{uuid4().hex}",
        uri="https://example.com/shell-task-tests",
        version="1.0.0",
        config=porch_server_config,
    ).register(update_config=True)


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
