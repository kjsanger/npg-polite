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


import json
import operator
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from socket import socket
from unittest.mock import Mock
from uuid import uuid4

import pytest
from pytest import mark as m, raises

from npg_polite.porch import Pipeline, Task
from npg_polite.task import (
    DatetimeParam,
    IntervalBounds,
    ShellCommandTask,
    make_interval_tasks,
)
from tests.conftest import porch_server_available


@m.describe("Shell command task input")
class TestShellCommandTask:
    @m.context("When serializing task input")
    @m.it("Round-trips through JSON without including lifecycle state")
    def test_round_trip(self, utc_period_tasks):
        task = utc_period_tasks[0]
        task.status = Task.Status.DONE
        restored = ShellCommandTask.from_serializable(
            json.loads(json.dumps(task.to_serializable()))
        )

        assert restored == task
        assert restored.status == Task.Status.PENDING
        assert restored != utc_period_tasks[1]
        assert restored != ShellCommandTask("true", task.params)

        with raises(TypeError):
            hash(task)

    @m.context("When callers modify supplied or serialized parameters")
    @m.it("Preserves the original task input")
    def test_defensive_copy(self):
        params = {"value": "original"}
        task = ShellCommandTask('printf "%s" {:value:}', params)
        params["value"] = "changed"
        task.to_serializable()["params"]["value"] = "also changed"

        assert task.params == {"value": "original"}

        # Use dynamic access to exercise operations forbidden by the public types.
        with raises(TypeError):
            operator.setitem(getattr(task, "params"), "value", "changed")
        with raises(AttributeError):
            setattr(task, "command", "false")

    @m.context("When task input is invalid")
    @m.it("Rejects it before execution or submission")
    @m.parametrize(
        "command, params, error",
        [
            (None, {}, TypeError),
            ("true\0", {}, ValueError),
            ("true", {"bad-name": "value"}, ValueError),
            ("true", {"1name": "value"}, ValueError),
            ("true", {"": "value"}, ValueError),
            ("true", {1: "value"}, ValueError),
            ("true", {"name": 1}, ValueError),
            ("true", {"name": "bad\0value"}, ValueError),
        ],
    )
    def test_invalid_input(self, command, params, error):
        with raises(error):
            ShellCommandTask(command, params)

    @m.context("When rendering repeated and escaped placeholders")
    @m.it("Quotes values once and preserves the serialized template")
    def test_render(self):
        command = "printf '%s' {:value:} {:value:} '{{:missing:}}'"
        task = ShellCommandTask(command, {"value": "a b", "unused": "allowed"})
        original = task.to_serializable()

        assert task.render_command() == "printf '%s' 'a b' 'a b' '{:missing:}'"
        assert task.to_serializable() == original
        assert task.command == command

        restored = ShellCommandTask.from_serializable(original)
        assert restored.render_command() == task.render_command()

    @m.context("When a template has invalid or missing placeholders")
    @m.it("Rejects construction and deserialization without exposing Python formatting")
    @m.parametrize(
        "marker",
        [
            "{:missing:}",
            "{: value :}",
            "{:value :}",
            "{: value:}",
            "{::}",
            "{:1value:}",
            "{:value",
            "value:}",
            "{{:value:}",
            "{{: value :}}",
            "{:value.attr:}",
            "{:value[0]:}",
            "{:value!r:}",
            "{:value:>8:}",
        ],
    )
    def test_invalid_template(self, marker):
        data = {"command": f"echo {marker}", "params": {"value": "present"}}

        with raises(ValueError):
            ShellCommandTask(**data)
        with raises(ValueError):
            ShellCommandTask.from_serializable(data)

    @m.context("When templates contain ordinary shell constructs")
    @m.it("Leaves braces and shell variable expansions intact")
    def test_shell_braces(self):
        command = "{ printf '%s' ${HOME} ${missing:-fallback} {a,b}; }"
        assert ShellCommandTask(command).render_command() == command


@m.describe("Shell command execution")
class TestShellExecution:
    @m.context("When a template escapes a placeholder")
    @m.it("Passes the literal marker to the command without parameter lookup")
    def test_literal_marker(self, capfd):
        task = ShellCommandTask("printf '%s' '{{:missing:}}'")

        assert task.execute(timeout=5) == 0
        assert "{:missing:}" in capfd.readouterr().out

    @m.context("When parameters contain shell syntax")
    @m.it("Passes literal values without executing them")
    @m.parametrize(
        "value",
        [
            "",
            "two words",
            "'quoted' \"value\"",
            "line one\nline two",
            "* ? [abc]",
            "back\\slash",
            "{:other:} {{:other:}}",
            "$(touch injected); `touch injected` & | >",
        ],
    )
    def test_literal_values(self, tmp_path, value):
        output = tmp_path / "output"
        injected = tmp_path / "injected"

        task = ShellCommandTask('printf "%s" {:value:} > output', {"value": value})

        assert task.execute(timeout=5, cwd=tmp_path) == 0
        assert output.read_text() == value
        assert not injected.exists()
        assert task.status == Task.Status.PENDING

    @m.context("When commands write to stdout and stderr")
    @m.it("Inherits the caller's output streams")
    def test_output(self, capfd):
        # If test logs are enabled, there may be additional noise in the outputs, so
        # look for these UUIDs
        out_uuid = str(uuid4())
        err_uuid = str(uuid4())
        assert (
            ShellCommandTask(f"printf {out_uuid}; printf {err_uuid} >&2").execute(
                timeout=5
            )
            == 0
        )

        output = capfd.readouterr()
        assert out_uuid in output.out
        assert err_uuid in output.err

    @m.context("When the parent environment supplies variables or local time")
    @m.it("Preserves ordinary variables and runs in UTC without Bash startup hooks")
    def test_environment(self, tmp_path, monkeypatch):
        startup = tmp_path / "startup"
        startup.write_text("exit 99\n")

        monkeypatch.setenv("BASH_ENV", str(startup))
        monkeypatch.setenv("TASK_stale", "stale")
        monkeypatch.setenv("TASK_value", "inherited")
        monkeypatch.setenv("TZ", "Europe/London")

        task = ShellCommandTask(
            'test "$TASK_stale" = stale && test "$TASK_value" = inherited && test {:value:} = bound && test "$TZ" = UTC',
            {"value": "bound"},
        )
        assert task.execute(timeout=5) == 0

    @m.context("When a command or pipeline fails")
    @m.it("Returns a nonzero exit code")
    @m.parametrize("command", ["exit 7", "false | true", 'printf "%s" "$TASK_missing"'])
    def test_failure(self, command):
        assert ShellCommandTask(command).execute(timeout=5) != 0

    @m.context("When the process cannot start")
    @m.it("Propagates the launch error")
    def test_launch_error(self, tmp_path):
        missing = tmp_path / "missing"

        with raises(FileNotFoundError):
            ShellCommandTask("true").execute(timeout=5, cwd=missing)

    @m.context("When the timeout is invalid")
    @m.it("Rejects it before launching the shell")
    @m.parametrize("timeout", [0, -1, float("nan")])
    def test_invalid_timeout(self, timeout):
        with raises(ValueError):
            ShellCommandTask("true").execute(timeout=timeout)

    @m.context("When a command exceeds its timeout")
    @m.it("Kills descendants even when they ignore termination")
    def test_timeout(self, timeout_child: tuple[socket, Path, Path]):
        listener, script, socket_path = timeout_child

        # Run the script provided by the timeout_child fixture in the background and wait
        task = ShellCommandTask(
            "{:python:} {:script:} {:socket:} & wait",
            {
                "python": sys.executable,
                "script": str(script),
                "socket": str(socket_path),
            },
        )

        with pytest.raises(subprocess.TimeoutExpired):
            task.execute(timeout=1)  # The timeout should expire and the task be killed

        # After the task is killed, accept the queued socket and check that the child
        # is no longer there.
        connection, _ = listener.accept()
        ready_byte = b"R"  # Flush any buffered "ready" byte
        with connection:
            connection.settimeout(2)
            assert connection.recv(1) == ready_byte, "Child did not signal readiness"
            assert connection.recv(1) == b"", "Child connection is still open"


@m.describe("Shell task execution with Porch")
@m.skipif(not porch_server_available, reason="Test Porch server is not available")
class TestShellCommandPorch:
    @m.context("When producers submit overlapping sets of UTC periods")
    @m.it("Creates only missing tasks and prevents concurrent duplicate claims")
    def test_overlapping_periods(
        self,
        pipeline: Pipeline[ShellCommandTask],
        utc_period_tasks: list[ShellCommandTask],
    ):
        assert [pipeline.add(task) for task in utc_period_tasks[:3]] == [
            True,
            True,
            True,
        ], "Add three new tasks as one list"
        assert [pipeline.add(task) for task in utc_period_tasks[1:]] == [
            False,
            False,
            True,
        ], "Add one new task in an overlapping list"
        assert len(pipeline.all()) == 4

        with ThreadPoolExecutor(max_workers=2) as workers:
            batches: list[list[ShellCommandTask]] = list(
                workers.map(lambda _: pipeline.claim(4), range(2))
            )

        claimed: list[ShellCommandTask] = [task for batch in batches for task in batch]
        assert len(claimed) == 4

        for expected in utc_period_tasks:
            assert claimed.count(expected) == 1

        for task in claimed:
            task = pipeline.run(task)
            assert task.execute(timeout=5) == 0
            pipeline.done(task)

        assert len(pipeline.succeeded()) == 4
        assert not pipeline.add(utc_period_tasks[0])
        assert pipeline.claim() == []

    @m.context("When a failed command is retried")
    @m.it("Preserves the original period and executes after an explicit retry")
    def test_retry(
        self,
        pipeline: Pipeline[ShellCommandTask],
        utc_period_tasks: list[ShellCommandTask],
        tmp_path,
    ):
        ready = tmp_path / "ready"
        output = tmp_path / "output"

        task = ShellCommandTask(
            'test -f {:ready:} && printf "%s/%s" {:start:} {:end:} > {:output:}',
            dict(
                utc_period_tasks[0].params,
                ready=ready.as_posix(),
                output=output.as_posix(),
            ),
        )
        original_input = task.to_serializable()
        assert pipeline.add(task)

        claimed = pipeline.run(pipeline.claim()[0])
        assert claimed.execute(timeout=5) != 0

        pipeline.fail(claimed)
        assert pipeline.failed() == [task]
        assert not pipeline.add(task)
        assert pipeline.claim() == []

        ready.touch()
        pipeline.retry(pipeline.failed()[0])
        retried = pipeline.run(pipeline.claim()[0])
        assert retried.to_serializable() == original_input
        assert retried.execute(timeout=5) == 0

        pipeline.done(retried)
        assert pipeline.succeeded() == [task]
        assert output.read_text() == f"{task.params['start']}/{task.params['end']}"


@m.describe("Interval task creation")
class TestIntervalTasks:
    @m.context("When the interval fits exactly within the overall time span")
    @m.it("Creates the expected number of complete interval tasks")
    @m.parametrize("hours, boundaries", [(1, [0, 1, 2, 3]), (3, [0, 3])])
    def test_whole_interval_creation(
        self, interval_bounds: IntervalBounds, hours, boundaries
    ):
        begin, end = interval_bounds
        command = "echo {:start:} {:stop:}"

        tasks = make_interval_tasks(command, begin, end, timedelta(hours=hours), None)

        assert len(tasks) == len(boundaries) - 1
        assert [task.params for task in tasks] == [
            {
                "start": (begin.datetime + timedelta(hours=start)).isoformat(),
                "stop": (begin.datetime + timedelta(hours=stop)).isoformat(),
            }
            for start, stop in zip(boundaries, boundaries[1:])
        ]
        assert all(task.command == command for task in tasks)
        assert all(task.status == Task.Status.PENDING for task in tasks)

    @m.context("When the interval does not fit exactly within the overall time span")
    @m.it(
        "Creates the expected number of complete interval tasks, plus a partial interval"
    )
    @m.parametrize("hours, boundaries", [(2, [0, 2, 3]), (4, [0, 3])])
    def test_partial_interval_creation(
        self, interval_bounds: IntervalBounds, hours, boundaries
    ):
        begin, end = interval_bounds

        tasks = make_interval_tasks(
            "echo {:start:} {:stop:}", begin, end, timedelta(hours=hours), None
        )

        assert len(tasks) == len(boundaries) - 1
        assert [task.params for task in tasks] == [
            {
                "start": (begin.datetime + timedelta(hours=start)).isoformat(),
                "stop": (begin.datetime + timedelta(hours=stop)).isoformat(),
            }
            for start, stop in zip(boundaries, boundaries[1:])
        ]

    @m.context("When shared parameters are supplied")
    @m.it("Creates tasks that all have those parameters")
    def test_shared_parameters(self, interval_bounds: IntervalBounds):
        begin, end = interval_bounds
        shared = {"dataset": "run 42", "output": "/tmp/results"}
        command = "echo {:start:} {:stop:} {:dataset:} {:output:}"

        tasks = make_interval_tasks(command, begin, end, timedelta(hours=1), shared)

        assert len(tasks) == 3
        assert shared == {"dataset": "run 42", "output": "/tmp/results"}
        for hour, task in enumerate(tasks):
            assert task.command == command
            assert task.params == {
                "start": (begin.datetime + timedelta(hours=hour)).isoformat(),
                "stop": (begin.datetime + timedelta(hours=hour + 1)).isoformat(),
                "dataset": "run 42",
                "output": "/tmp/results",
            }

    @m.context("When a parameter factory is supplied")
    @m.it("Creates tasks that have the expected dynamically generated parameters")
    def test_parameter_factory(self, interval_bounds: IntervalBounds):
        begin, end = interval_bounds
        calls: list[tuple[DatetimeParam, DatetimeParam]] = []

        def make_params(start: DatetimeParam, stop: DatetimeParam):
            calls.append((start, stop))
            return {
                "log": f"{start.datetime:%Y%m%dT%H%M}.log",
                "seconds": str(int((stop.datetime - start.datetime).total_seconds())),
            }

        command = "echo {:start:} {:stop:} {:dataset:} {:log:} {:seconds:}"
        tasks = make_interval_tasks(
            command,
            begin,
            end,
            timedelta(hours=2),
            {"dataset": "run 42"},
            make_params,
        )

        assert len(tasks) == 2
        midpoint = begin.datetime + timedelta(hours=2)
        assert [task.params for task in tasks] == [
            {
                "start": begin.datetime.isoformat(),
                "stop": midpoint.isoformat(),
                "dataset": "run 42",
                "log": "20260121T0000.log",
                "seconds": "7200",
            },
            {
                "start": midpoint.isoformat(),
                "stop": end.datetime.isoformat(),
                "dataset": "run 42",
                "log": "20260121T0200.log",
                "seconds": "3600",
            },
        ]
        assert calls == [
            (begin, DatetimeParam("stop", midpoint)),
            (DatetimeParam("start", midpoint), end),
        ]

    @m.context("When shared or dynamic parameter names clash with existing parameters")
    @m.it("Raises ValueError at task construction")
    @m.parametrize(
        "shared, dynamic, name, source",
        [
            ({"start": "replacement"}, None, "start", "shared"),
            ({"stop": "replacement"}, None, "stop", "shared"),
            (None, {"start": "replacement"}, "start", "dynamic"),
            (None, {"stop": "replacement"}, "stop", "dynamic"),
            ({"dataset": "shared"}, {"dataset": "dynamic"}, "dataset", "dynamic"),
        ],
    )
    def test_parameter_name_clash(
        self, interval_bounds: IntervalBounds, shared, dynamic, name, source
    ):
        begin, end = interval_bounds
        factory = None if dynamic is None else Mock(return_value=dynamic)

        with raises(ValueError, match=f"Name clash between '{name}'.*{source}"):
            make_interval_tasks(
                "echo {:start:} {:stop:}",
                begin,
                end,
                timedelta(hours=1),
                shared,
                factory,
            )
