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

"""Provide reusable tasks for trusted shell commands."""

import math
import os
import re
import shlex
import signal
import subprocess
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, Self

from structlog import get_logger

from npg_polite.porch import Task

SHELL_VALUE_PATTERN = re.compile(r"[a-zA-Z_][a-zA-Z0-9_]*")
"""Regex for the permitted values that can be inserted into shell commands via placeholders."""

PLACEHOLDER_PATTERN = re.compile(r"\{\{?:|:}}?")
""""Regex for shell command placeholders."""

TOKEN_PATTERN = r"\{\{:(?P<escaped>[a-zA-Z_][a-zA-Z0-9_]*):\}\}|\{:(?P<name>[a-zA-Z_][a-zA-Z0-9_]*):\}"
"""Regex for populated shell command placeholders, including the surrounding double-braces."""
TOKEN_ESCAPED_GROUP = "escaped"
TOKEN_NAME_GROUP = "name"

KILL_PROCESS_TIMEOUT: int = 3
"""Timeout for how long we wait for shell processes killed with SIGTERM before escalating to SIGKILL."""


def logger():
    return get_logger(__name__)


class ShellCommandTask(Task):
    """Represent a trusted Bash command with separately bound string parameters.

        Example: ::

            from npg_polite.task import ShellCommandTask

            task = ShellCommandTask(
                command="process-data --from {:start:} --to {:end:}",
                params={
                    "start": "2026-09-24T00:00:00Z",
                    "end": "2026-09-25T00:00:00Z",
                },
            )

    Parameters are interpolated into placeholders of the form `{:name:}` where the
    literal `name` corresponds to its name. No whitespace is permitted between the
    `{:` and `:}`  delimiters.

    Parameter names must match the regular expression `[A-Za-z_][A-Za-z0-9_]*`
    and values must be strings. Each value is quoted using `shlex.quote()` and then
    inserted into the template, replacing the placeholder.

    Rules for parameters and placeholders:

    Placeholders must not be quoted: write `--from {:start:}`, rather than
    `--from "{:start:}"`.

    Supply a complete path as a parameter rather than embedding a placeholder in a path.

    Do not insert placeholders into nested shell source; pass them as arguments instead,
    e.g. `bash -c 'printf "%s" "$1"' worker {:value:}`. The renderer does not parse
    Bash to enforce these placement rules. Better still, do not do this if you can help
    it.

    Use doubled outer braces to escape a placeholder: `{{:name:}}` renders as
    literal `{:name:}`. E.g. `printf '%s' '{{:name:}}'` prints the literal placeholder.
    Other shell braces, including `${variable}` and `{a,b}`, remain unchanged.

    Escaped placeholders and substituted values are never processed a second time.

    Malformed placeholders and missing parameters raise `ValueError` during construction
    or deserialization.

    Python formatting expressions, conversions, and format specifications are not
    supported.

    Unused parameters are allowed.

    Parameters are copied and exposed read-only.

    Commands and values may not contain NUL characters.

    Serialization returns a fresh dictionary, excluding lifecycle state and results.

    A minimal caller processes one claimed task as follows:

    Example: ::

        for task in pipeline.claim(num=1):
            task = pipeline.run(task)  # Confirm RUNNING before launching the command.
            try:
                code = task.execute(timeout=3600, cwd="/path/to/job")
            except Exception:
                pipeline.fail(task)
                raise
            else:
                if code == 0:
                    pipeline.done(task)
                else:
                    pipeline.fail(task)

    The `execute()` call is synchronous and does not change task status itself. Note that
    the above example does not handle errors raised while reporting task state changes
    to Porch (`fail()`, `done()`), which should be addressed by a caller in practice.

    Execution uses `/bin/bash` with unset-variable checking and `pipefail`, with STDOUT
    and STDERR inherited. Compound scripts must propagate other failures explicitly
    (e.g. using `&&`); otherwise an earlier failing command followed by a successful one
    can produce exit code zero.

    A finite, positive timeout is required. If you don't know how long the command
    should take, use a timeout at which you'd want to investigate hung jobs.

    On timeout the process group receives SIGTERM, and the shell gets up to
    KILL_PROCESS_TIMEOUT seconds to exit before any remaining group members receive
    SIGKILL.

    Jobs must remain in the foreground and must not detach from the process group.

    Task execution environment:

    The child environment inherits deployment settings, overrides `TZ` with `UTC`,
    and disables the `BASH_ENV` startup hook. The caller controls the working
    directory and installed executable versions. Use consistent deployment settings
    across retries.

    Task identity:

    Changing command text, parameters, values, or pipeline identity/version can create
    a new task for work that has already been processed. Keep these stable for ordinary
    retries. Timeout and working directory are execution options, not part of task
    identity.

    Task deduplication (by Porch) does not guarantee once-only command effects. A
    command may finish successfully, yet its task may crash before reporting back to
    Porch. Commands need their own idempotence or transactional completion records to
    tolerate retries. Investigate stranded `CLAIMED`/`RUNNING` tasks before re-queueing
    them.

    Security:

    Templates and placeholder values are trusted executable code. Do not insert
    untrusted values. Keep secrets out of both templates and values because commands
    may be logged.
    """

    _command: str
    _params: MappingProxyType[str, str]

    def __init__(self, command: str, params: Mapping[str, str] | None = None):
        super().__init__(Task.Status.PENDING)

        if "\0" in command:
            raise ValueError(f"Invalid command '{command}'; contains NUL characters")

        values = dict(params) if params is not None else {}
        for name, value in values.items():
            if not isinstance(name, str):
                raise ValueError(f"Invalid parameter name '{name}'; is not a string")
            if not isinstance(value, str):
                raise ValueError(f"Invalid parameter value '{value}'; is not a string")

            if "\0" in name:
                raise ValueError(
                    f"Invalid parameter name '{name}'; contains NUL characters"
                )
            if "\0" in value:
                raise ValueError(
                    f"Invalid parameter value '{value}'; contains NUL characters"
                )

            if not re.fullmatch(SHELL_VALUE_PATTERN, name):
                raise ValueError(
                    f"Invalid parameter name '{name}'; is not a valid shell identifier"
                )

        self._command = command
        self._params = MappingProxyType(values)
        self.render_command()  # Validate templates before they can be submitted.

    @property
    def command(self) -> str:
        """Return the command template."""
        return self._command

    @property
    def params(self) -> Mapping[str, str]:
        """Return the read-only parameters."""
        return self._params

    def to_serializable(self) -> dict[str, Any]:
        """Return the command and parameters as JSON-serializable task input."""
        return {"command": self.command, "params": dict(self.params)}

    @classmethod
    def from_serializable(cls, serializable: dict[str, Any]) -> Self:
        """Create a task from its serialized input."""
        return cls(**serializable)

    def render_command(self) -> str:
        """Render placeholders as shell-quoted values into a shell command template.

        See the class documentation for supported placeholder names and values.

        Raises:
            ValueError: If a marker is malformed or a parameter is missing.
        """
        parts: list[str] = []
        position = 0
        while placeholder := re.search(PLACEHOLDER_PATTERN, self.command[position:]):
            start = position + placeholder.start()
            parts.append(self.command[position:start])
            token = re.match(TOKEN_PATTERN, self.command[start:])
            if token is None:
                raise ValueError(f"Malformed command placeholder at offset {start}")

            if token.group(TOKEN_ESCAPED_GROUP) is not None:
                parts.append("{:" + token.group(TOKEN_ESCAPED_GROUP) + ":}")
            else:
                name = token.group(TOKEN_NAME_GROUP)
                if name not in self.params:
                    raise ValueError(f"Missing command parameter: '{name}'")
                parts.append(shlex.quote(self.params[name]))

            position = start + token.end()
        parts.append(self.command[position:])

        return "".join(parts)

    def execute(
        self, *, timeout: float | int, cwd: str | os.PathLike[str] | None = None
    ) -> int:
        """Execute the command synchronously and return its exit code.

        Use Bash with unset-variable checking and pipefail, inheriting STDOUT and
        STDERR. The child environment uses UTC.

        Other environment settings are inherited, except Bash's startup file hook.
        On timeout or interruption, terminate the process group, allow the shell up to
        KILL_PROCESS_TIMEOUT seconds to exit, then kill remaining group members.
        Commands must not detach from the process group.

        Args:
            timeout: Maximum execution time in seconds; must be finite and positive.
            cwd: Working directory, defaulting to the caller's working directory.

        Returns:
            The exit code of the command.

        Raises:
            ValueError: If the timeout is not a positive number.
            OSError: If the process cannot be started.
            subprocess.TimeoutExpired: If the command exceeds the timeout.
        """
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError(
                f"Invalid execution timeout '{timeout}'; the value must be a positive number"
            )

        env = {key: value for key, value in os.environ.items() if key != "BASH_ENV"}
        env["TZ"] = "UTC"
        command = self.render_command()
        process_args = [
            "/bin/bash",
            "--noprofile",
            "--norc",
            "-u",
            "-o",
            "pipefail",
            "-c",
            command,
        ]

        process = subprocess.Popen[bytes](
            process_args, cwd=cwd, env=env, start_new_session=True
        )
        try:
            pid = process.pid
            logger().info(
                "Shell task started",
                command=command,
                cwd=cwd,
                timeout=timeout,
                pid=pid,
            )
            logger().debug("Shell task environment", pid=pid)

            code = process.wait(timeout=timeout)
            logger().info(
                "Shall task completed", command=command, pid=pid, exit_code=code
            )
            return code
        except Exception:
            self._terminate(command, process)
            raise

    @staticmethod
    def _terminate(command: str, process: subprocess.Popen[bytes]) -> None:
        """Terminate the process group and reap the shell."""
        log = logger().bind(command=command, pid=process.pid)

        log.debug("Shell task being terminated")

        try:
            os.killpg(process.pid, signal.SIGTERM)
            log.debug("Shell task killed with SIGTERM")
        except ProcessLookupError:
            log.debug("Shell task process not found for SIGTERM attempt")

        try:
            process.wait(timeout=KILL_PROCESS_TIMEOUT)
        except subprocess.TimeoutExpired:
            log.error("Shell task termination attempt timed out")
        finally:
            # Descendants may remain even if the shell has already exited.
            try:
                os.killpg(process.pid, signal.SIGKILL)
                log.debug("Shell task killed with SIGKILL")
            except ProcessLookupError:
                log.debug("Shell task process not found for SIGKILL attempt")

            process.wait()
            log.debug("Shell task exited")
