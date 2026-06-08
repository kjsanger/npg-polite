#
# Copyright © 2024, 2026 Genome Research Ltd. All rights reserved.
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

import http
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Self
from urllib.parse import urljoin

import requests
from structlog import get_logger


def logger():
    return get_logger(__package__)


"""This module provides a task-centric API for interacting with a Porch server. It
hides the details sending requests to and receiving responses from the Porch server.
For a request/response-centric API, see https://github.com/wtsi-npg/npg_porch_cli
"""


class Task(ABC):
    """A Porch task i.e. an instance of a pipeline to be executed.

    The identity of a Porch task is defined by the pipeline name and version, plus
    the attributes and values of the task input.

    To define a new kind of Task, you need to create a subclass of 'Task'
    and implement a 'to_serializable' method, which returns the Porch task input,
    and a 'from_serializable' method which converts the same JSON back into a task
    object.

    E.g.
        class MyTask(Task):
            input1: str
            input2: int

            def __init__(self, input1: str = None, input2: int = None):
                self.input1 = input1
                self.input2 = input2

            def to_serializable(self) -> dict:
                return {
                    "input1": self.input1,
                    "input2": self.input2,
                }

            @classmethod
            def from_serializable(cls, serializable: dict):
                return cls(**serializable)

    """

    class Status(str, Enum):
        """The status of a task."""

        PENDING = "PENDING"
        CLAIMED = "CLAIMED"
        RUNNING = "RUNNING"
        DONE = "DONE"
        CANCELLED = "CANCELLED"
        FAILED = "FAILED"

    """The current status of the task."""
    status: Status

    def __init__(self, status: Status):
        if status is None:
            raise ValueError("status cannot be None")
        self.status = status

    def __eq__(self, other):
        if not isinstance(other, Task):
            return False

        return self.to_serializable() == other.to_serializable()

    def __hash__(self):
        return hash(self.to_serializable())

    @abstractmethod
    def to_serializable(self) -> dict:
        """Return a JSON-serializable dictionary of the task input."""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def from_serializable(cls, serializable: dict):
        """Create a new task from a JSON-serializable dictionary."""
        raise NotImplementedError


class Pipeline[T: Task = Task]:
    """A Porch "pipeline".

    A Porch pipeline is type of pub/sub queue where tasks are added by one process and
    later claimed and processed by another. The identity of a Porch pipeline is defined
    by the pipeline name, URI and version.

    When a new pipeline is created, it must be registered with the Porch server before
    tasks can be added to it. This is done using the `register` method. Once registered,
    a pipeline token must be obtained using the `new_token` method. This token is used
    to add, claim, and update tasks for this pipeline.

    A pipeline's `register` and `new_token` methods require an admin token. The other
    methods require a pipeline token.

    The identity of a Porch task is defined by the serializable task input; two task
    objects with the same inputs are considered the same task. Porch uses this to try
    to ensure that each task is created and processed once.

    To use this module for a new pipeline, you need to create a subclass of
    `Pipeline.Task` and implement the `to_serializable` and `from_serializable`
    methods. See the Porch documentation for more information on how the task
    attributes and values are serialized as JSON.

    For example:

        from porch import Pipeline

        class SumTask(Task):
            input1: int
            input2: int

            def __init__(self, input1: int = 0, input2: int = 0):
                super().__init__(Task.Status.PENDING)
                self.input1 = input1
                self.input2 = input2

            def to_serializable(self) -> dict:
                return {
                    "input1": self.input1,
                    "input2": self.input2,
                }

            def from_serializable(cls, serializable: dict):
                return cls(**serializable)

    When this is done, you can create a new pipeline and add tasks to it:

        p = Pipeline("Sum of two integers", "http://localhost/sum", "1.0.0")
        p = p.register()  # Needs to be done once

        tasks = [
            SumTask(10, 42),
            SumTask(10, 99),
        ]

        for task in tasks:
            p.add(task)

    Once tasks are added, you can claim and update their status as they are processed:

        claimed_task = p.claim()
        try:
            # Submit the task to a worker
            p.run(task)  # Tell the Porch server that the task is running
        except Exception as e:
            p.fail(task)  # Tell the Porch server that the task failed

    Porch will ensure that each task is created exactly once and that each task is
    claimed for processing once, by only one worker.

    This class uses the Porch REST API. See the Porch documentation for more
    information. It has a timeout of 10 seconds and will retry failed requests up to 3
    times with an exponential backoff starting at 15 seconds.

    Note: We could consider using only the first or first and second parts of the
    (SemVer) version number. This would allow bug-fix releases to be made that could
    re-run existing tasks for that version.
    """

    @dataclass
    class ServerConfig:
        """Configuration for a Porch pipeline server.

        This exists to collect external configuration for a pipeline in one place
        where it can be passed to the pipeline constructor.

        A configuration instance can be created directly and populated with tokens
        obtained from a secrets manager. E.g.

            config = ServerConfig(
                porch_url="https://example.com/porch",
                admin_token=token1,
                pipeline_token=token2,
            )

        Alternatively, configuration (and possibly tokens) can be read from a file.

        The token fields are set to not be included in the repr() output to avoid
        leaking sensitive information in logs.
        """

        url: str
        """The base URL of the Porch server."""

        pipeline_token: str = field(repr=False, default=None)
        admin_token: str = field(repr=False, default=None)

    def __init__(
        self, cls: type[T], name: str, uri: str, version: str, config: ServerConfig
    ):
        """Create a new pipeline with no pipeline access token set.

        Args:
            cls: The task class for this pipeline. Must be a subclass of Task.
            name: The pipeline name.
            uri: The pipeline URI.
            version: The pipeline version.
            config: The configuration for the pipeline server.
        """

        # Note that the task class is passed explicitly to the constructor because
        # it's not reliable (Python 3.12) to get the class parameter for a generic
        # type at runtime (using documented API) when using e.g.
        #
        #     p = Pipeline[ExampleTask](...)
        #
        # If Pipeline is subclassed e.g.
        #
        #     class FooPipeline(Pipeline[ExampleTask]):
        #         pass
        #
        #  p = FooPipeline(...)
        #
        # One can use:
        #
        #     cls = get_args(self.__orig_bases__[0])[0]
        #
        # However, I've not been able to get this to work without requiring a
        # subclass and that puts a burden on the user of the API to jump through
        # language hoops to get something that works.

        self.cls = cls
        self.name = name
        self.uri = uri
        self.version = version
        self.config = config

        self.timeout = 10

    def register(self, update_config=False) -> Self:
        """Register the pipeline with a Porch server.

        This needs to be done only once for each pipeline (i.e. unique name, URI, and
        version combination) and requires an admin token. If the pipeline already
        exists, this method will log a warning and return the existing pipeline.

        An admin token is required to use this method.

        Args:
            update_config: Update the config with the pipeline token if a new pipeline
                is created. Default is False.

        Returns:
            The pipeline object.
        """
        body = self._to_serializable()

        create_headers = self._headers(self.config.admin_token)
        create_response = self._request(
            "POST", self._pipeline_endpoint(), headers=create_headers, body=body
        )
        if create_response.status_code == 409:
            logger().info("A version of this pipeline already exists", pipeline=self)
        elif create_response.status_code != 201:
            create_response.raise_for_status()

        # The version attribute of the pipeline instance can't be created with the admin
        # token, unlike the other attributes. Instead, it requires a pipeline token,
        # which may not exist yet. If we haven't been provided one in the config, we
        # have to assume that there isn't one available yet and create a new one.
        if self.config.pipeline_token is None:
            pipeline_token = self.new_token(token_desc="pipeline creation event")
            if update_config:
                self.config.pipeline_token = pipeline_token
                logger().info(
                    "Updated the in-memory config with a new pipeline token",
                    pipeline=self,
                )
        else:
            pipeline_token = self.config.pipeline_token
            logger().debug(f"Using the pipeline token from config", pipeline=self)

        version_headers = self._headers(pipeline_token)
        version_response = self._request(
            "POST", self._version_endpoint(), headers=version_headers, body=body
        )
        if version_response.status_code == 409:
            logger().warn(
                f"The requested pipeline version already exists", pipeline=self
            )
            return self

        version_response.raise_for_status()
        logger().info("New pipeline created", pipeline=self)

        return self

    def new_token(self, token_desc: str) -> str:
        """Create a new token for the pipeline.

        This token is only valid for this pipeline and can be used to add and update
        tasks. The token should be stored securely. It cannot be obtained from the
        server again.

        An admin token is required to use this method.

        Args:
            token_desc: A description of the token's purpose.

        Returns:
            The token string.
        """
        url = urljoin(self._pipeline_endpoint(), f"{self.name}/token/{token_desc}")
        headers = self._headers(self.config.admin_token)

        response = self._request("POST", url, headers=headers)
        response.raise_for_status()
        logger().info("New pipeline token", pipeline=self.name, desc=token_desc)

        return response.json()["token"]

    def add(self, task: T) -> bool:
        """Add a new task for this pipeline. This method is idempotent, so adding the
        same task multiple times will not create duplicates.

        Args:
            task: The task to be queued, initially in the PENDING state.

        Returns:
            True if the task was added, False if it already exists.
        """
        url = self._task_endpoint()
        headers = self._headers(self.config.pipeline_token)
        body = self._to_serializable(task)

        response = self._request("POST", url, headers=headers, body=body)
        response.raise_for_status()
        logger().info("Task add", pipeline=self.name, task=task)

        return response.status_code == http.HTTPStatus.CREATED

    def all(self) -> list[T]:
        """Get all tasks for this pipeline."""
        return self._get_tasks()

    def pending(self) -> list[T]:
        """Get all tasks for this pipeline that are pending."""
        return self._get_tasks(status=Task.Status.PENDING)

    def claimed(self) -> list[T]:
        """Get all tasks for this pipeline that are claimed but not yet running."""
        return self._get_tasks(status=Task.Status.CLAIMED)

    def running(self) -> list[T]:
        """Get all tasks for this pipeline that are currently running."""
        return self._get_tasks(status=Task.Status.RUNNING)

    def succeeded(self) -> list[T]:
        """Get all tasks for this pipeline that have completed successfully."""
        return self._get_tasks(status=Task.Status.DONE)

    def cancelled(self) -> list[T]:
        """Get all tasks for this pipeline that have been cancelled."""
        return self._get_tasks(status=Task.Status.CANCELLED)

    def failed(self) -> list[T]:
        """Get all tasks for this pipeline that have failed."""
        return self._get_tasks(status=Task.Status.FAILED)

    def claim(self, num: int = 1) -> list[T]:
        """Claim a number of tasks for this pipeline.

        Args:
            num: The number of tasks to claim.

        Returns:
            The claimed tasks.
        """
        logger().info("Task claim", num=num)
        url = self._task_endpoint() + f"claim/?num_tasks={num}"
        headers = self._headers(self.config.pipeline_token)
        body = self._to_serializable()

        response = self._request("POST", url, headers=headers, body=body)
        response.raise_for_status()
        logger().debug("Claim response", response=response.json())

        claimed = [self._from_serializable(item) for item in response.json()]
        logger().info("Claimed tasks", claimed=claimed)

        return claimed

    def run(self, task: T) -> T:
        """Mark a task as running and return it. This should be called after claiming a task."""
        logger().info("Task run", pipeline=self, task=task)
        task.status = task.Status.RUNNING
        return self._update_task(task)

    def done(self, task: T) -> T:
        """Mark a task as done successfully and return it."""
        logger().info("Task done", pipeline=self, task=task)
        task.status = task.Status.DONE
        return self._update_task(task)

    def fail(self, task: T) -> T:
        """Mark a task as failed and return it."""
        logger().info("Task fail", pipeline=self, task=task)
        task.status = task.Status.FAILED
        return self._update_task(task)

    def cancel(self, task: T) -> T:
        """Mark a task as cancelled and return it."""
        logger().info("Task cancel", pipeline=self, task=task)
        task.status = task.Status.CANCELLED
        return self._update_task(task)

    def retry(self, task: T) -> T:
        """Mark a task as pending again and return it. This should be called after a task has
        succeeded, failed or been cancelled and needs to be retried or re-run."""
        logger().info("Task retry", pipeline=self, task=task)
        task.status = task.Status.PENDING
        return self._update_task(task)

    def _get_tasks(self, status: Task.Status | None = None) -> list[T]:
        """Get all tasks for this pipeline with an optional status filter."""
        url = self._task_endpoint() + f"?pipeline_name={self.name}"
        if status is not None:
            url += f"&status={status.value}"
        headers = self._headers(self.config.pipeline_token)

        response = self._request("GET", url, headers=headers)
        response.raise_for_status()

        return [self._from_serializable(item) for item in response.json()]

    def _update_task(self, task: T) -> T:
        """Update the status of a task and return it."""
        url = self._task_endpoint()
        headers = self._headers(self.config.pipeline_token)
        body = self._to_serializable(task)

        response = self._request("put", url, headers=headers, body=body)
        response.raise_for_status()

        return self._from_serializable(response.json())

    def _to_serializable(self, task: T | None = None) -> dict:
        """Convert task information to a JSON-serializable dictionary, ready to send
        to a Porch server."""
        pipeline = {
            "name": self.name,
            "uri": self.uri,
            "version": self.version,
        }

        if task is None:
            serializable = pipeline
        else:
            serializable = {
                "pipeline": pipeline,
                "task_input": task.to_serializable(),
                "status": task.status,
            }

        return serializable

    def _from_serializable(self, serializable: dict) -> T:
        """Create a task from a JSON-serializable dictionary received from a Porch
        server."""
        task = self.cls.from_serializable(serializable["task_input"])
        status = serializable["status"]

        if status not in Task.Status.__members__:
            raise ValueError(f"Invalid task status from server: {status}")
        task.status = status
        return task

    def _request(self, method: str, url: str, headers: dict, body: dict | None = None):
        """Make an HTTP request to a Porch server.

        This method will retry the request up to 3 times with an exponential backoff
        starting at 15 seconds. If the request fails after 3 attempts, the last error
        will be raised.
        """
        num_attempts = 3
        last_error = None
        wait = 15

        for attempt in range(num_attempts):
            logger().debug("Request", method=method, url=url, body=body)
            try:
                response = requests.request(
                    method, url, headers=headers, json=body, timeout=self.timeout
                )
                logger().debug(
                    "Response", status_code=response.status_code, attempt=attempt
                )

                return response
            except Exception as e:
                last_error = e
                logger().error(
                    "Request failed", error=str(e), attempt=attempt, waiting=wait
                )
                time.sleep(wait)
                wait *= 2

        raise last_error

    def _pipeline_endpoint(self) -> str:
        return urljoin(self.config.url, "pipelines/")

    def _task_endpoint(self) -> str:
        return urljoin(self.config.url, "tasks/")

    def _version_endpoint(self) -> str:
        return urljoin(self.config.url, "versions/")

    def __repr__(self):
        return f"Pipeline({self.name}, {self.uri}, {self.version})"

    @staticmethod
    def _headers(token: str) -> dict:
        return {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
