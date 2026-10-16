# -*- coding: utf-8 -*-
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

import os
from dataclasses import replace
from decimal import Decimal
from typing import Any
from uuid import uuid4

from npg.conf import IniData
from pytest import mark as m, raises

from conftest import TEST_CONFIG_FILE, TEST_CONFIG_SECTION, porch_server_available
from npg_polite import version
from npg_polite.porch import CompatibilityPolicy, Pipeline, PoliteVersionError, Task


class ExampleTask(Task):
    """
    Example task for testing purposes. It includes a UUID in its state so that every
    task is unique.
    """

    def __init__(
        self,
        item: str,
        quantity: int = 1,
        price: Decimal = Decimal("0.00"),
        uuid: str | None = None,
    ):
        super().__init__(Task.Status.PENDING)
        self.item = item
        self.quantity = quantity
        self.price = Decimal(price)
        self.uuid = uuid if uuid is not None else uuid4().hex

    def to_serializable(self) -> dict[str, Any]:
        return {
            "item": self.item,
            "quantity": self.quantity,
            "price": str(self.price),
            "uuid": self.uuid,
        }

    @classmethod
    def from_serializable(cls, serializable: dict[str, Any]):
        return cls(**serializable)

    def __repr__(self):
        return f"ExampleTask({self.item}, {self.quantity}, {self.price}, {self.uuid})"


class OtherExampleTask(ExampleTask):
    pass


@m.describe("Porch Task")
class TestPorchTask:
    @m.context("When two tasks have the same concrete type and serializable input")
    @m.it("Considers the tasks equal")
    def test_same_input_equal(self):
        task1 = ExampleTask(
            item="bread", quantity=2, price=Decimal("4.20"), uuid="same"
        )
        task2 = ExampleTask(
            item="bread", quantity=2, price=Decimal("4.20"), uuid="same"
        )

        assert task1 == task2

    @m.context("When two tasks have different serializable input")
    @m.it("Considers the tasks unequal")
    def test_different_input_not_equal(self):
        task1 = ExampleTask(
            item="bread", quantity=2, price=Decimal("4.20"), uuid="same"
        )
        task2 = ExampleTask(
            item="bread", quantity=3, price=Decimal("4.20"), uuid="same"
        )

        assert task1 != task2

    @m.context("When two tasks have the same serializable input but different types")
    @m.it("Considers the tasks unequal")
    def test_different_concrete_type_not_equal(self):
        task1 = ExampleTask(
            item="bread", quantity=2, price=Decimal("4.20"), uuid="same"
        )
        task2 = OtherExampleTask(
            item="bread", quantity=2, price=Decimal("4.20"), uuid="same"
        )

        assert task1 != task2

    @m.context("When two tasks have the same serializable input but different status")
    @m.it("Considers the tasks equal")
    def test_status_not_part_of_identity(self):
        task1 = ExampleTask(
            item="bread", quantity=2, price=Decimal("4.20"), uuid="same"
        )
        task2 = ExampleTask(
            item="bread", quantity=2, price=Decimal("4.20"), uuid="same"
        )
        task2.status = Task.Status.RUNNING

        assert task1 == task2

    @m.context("When hashing a task")
    @m.it("Raises a TypeError")
    def test_task_is_unhashable(self):
        task = ExampleTask(item="bread", quantity=2, price=Decimal("4.20"))

        with raises(TypeError):
            hash(task)


@m.describe("Pipeline compatibility policies")
class TestCompatibilityPolicy:
    @m.context("When a compatibility policy is applied")
    @m.it("Reports the expected version to Porch and preserves the release version")
    @m.parametrize(
        "policy, custom, expected",
        [
            (CompatibilityPolicy.EXACT, None, "1.2.3"),
            (CompatibilityPolicy.MAJOR_MINOR, None, "1.2"),
            (CompatibilityPolicy.MAJOR, None, "1"),
            (CompatibilityPolicy.CUSTOM, "workflow-v2", "workflow-v2"),
        ],
        ids=["exact", "major-minor", "major", "custom"],
    )
    def test_compatible_version(self, policy, custom, expected):
        assert policy.compatible_version("1.2.3", custom) == expected

        pipeline = Pipeline(
            ExampleTask,
            name="compatibility-test",
            uri="https://example.com/pipeline",
            version="1.2.3",
            config=Pipeline.ServerConfig(url="http://localhost"),
            compat_policy=policy,
            compat_version=custom,
        )
        task = ExampleTask(item="bread")

        assert pipeline.compatible_version() == expected
        assert pipeline.version == "1.2.3"
        assert pipeline._to_serializable()["version"] == expected
        assert pipeline._to_serializable(task)["pipeline"]["version"] == expected

    @m.context("When no compatibility policy is supplied")
    @m.it("Uses the exact release version")
    def test_default_policy(self):
        pipeline = Pipeline(
            ExampleTask,
            name="compatibility-test",
            uri="https://example.com/pipeline",
            version="1.2.3",
            config=Pipeline.ServerConfig(url="http://localhost"),
        )

        assert pipeline.compat_policy is CompatibilityPolicy.EXACT
        assert pipeline.compatible_version() == "1.2.3"

    @m.context("When an exact or custom policy receives a non-semantic version")
    @m.it("Does not require semantic version parsing")
    @m.parametrize(
        "policy, custom, expected",
        [
            (CompatibilityPolicy.EXACT, None, "release-tag"),
            (CompatibilityPolicy.CUSTOM, "workflow-v2", "workflow-v2"),
        ],
        ids=["exact", "custom"],
    )
    def test_non_semantic_version(self, policy, custom, expected):
        assert policy.compatible_version("release-tag", custom) == expected

    @m.context("When a semantic version policy receives the wrong number of components")
    @m.it("Raises a version error")
    @m.parametrize(
        "policy", [CompatibilityPolicy.MAJOR_MINOR, CompatibilityPolicy.MAJOR]
    )
    @m.parametrize("release_version", ["1", "1.2", "1.2.3.4"])
    def test_invalid_semantic_version(self, policy, release_version):
        with raises(PoliteVersionError, match="Invalid semantic version"):
            policy.compatible_version(release_version)

    @m.context("When the custom policy has no custom version")
    @m.it("Raises a version error")
    def test_missing_custom_version(self):
        with raises(PoliteVersionError, match="No custom version was supplied"):
            CompatibilityPolicy.CUSTOM.compatible_version("1.2.3")


@m.describe("Porch Pipeline")
@m.skipif(not porch_server_available(), reason="Test Porch server is not available")
class TestPorchPipeline:
    @m.context("When configured from an INI file")
    @m.it("Loads the configuration")
    def test_configure(self):
        config = IniData(Pipeline.ServerConfig).from_file(
            TEST_CONFIG_FILE, TEST_CONFIG_SECTION
        )

        assert config is not None
        assert config.admin_token == "0" * 32
        assert config.pipeline_token is None
        assert config.url is not None

    @m.context("When configured from both an INI file and environment variables")
    @m.it("Loads the configuration and falls back to environment variables")
    def test_configure_env(self):
        env_prefix = "PORCH_"
        pipeline_token = "1" * 32
        os.environ[env_prefix + "PIPELINE_TOKEN"] = pipeline_token

        config: Pipeline.ServerConfig = IniData(
            Pipeline.ServerConfig, use_env=True, env_prefix=env_prefix
        ).from_file(TEST_CONFIG_FILE, TEST_CONFIG_SECTION)

        assert config is not None
        assert config.admin_token == "0" * 32
        assert config.pipeline_token == pipeline_token
        assert config.url is not None

    @m.context("When a pipeline has been defined")
    @m.it("Can be registered")
    def test_register_pipeline(self, porch_server_config):
        p = Pipeline(
            ExampleTask,
            name="test_register_pipeline",
            uri="http://www.sanger.ac.uk",
            version=version(),
            config=porch_server_config,
        )

        assert p.register() == p

    @m.context("When a pipline is registered and config update is requested")
    @m.it("Updates the config with a new pipeline token")
    def test_register_pipeline_new_token(self, porch_server_config):
        tmp_config = replace(porch_server_config)
        assert tmp_config.pipeline_token is None

        p = Pipeline(
            ExampleTask,
            name="test_register_pipeline_new_token",
            uri="http://www.sanger.ac.uk",
            version=version(),
            config=tmp_config,
        )
        p = p.register(update_config=True)

        assert p.config.pipeline_token is not None

    @m.context("After a pipeline is registered")
    @m.it("Can create a new token")
    def test_new_token(self, porch_server_config):
        p = Pipeline(
            ExampleTask,
            name="test_new_token",
            uri="http://www.sanger.ac.uk",
            version=version(),
            config=porch_server_config,
        )
        p = p.register()

        t = p.new_token("test_new_token")
        assert t is not None

    @m.context("After a pipeline is registered")
    @m.it("Can have tasks added to it")
    def test_add_task(self, porch_server_config):
        p = Pipeline(
            ExampleTask,
            name="test_add_task",
            uri="http://www.sanger.ac.uk",
            version=version(),
            config=porch_server_config,
        )
        p = p.register()

        token = p.new_token("test_add_task")
        porch_server_config.pipeline_token = token

        task1 = ExampleTask(item="bread", quantity=2, price=Decimal("4.20"))
        assert p.add(task1)  # True because task is not present
        assert task1 in p.all()

        task2 = ExampleTask(item="sugar", quantity=2, price=Decimal("0.78"))
        assert p.add(task2)
        assert task2 in p.all()

        assert not p.add(task1)  # False because task is already present

    @m.context("When identical tasks are submitted to different pipeline versions")
    @m.it("Creates a separate task for each version and rejects duplicates within it")
    def test_add_task_different_pipeline_versions(self, porch_server_config):
        name = f"test-task-versions-{uuid4().hex}"
        pipelines = [
            Pipeline(
                ExampleTask,
                name=name,
                uri="http://www.sanger.ac.uk",
                version=pipeline_version,
                config=porch_server_config,
            ).register(update_config=True)
            for pipeline_version in ("1.0.0", "2.0.0")
        ]
        task = ExampleTask(item="bread", quantity=2, price=Decimal("4.20"))

        for pipeline in pipelines:
            assert pipeline.add(task), f"Create task for version {pipeline.version}"

        for pipeline in pipelines:
            assert not pipeline.add(
                task
            ), f"Reject duplicate task for version {pipeline.version}"

        # all() lists by pipeline name, including tasks from both versions.
        tasks = pipelines[0].all()
        assert len(tasks) == 2
        assert tasks.count(task) == 2

    @m.context("After a task has been added to a pipeline")
    @m.it("Can be claimed")
    def test_claim_task(self, porch_server_config):
        p = Pipeline(
            ExampleTask,
            name="test_claim_task",
            uri="http://www.sanger.ac.uk",
            version=version(),
            config=porch_server_config,
        )
        p = p.register()

        token = p.new_token("test_claim_task")
        porch_server_config.pipeline_token = token

        task = ExampleTask(item="bread", quantity=2, price=Decimal("4.20"))
        p.add(task)

        assert task in p.all()
        assert task in p.claim(1000)

    @m.context("After a task has been claimed")
    @m.it("Can be updated")
    def test_update_task(self, porch_server_config):
        p = Pipeline(
            ExampleTask,
            name="test_update_task",
            uri="http://www.sanger.ac.uk",
            version=version(),
            config=porch_server_config,
        )
        p = p.register()

        token = p.new_token("test_update_task")
        porch_server_config.pipeline_token = token

        tasks = [
            ExampleTask(item="bread", quantity=i, price=Decimal(str(i * 10)))
            for i in range(1, 10)
        ]

        for task in tasks:
            assert p.add(task)

        claimed = p.claim(1000)

        for task in tasks:
            assert task in p.all()
            assert task in claimed

        for task in tasks:
            if task.quantity % 2 == 0:
                assert p.run(task)
                assert task in p.running()

                if task.quantity == 4:
                    assert p.fail(task)
                    assert task in p.failed()

        for task in tasks:
            if task.quantity % 2 == 1:
                assert p.run(task)
                assert task in p.running()

                assert p.fail(task)
                assert task in p.failed()

                assert p.retry(task)
                assert task in p.pending()

                assert p.run(task)
                assert task in p.running()

                assert p.done(task)
                assert task in p.succeeded()
