# -*- coding: utf-8 -*-
#
# Copyright © 2024 Genome Research Ltd. All rights reserved.
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
from decimal import Decimal
from uuid import uuid4

from npg.conf import IniData
from pytest import mark as m

from conftest import TEST_CONFIG_FILE, TEST_CONFIG_SECTION, porch_server_available
from npg_polite import version
from npg_polite.porch import Pipeline, Task

porch_server_is_up = m.skipif(
    not porch_server_available(), reason="Test Porch server is not available"
)


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
        uuid: str = None,
    ):
        super().__init__(Task.Status.PENDING)
        self.item = item
        self.quantity = quantity
        self.price = Decimal(price)
        self.uuid = uuid if uuid is not None else uuid4().hex

    def to_serializable(self) -> dict:
        return {
            "item": self.item,
            "quantity": self.quantity,
            "price": str(self.price),
            "uuid": self.uuid,
        }

    @classmethod
    def from_serializable(cls, serializable: dict):
        return cls(**serializable)

    def __repr__(self):
        return f"ExampleTask({self.item}, {self.quantity}, {self.price}, {self.uuid})"


@m.describe("Porch Pipeline")
@porch_server_is_up
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
