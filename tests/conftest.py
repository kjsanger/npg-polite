# -*- coding: utf-8 -*-
#
# Copyright © 2025 Genome Research Ltd. All rights reserved.
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

from pathlib import Path

import pytest
import requests
from npg.conf import IniData
from requests import HTTPError

from npg_polite.porch import Pipeline

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
    except requests.ConnectionError, HTTPError:
        return False
    except Exception:
        raise


@pytest.fixture(scope="function")
def porch_server_config() -> Pipeline.ServerConfig:
    return IniData(Pipeline.ServerConfig).from_file(
        TEST_CONFIG_FILE, TEST_CONFIG_SECTION
    )
