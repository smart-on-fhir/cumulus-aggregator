import os

from unittest import mock

import awswrangler
import pandas

from pytest_mock import MockerFixture

from src.handlers.dashboard.get_data_packages import data_packages_handler
from tests.utils import get_mock_metadata, MOCK_ENV, DATA_PACKAGE_COUNT


@mock.patch.dict(os.environ, MOCK_ENV)
def test_get_data_packages(mock_bucket):
    res = data_packages_handler({}, {})
    assert res["statusCode"] == 200
    assert DATA_PACKAGE_COUNT == 2
    assert "Access-Control-Allow-Origin" in res["headers"]
