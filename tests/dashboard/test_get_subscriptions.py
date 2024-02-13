import os
from unittest import mock

from src.handlers.dashboard.get_data_packages import data_packages_handler
from tests.mock_utils import DATA_PACKAGE_COUNT, MOCK_ENV


@mock.patch.dict(os.environ, MOCK_ENV)
def test_get_data_packages(mock_bucket):
    res = data_packages_handler({}, {})
    assert res["statusCode"] == 200
    assert DATA_PACKAGE_COUNT == 2
    assert "Access-Control-Allow-Origin" in res["headers"]
