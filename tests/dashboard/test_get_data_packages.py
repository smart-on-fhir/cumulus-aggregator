import json
import os
import pathlib
from unittest import mock

from src.dashboard.get_data_packages import get_data_packages
from tests.mock_utils import DATA_PACKAGE_COUNT, MOCK_ENV


@mock.patch.dict(os.environ, MOCK_ENV)
def test_get_data_packages(mock_bucket):
    with open(pathlib.Path(__file__).parent.parent / "./test_data/data_packages_cache.json") as f:
        data = json.load(f)
    res = get_data_packages.data_packages_handler({}, {})
    assert res["statusCode"] == 200
    assert DATA_PACKAGE_COUNT == len(data)
    assert "Access-Control-Allow-Origin" in res["headers"]
    assert json.loads(res["body"]) == data
    res = get_data_packages.data_packages_handler(
        {"queryStringParameters": {"name": "encounter"}}, {}
    )
    data = json.loads(res["body"])
    assert res["statusCode"] == 200
    assert 2 == len(json.loads(res["body"]))
    for item in data:
        assert item["name"] == "encounter"
    res = get_data_packages.data_packages_handler(
        {"pathParameters": {"data_package_id": "other_study__document__100"}}, {}
    )
    data = json.loads(res["body"])
    assert res["statusCode"] == 200
    assert 9 == len(data)
    assert data["id"] == "other_study__document__100"
    res = get_data_packages.data_packages_handler(
        {"pathParameters": {"data_package_id": "not_an_id"}}, {}
    )
    data = json.loads(res["body"])
    assert res["statusCode"] == 404
