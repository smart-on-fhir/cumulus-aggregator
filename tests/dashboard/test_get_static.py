import json

import boto3
import pytest

from src.dashboard.get_static import get_static
from tests.mock_utils import TEST_BUCKET


@pytest.mark.parametrize(
    "params,query_params,status,expected",
    [
        (None, None, 404, None),
        (
            {"path": "test"},
            None,
            200,
            {"a": 1},
        ),
        (
            {"path": "missing"},
            None,
            404,
            None,
        ),
        (
            {"path": "test"},
            {"a": "b"},
            200,
            {"b": 2},
        ),
        (
            {"path": "test"},
            {"missing": "b"},
            404,
            {"b": 2},
        ),
        (
            {"path": "test"},
            {"a": "missing"},
            404,
            {"b": 2},
        ),
    ],
)
def test_get_study_periods(mock_bucket, params, query_params, status, expected):
    client = boto3.client("s3", region_name="us-east-1")
    bucket = TEST_BUCKET
    client.put_object(Bucket=bucket, Key="static/test", Body=json.dumps({"a": 1}))
    client.put_object(Bucket=bucket, Key="static/test?a=b", Body=json.dumps({"b": 2}))
    event = {"pathParameters": params, "queryStringParameters": query_params}
    res = get_static.static_handler(event, {})
    assert res["statusCode"] == status
    if status == 200:
        assert json.loads(res["body"]) == expected
