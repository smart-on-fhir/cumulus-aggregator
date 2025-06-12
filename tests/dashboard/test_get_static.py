import json

import boto3
import pytest

from src.dashboard.get_static import get_static
from tests.mock_utils import TEST_BUCKET


@pytest.mark.parametrize(
    "params,status,expected",
    [
        (None, 404, None),
        (
            {"path": "test"},
            200,
            {"a": 1},
        ),
        (
            {"path": "missing"},
            404,
            None,
        ),
    ],
)
def test_get_study_periods(mock_bucket, params, status, expected):
    client = boto3.client("s3", region_name="us-east-1")
    bucket = TEST_BUCKET
    client.put_object(Bucket=bucket, Key="static/test", Body=json.dumps({"a": 1}))
    event = {"pathParameters": params}
    res = get_static.static_handler(event, {})
    assert res["statusCode"] == status
    if status == 200:
        assert json.loads(res["body"]) == expected
