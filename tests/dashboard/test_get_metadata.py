import boto3
import json
import os

import pytest
from datetime import datetime, timezone
from unittest import mock

from src.handlers.shared.enums import BucketPath
from src.handlers.shared.functions import read_metadata, write_metadata
from src.handlers.dashboard.get_metadata import metadata_handler
from tests.utils import get_mock_metadata, TEST_BUCKET


@mock.patch.dict(os.environ, {"BUCKET_NAME": TEST_BUCKET})
@pytest.mark.parametrize(
    "params,status,expected",
    [
        (None, 200, get_mock_metadata()),
        ({"site": "general_hospital"}, 200, get_mock_metadata()["general_hospital"]),
        (
            {"site": "general_hospital", "study": "covid"},
            200,
            get_mock_metadata()["general_hospital"]["covid"],
        ),
        (
            {"site": "general_hospital", "study": "covid", "data_package": "encounter"},
            200,
            get_mock_metadata()["general_hospital"]["covid"]["encounter"],
        ),
        ({"site": "chicago_hope", "study": "covid"}, 500, None),
        ({"site": "general_hospital", "study": "flu"}, 500, None),
    ],
)
def test_get_metadata(mock_bucket, params, status, expected):
    event = {"pathParameters": params}

    res = metadata_handler(event, {})
    assert res["statusCode"] == status
    if status == 200:
        assert json.loads(res["body"]) == expected
