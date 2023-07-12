import boto3
import json
import os

import pytest
from datetime import datetime, timezone
from unittest import mock

from src.handlers.shared.enums import BucketPath
from src.handlers.shared.functions import read_metadata, write_metadata
from src.handlers.dashboard.get_study_periods import study_periods_handler
from tests.utils import get_mock_study_metadata


@pytest.mark.parametrize(
    "params,status,expected",
    [
        (None, 200, get_mock_study_metadata()),
        (
            {"site": "general_hospital"},
            200,
            get_mock_study_metadata()["general_hospital"],
        ),
        (
            {"site": "general_hospital", "study": "study"},
            200,
            get_mock_study_metadata()["general_hospital"]["study"],
        ),
        ({"site": "chicago_hope", "study": "study"}, 500, None),
        ({"site": "general_hospital", "study": "flu"}, 500, None),
    ],
)
def test_get_study_periods(mock_bucket, params, status, expected):
    event = {"pathParameters": params}

    res = study_periods_handler(event, {})
    assert res["statusCode"] == status
    if status == 200:
        assert json.loads(res["body"]) == expected
