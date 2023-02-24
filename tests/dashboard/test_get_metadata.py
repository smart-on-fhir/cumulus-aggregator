import boto3
import json
import os

import pytest
from datetime import datetime, timezone
from moto import mock_s3
from unittest import mock

from src.handlers.shared.enums import BucketPath
from src.handlers.shared.functions import read_metadata, write_metadata
from src.handlers.dashboard.get_metadata import metadata_handler


def get_mock_metadata():
    return {
        "general_hospital": {
            "covid": {
                "version": "1.0",
                "last_upload": "2023-02-24T15:03:34+00:00",
                "last_data_update": "2023-02-24T15:03:40.657583+00:00",
                "last_aggregation": "2023-02-24T15:08:07.504595+00:00",
                "last_error": None,
                "earliest_data": None,
                "latest_data": None,
                "deleted": None,
            },
            "lyme": {
                "version": "1.0",
                "last_upload": "2023-02-24T15:43:57+00:00",
                "last_data_update": "2023-02-24T15:44:03.861574+00:00",
                "last_aggregation": "2023-02-24T15:44:03.861574+00:00",
                "last_error": None,
                "earliest_data": None,
                "latest_data": None,
                "deleted": None,
            },
        },
        "st_elsewhere": {
            "covid": {
                "version": "1.0",
                "last_upload": "2023-02-24T15:08:06+00:00",
                "last_data_update": "2023-02-24T15:08:07.771080+00:00",
                "last_aggregation": "2023-02-24T15:08:07.771080+00:00",
                "last_error": None,
                "earliest_data": None,
                "latest_data": None,
                "deleted": None,
            }
        },
    }


@pytest.fixture(autouse=True)
def mock_bucket():
    mocks3 = mock_s3()
    mocks3.start()
    bucket_name = "cumulus-aggregator-site-counts"
    metadata = get_mock_metadata()
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket=bucket_name)
    write_metadata(s3_client, bucket_name, metadata)
    yield
    mocks3.stop()


@mock.patch.dict(os.environ, {"BUCKET_NAME": "cumulus-aggregator-site-counts"})
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
        ({"site": "chicago_hope", "study": "covid"}, 500, None),
        ({"site": "general_hospital", "study": "flu"}, 500, None),
    ],
)
def test_get_metadata(params, status, expected):
    event = {"pathParameters": params}

    res = metadata_handler(event, {})
    assert res["statusCode"] == status
    if status == 200:
        assert json.loads(res["body"]) == expected


"""
    def test_get_all_metadata(self):
        event = {"pathParameters": None}

        res = metadata_handler(event, {})
        assert res["statusCode"] == 200
        assert json.loads(res["body"]) == get_mock_metadata()

    def test_get_site_metadata(self):
        event = {"pathParameters": {"site": "general_hospital"}}

        res = metadata_handler(event, {})
        assert res["statusCode"] == 200
        assert json.loads(res["body"]) == get_mock_metadata()["general_hospital"]

    def test_get_study_metadata(self):
        event = {"pathParameters": {"site": "general_hospital", "study": "covid"}}

        res = metadata_handler(event, {})
        assert res["statusCode"] == 200
        assert json.loads(res["body"]) == get_mock_metadata()["general_hospital"]["covid"]

    def test_get_invalid_site_metadata(self):
        event = {"pathParameters": {"site": "chicago_hope", "study": "covid"}}

        res = metadata_handler(event, {})
        assert res["statusCode"] == 500

    def test_get_invalid_study_metadata(self):
        event = {"pathParameters": {"site": "general_hospital", "study": "flu"}}

        res = metadata_handler(event, {})
        assert res["statusCode"] == 500
"""
