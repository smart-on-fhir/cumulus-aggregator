import boto3
import json
import os

from datetime import datetime, timezone
from moto import mock_s3
from unittest import TestCase, mock

from src.handlers.shared.enums import BucketPath
from src.handlers.shared.functions import read_metadata, write_metadata
from src.handlers.dashboard.get_metadata import metadata_handler


@mock_s3
@mock.patch.dict(os.environ, {"BUCKET_NAME": "cumulus-aggregator-site-counts"})
class TestPowersetMerge(TestCase):
    def setUp(self):
        self.bucket_name = "cumulus-aggregator-site-counts"
        self.metadata = {
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
        self.s3_client = boto3.client("s3", region_name="us-east-1")
        self.s3_client.create_bucket(Bucket=self.bucket_name)
        write_metadata(self.s3_client, self.bucket_name, self.metadata)

    def test_get_all_metadata(self):
        event = {"pathParameters": None}

        res = metadata_handler(event, {})
        assert res["statusCode"] == 200
        assert json.loads(res["body"]) == self.metadata

    def test_get_site_metadata(self):
        event = {"pathParameters": {"site": "general_hospital"}}

        res = metadata_handler(event, {})
        assert res["statusCode"] == 200
        assert json.loads(res["body"]) == self.metadata["general_hospital"]

    def test_get_study_metadata(self):
        event = {"pathParameters": {"site": "general_hospital", "study": "covid"}}

        res = metadata_handler(event, {})
        assert res["statusCode"] == 200
        assert json.loads(res["body"]) == self.metadata["general_hospital"]["covid"]

    def test_get_invalid_site_metadata(self):
        event = {"pathParameters": {"site": "chicago_hope", "study": "covid"}}

        res = metadata_handler(event, {})
        assert res["statusCode"] == 500

    def test_get_invalid_study_metadata(self):
        event = {"pathParameters": {"site": "general_hospital", "study": "flu"}}

        res = metadata_handler(event, {})
        assert res["statusCode"] == 500
