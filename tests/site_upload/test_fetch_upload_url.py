import boto3
import json
import pytest
from moto import mock_s3
from unittest import TestCase, mock

from src.handlers.site_upload.fetch_upload_url import upload_url_handler

builtin_open = open


def mock_open(*args, **kwargs):
    if args[0] == "src/handlers/site_upload/site_data/metadata.json":
        return mock.mock_open()(*args, **kwargs)
    return builtin_open(*args, **kwargs)


def mock_json_load(*args):
    return {"test": {"path": "testpath"}}


@mock_s3
class TestFetchUploadUrl(TestCase):
    def setUp(self):
        self.bucket_name = "cumulus-aggregator-site-counts"
        self.s3_client = boto3.client("s3", region_name="us-east-1")
        self.s3_client.create_bucket(Bucket=self.bucket_name)

    @mock.patch("builtins.open", mock_open)
    @mock.patch("json.load", mock_json_load)
    def test_fetch_upload_url(self):
        body = json.dumps({"study": "covid", "filename": "covid.csv"})
        headers = {"user": "test"}
        response = upload_url_handler({"body": body, "headers": headers}, None)
        self.assertEqual(response["statusCode"], 200)

        body = json.dumps({})
        response = upload_url_handler({"body": body}, None)
        self.assertEqual(response["statusCode"], 500)
