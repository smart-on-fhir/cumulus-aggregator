import boto3
import json

from moto import mock_s3
from unittest import TestCase

from src.handlers.fetch_upload_url import upload_url_handler


@mock_s3
class TestFetchUploadUrl(TestCase):
    def setUp(self):
        self.bucket_name = "cumulus-aggregator-site-counts"
        self.s3_client = boto3.client("s3", region_name="us-east-1")
        self.s3_client.create_bucket(Bucket=self.bucket_name)

    def test_fetch_upload_url(self):
        body = json.dumps({"study": "covid", "filename": "covid.csv"})
        headers = {"user": "elsewhere"}
        response = upload_url_handler({"body": body, "headers": headers}, None)
        print(response)
        self.assertEqual(response["statusCode"], 200)

        body = json.dumps({})
        response = upload_url_handler({"body": body}, None)
        self.assertEqual(response["statusCode"], 500)
