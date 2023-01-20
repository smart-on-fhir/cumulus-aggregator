import boto3
import unittest
from src.handlers.powerset_merge import powerset_merge_handler
from moto import mock_s3


@mock_s3
class TestPowersetMerge(unittest.TestCase):
    def setUp(self):
        self.bucket_name = "cumulus-aggregator-site-counts"
        self.s3_client = boto3.client("s3", region_name="us-east-1")
        self.s3_client.create_bucket(Bucket=self.bucket_name)
        self.s3_client.upload_file(
            "./tests/test_data/cube_simple_example.csv",
            self.bucket_name,
            "site_uploads/a_test.csv",
        )
        self.s3_client.upload_file(
            "./tests/test_data/cube_simple_example.csv",
            self.bucket_name,
            "latest_data/b_test.csv",
        )

    def test_process_upload(self):
        event = {"Records": [{"s3": {"object": {"key": "site_uploads/a_test.csv"}}}]}
        res = powerset_merge_handler(event, {})
        assert res["statusCode"] == 200
        res = self.s3_client.list_objects_v2(Bucket=self.bucket_name)
        print(res["Contents"])
        assert len(res["Contents"]) == 3
        for item in res["Contents"]:
            assert item["Key"].startswith("latest_data") == True

    def test_file_not_found(self):
        event = {
            "Records": [{"s3": {"object": {"key": "site_uploads/missing_file.csv"}}}]
        }
        res = powerset_merge_handler(event, {})
        assert res["statusCode"] == 500
