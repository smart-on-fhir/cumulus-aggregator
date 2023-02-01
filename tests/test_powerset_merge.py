import boto3

from moto import mock_s3
from unittest import TestCase

from src.handlers.enums import BucketPath
from src.handlers.powerset_merge import powerset_merge_handler


@mock_s3
class TestPowersetMerge(TestCase):
    def setUp(self):
        self.bucket_name = "cumulus-aggregator-site-counts"
        self.s3_client = boto3.client("s3", region_name="us-east-1")
        self.s3_client.create_bucket(Bucket=self.bucket_name)
        self.s3_client.upload_file(
            "./tests/test_data/cube_simple_example.csv",
            self.bucket_name,
            f"{BucketPath.UPLOAD.value}/covid/general/a_test.csv",
        )
        self.s3_client.upload_file(
            "./tests/test_data/cube_simple_example.csv",
            self.bucket_name,
            f"{BucketPath.LAST_VALID.value}/covid/elsewhere/b_test.csv",
        )

    def test_process_upload(self):
        event = {
            "Records": [
                {
                    "s3": {
                        "object": {
                            "key": f"{BucketPath.UPLOAD.value}/covid/general/a_test.csv"
                        }
                    }
                }
            ]
        }
        res = powerset_merge_handler(event, {})
        assert res["statusCode"] == 200
        res = self.s3_client.list_objects_v2(Bucket=self.bucket_name)
        assert len(res["Contents"]) == 3
        for item in res["Contents"]:
            if not item["Key"].endswith("aggregate.csv"):
                assert item["Key"].startswith(BucketPath.LAST_VALID.value) == True
            else:
                assert item["Key"].startswith(BucketPath.AGGREGATE.value) == True

    def test_file_not_found(self):
        event = {
            "Records": [
                {
                    "s3": {
                        "object": {
                            "key": f"{BucketPath.UPLOAD.value}/covid/general/missing_file.csv"
                        }
                    }
                }
            ]
        }
        res = powerset_merge_handler(event, {})
        assert res["statusCode"] == 500

    def test_invalid_dataset(self):
        self.s3_client.upload_file(
            "./tests/test_powerset_merge.py",
            self.bucket_name,
            f"{BucketPath.UPLOAD.value}/covid/elsewhere/b_test.csv",
        )
        event = {
            "Records": [
                {
                    "s3": {
                        "object": {
                            "key": f"{BucketPath.UPLOAD.value}/covid/elsewhere/b_test.csv"
                        }
                    }
                }
            ]
        }
        res = powerset_merge_handler(event, {})
        assert res["statusCode"] == 200
        res = self.s3_client.list_objects_v2(
            Bucket=self.bucket_name, Prefix=BucketPath.ERROR.value
        )
        assert len(res["Contents"]) == 1
        res = self.s3_client.list_objects_v2(
            Bucket=self.bucket_name, Prefix=BucketPath.AGGREGATE.value
        )
        assert len(res["Contents"]) == 1
