import boto3

from datetime import datetime, timezone
from moto import mock_s3
from unittest import TestCase

from src.handlers.shared.enums import BucketPath
from src.handlers.shared.functions import read_metadata, write_metadata
from src.handlers.site_upload.powerset_merge import powerset_merge_handler


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
        write_metadata(
            self.s3_client,
            self.bucket_name,
            {
                "elsewhere": {
                    "covid": {
                        "version": "1.0",
                        "last_upload": str(datetime.now(timezone.utc)),
                        "last_data_update": str(datetime.now(timezone.utc)),
                        "last_aggregation": None,
                        "last_error": None,
                        "earliest_data": None,
                        "latest_data": None,
                        "deleted": None,
                    }
                }
            },
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
        assert len(res["Contents"]) == 5
        for item in res["Contents"]:
            if item["Key"].endswith("aggregate.parquet"):
                assert item["Key"].startswith(BucketPath.AGGREGATE.value) == True
            elif item["Key"].endswith("aggregate.csv"):
                assert item["Key"].startswith(BucketPath.CSVAGGREGATE.value) == True
            elif item["Key"].endswith("transactions.json"):
                assert item["Key"].startswith(BucketPath.META.value) == True
                metadata = read_metadata(self.s3_client, self.bucket_name)
                assert "general" in metadata.keys()
                assert metadata["elsewhere"]["covid"]["last_aggregation"] != None
            else:
                assert item["Key"].startswith(BucketPath.LAST_VALID.value) == True

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
            "./tests/site_upload/test_powerset_merge.py",
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
