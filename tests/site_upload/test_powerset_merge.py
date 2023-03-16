import boto3
import os

from datetime import datetime, timezone
from moto import mock_s3
from unittest import TestCase, mock

from src.handlers.shared.enums import BucketPath
from src.handlers.shared.functions import read_metadata, write_metadata
from src.handlers.site_upload.powerset_merge import powerset_merge_handler


@mock_s3
@mock.patch.dict(os.environ, {"BUCKET_NAME": "cumulus-aggregator-site-counts"})
class TestPowersetMerge(TestCase):
    def setUp(self):
        self.bucket_name = "cumulus-aggregator-site-counts"
        self.s3_client = boto3.client("s3", region_name="us-east-1")
        self.s3_client.create_bucket(Bucket=self.bucket_name)
        self.s3_client.upload_file(
            "./tests/test_data/cube_simple_example.parquet",
            self.bucket_name,
            f"{BucketPath.UPLOAD.value}/covid/encounter/general/a_test.parquet",
        )
        self.s3_client.upload_file(
            "./tests/test_data/cube_simple_example.parquet",
            self.bucket_name,
            f"{BucketPath.LAST_VALID.value}/covid/encounter/elsewhere/b_test.parquet",
        )
        write_metadata(
            self.s3_client,
            self.bucket_name,
            {
                "elsewhere": {
                    "covid": {
                        "encounter": {
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
                }
            },
        )

    def test_process_upload(self):
        event = {
            "Records": [
                {
                    "s3": {
                        "object": {
                            "key": f"{BucketPath.UPLOAD.value}/covid/encounter/general/a_test.parquet"
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
                assert (
                    metadata["general"]["covid"]["encounter"]["last_aggregation"]
                    != None
                )
            else:
                assert item["Key"].startswith(BucketPath.LAST_VALID.value) == True

    @mock.patch("src.handlers.site_upload.powerset_merge.datetime")
    def test_dataset_archive(self, mock_dt):
        mock_dt.now = mock.Mock(return_value=datetime(2020, 1, 1))
        self.s3_client.upload_file(
            "./tests/test_data/cube_simple_example.parquet",
            self.bucket_name,
            f"{BucketPath.LAST_VALID.value}/covid/encounter/general/a_test.parquet",
        )
        event = {
            "Records": [
                {
                    "s3": {
                        "object": {
                            "key": f"{BucketPath.UPLOAD.value}/covid/encounter/general/a_test.parquet"
                        }
                    }
                }
            ]
        }
        res = powerset_merge_handler(event, {})
        assert res["statusCode"] == 200
        res = self.s3_client.list_objects_v2(Bucket=self.bucket_name)
        assert len(res["Contents"]) == 6
        keys = []
        for resource in res["Contents"]:
            keys.append(resource["Key"])
        assert (
            "archive/covid/encounter/general/a_test.2020-01-01T00:00:00.parquet" in keys
        )

    def test_file_not_found(self):
        event = {
            "Records": [
                {
                    "s3": {
                        "object": {
                            "key": f"{BucketPath.UPLOAD.value}/covid/encounter/general/missing_file.parquet"
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
            f"{BucketPath.UPLOAD.value}/covid/encounter/elsewhere/b_test.parquet",
        )
        event = {
            "Records": [
                {
                    "s3": {
                        "object": {
                            "key": f"{BucketPath.UPLOAD.value}/covid/encounter/elsewhere/b_test.parquet"
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

    def test_invalid_filetype(self):
        self.s3_client.upload_file(
            "./tests/test_data/cube_simple_example.csv",
            self.bucket_name,
            f"{BucketPath.UPLOAD.value}/covid/encounter/elsewhere/b_test.csv",
        )
        event = {
            "Records": [
                {
                    "s3": {
                        "object": {
                            "key": f"{BucketPath.UPLOAD.value}/covid/encounter/elsewhere/b_test.csv"
                        }
                    }
                }
            ]
        }
        res = powerset_merge_handler(event, {})
        assert res["statusCode"] == 500
        res = self.s3_client.list_objects_v2(
            Bucket=self.bucket_name, Prefix=BucketPath.ERROR.value
        )
