import boto3
import json
import os

import pytest
from moto import mock_s3, mock_athena
from unittest import mock

from src.handlers.shared.enums import BucketPath
from src.handlers.shared.functions import read_metadata, write_metadata
from tests.utils import get_mock_metadata, get_mock_auth, TEST_BUCKET, ITEM_COUNT


def _init_mock_data(s3_client, bucket_name, site, study, subscription):
    s3_client.upload_file(
        "./tests/test_data/cube_simple_example.parquet",
        bucket_name,
        f"{BucketPath.AGGREGATE.value}/{site}/{study}/"
        f"{site}__{subscription}/{site}__{subscription}__aggregate.parquet",
    )
    s3_client.upload_file(
        "./tests/test_data/cube_simple_example.csv",
        bucket_name,
        f"{BucketPath.CSVAGGREGATE.value}/{site}/{study}/"
        f"{site}__{subscription}/{site}__{subscription}__aggregate.csv",
    )


@pytest.fixture
def mock_bucket():
    """Mock for testing S3 usage. Should reset before each individual test."""
    s3 = mock_s3()
    s3.start()
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket=TEST_BUCKET)
    aggregate_params = [
        ["general_hospital", "covid", "encounter"],
        ["general_hospital", "lyme", "encounter"],
        ["st_elsewhere", "covid", "encounter"],
    ]
    for param_list in aggregate_params:
        _init_mock_data(s3_client, TEST_BUCKET, *param_list)
    metadata = get_mock_metadata()
    write_metadata(s3_client, TEST_BUCKET, metadata)
    yield
    s3.stop()


def test_mock_bucket():
    s3_client = boto3.client("s3", region_name="us-east-1")
    item = s3_client.list_objects_v2(Bucket=TEST_BUCKET)
    assert (len(item["Contents"])) == ITEM_COUNT
