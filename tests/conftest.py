"""Fixture generation for support of unit testing standardization
"""
import os

from unittest import mock

import boto3
import pytest

from moto import mock_s3, mock_athena, mock_sns

from scripts.credential_management import create_auth, create_meta
from src.handlers.shared.enums import BucketPath
from src.handlers.shared.functions import write_metadata
from tests.utils import get_mock_metadata, get_mock_study_metadata, ITEM_COUNT, MOCK_ENV


def _init_mock_data(s3_client, bucket_name, site, study, data_package):
    s3_client.upload_file(
        "./tests/test_data/cube_simple_example.parquet",
        bucket_name,
        f"{BucketPath.AGGREGATE.value}/{site}/{study}/"
        f"{site}__{data_package}/{site}__{data_package}__aggregate.parquet",
    )
    s3_client.upload_file(
        "./tests/test_data/cube_simple_example.csv",
        bucket_name,
        f"{BucketPath.CSVAGGREGATE.value}/{site}/{study}/"
        f"{site}__{data_package}/{site}__{data_package}__aggregate.csv",
    )
    create_auth(s3_client, bucket_name, "general_1", "test_1", "general")
    create_meta(s3_client, bucket_name, "general", "general_hospital")
    create_auth(s3_client, bucket_name, "elsewhere_2", "test_2", "elsewhere")
    create_meta(s3_client, bucket_name, "elsewhere", "st_elsewhere")
    create_auth(s3_client, bucket_name, "hope_3", "test_3", "hope")
    create_meta(s3_client, bucket_name, "hope", "chicago_hope")


@pytest.fixture(autouse=True)
def mock_env():
    with mock.patch.dict(os.environ, MOCK_ENV):
        yield


@pytest.fixture
def mock_bucket():
    """Mock for testing S3 usage. Should reset before each individual test."""
    s3 = mock_s3()
    s3.start()
    s3_client = boto3.client("s3", region_name="us-east-1")

    bucket = os.environ["BUCKET_NAME"]
    s3_client.create_bucket(Bucket=bucket)
    aggregate_params = [
        ["general_hospital", "covid", "encounter"],
        ["general_hospital", "lyme", "encounter"],
        ["st_elsewhere", "covid", "encounter"],
    ]
    for param_list in aggregate_params:
        _init_mock_data(s3_client, bucket, *param_list)
    metadata = get_mock_metadata()
    write_metadata(s3_client, bucket, metadata)
    study_metadata = get_mock_study_metadata()
    write_metadata(s3_client, bucket, study_metadata, meta_type="study_periods")
    yield
    s3.stop()


@pytest.fixture
def mock_notification():
    sns = mock_sns()
    sns.start()
    sns_client = boto3.client("sns", region_name="us-east-1")
    sns_client.create_topic(Name="test-counts")
    sns_client.create_topic(Name="test-meta")
    yield
    sns.stop()


@pytest.fixture
def mock_db():
    """Leaving this unused here for now - there are some low level inconsistencies
    between moto and AWS wrangler w.r.t. how workgroups are mocked out, but we might
    be able to use this in the future/mock AWSwranger below the entrypoint if we are
    concerned.

    https://stackoverflow.com/a/73208335/5318482 discusses this a bit, but doesn't
    adress mocking out the aws workgroup response (though setting the workgroup
    to primary helped a bit since it has default permissions).
    """
    athena = mock_athena()
    athena.start()
    athena_client = boto3.client("athena", region_name="us-east-1")
    athena_client.start_query_execution(
        QueryString=f"create database {os.environ['TEST_GLUE_DB']}",
        ResultConfiguration={
            "OutputLocation": f"s3://{os.environ['TEST_BUCKET']}/athena/"
        },
    )
    yield
    athena.stop()


def test_mock_bucket():
    s3_client = boto3.client("s3", region_name="us-east-1")
    item = s3_client.list_objects_v2(Bucket=os.environ["TEST_BUCKET"])
    assert (len(item["Contents"])) == ITEM_COUNT
