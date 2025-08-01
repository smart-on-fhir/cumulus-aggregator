"""Fixture generation for support of unit testing standardization

One bit of implicit logic here is in relation to the three fake sites used
(all of which reference fictional TV hospitals, so no one is accidentally
confused by the presence of their institution). Their starting state is
as follows:

princeton_plainsboro_teaching_hospital - this is a site with prior
data, which is usually the one that is doing the uploading
st_elsewhere - this is a site with prior data, which is usually not
involved in the current upload
chicago_hope - this is a site that has no prior data, and may be
used to test creation of resources

Similarly, we have three study names that are used to express a similar
set of states:
- 'study', which has existing data, and is fine for uploading data to
- 'other_study', which also has existing data, but should not be uploaded
  to - this helps check for unintended side effects
- 'new_study' - not mentioned in this file, but this is for uploading
  data in the mode of a study which heretofore has not existed in the
  aggregator
"""

import datetime
import os
import re
from unittest import mock

import boto3
import duckdb
import pytest
from moto import mock_athena, mock_s3, mock_sns, mock_sqs

from scripts import credential_management
from src.shared import enums, functions
from tests import mock_utils


def _init_mock_data(s3_client, bucket, study, data_package, version):
    """Creates data in bucket for use in unit tests

    The following items are added:
        - Aggregates, with a site of plainsboro, in parquet and csv, for the
          study provided
        - Flat tables, with a site of plainsboro, in parquet and csv, for the
          study provided
        - a data_package cache for api testing
        - credentials for the 3 unit test hospitals (princeton, elsewhere, hope)

    This can be lazily reinvoked for multiple sites.
    """
    s3_client.upload_file(
        "./tests/test_data/count_synthea_patient_agg.parquet",
        bucket,
        f"{enums.BucketPath.AGGREGATE.value}/{study}/"
        f"{study}__{data_package}/{study}__{data_package}__{version}/"
        f"{study}__{data_package}__aggregate.parquet",
    )
    s3_client.upload_file(
        "./tests/test_data/meta_date.parquet",
        bucket,
        f"{enums.BucketPath.STUDY_META.value}/{study}/"
        f"{study}__{data_package}/{mock_utils.EXISTING_SITE}/"
        f"{version}/{study}__meta_date.parquet",
    )
    s3_client.upload_file(
        "./tests/test_data/flat_synthea_q_date_recent.parquet",
        bucket,
        f"{enums.BucketPath.FLAT.value}/{study}/{mock_utils.EXISTING_SITE}/"
        f"{study}__c_{data_package}__{mock_utils.EXISTING_SITE}__{version}/"
        f"{study}__c_{data_package}__flat.parquet",
    )
    s3_client.upload_file(
        "./tests/test_data/data_packages_cache.json",
        bucket,
        f"{enums.BucketPath.CACHE.value}/{enums.JsonFilename.DATA_PACKAGES.value}.json",
    )


@pytest.fixture(scope="session", autouse=True)
def mock_env():
    with mock.patch.dict(os.environ, mock_utils.MOCK_ENV):
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
        [
            mock_utils.EXISTING_STUDY,
            mock_utils.EXISTING_DATA_P,
            mock_utils.EXISTING_VERSION,
        ],
        [
            mock_utils.OTHER_STUDY,
            mock_utils.EXISTING_DATA_P,
            mock_utils.EXISTING_VERSION,
        ],
    ]
    for param_list in aggregate_params:
        _init_mock_data(s3_client, bucket, *param_list)

    credential_management.create_meta(
        s3_client, bucket, "ppth", "princeton_plainsboro_teaching_hospital"
    )
    credential_management.create_meta(s3_client, bucket, "elsewhere", "st_elsewhere")
    credential_management.create_meta(s3_client, bucket, "hope", "chicago_hope")

    metadata = mock_utils.get_mock_metadata()
    functions.write_metadata(s3_client=s3_client, s3_bucket_name=bucket, metadata=metadata)
    study_metadata = mock_utils.get_mock_study_metadata()
    functions.write_metadata(
        s3_client=s3_client,
        s3_bucket_name=bucket,
        metadata=study_metadata,
        meta_type=enums.JsonFilename.STUDY_PERIODS.value,
    )
    column_types_metadata = mock_utils.get_mock_column_types_metadata()
    functions.write_metadata(
        s3_client=s3_client,
        s3_bucket_name=bucket,
        metadata=column_types_metadata,
        meta_type=enums.JsonFilename.COLUMN_TYPES.value,
    )
    yield
    s3.stop()


@pytest.fixture
def mock_notification():
    """Mocks for SNS topics.

    Make sure the topic name matches the end of the ARN defined in mock_utils.py"""
    sns = mock_sns()
    sns.start()
    sns_client = boto3.client("sns", region_name="us-east-1")
    sns_client.create_topic(Name="test-counts")
    sns_client.create_topic(Name="test-flat")
    sns_client.create_topic(Name="test-meta")
    sns_client.create_topic(Name="test-cache")
    sns_client.create_topic(Name="test-payload")
    yield
    sns.stop()


@pytest.fixture
def mock_queue():
    """Mocks for SQS queues.

    Make sure the queue name matches the end of the ARN defined in mock_utils.py"""
    sqs = mock_sqs()
    sqs.start()
    sqs_client = boto3.client("sqs", region_name="us-east-1")
    sqs_client.create_queue(QueueName="test-lockfile-cleanup")
    yield
    sqs.stop()


@pytest.fixture
def mock_athena_db():
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
        ResultConfiguration={"OutputLocation": f"s3://{os.environ['TEST_BUCKET']}/athena/"},
    )
    yield
    athena.stop()


@pytest.fixture
def mock_db(tmp_path):
    def _compat_regexp_like(string: str | None, pattern: str | None) -> bool:
        match = re.search(pattern, string)
        return match is not None

    def _compat_from_iso8601_timestamp(
        value: str | datetime.datetime,
    ) -> datetime.datetime:
        if type(value) is str:
            return datetime.datetime.fromisoformat(value)
        return value

    db = duckdb.connect(tmp_path / "duck.db")
    db.create_function(
        # DuckDB's version is regexp_matches.
        "regexp_like",
        _compat_regexp_like,
        None,
        duckdb.typing.BOOLEAN,
    )
    db.create_function(
        "from_iso8601_timestamp",
        _compat_from_iso8601_timestamp,
        None,
        duckdb.typing.TIMESTAMP,
    )
    yield db


def test_mock_bucket():
    s3_client = boto3.client("s3", region_name="us-east-1")
    item = s3_client.list_objects_v2(Bucket=os.environ["TEST_BUCKET"])
    assert (len(item["Contents"])) == mock_utils.ITEM_COUNT
