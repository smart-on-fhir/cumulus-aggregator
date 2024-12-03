import io
from unittest import mock

import pandas
import pytest

from src.shared import enums, s3_manager
from tests import mock_utils

SNS_EVENT = {
    "Records": [
        {
            "Sns": {
                "TopicArn": "arn",
                "Message": "/study/study__encounter/site/study__encounter__version/file.parquet",
            }
        }
    ]
}


def test_init_manager(mock_bucket):
    manager = s3_manager.S3Manager(SNS_EVENT)
    assert manager.s3_bucket_name == "cumulus-aggregator-site-counts-test"
    assert manager.event_source == "arn"
    assert manager.s3_key == "/study/study__encounter/site/study__encounter__version/file.parquet"
    assert manager.study == "study"
    assert manager.data_package == "encounter"
    assert manager.site == "site"
    assert manager.version == "version"
    assert manager.metadata == mock_utils.get_mock_metadata()
    assert manager.types_metadata == mock_utils.get_mock_column_types_metadata()
    assert (
        manager.parquet_aggregate_path
        == "s3://cumulus-aggregator-site-counts-test/aggregates/study/study__encounter/study__encounter__version/study__encounter__aggregate.parquet"
    )
    assert (
        manager.csv_aggregate_path
        == "s3://cumulus-aggregator-site-counts-test/csv_aggregates/study/study__encounter/version/study__encounter__aggregate.csv"
    )
    assert manager.parquet_flat_key == (
        "flat/study/site/study__encounter__version/study__encounter_site__flat.parquet"
    )
    assert manager.csv_flat_key == (
        "csv_flat/study/site/study__encounter__version/study__encounter_site__flat.parquet"
    )


@pytest.mark.parametrize(
    "file,dest",
    [
        (
            "s3://cumulus-aggregator-site-counts-test/aggregates/study/study__encounter/study__encounter__099/study__encounter__aggregate.parquet",
            "s3://cumulus-aggregator-site-counts-test/aggregates/study/study__encounter/study__encounter__099/study__encounter__aggregate_moved.parquet",
        ),
        (
            "aggregates/study/study__encounter/study__encounter__099/study__encounter__aggregate.parquet",
            "aggregates/study/study__encounter/study__encounter__099/study__encounter__aggregate_moved.parquet",
        ),
    ],
)
def test_copy_file(mock_bucket, file, dest):
    manager = s3_manager.S3Manager(SNS_EVENT)
    manager.copy_file(file, dest)
    files = [
        file["Key"]
        for file in manager.s3_client.list_objects_v2(Bucket=manager.s3_bucket_name)["Contents"]
    ]
    assert any(
        "study/study__encounter/study__encounter__099/study__encounter__aggregate_moved.parquet"
        in file
        for file in files
    )
    assert any(
        "study/study__encounter/study__encounter__099/study__encounter__aggregate.parquet" in file
        for file in files
    )


def test_get_list(mock_bucket):
    manager = s3_manager.S3Manager(SNS_EVENT)
    assert manager.get_data_package_list(enums.BucketPath.AGGREGATE.value) == [
        "s3://cumulus-aggregator-site-counts-test/aggregates/study/study__encounter/study__encounter__099/study__encounter__aggregate.parquet"
    ]


@pytest.mark.parametrize(
    "file,dest",
    [
        (
            "s3://cumulus-aggregator-site-counts-test/aggregates/study/study__encounter/study__encounter__099/study__encounter__aggregate.parquet",
            "s3://cumulus-aggregator-site-counts-test/aggregates/study/study__encounter/study__encounter__099/study__encounter__aggregate_moved.parquet",
        ),
        (
            "aggregates/study/study__encounter/study__encounter__099/study__encounter__aggregate.parquet",
            "aggregates/study/study__encounter/study__encounter__099/study__encounter__aggregate_moved.parquet",
        ),
    ],
)
def test_move_file(mock_bucket, file, dest):
    manager = s3_manager.S3Manager(SNS_EVENT)
    manager.move_file(file, dest)
    files = [
        file["Key"]
        for file in manager.s3_client.list_objects_v2(Bucket=manager.s3_bucket_name)["Contents"]
    ]
    assert any(
        "study/study__encounter/study__encounter__099/study__encounter__aggregate_moved.parquet"
        in file
        for file in files
    )
    assert not any(
        "study/study__encounter/study__encounter__099/study__encounter__aggregate.parquet" in file
        for file in files
    )


@mock.patch("boto3.client")
def test_cache_api(mock_client, mock_bucket):
    manager = s3_manager.S3Manager(SNS_EVENT)
    manager.cache_api()
    publish_args = mock_client.mock_calls[-1][2]
    assert publish_args["TopicArn"] == mock_utils.TEST_CACHE_API_ARN
    assert publish_args["Message"] == "data_packages"
    assert publish_args["Subject"] == "data_packages"


def test_write_csv(mock_bucket):
    df = pandas.DataFrame(data={"foo": [1, 2], "bar": [11, 22]})
    manager = s3_manager.S3Manager(SNS_EVENT)
    manager.write_csv(df)
    df2 = pandas.read_csv(
        io.BytesIO(
            manager.s3_client.get_object(
                Bucket=manager.s3_bucket_name,
                Key=(
                    "csv_aggregates/study/study__encounter/version/"
                    "study__encounter__aggregate.csv"
                ),
            )["Body"].read()
        )
    )
    assert df.compare(df2).empty


@mock.patch("src.shared.s3_manager.S3Manager.cache_api")
def test_write_parquet(mock_cache, mock_bucket):
    df = pandas.DataFrame(data={"foo": [1, 2], "bar": [11, 22]})
    manager = s3_manager.S3Manager(SNS_EVENT)
    manager.write_parquet(df, False)
    df2 = pandas.read_parquet(
        io.BytesIO(
            manager.s3_client.get_object(
                Bucket=manager.s3_bucket_name,
                Key=(
                    "aggregates/study/study__encounter/study__encounter__version/"
                    "study__encounter__aggregate.parquet"
                ),
            )["Body"].read()
        )
    )
    assert df.compare(df2).empty
    assert not mock_cache.called
    manager.write_parquet(
        df,
        True,
        path=(
            f"s3://{mock_utils.TEST_BUCKET}/aggregates/study/study__encounter/"
            "study__encounter__version/study__encounter__aggregate.parquet"
        ),
    )
    assert mock_cache.called


def test_update_local_metadata(mock_bucket):
    manager = s3_manager.S3Manager(SNS_EVENT)
    original_transactions = manager.metadata.copy()
    original_types = manager.types_metadata.copy()
    other_dict = {}
    manager.update_local_metadata(
        key="foo",
        site=mock_utils.NEW_SITE,
        value="bar",
        metadata=other_dict,
        extra_items={"foobar": "baz"},
    )
    assert mock_utils.NEW_SITE in other_dict.keys()
    assert "foo" in other_dict[mock_utils.NEW_SITE]["study"]["encounter"]["version"].keys()
    assert "foobar" in other_dict[mock_utils.NEW_SITE]["study"]["encounter"]["version"].keys()
    assert original_transactions == manager.metadata
    assert original_types == manager.types_metadata
    manager.update_local_metadata("foo")
    assert original_transactions != manager.metadata
    assert original_types == manager.types_metadata
    assert "foo" in manager.metadata["site"]["study"]["encounter"]["version"].keys()


def test_write_local_metadata(mock_bucket):
    manager = s3_manager.S3Manager(SNS_EVENT)
    manager.metadata = {"foo": "bar"}
    manager.write_local_metadata()
    metadata = manager.s3_client.get_object(
        Bucket=manager.s3_bucket_name, Key="metadata/transactions.json"
    )["Body"].read()
    assert metadata == b'{\n  "foo": "bar"\n}'
