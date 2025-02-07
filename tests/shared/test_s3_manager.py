import copy
import io
from unittest import mock

import freezegun
import pandas
import pytest

from src.shared import enums, s3_manager
from tests import mock_utils


def mock_sns_event(site, study, data_package, version):
    return {
        "Records": [
            {
                "Sns": {
                    "TopicArn": "arn",
                    "Message": (
                        f"/{study}/{study}__{data_package}/{site}/"
                        f"{study}__{data_package}__{version}/file.parquet"
                    ),
                }
            }
        ]
    }


def test_init_manager(mock_bucket):
    manager = s3_manager.S3Manager(
        mock_sns_event(
            mock_utils.EXISTING_SITE,
            mock_utils.EXISTING_STUDY,
            mock_utils.EXISTING_DATA_P,
            mock_utils.EXISTING_VERSION,
        )
    )
    assert manager.s3_bucket_name == "cumulus-aggregator-site-counts-test"
    assert manager.event_source == "arn"
    assert manager.s3_key == (
        f"/{mock_utils.EXISTING_STUDY}/{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}/"
        f"{mock_utils.EXISTING_SITE}/{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}__"
        f"{mock_utils.EXISTING_VERSION}/file.parquet"
    )
    assert manager.study == mock_utils.EXISTING_STUDY
    assert manager.data_package == mock_utils.EXISTING_DATA_P
    assert manager.site == mock_utils.EXISTING_SITE
    assert manager.version == mock_utils.EXISTING_VERSION
    assert manager.metadata == mock_utils.get_mock_metadata()
    assert manager.types_metadata == mock_utils.get_mock_column_types_metadata()
    assert (
        manager.parquet_aggregate_path
        == "s3://cumulus-aggregator-site-counts-test/aggregates/study/study__encounter/study__encounter__099/study__encounter__aggregate.parquet"
    )
    assert (
        manager.csv_aggregate_path
        == "s3://cumulus-aggregator-site-counts-test/csv_aggregates/study/study__encounter/099/study__encounter__aggregate.csv"
    )
    assert manager.parquet_flat_key == (
        f"flat/study/{mock_utils.EXISTING_SITE}/{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}__{mock_utils.EXISTING_VERSION}/{mock_utils.EXISTING_STUDY}__encounter_{mock_utils.EXISTING_SITE}__flat.parquet"
    )
    assert manager.csv_flat_key == (
        f"csv_flat/study/{mock_utils.EXISTING_SITE}/{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}__{mock_utils.EXISTING_VERSION}/{mock_utils.EXISTING_STUDY}__encounter_{mock_utils.EXISTING_SITE}__flat.parquet"
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
    manager = s3_manager.S3Manager(
        mock_sns_event(
            mock_utils.EXISTING_SITE,
            mock_utils.EXISTING_STUDY,
            mock_utils.EXISTING_DATA_P,
            mock_utils.EXISTING_VERSION,
        )
    )
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
    manager = s3_manager.S3Manager(
        mock_sns_event(
            mock_utils.EXISTING_SITE,
            mock_utils.EXISTING_STUDY,
            mock_utils.EXISTING_DATA_P,
            mock_utils.EXISTING_VERSION,
        )
    )
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
    manager = s3_manager.S3Manager(
        mock_sns_event(
            mock_utils.EXISTING_SITE,
            mock_utils.EXISTING_STUDY,
            mock_utils.EXISTING_DATA_P,
            mock_utils.EXISTING_VERSION,
        )
    )
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
    manager = s3_manager.S3Manager(
        mock_sns_event(
            mock_utils.EXISTING_SITE,
            mock_utils.EXISTING_STUDY,
            mock_utils.EXISTING_DATA_P,
            mock_utils.EXISTING_VERSION,
        )
    )
    manager.cache_api()
    publish_args = mock_client.mock_calls[-1][2]
    assert publish_args["TopicArn"] == mock_utils.TEST_CACHE_API_ARN
    assert publish_args["Message"] == "data_packages"
    assert publish_args["Subject"] == "data_packages"


def test_write_csv(mock_bucket):
    df = pandas.DataFrame(data={"foo": [1, 2], "bar": [11, 22]})
    manager = s3_manager.S3Manager(
        mock_sns_event(
            mock_utils.EXISTING_SITE,
            mock_utils.EXISTING_STUDY,
            mock_utils.EXISTING_DATA_P,
            mock_utils.EXISTING_VERSION,
        )
    )
    manager.write_csv(df)
    df2 = pandas.read_csv(
        io.BytesIO(
            manager.s3_client.get_object(
                Bucket=manager.s3_bucket_name,
                Key=(
                    "csv_aggregates/study/study__encounter/099/" "study__encounter__aggregate.csv"
                ),
            )["Body"].read()
        )
    )
    assert df.compare(df2).empty


@mock.patch("src.shared.s3_manager.S3Manager.cache_api")
def test_write_parquet(mock_cache, mock_bucket):
    df = pandas.DataFrame(data={"foo": [1, 2], "bar": [11, 22]})
    manager = s3_manager.S3Manager(
        mock_sns_event(
            mock_utils.EXISTING_SITE,
            mock_utils.EXISTING_STUDY,
            mock_utils.EXISTING_DATA_P,
            mock_utils.EXISTING_VERSION,
        )
    )
    manager.write_parquet(df, False)
    df2 = pandas.read_parquet(
        io.BytesIO(
            manager.s3_client.get_object(
                Bucket=manager.s3_bucket_name,
                Key=(
                    "aggregates/study/study__encounter/study__encounter__099/"
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
            "study__encounter__099/study__encounter__aggregate.parquet"
        ),
    )
    assert mock_cache.called


@pytest.mark.parametrize(
    "site,study,data_package,version,metadata_type,target,extras",
    [
        (
            mock_utils.EXISTING_SITE,
            mock_utils.EXISTING_STUDY,
            mock_utils.EXISTING_DATA_P,
            mock_utils.EXISTING_VERSION,
            enums.JsonFilename.COLUMN_TYPES.value,
            enums.ColumnTypesKeys.COLUMNS.value,
            {},
        ),
        (
            mock_utils.EXISTING_SITE,
            mock_utils.EXISTING_STUDY,
            mock_utils.EXISTING_DATA_P,
            mock_utils.EXISTING_VERSION,
            enums.JsonFilename.COLUMN_TYPES.value,
            enums.ColumnTypesKeys.LAST_DATA_UPDATE.value,
            {"foo": "bar"},
        ),
        (
            mock_utils.NEW_SITE,
            mock_utils.EXISTING_STUDY,
            mock_utils.EXISTING_DATA_P,
            mock_utils.EXISTING_VERSION,
            enums.JsonFilename.COLUMN_TYPES.value,
            enums.ColumnTypesKeys.COLUMNS.value,
            {"foo": "bar"},
        ),
        (
            mock_utils.NEW_SITE,
            mock_utils.EXISTING_STUDY,
            mock_utils.EXISTING_DATA_P,
            mock_utils.EXISTING_VERSION,
            enums.JsonFilename.COLUMN_TYPES.value,
            enums.ColumnTypesKeys.LAST_DATA_UPDATE.value,
            {"foo": "bar"},
        ),
        (
            mock_utils.NEW_SITE,
            mock_utils.NEW_STUDY,
            mock_utils.EXISTING_DATA_P,
            mock_utils.EXISTING_VERSION,
            enums.JsonFilename.COLUMN_TYPES.value,
            enums.ColumnTypesKeys.COLUMNS.value,
            {"foo": "bar"},
        ),
        (
            mock_utils.NEW_SITE,
            mock_utils.NEW_STUDY,
            mock_utils.EXISTING_DATA_P,
            mock_utils.EXISTING_VERSION,
            enums.JsonFilename.COLUMN_TYPES.value,
            enums.ColumnTypesKeys.LAST_DATA_UPDATE.value,
            {"foo": "bar"},
        ),
        (
            mock_utils.NEW_SITE,
            mock_utils.NEW_STUDY,
            mock_utils.NEW_DATA_P,
            mock_utils.EXISTING_VERSION,
            enums.JsonFilename.COLUMN_TYPES.value,
            enums.ColumnTypesKeys.COLUMNS.value,
            {"foo": "bar"},
        ),
        (
            mock_utils.NEW_SITE,
            mock_utils.NEW_STUDY,
            mock_utils.NEW_DATA_P,
            mock_utils.EXISTING_VERSION,
            enums.JsonFilename.COLUMN_TYPES.value,
            enums.ColumnTypesKeys.LAST_DATA_UPDATE.value,
            {"foo": "bar"},
        ),
        (
            mock_utils.NEW_SITE,
            mock_utils.NEW_STUDY,
            mock_utils.NEW_DATA_P,
            mock_utils.NEW_VERSION,
            enums.JsonFilename.COLUMN_TYPES.value,
            enums.ColumnTypesKeys.COLUMNS.value,
            {"foo": "bar"},
        ),
        (
            mock_utils.NEW_SITE,
            mock_utils.NEW_STUDY,
            mock_utils.NEW_DATA_P,
            mock_utils.NEW_VERSION,
            enums.JsonFilename.COLUMN_TYPES.value,
            enums.ColumnTypesKeys.LAST_DATA_UPDATE.value,
            {"foo": "bar"},
        ),
        (
            mock_utils.EXISTING_SITE,
            mock_utils.EXISTING_STUDY,
            mock_utils.EXISTING_DATA_P,
            mock_utils.EXISTING_VERSION,
            enums.JsonFilename.TRANSACTIONS.value,
            enums.ColumnTypesKeys.LAST_DATA_UPDATE.value,
            {"foo": "bar"},
        ),
        (
            mock_utils.NEW_SITE,
            mock_utils.NEW_STUDY,
            mock_utils.NEW_DATA_P,
            mock_utils.NEW_VERSION,
            enums.JsonFilename.TRANSACTIONS.value,
            enums.ColumnTypesKeys.LAST_DATA_UPDATE.value,
            {"foo": "bar"},
        ),
    ],
)
@freezegun.freeze_time("2025-01-01")
def test_update_local_metadata(
    mock_bucket, site, study, data_package, version, metadata_type, target, extras
):
    manager = s3_manager.S3Manager(mock_sns_event(site, study, data_package, version))
    if metadata_type == enums.JsonFilename.COLUMN_TYPES.value:
        metadata = manager.types_metadata
    else:
        metadata = manager.metadata
    original_transactions = copy.deepcopy(manager.metadata)
    original_types = copy.deepcopy(manager.types_metadata)
    mock_columns = {"cnt": "integer", "test": "string"}
    value = mock_columns if target == enums.ColumnTypesKeys.COLUMNS.value else None
    manager.update_local_metadata(
        key=target,
        site=site,
        # value should be ignored except when metadata_type is COLUMN_TYPES and the key is column
        value=value,
        metadata=metadata,
        meta_type=metadata_type,
        extra_items=extras,
    )
    if metadata_type != enums.JsonFilename.COLUMN_TYPES.value:
        assert original_types == manager.types_metadata
    else:
        assert original_types != manager.types_metadata
    if metadata_type != enums.JsonFilename.TRANSACTIONS.value:
        assert original_transactions == manager.metadata
    else:
        assert original_transactions != manager.metadata
    if metadata_type == enums.JsonFilename.COLUMN_TYPES.value:
        assert study in metadata.keys()
        assert version in metadata[study][data_package].keys()
        for extra in extras:
            assert extra in metadata[study][data_package][version].keys()
        if target == "columns":
            assert metadata[study][data_package][version]["columns"] == mock_columns
        else:
            assert metadata[study][data_package][version]["columns"] != mock_columns
    elif metadata_type == enums.JsonFilename.TRANSACTIONS.value:
        assert site in metadata.keys()
        assert version in metadata[site][study][data_package].keys()
        for extra in extras:
            assert extra in metadata[site][study][data_package][version].keys()


def test_write_local_metadata(mock_bucket):
    manager = s3_manager.S3Manager(
        mock_sns_event(
            mock_utils.EXISTING_SITE,
            mock_utils.EXISTING_STUDY,
            mock_utils.EXISTING_DATA_P,
            mock_utils.EXISTING_VERSION,
        )
    )
    manager.metadata = {"foo": "bar"}
    manager.write_local_metadata()
    metadata = manager.s3_client.get_object(
        Bucket=manager.s3_bucket_name, Key="metadata/transactions.json"
    )["Body"].read()
    assert metadata == b'{\n  "foo": "bar"\n}'
