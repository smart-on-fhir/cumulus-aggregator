import copy
import json
import os
from unittest import mock

import boto3
import botocore
import freezegun
import pandas
import pytest

from src.shared import enums, functions, s3_manager
from tests import mock_utils


def mock_sns_event(site, study, data_package, version):
    return {
        "Records": [
            {
                "Sns": {
                    "TopicArn": "arn",
                    "Message": (
                        f"latest/{study}/{study}__{data_package}/{site}/"
                        f"{study}__{data_package}__{version}/file.parquet"
                    ),
                }
            }
        ]
    }


def test_init_manager(mock_bucket):
    expected_transaction = (
        f"{enums.BucketPath.META.value}/transactions/"
        f"{mock_utils.EXISTING_SITE}__{mock_utils.EXISTING_STUDY}.json"
    )
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
        f"latest/{mock_utils.EXISTING_STUDY}/{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}/"
        f"{mock_utils.EXISTING_SITE}/{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}__"
        f"{mock_utils.EXISTING_VERSION}/file.parquet"
    )
    assert manager.study == mock_utils.EXISTING_STUDY
    assert manager.data_package == mock_utils.EXISTING_DATA_P
    assert manager.site == mock_utils.EXISTING_SITE
    assert manager.version == mock_utils.EXISTING_VERSION
    assert manager.metadata == mock_utils.get_mock_metadata()
    assert manager.types_metadata == mock_utils.get_mock_column_types_metadata()
    assert manager.transaction == (
        f"{enums.BucketPath.META.value}/transactions/"
        f"{mock_utils.EXISTING_SITE}__{mock_utils.EXISTING_STUDY}.json"
    )
    assert manager.parquet_aggregate_key == (
        "aggregates/study/study__encounter/study__encounter__099/"
        "study__encounter__aggregate.parquet"
    )
    assert manager.parquet_flat_key == (
        f"flat/study/{mock_utils.EXISTING_SITE}/{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}"
        f"__{mock_utils.EXISTING_SITE}__{mock_utils.EXISTING_VERSION}/"
        f"{mock_utils.EXISTING_STUDY}__encounter__{mock_utils.EXISTING_SITE}__flat.parquet"
    )
    assert manager.transaction == expected_transaction
    manager = s3_manager.S3Manager(
        {},
        site=mock_utils.EXISTING_SITE,
        study=mock_utils.EXISTING_STUDY,
        data_package=mock_utils.EXISTING_DATA_P,
        version=mock_utils.EXISTING_VERSION,
    )
    assert manager.study == mock_utils.EXISTING_STUDY
    assert manager.data_package == mock_utils.EXISTING_DATA_P
    assert manager.site == mock_utils.EXISTING_SITE
    assert manager.version == mock_utils.EXISTING_VERSION
    assert manager.transaction == (
        f"{enums.BucketPath.META.value}/transactions/"
        f"{mock_utils.EXISTING_SITE}__{mock_utils.EXISTING_STUDY}.json"
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
    assert publish_args["TopicArn"] == mock_utils.TEST_COMPLETENESS_ARN
    assert publish_args["Message"] == json.dumps(
        {"site": mock_utils.EXISTING_SITE, "study": mock_utils.EXISTING_STUDY}
    )
    assert publish_args["Subject"] == "check_completeness"


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
    manager.write_parquet(
        df,
        key=(
            "aggregates/study/study__encounter/"
            "study__encounter__099/study__encounter__aggregate.parquet"
        ),
    )
    assert mock_cache.called


def test_presigned_error_handling(mock_bucket):
    manager = s3_manager.S3Manager(
        mock_sns_event(
            mock_utils.EXISTING_SITE,
            mock_utils.EXISTING_STUDY,
            mock_utils.EXISTING_DATA_P,
            mock_utils.EXISTING_VERSION,
        )
    )
    mock_client = mock.MagicMock()
    mock_client.generate_presigned_url.side_effect = botocore.exceptions.ClientError(
        error_response={}, operation_name="op"
    )
    manager.s3_client = mock_client
    res = manager.get_presigned_download_url("missing.parquet")
    assert res is None


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
    mock_bucket, site, study, data_package, version, metadata_type, target, extras, mock_queue
):
    dp_id = f"{study}__{data_package}__{version}"
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
        assert dp_id in metadata[study][data_package].keys()
        for extra in extras:
            assert extra in metadata[study][data_package][dp_id].keys()
        if target == "columns":
            assert metadata[study][data_package][dp_id]["columns"] == mock_columns
        else:
            assert metadata[study][data_package][dp_id]["columns"] != mock_columns
    elif metadata_type == enums.JsonFilename.TRANSACTIONS.value:
        assert site in metadata.keys()
        assert dp_id in metadata[site][study][data_package].keys()
        for extra in extras:
            assert extra in metadata[site][study][data_package][dp_id].keys()


def test_write_local_metadata(mock_bucket, mock_env, mock_queue):
    manager = s3_manager.S3Manager(
        mock_sns_event(
            mock_utils.EXISTING_SITE,
            mock_utils.EXISTING_STUDY,
            mock_utils.EXISTING_DATA_P,
            mock_utils.EXISTING_VERSION,
        )
    )
    manager.metadata = {"a": "b", "foo": "bar"}
    manager.metadata_delta = {"foo": "bar"}
    manager.write_local_metadata()
    sqs_client = boto3.client("sqs", region_name="us-east-1")
    res = sqs_client.receive_message(
        QueueUrl=mock_utils.TEST_METADATA_UPDATE_URL, MaxNumberOfMessages=10
    )
    assert len(res["Messages"]) == 1
    message = json.loads(res["Messages"][0]["Body"])
    assert message == {
        "s3_bucket_name": "cumulus-aggregator-site-counts-test",
        "key": "metadata/transactions.json",
        "updates": '{\n  "foo": "bar"\n}',
    }


@mock.patch.dict(os.environ, mock_utils.MOCK_ENV)
def test_validate_transaction(mock_bucket, mock_queue):
    manager = s3_manager.S3Manager(
        mock_sns_event(
            mock_utils.EXISTING_SITE,
            mock_utils.EXISTING_STUDY,
            mock_utils.EXISTING_DATA_P,
            mock_utils.EXISTING_VERSION,
        )
    )
    assert (
        "Contents"
        not in manager.s3_client.list_objects_v2(
            Bucket=manager.s3_bucket_name, Prefix=f"{enums.BucketPath.META.value}/transactions/"
        ).keys()
    )
    transaction = manager.request_or_validate_transaction()
    transaction = functions.get_s3_json_as_dict(
        manager.s3_bucket_name,
        (
            f"{enums.BucketPath.META.value}/transactions/"
            f"{mock_utils.EXISTING_SITE}__{mock_utils.EXISTING_STUDY}.json"
        ),
    )
    assert transaction == transaction
    transaction = manager.request_or_validate_transaction(transaction["id"])
    assert transaction == transaction
    with pytest.raises(s3_manager.errors.AggregatorStudyProcessingError):
        manager.request_or_validate_transaction()
    with pytest.raises(s3_manager.errors.AggregatorStudyProcessingError):
        manager.request_or_validate_transaction("invalid transaction string")


def test_delete_transaction(mock_bucket):
    manager = s3_manager.S3Manager(
        mock_sns_event(
            mock_utils.EXISTING_SITE,
            mock_utils.EXISTING_STUDY,
            mock_utils.EXISTING_DATA_P,
            mock_utils.EXISTING_VERSION,
        )
    )
    manager.write_data_to_file(
        path=(
            f"{enums.BucketPath.META.value}/transactions/"
            f"{mock_utils.EXISTING_SITE}__{mock_utils.EXISTING_STUDY}.json"
        ),
        data="foo",
    )
    assert (
        len(
            manager.s3_client.list_objects_v2(
                Bucket=manager.s3_bucket_name, Prefix=f"{enums.BucketPath.META.value}/transactions/"
            )["Contents"]
        )
        == 1
    )
    manager.delete_transaction()
    assert (
        "Contents"
        not in manager.s3_client.list_objects_v2(
            Bucket=manager.s3_bucket_name, Prefix=f"{enums.BucketPath.META.value}/transactions/"
        ).keys()
    )
