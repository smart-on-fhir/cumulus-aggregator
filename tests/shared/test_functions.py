"""Unit tests for shared functions.


As of this writing, since a lot of this was historically covered by other tests,
this file does not contain a 1-1 set of tests to the source module,
instead focusing only on edge case scenarios (though in those cases, tests
should be comprehensive). 1-1 coverage is a desirable long term goal.
"""

from contextlib import nullcontext as does_not_raise
from unittest import mock

import boto3
import pandas
import pytest

from src.shared import enums, errors, functions, pandas_functions
from tests import mock_utils


@pytest.mark.parametrize(
    "copy_res,del_res,raises",
    [
        (200, 204, does_not_raise()),
        (400, 204, pytest.raises(functions.S3UploadError)),
        (200, 400, pytest.raises(functions.S3UploadError)),
    ],
)
def test_move_s3_file(copy_res, del_res, raises):
    s3_client = mock.MagicMock()
    s3_client.copy_object.return_value = {"ResponseMetadata": {"HTTPStatusCode": copy_res}}
    s3_client.delete_object.return_value = {"ResponseMetadata": {"HTTPStatusCode": del_res}}
    with raises:
        functions.move_s3_file(
            s3_client=s3_client, s3_bucket_name="bucket", old_key="old", new_key="new"
        )


@pytest.mark.parametrize(
    "meta_type,raises",
    [("column_types", does_not_raise()), ("wrong_value", pytest.raises(ValueError))],
)
def test_check_meta_type(meta_type, raises):
    with raises:
        functions.check_meta_type(meta_type)


def test_column_datatypes():
    df = pandas.DataFrame(
        data={
            "study_year": ["2020-01-01"],
            "study_month": ["2020-01-01"],
            "study_week": ["2020-01-01"],
            "study_day": ["2020-01-01"],
            "cnt_item": [10],
            "int": pandas.Series([10], dtype="Int32"),
            "float": pandas.Series([10.1], dtype="Float32"),
            "bool": pandas.Series([True], dtype="boolean"),
            "string": ["string"],
        }
    )
    col_types = pandas_functions.get_column_datatypes(df)
    assert col_types == {
        "study_year": {"type": "year", "distinct_values_count": 1},
        "study_month": {"type": "month", "distinct_values_count": 1},
        "study_week": {"type": "week", "distinct_values_count": 1},
        "study_day": {"type": "day", "distinct_values_count": 1},
        "cnt_item": {"type": "integer"},
        "int": {"type": "integer", "distinct_values_count": 1},
        "float": {"type": "float", "distinct_values_count": 1},
        "bool": {"type": "boolean", "distinct_values_count": 1},
        "string": {"type": "string", "distinct_values_count": 1},
    }


def test_update_metadata_error(mock_bucket):
    with pytest.raises(ValueError):
        enums.JsonFilename.FOO = "foo"
        functions.update_metadata(
            metadata={}, study="", data_package="", version="", target="", meta_type="foo"
        )


def test_get_s3_keys(mock_bucket):
    s3_client = boto3.client("s3")
    res = functions.get_s3_keys(s3_client, mock_utils.TEST_BUCKET, "")
    assert len(res) == mock_utils.ITEM_COUNT
    res = functions.get_s3_keys(s3_client, mock_utils.TEST_BUCKET, "", max_keys=2)
    assert len(res) == mock_utils.ITEM_COUNT
    res = functions.get_s3_keys(s3_client, mock_utils.TEST_BUCKET, "cache")
    assert res == ["cache/data_packages.json"]


def test_latest_data_package_version(mock_bucket):
    version = functions.get_latest_data_package_version(
        mock_utils.TEST_BUCKET, f"{enums.BucketPath.AGGREGATE.value}/{mock_utils.EXISTING_STUDY}"
    )
    assert version == mock_utils.EXISTING_VERSION
    s3_client = boto3.client("s3")
    s3_client.upload_file(
        "./tests/test_data/count_synthea_patient_agg.parquet",
        mock_utils.TEST_BUCKET,
        f"{enums.BucketPath.AGGREGATE.value}/{mock_utils.EXISTING_STUDY}/"
        f"{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}/"
        f"{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}__{mock_utils.NEW_VERSION}/"
        f"{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}__aggregate.parquet",
    )
    version = functions.get_latest_data_package_version(
        mock_utils.TEST_BUCKET, f"{enums.BucketPath.AGGREGATE.value}/{mock_utils.EXISTING_STUDY}"
    )
    version = functions.get_latest_data_package_version(
        mock_utils.TEST_BUCKET, f"{enums.BucketPath.AGGREGATE.value}/not_a_study"
    )
    assert version is None


@pytest.mark.parametrize(
    "path,expected,raises",
    [
        (
            f"{enums.BucketPath.AGGREGATE.value}/{mock_utils.EXISTING_STUDY}/"
            f"{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}/"
            f"{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}__{mock_utils.EXISTING_VERSION}/"
            f"{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}__aggregate.parquet",
            functions.PackageMetadata(
                mock_utils.EXISTING_STUDY,
                None,
                mock_utils.EXISTING_DATA_P,
                mock_utils.EXISTING_VERSION,
            ),
            does_not_raise(),
        ),
        (
            f"{enums.BucketPath.LAST_VALID.value}/{mock_utils.EXISTING_STUDY}/"
            f"{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}/"
            f"{mock_utils.EXISTING_SITE}/{mock_utils.EXISTING_VERSION}/"
            f"{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}__aggregate.parquet",
            functions.PackageMetadata(
                mock_utils.EXISTING_STUDY,
                mock_utils.EXISTING_SITE,
                mock_utils.EXISTING_DATA_P,
                mock_utils.EXISTING_VERSION,
            ),
            does_not_raise(),
        ),
        (
            f"{enums.BucketPath.FLAT.value}/{mock_utils.EXISTING_STUDY}/"
            f"{mock_utils.EXISTING_SITE}/"
            f"{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}__"
            f"{mock_utils.EXISTING_SITE}__{mock_utils.EXISTING_VERSION}/"
            f"{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}__aggregate.parquet",
            functions.PackageMetadata(
                mock_utils.EXISTING_STUDY,
                mock_utils.EXISTING_SITE,
                mock_utils.EXISTING_DATA_P,
                mock_utils.EXISTING_VERSION,
            ),
            does_not_raise(),
        ),
        (
            f"{enums.BucketPath.UPLOAD.value}/{mock_utils.EXISTING_STUDY}/"
            f"{mock_utils.EXISTING_DATA_P}/{mock_utils.EXISTING_SITE}/"
            f"{mock_utils.EXISTING_VERSION}/"
            f"{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}__aggregate.parquet",
            functions.PackageMetadata(
                mock_utils.EXISTING_STUDY,
                mock_utils.EXISTING_SITE,
                mock_utils.EXISTING_DATA_P,
                mock_utils.EXISTING_VERSION,
            ),
            does_not_raise(),
        ),
        ("badkey", None, pytest.raises(errors.AggregatorS3Error)),
        (
            "badsubbucket/site/study/verison/data.parquet",
            None,
            pytest.raises(errors.AggregatorS3Error),
        ),
    ],
)
def test_parse_s3_key(path, expected, raises):
    with raises:
        package = functions.parse_s3_key(path)
        assert package == expected
