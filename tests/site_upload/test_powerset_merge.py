import io
import json
from contextlib import nullcontext as does_not_raise
from datetime import UTC, datetime

import awswrangler
import boto3
import pandas
import pytest
from freezegun import freeze_time
from pandas import DataFrame, read_parquet

from src.shared import enums
from src.site_upload.powerset_merge import powerset_merge
from tests import mock_utils


@freeze_time("2020-01-01")
@pytest.mark.parametrize(
    "upload_file,upload_path,event_key,archives,duplicates,status,expected_contents,expected_rows,first_row,last_row",
    [
        (  # Adding a new data package to a site with uploads
            "./tests/test_data/count_synthea_patient.parquet",
            f"/{mock_utils.NEW_STUDY}/{mock_utils.NEW_STUDY}__{mock_utils.EXISTING_DATA_P}/{mock_utils.EXISTING_SITE}/"
            f"{mock_utils.EXISTING_VERSION}/encounter.parquet",
            f"/{mock_utils.NEW_STUDY}/{mock_utils.NEW_STUDY}__{mock_utils.EXISTING_DATA_P}/{mock_utils.EXISTING_SITE}/"
            f"{mock_utils.EXISTING_VERSION}/encounter.parquet",
            False,
            False,
            200,
            mock_utils.ITEM_COUNT + 2,
            506,
            [1103, pandas.NA, pandas.NA, pandas.NA, pandas.NA],
            [10, pandas.NA, 78, "Not Hispanic or Latino", "princeton_plainsboro_teaching_hospital"],
        ),
        (  # Adding a new data package to a site without uploads
            "./tests/test_data/count_synthea_patient.parquet",
            f"/{mock_utils.NEW_STUDY}/{mock_utils.NEW_STUDY}__{mock_utils.EXISTING_DATA_P}/{mock_utils.NEW_SITE}"
            f"/{mock_utils.EXISTING_VERSION}/encounter.parquet",
            f"/{mock_utils.NEW_STUDY}/{mock_utils.NEW_STUDY}__{mock_utils.EXISTING_DATA_P}/{mock_utils.NEW_SITE}"
            f"/{mock_utils.EXISTING_VERSION}/encounter.parquet",
            False,
            False,
            200,
            mock_utils.ITEM_COUNT + 2,
            506,
            [1103, pandas.NA, pandas.NA, pandas.NA, pandas.NA],
            [10, pandas.NA, 78, "Not Hispanic or Latino", "chicago_hope"],
        ),
        (  # Updating an existing data package
            "./tests/test_data/count_synthea_patient.parquet",
            f"/{mock_utils.EXISTING_STUDY}/{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}/{mock_utils.EXISTING_SITE}"
            f"/{mock_utils.EXISTING_VERSION}/encounter.parquet",
            f"/{mock_utils.EXISTING_STUDY}/{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}/{mock_utils.EXISTING_SITE}"
            f"/{mock_utils.EXISTING_VERSION}/encounter.parquet",
            True,
            False,
            200,
            mock_utils.ITEM_COUNT + 2,
            506,
            [1103, pandas.NA, pandas.NA, pandas.NA, pandas.NA],
            [10, pandas.NA, 78, "Not Hispanic or Latino", "princeton_plainsboro_teaching_hospital"],
        ),
        (  # Updating an existing data package w/ extra files
            "./tests/test_data/count_synthea_patient.parquet",
            f"/{mock_utils.EXISTING_STUDY}/{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}/{mock_utils.EXISTING_SITE}"
            f"/{mock_utils.EXISTING_VERSION}/encounter.parquet",
            f"/{mock_utils.EXISTING_STUDY}/{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}/{mock_utils.EXISTING_SITE}"
            f"/{mock_utils.EXISTING_VERSION}/encounter.parquet",
            True,
            True,
            200,
            mock_utils.ITEM_COUNT + 3,
            506,
            [1103, pandas.NA, pandas.NA, pandas.NA, pandas.NA],
            [10, pandas.NA, 78, "Not Hispanic or Latino", "princeton_plainsboro_teaching_hospital"],
        ),
        (  # New version of existing data package
            "./tests/test_data/count_synthea_patient.parquet",
            f"/{mock_utils.EXISTING_STUDY}/{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}/{mock_utils.EXISTING_SITE}"
            f"/{mock_utils.NEW_VERSION}/encounter.parquet",
            f"/{mock_utils.EXISTING_STUDY}/{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}/{mock_utils.EXISTING_SITE}"
            f"/{mock_utils.NEW_VERSION}/encounter.parquet",
            True,
            False,
            200,
            mock_utils.ITEM_COUNT + 3,
            506,
            [1103, pandas.NA, pandas.NA, pandas.NA, pandas.NA],
            [10, pandas.NA, 78, "Not Hispanic or Latino", "princeton_plainsboro_teaching_hospital"],
        ),
        (  # Invalid parquet file
            "./tests/site_upload/test_powerset_merge.py",
            f"/{mock_utils.NEW_STUDY}/{mock_utils.NEW_STUDY}__{mock_utils.EXISTING_DATA_P}/{mock_utils.EXISTING_SITE}"
            f"/{mock_utils.EXISTING_VERSION}/patient.parquet",
            f"/{mock_utils.NEW_STUDY}/{mock_utils.NEW_STUDY}__{mock_utils.EXISTING_DATA_P}/{mock_utils.EXISTING_SITE}"
            f"/{mock_utils.EXISTING_VERSION}/patient.parquet",
            False,
            False,
            500,
            mock_utils.ITEM_COUNT + 1,
            0,
            [],
            [],
        ),
        (  # Checks presence of commas in strings does not cause an error
            "./tests/test_data/cube_strings_with_commas.parquet",
            f"/{mock_utils.NEW_STUDY}/{mock_utils.NEW_STUDY}__{mock_utils.EXISTING_DATA_P}/{mock_utils.EXISTING_SITE}"
            f"/{mock_utils.EXISTING_VERSION}/encounter.parquet",
            f"/{mock_utils.NEW_STUDY}/{mock_utils.NEW_STUDY}__{mock_utils.EXISTING_DATA_P}/{mock_utils.EXISTING_SITE}"
            f"/{mock_utils.EXISTING_VERSION}/encounter.parquet",
            False,
            False,
            200,
            mock_utils.ITEM_COUNT + 2,
            30,
            [37990, pandas.NA, pandas.NA, pandas.NA],
            [
                27,
                "female",
                "American Indian, or Alaska Native",
                "princeton_plainsboro_teaching_hospital",
            ],
        ),
        (  # ensuring that a data package that is a substring does not get
            # merged by substr match
            "./tests/test_data/count_synthea_patient.parquet",
            f"/{mock_utils.EXISTING_STUDY}/{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P[0:-2]}/{mock_utils.EXISTING_SITE}/"
            f"{mock_utils.EXISTING_VERSION}/encount.parquet",
            f"/{mock_utils.EXISTING_STUDY}/{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P[0:-2]}/{mock_utils.EXISTING_SITE}/"
            f"{mock_utils.EXISTING_VERSION}/encount.parquet",
            False,
            False,
            200,
            mock_utils.ITEM_COUNT + 2,
            506,
            [1103, pandas.NA, pandas.NA, pandas.NA, pandas.NA],
            [10, pandas.NA, 78, "Not Hispanic or Latino", "princeton_plainsboro_teaching_hospital"],
        ),
        (  # Empty file upload
            None,
            f"/{mock_utils.NEW_STUDY}/{mock_utils.NEW_STUDY}__{mock_utils.EXISTING_DATA_P}/{mock_utils.EXISTING_SITE}"
            f"/{mock_utils.EXISTING_VERSION}/encounter.parquet",
            f"/{mock_utils.NEW_STUDY}/{mock_utils.NEW_STUDY}__{mock_utils.EXISTING_DATA_P}/{mock_utils.EXISTING_SITE}"
            f"/{mock_utils.EXISTING_VERSION}/encounter.parquet",
            False,
            False,
            500,
            mock_utils.ITEM_COUNT + 1,
            0,
            [],
            [],
        ),
        (  # Race condition - file deleted before job starts
            None,
            None,
            f"/{mock_utils.NEW_STUDY}/{mock_utils.NEW_STUDY}__{mock_utils.EXISTING_DATA_P}/{mock_utils.EXISTING_SITE}"
            f"/{mock_utils.EXISTING_VERSION}/encounter.parquet",
            False,
            False,
            500,
            mock_utils.ITEM_COUNT,
            0,
            [],
            [],
        ),
    ],
)
def test_powerset_merge_single_upload(
    upload_file,
    upload_path,
    event_key,
    archives,
    duplicates,
    status,
    expected_contents,
    mock_bucket,
    mock_notification,
    mock_queue,
    expected_rows,
    first_row,
    last_row,
):
    s3_client = boto3.client("s3", region_name="us-east-1")
    sqs_client = boto3.client("sqs", region_name="us-east-1")
    if upload_file is not None:
        s3_client.upload_file(
            upload_file,
            mock_utils.TEST_BUCKET,
            f"{enums.BucketPath.LATEST.value}{upload_path}",
        )
    elif upload_path is not None:
        with io.BytesIO(DataFrame().to_parquet()) as upload_fileobj:
            s3_client.upload_fileobj(
                upload_fileobj,
                mock_utils.TEST_BUCKET,
                f"{enums.BucketPath.LATEST.value}{upload_path}",
            )
    if archives:
        s3_client.upload_file(
            upload_file,
            mock_utils.TEST_BUCKET,
            f"{enums.BucketPath.LAST_VALID.value}{upload_path}",
        )
    if duplicates:
        duplicate_path = upload_path.replace(".parquet", "duplicate.parquet")
        s3_client.upload_file(
            upload_file,
            mock_utils.TEST_BUCKET,
            f"{enums.BucketPath.LAST_VALID.value}{duplicate_path}",
        )

    event = {
        "Records": [
            {
                "Sns": {
                    "Message": f"{enums.BucketPath.LATEST.value}{event_key}",
                    "TopicArn": "TOPIC_PROCESS_COUNTS_ARN",
                },
            }
        ]
    }
    # This array looks like:
    # ['', 'study', 'study__package', 'site', 'version','file']
    event_list = event_key.split("/")
    study = event_list[1]
    data_package = event_list[2]
    site = event_list[3]
    version = event_list[4]
    dp_id = f"{data_package}__{version}"
    res = powerset_merge.powerset_merge_handler(event, {})
    assert res["statusCode"] == status
    s3_res = s3_client.list_objects_v2(Bucket=mock_utils.TEST_BUCKET)
    assert len(s3_res["Contents"]) == expected_contents
    for item in s3_res["Contents"]:
        if item["Key"].endswith("aggregate.parquet"):
            assert item["Key"].startswith(enums.BucketPath.AGGREGATE.value)
            # This finds the aggregate that was created/updated - ie it skips mocks
            if study in item["Key"] and status == 200:
                agg_df = awswrangler.s3.read_parquet(f"s3://{mock_utils.TEST_BUCKET}/{item['Key']}")
                assert (agg_df["site"].eq(site)).any()
                assert expected_rows == len(agg_df)
                assert first_row == agg_df.iloc[0].to_list()
                assert last_row == agg_df.iloc[-1].to_list()
        elif item["Key"].startswith(enums.BucketPath.LAST_VALID.value):
            if item["Key"].endswith(".parquet"):
                assert item["Key"] == (f"{enums.BucketPath.LAST_VALID.value}{upload_path}")
            elif item["Key"].endswith(".csv"):
                assert f"{upload_path.replace('.parquet', '.csv')}" in item["Key"]
            else:
                raise Exception(f"Invalid csv found at {item['Key']}")
        else:
            assert (
                item["Key"].startswith(enums.BucketPath.ARCHIVE.value)
                or item["Key"].startswith(enums.BucketPath.ERROR.value)
                or item["Key"].startswith(enums.BucketPath.ADMIN.value)
                or item["Key"].startswith(enums.BucketPath.CACHE.value)
                or item["Key"].startswith(enums.BucketPath.FLAT.value)
                or item["Key"].startswith(enums.BucketPath.STUDY_META.value)
                or item["Key"].startswith(enums.BucketPath.META.value)
            )
    if archives:
        keys = []
        for resource in s3_res["Contents"]:
            keys.append(resource["Key"])
        date_str = datetime.now(UTC).isoformat()
        archive_path = f"/{date_str}.".join(upload_path.rsplit("/", 1))
        assert f"{enums.BucketPath.ARCHIVE.value}{archive_path}" in keys
    if res["statusCode"] == 200:
        sqs_res = sqs_client.receive_message(
            QueueUrl=mock_utils.TEST_METADATA_UPDATE_URL, MaxNumberOfMessages=10
        )
        assert len(sqs_res["Messages"]) == 2
        transactions = json.loads(sqs_res["Messages"][0]["Body"])
        assert transactions["key"] == "metadata/transactions.json"
        t_updates = json.loads(transactions["updates"])
        assert (
            t_updates[site][study][data_package.split("__")[1]][dp_id]["last_data_update"]
            == datetime.now(UTC).isoformat()
        )
        assert (
            "transaction_format_version"
            in t_updates[site][study][data_package.split("__")[1]][dp_id].keys()
        )

        column_types = json.loads(sqs_res["Messages"][1]["Body"])
        assert column_types["key"] == "metadata/column_types.json"
        c_updates = json.loads(column_types["updates"])
        assert (
            c_updates[study][data_package.split("__")[1]][dp_id]["last_data_update"]
            == datetime.now(UTC).isoformat()
        )
        assert (
            "column_types_format_version"
            in c_updates[study][data_package.split("__")[1]][dp_id].keys()
        )


@freeze_time("2020-01-01")
@pytest.mark.parametrize(
    "upload_file,archives,expected_errors",
    [
        ("./tests/test_data/count_synthea_patient.parquet", False, 0),
        ("./tests/test_data/other_schema.parquet", False, 1),
        ("./tests/test_data/other_schema.parquet", True, 1),
    ],
)
def test_powerset_merge_join_study_data(
    upload_file,
    archives,
    expected_errors,
    mock_bucket,
    mock_notification,
    mock_queue,
):
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.upload_file(
        upload_file,
        mock_utils.TEST_BUCKET,
        f"{enums.BucketPath.LATEST.value}/{mock_utils.EXISTING_STUDY}/"
        f"{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}/{mock_utils.NEW_SITE}/"
        f"{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}__{mock_utils.EXISTING_VERSION}/encounter.parquet",
    )

    s3_client.upload_file(
        "./tests/test_data/count_synthea_patient.parquet",
        mock_utils.TEST_BUCKET,
        f"{enums.BucketPath.LAST_VALID.value}/{mock_utils.EXISTING_STUDY}/"
        f"{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}/{mock_utils.EXISTING_SITE}/"
        f"{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}__{mock_utils.EXISTING_VERSION}/encounter.parquet",
    )

    if archives:
        s3_client.upload_file(
            "./tests/test_data/count_synthea_patient.parquet",
            mock_utils.TEST_BUCKET,
            f"{enums.BucketPath.LAST_VALID.value}/{mock_utils.EXISTING_STUDY}/"
            f"{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}/{mock_utils.NEW_SITE}/"
            f"{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}__{mock_utils.EXISTING_VERSION}/encounter.parquet",
        )

    event = {
        "Records": [
            {
                "Sns": {
                    "Message": f"{enums.BucketPath.LATEST.value}/{mock_utils.EXISTING_STUDY}"
                    f"/{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}/{mock_utils.NEW_SITE}"
                    f"/{mock_utils.EXISTING_VERSION}/encounter.parquet",
                    "TopicArn": "TOPIC_PROCESS_COUNTS_ARN",
                }
            }
        ]
    }
    res = powerset_merge.powerset_merge_handler(event, {})
    assert res["statusCode"] == 200
    errors = 0
    s3_res = s3_client.list_objects_v2(Bucket=mock_utils.TEST_BUCKET)
    for item in s3_res["Contents"]:
        if item["Key"].startswith(enums.BucketPath.ERROR.value):
            errors += 1
        elif item["Key"].startswith(f"{enums.BucketPath.AGGREGATE.value}/study"):
            agg_df = awswrangler.s3.read_parquet(f"s3://{mock_utils.TEST_BUCKET}/{item['Key']}")
            # if a file cant be merged and there's no fallback, we expect
            # [pandas.NA, site_name], otherwise, [pandas.NA, site_name, uploading_site_name]
            if errors != 0 and not archives:
                assert len(agg_df["site"].unique() == 2)
            else:
                assert len(agg_df["site"].unique() == 3)
    assert errors == expected_errors


# Explicitly testing for raising errors during concat due to them being appropriately
# handled by the generic error handler
@pytest.mark.parametrize(
    "upload_file,load_empty,raises",
    [
        ("./tests/test_data/count_synthea_patient.parquet", False, does_not_raise()),
        (
            "./tests/test_data/cube_simple_example.parquet",
            False,
            pytest.raises(powerset_merge.MergeError),
        ),
        (
            "./tests/test_data/count_synthea_empty.parquet",
            True,
            pytest.raises(powerset_merge.MergeError),
        ),
    ],
)
def test_expand_and_concat(mock_bucket, upload_file, load_empty, raises):
    with raises:
        df = read_parquet("./tests/test_data/count_synthea_patient_agg.parquet")
        s3_path = "/test/uploaded.parquet"
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.upload_file(
            upload_file,
            mock_utils.TEST_BUCKET,
            s3_path,
        )
        powerset_merge.expand_and_concat_powersets(
            df, f"s3://{mock_utils.TEST_BUCKET}/{s3_path}", mock_utils.EXISTING_STUDY
        )
