import io
from contextlib import nullcontext as does_not_raise
from datetime import UTC, datetime

import awswrangler
import boto3
import pytest
from freezegun import freeze_time
from pandas import DataFrame, read_parquet

from src.shared import enums, functions
from src.site_upload.powerset_merge import powerset_merge
from tests.mock_utils import (
    EXISTING_DATA_P,
    EXISTING_SITE,
    EXISTING_STUDY,
    EXISTING_VERSION,
    ITEM_COUNT,
    NEW_SITE,
    NEW_STUDY,
    NEW_VERSION,
    TEST_BUCKET,
    get_mock_column_types_metadata,
    get_mock_metadata,
)


@freeze_time("2020-01-01")
@pytest.mark.parametrize(
    "upload_file,upload_path,event_key,archives,duplicates,status,expected_contents",
    [
        (  # Adding a new data package to a site with uploads
            "./tests/test_data/count_synthea_patient.parquet",
            f"/{NEW_STUDY}/{NEW_STUDY}__{EXISTING_DATA_P}/{EXISTING_SITE}/"
            f"{EXISTING_VERSION}/encounter.parquet",
            f"/{NEW_STUDY}/{NEW_STUDY}__{EXISTING_DATA_P}/{EXISTING_SITE}/"
            f"{EXISTING_VERSION}/encounter.parquet",
            False,
            False,
            200,
            ITEM_COUNT + 2,
        ),
        (  # Adding a new data package to a site without uploads
            "./tests/test_data/count_synthea_patient.parquet",
            f"/{NEW_STUDY}/{NEW_STUDY}__{EXISTING_DATA_P}/{NEW_SITE}"
            f"/{EXISTING_VERSION}/encounter.parquet",
            f"/{NEW_STUDY}/{NEW_STUDY}__{EXISTING_DATA_P}/{NEW_SITE}"
            f"/{EXISTING_VERSION}/encounter.parquet",
            False,
            False,
            200,
            ITEM_COUNT + 2,
        ),
        (  # Updating an existing data package
            "./tests/test_data/count_synthea_patient.parquet",
            f"/{EXISTING_STUDY}/{EXISTING_STUDY}__{EXISTING_DATA_P}/{EXISTING_SITE}"
            f"/{EXISTING_VERSION}/encounter.parquet",
            f"/{EXISTING_STUDY}/{EXISTING_STUDY}__{EXISTING_DATA_P}/{EXISTING_SITE}"
            f"/{EXISTING_VERSION}/encounter.parquet",
            True,
            False,
            200,
            ITEM_COUNT + 2,
        ),
        (  # Updating an existing data package w/ extra files
            "./tests/test_data/count_synthea_patient.parquet",
            f"/{EXISTING_STUDY}/{EXISTING_STUDY}__{EXISTING_DATA_P}/{EXISTING_SITE}"
            f"/{EXISTING_VERSION}/encounter.parquet",
            f"/{EXISTING_STUDY}/{EXISTING_STUDY}__{EXISTING_DATA_P}/{EXISTING_SITE}"
            f"/{EXISTING_VERSION}/encounter.parquet",
            True,
            True,
            200,
            ITEM_COUNT + 3,
        ),
        (  # New version of existing data package
            "./tests/test_data/count_synthea_patient.parquet",
            f"/{EXISTING_STUDY}/{EXISTING_STUDY}__{EXISTING_DATA_P}/{EXISTING_SITE}"
            f"/{NEW_VERSION}/encounter.parquet",
            f"/{EXISTING_STUDY}/{EXISTING_STUDY}__{EXISTING_DATA_P}/{EXISTING_SITE}"
            f"/{NEW_VERSION}/encounter.parquet",
            True,
            False,
            200,
            ITEM_COUNT + 3,
        ),
        (  # Invalid parquet file
            "./tests/site_upload/test_powerset_merge.py",
            f"/{NEW_STUDY}/{NEW_STUDY}__{EXISTING_DATA_P}/{EXISTING_SITE}"
            f"/{EXISTING_VERSION}/patient.parquet",
            f"/{NEW_STUDY}/{NEW_STUDY}__{EXISTING_DATA_P}/{EXISTING_SITE}"
            f"/{EXISTING_VERSION}/patient.parquet",
            False,
            False,
            500,
            ITEM_COUNT + 1,
        ),
        (  # Checks presence of commas in strings does not cause an error
            "./tests/test_data/cube_strings_with_commas.parquet",
            f"/{NEW_STUDY}/{NEW_STUDY}__{EXISTING_DATA_P}/{EXISTING_SITE}"
            f"/{EXISTING_VERSION}/encounter.parquet",
            f"/{NEW_STUDY}/{NEW_STUDY}__{EXISTING_DATA_P}/{EXISTING_SITE}"
            f"/{EXISTING_VERSION}/encounter.parquet",
            False,
            False,
            200,
            ITEM_COUNT + 2,
        ),
        (  # ensuring that a data package that is a substring does not get
            # merged by substr match
            "./tests/test_data/count_synthea_patient.parquet",
            f"/{EXISTING_STUDY}/{EXISTING_STUDY}__{EXISTING_DATA_P[0:-2]}/{EXISTING_SITE}/"
            f"{EXISTING_VERSION}/encount.parquet",
            f"/{EXISTING_STUDY}/{EXISTING_STUDY}__{EXISTING_DATA_P[0:-2]}/{EXISTING_SITE}/"
            f"{EXISTING_VERSION}/encount.parquet",
            False,
            False,
            200,
            ITEM_COUNT + 2,
        ),
        (  # Empty file upload
            None,
            f"/{NEW_STUDY}/{NEW_STUDY}__{EXISTING_DATA_P}/{EXISTING_SITE}"
            f"/{EXISTING_VERSION}/encounter.parquet",
            f"/{NEW_STUDY}/{NEW_STUDY}__{EXISTING_DATA_P}/{EXISTING_SITE}"
            f"/{EXISTING_VERSION}/encounter.parquet",
            False,
            False,
            500,
            ITEM_COUNT + 1,
        ),
        (  # Race condition - file deleted before job starts
            None,
            None,
            f"/{NEW_STUDY}/{NEW_STUDY}__{EXISTING_DATA_P}/{EXISTING_SITE}"
            f"/{EXISTING_VERSION}/encounter.parquet",
            False,
            False,
            500,
            ITEM_COUNT,
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
):
    s3_client = boto3.client("s3", region_name="us-east-1")
    if upload_file is not None:
        s3_client.upload_file(
            upload_file,
            TEST_BUCKET,
            f"{enums.BucketPath.LATEST.value}{upload_path}",
        )
    elif upload_path is not None:
        with io.BytesIO(DataFrame().to_parquet()) as upload_fileobj:
            s3_client.upload_fileobj(
                upload_fileobj,
                TEST_BUCKET,
                f"{enums.BucketPath.LATEST.value}{upload_path}",
            )
    if archives:
        s3_client.upload_file(
            upload_file,
            TEST_BUCKET,
            f"{enums.BucketPath.LAST_VALID.value}{upload_path}",
        )
    if duplicates:
        duplicate_path = upload_path.replace(".parquet", "duplicate.parquet")
        s3_client.upload_file(
            upload_file,
            TEST_BUCKET,
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
    s3_res = s3_client.list_objects_v2(Bucket=TEST_BUCKET)
    assert len(s3_res["Contents"]) == expected_contents
    for item in s3_res["Contents"]:
        if item["Key"].endswith("aggregate.parquet"):
            assert item["Key"].startswith(enums.BucketPath.AGGREGATE.value)
            # This finds the aggregate that was created/updated - ie it skips mocks
            if study in item["Key"] and status == 200:
                agg_df = awswrangler.s3.read_parquet(f"s3://{TEST_BUCKET}/{item['Key']}")
                assert (agg_df["site"].eq(site)).any()
        elif item["Key"].endswith("transactions.json"):
            assert item["Key"].startswith(enums.BucketPath.META.value)
            metadata = functions.read_metadata(s3_client, TEST_BUCKET)
            if res["statusCode"] == 200:
                assert (
                    metadata[site][study][data_package.split("__")[1]][dp_id]["last_aggregation"]
                    == datetime.now(UTC).isoformat()
                )

            else:
                assert (
                    metadata["princeton_plainsboro_teaching_hospital"]["study"]["encounter"]["099"][
                        "last_aggregation"
                    ]
                    == get_mock_metadata()["princeton_plainsboro_teaching_hospital"]["study"][
                        "encounter"
                    ]["099"]["last_aggregation"]
                )
            if upload_file is not None and study != NEW_STUDY:
                # checking to see that merge powerset didn't touch last upload
                assert (
                    metadata[site][study][data_package.split("__")[1]][dp_id]["last_upload"]
                    != datetime.now(UTC).isoformat()
                )
        elif item["Key"].endswith("column_types.json"):
            assert item["Key"].startswith(enums.BucketPath.META.value)
            metadata = functions.read_metadata(
                s3_client, TEST_BUCKET, meta_type=enums.JsonFilename.COLUMN_TYPES.value
            )
            if res["statusCode"] == 200:
                last_update = metadata[study][data_package.split("__")[1]][dp_id][
                    "last_data_update"
                ]
                assert last_update == datetime.now(UTC).isoformat()

            else:
                assert (
                    metadata["study"]["encounter"]["study__encounter__099"]["last_data_update"]
                    == get_mock_column_types_metadata()["study"]["encounter"][
                        "study__encounter__099"
                    ]["last_data_update"]
                )
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
                or item["Key"].endswith("study_periods.json")
            )
    if archives:
        keys = []
        for resource in s3_res["Contents"]:
            keys.append(resource["Key"])
        date_str = datetime.now(UTC).isoformat()
        archive_path = f"/{date_str}.".join(upload_path.rsplit("/", 1))
        assert f"{enums.BucketPath.ARCHIVE.value}{archive_path}" in keys


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
):
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.upload_file(
        upload_file,
        TEST_BUCKET,
        f"{enums.BucketPath.LATEST.value}/{EXISTING_STUDY}/"
        f"{EXISTING_STUDY}__{EXISTING_DATA_P}/{NEW_SITE}/"
        f"{EXISTING_STUDY}__{EXISTING_DATA_P}__{EXISTING_VERSION}/encounter.parquet",
    )

    s3_client.upload_file(
        "./tests/test_data/count_synthea_patient.parquet",
        TEST_BUCKET,
        f"{enums.BucketPath.LAST_VALID.value}/{EXISTING_STUDY}/"
        f"{EXISTING_STUDY}__{EXISTING_DATA_P}/{EXISTING_SITE}/"
        f"{EXISTING_STUDY}__{EXISTING_DATA_P}__{EXISTING_VERSION}/encounter.parquet",
    )

    if archives:
        s3_client.upload_file(
            "./tests/test_data/count_synthea_patient.parquet",
            TEST_BUCKET,
            f"{enums.BucketPath.LAST_VALID.value}/{EXISTING_STUDY}/"
            f"{EXISTING_STUDY}__{EXISTING_DATA_P}/{NEW_SITE}/"
            f"{EXISTING_STUDY}__{EXISTING_DATA_P}__{EXISTING_VERSION}/encounter.parquet",
        )

    event = {
        "Records": [
            {
                "Sns": {
                    "Message": f"{enums.BucketPath.LATEST.value}/{EXISTING_STUDY}"
                    f"/{EXISTING_STUDY}__{EXISTING_DATA_P}/{NEW_SITE}"
                    f"/{EXISTING_VERSION}/encounter.parquet",
                    "TopicArn": "TOPIC_PROCESS_COUNTS_ARN",
                }
            }
        ]
    }
    res = powerset_merge.powerset_merge_handler(event, {})
    assert res["statusCode"] == 200
    errors = 0
    s3_res = s3_client.list_objects_v2(Bucket=TEST_BUCKET)
    for item in s3_res["Contents"]:
        if item["Key"].startswith(enums.BucketPath.ERROR.value):
            errors += 1
        elif item["Key"].startswith(f"{enums.BucketPath.AGGREGATE.value}/study"):
            agg_df = awswrangler.s3.read_parquet(f"s3://{TEST_BUCKET}/{item['Key']}")
            # if a file cant be merged and there's no fallback, we expect
            # [<NA>, site_name], otherwise, [<NA>, site_name, uploading_site_name]
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
            TEST_BUCKET,
            s3_path,
        )
        powerset_merge.expand_and_concat_powersets(
            df, f"s3://{TEST_BUCKET}/{s3_path}", EXISTING_STUDY
        )
