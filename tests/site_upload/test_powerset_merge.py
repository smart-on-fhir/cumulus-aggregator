import io
import os
from contextlib import nullcontext as does_not_raise
from datetime import datetime, timezone
from unittest import mock

import awswrangler
import boto3
import pytest
from freezegun import freeze_time
from pandas import DataFrame, read_parquet

from src.handlers.shared.enums import BucketPath
from src.handlers.shared.functions import read_metadata, write_metadata
from src.handlers.site_upload.powerset_merge import (
    MergeError,
    expand_and_concat_sets,
    powerset_merge_handler,
)
from tests.utils import (
    EXISTING_DATA_P,
    EXISTING_SITE,
    EXISTING_STUDY,
    EXISTING_VERSION,
    ITEM_COUNT,
    MOCK_ENV,
    NEW_DATA_P,
    NEW_SITE,
    NEW_STUDY,
    NEW_VERSION,
    OTHER_SITE,
    OTHER_STUDY,
    TEST_BUCKET,
    get_mock_metadata,
)


@freeze_time("2020-01-01")
@pytest.mark.parametrize(
    "upload_file,upload_path,event_key,archives,status,expected_contents",
    [
        (  # Adding a new data package to a site with uploads
            "./tests/test_data/count_synthea_patient.parquet",
            f"/{NEW_STUDY}/{NEW_STUDY}__{EXISTING_DATA_P}/{EXISTING_SITE}/"
            f"{EXISTING_VERSION}/encounter.parquet",
            f"/{NEW_STUDY}/{NEW_STUDY}__{EXISTING_DATA_P}/{EXISTING_SITE}/"
            f"{EXISTING_VERSION}/encounter.parquet",
            False,
            200,
            ITEM_COUNT + 3,
        ),
        (  # Adding a new data package to a site without uploads
            "./tests/test_data/count_synthea_patient.parquet",
            f"/{NEW_STUDY}/{NEW_STUDY}__{EXISTING_DATA_P}/{NEW_SITE}"
            f"/{EXISTING_VERSION}/encounter.parquet",
            f"/{NEW_STUDY}/{NEW_STUDY}__{EXISTING_DATA_P}/{NEW_SITE}"
            f"/{EXISTING_VERSION}/encounter.parquet",
            False,
            200,
            ITEM_COUNT + 3,
        ),
        (  # Updating an existing data package
            "./tests/test_data/count_synthea_patient.parquet",
            f"/{EXISTING_STUDY}/{EXISTING_STUDY}__{EXISTING_DATA_P}/{EXISTING_SITE}"
            f"/{EXISTING_VERSION}/encounter.parquet",
            f"/{EXISTING_STUDY}/{EXISTING_STUDY}__{EXISTING_DATA_P}/{EXISTING_SITE}"
            f"/{EXISTING_VERSION}/encounter.parquet",
            True,
            200,
            ITEM_COUNT + 2,
        ),
        (  # New version of existing data package
            "./tests/test_data/count_synthea_patient.parquet",
            f"/{EXISTING_STUDY}/{EXISTING_STUDY}__{EXISTING_DATA_P}/{EXISTING_SITE}"
            f"/{NEW_VERSION}/encounter.parquet",
            f"/{EXISTING_STUDY}/{EXISTING_STUDY}__{EXISTING_DATA_P}/{EXISTING_SITE}"
            f"/{NEW_VERSION}/encounter.parquet",
            True,
            200,
            ITEM_COUNT + 4,
        ),
        (  # Invalid parquet file
            "./tests/site_upload/test_powerset_merge.py",
            f"/{NEW_STUDY}/{NEW_STUDY}__{EXISTING_DATA_P}/{EXISTING_SITE}"
            f"/{EXISTING_VERSION}/patient.parquet",
            f"/{NEW_STUDY}/{NEW_STUDY}__{EXISTING_DATA_P}/{EXISTING_SITE}"
            f"/{EXISTING_VERSION}/patient.parquet",
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
            200,
            ITEM_COUNT + 3,
        ),
        (  # ensuring that a data package that is a substring does not get
            # merged by substr match
            "./tests/test_data/count_synthea_patient.parquet",
            f"/{EXISTING_STUDY}/{EXISTING_STUDY}__{EXISTING_DATA_P[0:-2]}/"
            f"{EXISTING_SITE}/{EXISTING_VERSION}/encount.parquet",
            f"/{EXISTING_STUDY}/{EXISTING_STUDY}__{EXISTING_DATA_P[0:-2]}/"
            f"{EXISTING_SITE}/{EXISTING_VERSION}/encount.parquet",
            False,
            200,
            ITEM_COUNT + 3,
        ),
        (  # Empty file upload
            None,
            f"/{NEW_STUDY}/{NEW_STUDY}__{EXISTING_DATA_P}/{EXISTING_SITE}"
            f"/{EXISTING_VERSION}/encounter.parquet",
            f"/{NEW_STUDY}/{NEW_STUDY}__{EXISTING_DATA_P}/{EXISTING_SITE}"
            f"/{EXISTING_VERSION}/encounter.parquet",
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
            500,
            ITEM_COUNT,
        ),
    ],
)
@mock.patch.dict(os.environ, MOCK_ENV)
def test_powerset_merge_single_upload(
    upload_file,
    upload_path,
    event_key,
    archives,
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
            f"{BucketPath.LATEST.value}{upload_path}",
        )
    elif upload_path is not None:
        with io.BytesIO(DataFrame().to_parquet()) as upload_fileobj:
            s3_client.upload_fileobj(
                upload_fileobj,
                TEST_BUCKET,
                f"{BucketPath.LATEST.value}{upload_path}",
            )
    if archives:
        s3_client.upload_file(
            upload_file,
            TEST_BUCKET,
            f"{BucketPath.LAST_VALID.value}{upload_path}",
        )

    event = {
        "Records": [
            {
                "Sns": {"Message": f"{BucketPath.LATEST.value}{event_key}"},
            }
        ]
    }
    # This array looks like:
    # ['', 'study', 'package', 'site', 'file']
    event_list = event_key.split("/")
    study = event_list[1]
    data_package = event_list[2]
    site = event_list[3]
    version = event_list[4]
    res = powerset_merge_handler(event, {})
    assert res["statusCode"] == status
    s3_res = s3_client.list_objects_v2(Bucket=TEST_BUCKET)
    assert len(s3_res["Contents"]) == expected_contents
    for item in s3_res["Contents"]:
        if item["Key"].endswith("aggregate.parquet"):
            assert item["Key"].startswith(BucketPath.AGGREGATE.value)
            # This finds the aggregate that was created/updated - ie it skips mocks
            if study in item["Key"] and status == 200:
                agg_df = awswrangler.s3.read_parquet(
                    f"s3://{TEST_BUCKET}/{item['Key']}"
                )
                assert (agg_df["site"].eq(site)).any()
        elif item["Key"].endswith("aggregate.csv"):
            assert item["Key"].startswith(BucketPath.CSVAGGREGATE.value)
        elif item["Key"].endswith("transactions.json"):
            assert item["Key"].startswith(BucketPath.META.value)
            metadata = read_metadata(s3_client, TEST_BUCKET)
            if res["statusCode"] == 200:
                assert (
                    metadata[site][study][data_package.split("__")[1]][version][
                        "last_aggregation"
                    ]
                    == datetime.now(timezone.utc).isoformat()
                )

            else:
                assert (
                    metadata["princeton_plainsboro_teaching_hospital"]["study"][
                        "encounter"
                    ]["099"]["last_aggregation"]
                    == get_mock_metadata()["princeton_plainsboro_teaching_hospital"][
                        "study"
                    ]["encounter"]["099"]["last_aggregation"]
                )
            if upload_file is not None:
                # checking to see that merge powerset didn't touch last upload
                assert (
                    metadata[site][study][data_package.split("__")[1]][version][
                        "last_upload"
                    ]
                    != datetime.now(timezone.utc).isoformat()
                )

        elif item["Key"].startswith(BucketPath.LAST_VALID.value):
            assert item["Key"] == (f"{BucketPath.LAST_VALID.value}{upload_path}")
        else:
            assert (
                item["Key"].startswith(BucketPath.ARCHIVE.value)
                or item["Key"].startswith(BucketPath.ERROR.value)
                or item["Key"].startswith(BucketPath.ADMIN.value)
                or item["Key"].startswith(BucketPath.CACHE.value)
                or item["Key"].endswith("study_periods.json")
            )
    if archives:
        keys = []
        for resource in s3_res["Contents"]:
            keys.append(resource["Key"])
        date_str = datetime.now(timezone.utc).isoformat()
        archive_path = f".{date_str}.".join(upload_path.split("."))
        assert f"{BucketPath.ARCHIVE.value}{archive_path}" in keys


@freeze_time("2020-01-01")
@pytest.mark.parametrize(
    "upload_file,archives,expected_errors",
    [
        ("./tests/test_data/count_synthea_patient.parquet", False, 0),
        ("./tests/test_data/other_schema.parquet", False, 1),
        ("./tests/test_data/other_schema.parquet", True, 1),
    ],
)
@mock.patch.dict(os.environ, MOCK_ENV)
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
        f"{BucketPath.LATEST.value}/{EXISTING_STUDY}/"
        f"{EXISTING_STUDY}__{EXISTING_DATA_P}/{NEW_SITE}/"
        f"{EXISTING_VERSION}/encounter.parquet",
    )

    s3_client.upload_file(
        "./tests/test_data/count_synthea_patient.parquet",
        TEST_BUCKET,
        f"{BucketPath.LAST_VALID.value}/{EXISTING_STUDY}/"
        f"{EXISTING_STUDY}__{EXISTING_DATA_P}/{EXISTING_SITE}/"
        f"{EXISTING_VERSION}/encounter.parquet",
    )

    if archives:
        s3_client.upload_file(
            "./tests/test_data/count_synthea_patient.parquet",
            TEST_BUCKET,
            f"{BucketPath.LAST_VALID.value}/{EXISTING_STUDY}/"
            f"{EXISTING_STUDY}__{EXISTING_DATA_P}/{NEW_SITE}/"
            f"{EXISTING_VERSION}/encounter.parquet",
        )

    event = {
        "Records": [
            {
                "Sns": {
                    "Message": f"{BucketPath.LATEST.value}/{EXISTING_STUDY}"
                    f"/{EXISTING_STUDY}__{EXISTING_DATA_P}/{NEW_SITE}"
                    f"/{EXISTING_VERSION}/encounter.parquet"
                },
            }
        ]
    }
    res = powerset_merge_handler(event, {})
    assert res["statusCode"] == 200
    errors = 0
    s3_res = s3_client.list_objects_v2(Bucket=TEST_BUCKET)
    for item in s3_res["Contents"]:
        if item["Key"].startswith(BucketPath.ERROR.value):
            errors += 1
        elif item["Key"].startswith(f"{BucketPath.AGGREGATE.value}/study"):
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
            pytest.raises(MergeError),
        ),
        (
            "./tests/test_data/count_synthea_empty.parquet",
            True,
            pytest.raises(MergeError),
        ),
    ],
)
def test_expand_and_concat(mock_bucket, upload_file, load_empty, raises):
    with raises:
        df = read_parquet("./tests/test_data/count_synthea_patient_agg.parquet")
        s3_path = f"/test/uploaded.parquet"
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.upload_file(
            upload_file,
            TEST_BUCKET,
            s3_path,
        )
        expand_and_concat_sets(df, f"s3://{TEST_BUCKET}/{s3_path}", EXISTING_STUDY)
