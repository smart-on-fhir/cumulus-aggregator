import boto3
import io
import os

from unittest import mock

import awswrangler
import pytest

from datetime import datetime, timezone
from pandas import DataFrame
from freezegun import freeze_time

from src.handlers.shared.enums import BucketPath
from src.handlers.shared.functions import read_metadata, write_metadata
from src.handlers.site_upload.powerset_merge import powerset_merge_handler

from tests.utils import get_mock_metadata, TEST_BUCKET, ITEM_COUNT, MOCK_ENV


SITE_NAME = "princeton_plainsboro_teaching_hospital"
NEW_SITE_NAME = "chicago_hope"
NEW_STUDY_NAME = "new_study"
EXISTING_STUDY_NAME = "study"
DATA_P_NAME = "encounter"

"""
"""


@freeze_time("2020-01-01")
@pytest.mark.parametrize(
    "site,upload_file,upload_path,event_key,archives,status,expected_contents",
    [
        (  # Adding a new data package to a site with uploads
            f"{SITE_NAME}",
            "./tests/test_data/count_synthea_patient.parquet",
            f"/{NEW_STUDY_NAME}/{DATA_P_NAME}/{SITE_NAME}/encounter.parquet",
            f"/{NEW_STUDY_NAME}/{DATA_P_NAME}/{SITE_NAME}/encounter.parquet",
            False,
            200,
            ITEM_COUNT + 3,
        ),
        (  # Adding a new data package to a site without uploads
            f"{NEW_SITE_NAME}",
            "./tests/test_data/count_synthea_patient.parquet",
            f"/{NEW_STUDY_NAME}/{DATA_P_NAME}/{NEW_SITE_NAME}/encounter.parquet",
            f"/{NEW_STUDY_NAME}/{DATA_P_NAME}/{NEW_SITE_NAME}/encounter.parquet",
            False,
            200,
            ITEM_COUNT + 3,
        ),
        (  # Updating an existing data package
            f"{SITE_NAME}",
            "./tests/test_data/count_synthea_patient.parquet",
            f"/{EXISTING_STUDY_NAME}/{DATA_P_NAME}/{SITE_NAME}/encounter.parquet",
            f"/{EXISTING_STUDY_NAME}/{DATA_P_NAME}/{SITE_NAME}/encounter.parquet",
            True,
            200,
            ITEM_COUNT + 2,
        ),
        (  # Invalid parquet file
            f"{SITE_NAME}",
            "./tests/site_upload/test_powerset_merge.py",
            f"/{NEW_STUDY_NAME}/{DATA_P_NAME}/{SITE_NAME}/patient.parquet",
            f"/{NEW_STUDY_NAME}/{DATA_P_NAME}/{SITE_NAME}/patient.parquet",
            False,
            500,
            ITEM_COUNT + 1,
        ),
        (  # Checks presence of commas in strings does not cause an error
            f"{SITE_NAME}",
            "./tests/test_data/cube_strings_with_commas.parquet",
            f"/{NEW_STUDY_NAME}/{DATA_P_NAME}/{SITE_NAME}/encounter.parquet",
            f"/{NEW_STUDY_NAME}/{DATA_P_NAME}/{SITE_NAME}/encounter.parquet",
            False,
            200,
            ITEM_COUNT + 3,
        ),
        (  # Empty file upload
            f"{SITE_NAME}",
            None,
            f"/{NEW_STUDY_NAME}/{DATA_P_NAME}/{SITE_NAME}/encounter.parquet",
            f"/{NEW_STUDY_NAME}/{DATA_P_NAME}/{SITE_NAME}/encounter.parquet",
            False,
            500,
            ITEM_COUNT + 1,
        ),
    ],
)
@mock.patch.dict(os.environ, MOCK_ENV)
def test_powerset_merge_single_upload(
    site,
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
    else:
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
                "awsRegion": "us-east-1",
                "Sns": {"Message": f"{BucketPath.LATEST.value}{event_key}"},
            }
        ]
    }
    # This array looks like:
    # ['', 'study', 'package', 'site', 'file']
    event_list = event_key.split("/")
    expected_study = event_list[1]
    expected_package = event_list[2]
    expected_site = event_list[3]
    res = powerset_merge_handler(event, {})
    assert res["statusCode"] == status
    s3_res = s3_client.list_objects_v2(Bucket=TEST_BUCKET)
    assert len(s3_res["Contents"]) == expected_contents
    for item in s3_res["Contents"]:
        if item["Key"].endswith("aggregate.parquet"):
            assert item["Key"].startswith(BucketPath.AGGREGATE.value)
            # This finds the aggregate that was created/updated - ie it skips mocks
            if (
                expected_study in item["Key"]
                and expected_study in item["Key"]
                and status == 200
            ):
                agg_df = awswrangler.s3.read_parquet(
                    f"s3://{TEST_BUCKET}/{item['Key']}"
                )
                assert (agg_df["site"].eq(expected_site)).any()
        elif item["Key"].endswith("aggregate.csv"):
            assert item["Key"].startswith(BucketPath.CSVAGGREGATE.value)
        elif item["Key"].endswith("transactions.json"):
            assert item["Key"].startswith(BucketPath.META.value)
            metadata = read_metadata(s3_client, TEST_BUCKET)
            if res["statusCode"] == 200:
                study = event_key.split("/")[1]
                assert (
                    metadata[site][study][DATA_P_NAME]["last_aggregation"]
                    == datetime.now(timezone.utc).isoformat()
                )
            else:
                assert (
                    metadata["general_hospital"]["study"]["encounter"][
                        "last_aggregation"
                    ]
                    == get_mock_metadata()["general_hospital"]["study"]["encounter"][
                        "last_aggregation"
                    ]
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
        ("./tests/test_data/cube_simple_example.parquet", False, 1),
        ("./tests/test_data/cube_simple_example.parquet", True, 1),
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
        f"{BucketPath.LATEST.value}/{EXISTING_STUDY_NAME}/"
        f"{DATA_P_NAME}/{NEW_SITE_NAME}/encounter.parquet",
    )

    s3_client.upload_file(
        "./tests/test_data/count_synthea_patient.parquet",
        TEST_BUCKET,
        f"{BucketPath.LAST_VALID.value}/{EXISTING_STUDY_NAME}/"
        f"{DATA_P_NAME}/{SITE_NAME}/encounter.parquet",
    )

    if archives:
        s3_client.upload_file(
            "./tests/test_data/count_synthea_patient.parquet",
            TEST_BUCKET,
            f"{BucketPath.LAST_VALID.value}/{EXISTING_STUDY_NAME}/"
            f"{DATA_P_NAME}/{NEW_SITE_NAME}/encounter.parquet",
        )

    event = {
        "Records": [
            {
                "awsRegion": "us-east-1",
                "Sns": {
                    "Message": f"{BucketPath.LATEST.value}/{EXISTING_STUDY_NAME}"
                    f"/{DATA_P_NAME}/{NEW_SITE_NAME}/encounter.parquet"
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
        if item["Key"].startswith(f"{BucketPath.AGGREGATE.value}/study"):
            agg_df = awswrangler.s3.read_parquet(f"s3://{TEST_BUCKET}/{item['Key']}")
            # if a file cant be merged and there's no fallback, we expect
            # [<NA>, site_name], otherwise, [<NA>, site_name, uploading_site_name]
            if errors != 0 and not archives:
                assert len(agg_df["site"].unique() == 2)
            else:
                assert len(agg_df["site"].unique() == 3)
    assert errors == expected_errors
