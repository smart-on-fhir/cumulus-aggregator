import csv
from datetime import UTC, datetime

import boto3
import pytest
from freezegun import freeze_time

from src.shared import enums, functions
from src.site_upload.study_period import study_period
from tests.mock_utils import (
    EXISTING_DATA_P,
    EXISTING_SITE,
    EXISTING_STUDY,
    EXISTING_VERSION,
    NEW_SITE,
    NEW_STUDY,
    NEW_VERSION,
    TEST_BUCKET,
)


@freeze_time("2020-01-01")
@pytest.mark.parametrize(
    "upload_file,upload_path,event_key,multiple_files,status",
    [
        (  # Adding a new study to an existing site
            "./tests/test_data/meta_date.parquet",
            (
                f"/{NEW_STUDY}/{NEW_STUDY}__{EXISTING_DATA_P}/{EXISTING_SITE}"
                f"/{EXISTING_VERSION}/test_meta_date.parquet"
            ),
            (
                f"/{NEW_STUDY}/{NEW_STUDY}__{EXISTING_DATA_P}/{EXISTING_SITE}"
                f"/{EXISTING_VERSION}/test_meta_date.parquet"
            ),
            False,
            200,
        ),
        (  # Adding a new study to a new site
            "./tests/test_data/meta_date.parquet",
            f"/{NEW_STUDY}/{NEW_STUDY}__{EXISTING_DATA_P}/{NEW_SITE}"
            f"/{EXISTING_VERSION}/test_meta_date.parquet",
            f"/{NEW_STUDY}/{NEW_STUDY}__{EXISTING_DATA_P}/{NEW_SITE}"
            f"/{EXISTING_VERSION}/test_meta_date.parquet",
            False,
            200,
        ),
        (  # newer version of existing study
            "./tests/test_data/meta_date.parquet",
            (
                f"/{EXISTING_STUDY}/{EXISTING_STUDY}__{EXISTING_DATA_P}"
                f"/{EXISTING_SITE}/{NEW_VERSION}/test_meta_date.parquet"
            ),
            (
                f"/{EXISTING_STUDY}/{EXISTING_STUDY}__{EXISTING_DATA_P}"
                f"/{EXISTING_SITE}/{NEW_VERSION}/test_meta_date.parquet"
            ),
            False,
            200,
        ),
        (  # updating an existing study
            "./tests/test_data/meta_date.parquet",
            (
                f"/{EXISTING_STUDY}/{EXISTING_STUDY}__{EXISTING_DATA_P}"
                f"/{EXISTING_SITE}/{EXISTING_VERSION}/test_meta_date.parquet"
            ),
            (
                f"/{EXISTING_STUDY}/{EXISTING_STUDY}__{EXISTING_DATA_P}"
                f"/{EXISTING_SITE}/{EXISTING_VERSION}/test_meta_date.parquet"
            ),
            False,
            200,
        ),
        (  # updating an existing study
            "./tests/test_data/meta_date.parquet",
            (
                f"/{EXISTING_STUDY}/{EXISTING_STUDY}__{EXISTING_DATA_P}"
                f"/{EXISTING_SITE}/{EXISTING_VERSION}/test_meta_date.parquet"
            ),
            (
                f"/{EXISTING_STUDY}/{EXISTING_STUDY}__{EXISTING_DATA_P}"
                f"/{EXISTING_SITE}/{EXISTING_VERSION}/test_meta_date.parquet"
            ),
            False,
            200,
        ),
        (  # updating an existing study with a differently named file
            "./tests/test_data/meta_date.parquet",
            (
                f"/{EXISTING_STUDY}/{EXISTING_STUDY}__{EXISTING_DATA_P}"
                f"/{EXISTING_SITE}/{EXISTING_VERSION}/test_meta_date.parquet"
            ),
            (
                f"/{EXISTING_STUDY}/{EXISTING_STUDY}__{EXISTING_DATA_P}"
                f"/{EXISTING_SITE}/{EXISTING_VERSION}/test_meta_date.parquet"
            ),
            True,
            200,
        ),
        (  # invalid file
            "./tests/test_data/meta_date.parquet",
            None,
            (
                f"/{EXISTING_STUDY}/{EXISTING_STUDY}__{EXISTING_DATA_P}"
                f"/{EXISTING_SITE}/{EXISTING_VERSION}/wrong.parquet"
            ),
            False,
            500,
        ),
    ],
)
def test_process_upload(
    upload_file,
    upload_path,
    event_key,
    multiple_files,
    status,
    mock_bucket,
):
    s3_client = boto3.client("s3", region_name="us-east-1")
    # we'll get rid of the fixture data in study_meta because we care
    # about timestamps in this case
    # TODO - consider having a intermediate position for files to land so
    # we don't have to do timestamp processing
    s3_client.delete_object(
        Bucket=TEST_BUCKET,
        Key=(
            "study_metadata/study/study__encounter/princeton_plainsboro_teaching_hospital/"
            "099/study__meta_date.parquet"
        ),
    )
    if multiple_files:
        s3_client.upload_file(
            upload_file,
            TEST_BUCKET,
            (
                f"{enums.BucketPath.STUDY_META.value}"
                f"/{EXISTING_STUDY}/{EXISTING_STUDY}__{EXISTING_DATA_P}"
                f"/{EXISTING_SITE}/{EXISTING_VERSION}/test_old_meta_date.parquet"
            ),
        )
    if upload_file is not None:
        s3_client.upload_file(
            upload_file,
            TEST_BUCKET,
            f"{enums.BucketPath.STUDY_META.value}{upload_path}",
        )
    event = {"Records": [{"Sns": {"Message": f"{enums.BucketPath.STUDY_META.value}{event_key}"}}]}
    res = study_period.study_period_handler(event, {})
    assert res["statusCode"] == status
    metadata = functions.read_metadata(s3_client, TEST_BUCKET, meta_type="study_periods")
    if upload_file is not None and upload_path is not None:
        path_params = upload_path.split("/")
        study = path_params[1]
        site = path_params[3]
        version = path_params[4]
        assert study in metadata[site]
        assert metadata[site][study][version]["last_data_update"] == datetime.now(UTC).isoformat()
        with open("./tests/test_data/meta_date.csv") as file:
            reader = csv.reader(file)
            # discarding CSV header row
            next(reader)
            row = next(reader)
            assert metadata[site][study][version]["earliest_date"] == f"{row[0]}T00:00:00"
            assert metadata[site][study][version]["latest_date"] == f"{row[1]}T00:00:00"
    if multiple_files:
        res = s3_client.list_objects_v2(
            Bucket=TEST_BUCKET,
            Prefix=(
                f"{enums.BucketPath.STUDY_META.value}"
                f"/{EXISTING_STUDY}/{EXISTING_STUDY}__{EXISTING_DATA_P}"
                f"/{EXISTING_SITE}/{EXISTING_VERSION}"
            ),
        )
        assert len(res["Contents"]) == 1
        assert res["Contents"][0]["Key"] == f"{enums.BucketPath.STUDY_META.value}{upload_path}"
        assert res["Contents"][0]["LastModified"] == datetime(2020, 1, 1, tzinfo=UTC)
