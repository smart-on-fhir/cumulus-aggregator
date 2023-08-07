import csv
import os
from datetime import datetime, timezone

import boto3
import pytest
from freezegun import freeze_time

from src.handlers.shared.enums import BucketPath
from src.handlers.shared.functions import read_metadata, write_metadata
from src.handlers.site_upload.study_period import study_period_handler
from tests.utils import (
    EXISTING_DATA_P,
    EXISTING_SITE,
    EXISTING_STUDY,
    EXISTING_VERSION,
    NEW_DATA_P,
    NEW_SITE,
    NEW_STUDY,
    NEW_VERSION,
    OTHER_SITE,
    OTHER_STUDY,
    TEST_BUCKET,
    get_mock_study_metadata,
)


@freeze_time("2020-01-01")
@pytest.mark.parametrize(
    "upload_file,upload_path,event_key,status",
    [
        (  # Adding a new study to an existing site
            "./tests/test_data/meta_date.parquet",
            (
                f"/{NEW_STUDY}/{EXISTING_DATA_P}/{EXISTING_SITE}"
                f"/{EXISTING_VERSION}/test_meta_date.parquet"
            ),
            (
                f"/{NEW_STUDY}/{EXISTING_DATA_P}/{EXISTING_SITE}"
                f"/{EXISTING_VERSION}/test_meta_date.parquet"
            ),
            200,
        ),
        (  # Adding a new study to a new site
            "./tests/test_data/meta_date.parquet",
            f"/{NEW_STUDY}/{EXISTING_DATA_P}/{NEW_SITE}"
            f"/{EXISTING_VERSION}/test_meta_date.parquet",
            f"/{NEW_STUDY}/{EXISTING_DATA_P}/{NEW_SITE}"
            f"/{EXISTING_VERSION}/test_meta_date.parquet",
            200,
        ),
        (  # newer version of existing study
            "./tests/test_data/meta_date.parquet",
            (
                f"/{EXISTING_STUDY}/{EXISTING_DATA_P}/{EXISTING_SITE}"
                f"/{NEW_VERSION}/test_meta_date.parquet"
            ),
            (
                f"/{EXISTING_STUDY}/{EXISTING_DATA_P}/{EXISTING_SITE}"
                f"/{NEW_VERSION}/test_meta_date.parquet"
            ),
            200,
        ),
        (  # updating an existing study
            "./tests/test_data/meta_date.parquet",
            (
                f"/{EXISTING_STUDY}/{EXISTING_DATA_P}/{EXISTING_SITE}"
                f"/{EXISTING_VERSION}/test_meta_date.parquet"
            ),
            (
                f"/{EXISTING_STUDY}/{EXISTING_DATA_P}/{EXISTING_SITE}"
                f"/{EXISTING_VERSION}/test_meta_date.parquet"
            ),
            200,
        ),
        (  # invalid file
            "./tests/test_data/meta_date.parquet",
            None,
            (
                f"/{EXISTING_STUDY}/{EXISTING_DATA_P}/{EXISTING_SITE}"
                f"/{EXISTING_VERSION}/wrong.parquet"
            ),
            500,
        ),
    ],
)
def test_process_upload(
    upload_file,
    upload_path,
    event_key,
    status,
    mock_bucket,
):
    s3_client = boto3.client("s3", region_name="us-east-1")
    if upload_file is not None:
        s3_client.upload_file(
            upload_file,
            TEST_BUCKET,
            f"{BucketPath.STUDY_META.value}{upload_path}",
        )
    event = {
        "Records": [{"Sns": {"Message": f"{BucketPath.STUDY_META.value}{event_key}"}}]
    }
    res = study_period_handler(event, {})
    assert res["statusCode"] == status
    metadata = read_metadata(s3_client, TEST_BUCKET, meta_type="study_periods")
    if upload_file is not None and upload_path is not None:
        path_params = upload_path.split("/")
        study = path_params[1]
        site = path_params[3]
        version = path_params[4]
        assert study in metadata[site]
        assert (
            metadata[site][study][version]["last_data_update"]
            == datetime.now(timezone.utc).isoformat()
        )
        with open("./tests/test_data/meta_date.csv", "r") as file:
            reader = csv.reader(file)
            # discarding CSV header row
            next(reader)
            row = next(reader)
            assert (
                metadata[site][study][version]["earliest_date"] == f"{row[0]}T00:00:00"
            )
            assert metadata[site][study][version]["latest_date"] == f"{row[1]}T00:00:00"
