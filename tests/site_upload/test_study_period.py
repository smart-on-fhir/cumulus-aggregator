import boto3
import csv
import os

import pytest
from datetime import datetime, timezone
from freezegun import freeze_time

from src.handlers.shared.enums import BucketPath
from src.handlers.shared.functions import read_metadata, write_metadata
from src.handlers.site_upload.study_period import study_period_handler

from tests.utils import get_mock_study_metadata, TEST_BUCKET


@freeze_time("2020-01-01")
@pytest.mark.parametrize(
    "site,upload_file,upload_path,event_key,status,study_key",
    [
        (  # Adding a new study to an existing site
            "general_hospital",
            "./tests/test_data/meta_date.parquet",
            "/test/test_meta_date/general_hospital/test_meta_date.parquet",
            "/test/test_meta_date/general_hospital/test_meta_date.parquet",
            200,
            "test",
        ),
        (  # Adding a new study to a new site
            "chicago_hope",
            "./tests/test_data/meta_date.parquet",
            "/test/test_meta_date/chicago_hope/test_meta_date.parquet",
            "/test/test_meta_date/chicago_hope/test_meta_date.parquet",
            200,
            "test",
        ),
        (  # updating an existing study
            "general_hospital",
            "./tests/test_data/meta_date.parquet",
            "/covid/test_meta_date/general_hospital/test_meta_date.parquet",
            "/covid/test_meta_date/general_hospital/test_meta_date.parquet",
            200,
            "covid",
        ),
        (  # invalid file
            "general_hospital",
            "./tests/test_data/meta_date.parquet",
            None,
            "/covid/test_meta_date/general_hospital/wrong.parquet",
            500,
            None,
        ),
    ],
)
def test_process_upload(
    site,
    upload_file,
    upload_path,
    event_key,
    status,
    study_key,
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
    if study_key is not None:
        assert study_key in metadata[site]
        assert (
            metadata[site][study_key]["last_data_update"]
            == datetime.now(timezone.utc).isoformat()
        )
        with open("./tests/test_data/meta_date.csv", "r") as file:
            reader = csv.reader(file)
            # discarding CSV header row
            next(reader)
            row = next(reader)
            assert metadata[site][study_key]["earliest_date"] == f"{row[0]}T00:00:00"
            assert metadata[site][study_key]["latest_date"] == f"{row[1]}T00:00:00"
