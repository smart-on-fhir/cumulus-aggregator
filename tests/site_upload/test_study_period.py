import boto3
import os

import pytest
from datetime import datetime
from freezegun import freeze_time
from unittest import mock

from src.handlers.shared.enums import BucketPath
from src.handlers.shared.functions import read_metadata, write_metadata
from src.handlers.site_upload.study_period import study_period_handler

from tests.utils import get_mock_study_metadata, TEST_BUCKET


@freeze_time("2020-01-01")
@mock.patch("src.handlers.site_upload.study_period.datetime")
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
    mock_dt,
    site,
    upload_file,
    upload_path,
    event_key,
    status,
    study_key,
    mock_bucket,
):
    mock_dt.now = mock.Mock(return_value=datetime(2020, 1, 1))
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
        assert metadata[site][study_key]["last_data_update"] == "2020-01-01T00:00:00"
