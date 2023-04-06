import boto3
import os

import pytest

from datetime import datetime
from freezegun import freeze_time
from unittest import mock

from src.handlers.shared.enums import BucketPath
from src.handlers.shared.functions import read_metadata, write_metadata
from src.handlers.site_upload.process_upload import process_upload_handler

from tests.utils import TEST_BUCKET, ITEM_COUNT


@freeze_time("2020-01-01")
@mock.patch("src.handlers.site_upload.powerset_merge.datetime")
@pytest.mark.parametrize(
    "site,upload_file,upload_path,event_key,status,expected_contents",
    [
        (  # Adding a new data package to a site with uploads
            "general_hospital",
            "./tests/test_data/cube_simple_example.parquet",
            "/covid/encounter/general_hospital/document.parquet",
            "/covid/encounter/general_hospital/document.parquet",
            200,
            ITEM_COUNT + 1,
        ),
        (  # Adding a new data package to a site without uploads
            "chicago_hope",
            "./tests/test_data/cube_simple_example.parquet",
            "/covid/encounter/chicago_hope/document.parquet",
            "/covid/encounter/chicago_hope/document.parquet",
            200,
            ITEM_COUNT + 1,
        ),
        (  # Updating an existing data package
            "general_hospital",
            "./tests/test_data/cube_simple_example.parquet",
            "/covid/encounter/general_hospital/encounter.parquet",
            "/covid/encounter/general_hospital/encounter.parquet",
            200,
            ITEM_COUNT + 1,
        ),
        (  # Non-parquet file
            "general_hospital",
            "./tests/test_data/cube_simple_example.csv",
            "/covid/encounter/general_hospital/document.csv",
            "/covid/encounter/general_hospital/document.csv",
            500,
            ITEM_COUNT + 1,
        ),
        (  # S3 event dispatched when file is not present
            "general_hospital",
            None,
            None,
            "/covid/encounter/general_hospital/missing.parquet",
            500,
            ITEM_COUNT,
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
    expected_contents,
    mock_bucket,
    mock_notification,
):
    mock_dt.now = mock.Mock(return_value=datetime(2020, 1, 1))
    s3_client = boto3.client("s3", region_name="us-east-1")
    if upload_file is not None:
        s3_client.upload_file(
            upload_file,
            TEST_BUCKET,
            f"{BucketPath.UPLOAD.value}{upload_path}",
        )
    event = {
        "Records": [
            {"s3": {"object": {"key": f"{BucketPath.UPLOAD.value}{event_key}"}}}
        ]
    }

    res = process_upload_handler(event, {})
    assert res["statusCode"] == status
    s3_res = s3_client.list_objects_v2(Bucket=TEST_BUCKET)
    assert len(s3_res["Contents"]) == expected_contents
    for item in s3_res["Contents"]:
        if item["Key"].endswith("aggregate.parquet"):
            assert item["Key"].startswith(BucketPath.AGGREGATE.value) is True
        elif item["Key"].endswith("aggregate.csv"):
            assert item["Key"].startswith(BucketPath.CSVAGGREGATE.value) is True
        elif item["Key"].endswith("transactions.json"):
            assert item["Key"].startswith(BucketPath.META.value) is True
            metadata = read_metadata(s3_client, TEST_BUCKET)
            if upload_file is not None:
                assert (
                    metadata[site]["covid"]["encounter"]["last_uploaded_date"]
                    == "2020-01-01T00:00:00+00:00"
                )
        else:
            assert (
                item["Key"].startswith(BucketPath.LATEST.value) is True
                or item["Key"].startswith(BucketPath.LAST_VALID.value) is True
                or item["Key"].startswith(BucketPath.ERROR.value) is True
                or item["Key"].startswith(BucketPath.ADMIN.value) is True
            )
