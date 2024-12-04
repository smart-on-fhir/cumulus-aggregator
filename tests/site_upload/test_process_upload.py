from datetime import UTC, datetime

import boto3
import pytest
from freezegun import freeze_time

from src.shared import enums, functions
from src.site_upload.process_upload import process_upload
from tests.mock_utils import (
    EXISTING_DATA_P,
    EXISTING_SITE,
    EXISTING_STUDY,
    EXISTING_VERSION,
    ITEM_COUNT,
    NEW_DATA_P,
    NEW_SITE,
    NEW_VERSION,
    TEST_BUCKET,
)


@freeze_time("2020-01-01")
@pytest.mark.parametrize(
    "upload_file,upload_path,event_key,status,expected_contents",
    [
        (  # Adding a new data package to a site with uploads
            "./tests/test_data/cube_simple_example.parquet",
            f"/{EXISTING_STUDY}/{NEW_DATA_P}/{EXISTING_SITE}"
            f"/{EXISTING_VERSION}/document.parquet",
            f"/{EXISTING_STUDY}/{NEW_DATA_P}/{EXISTING_SITE}"
            f"/{EXISTING_VERSION}/document.parquet",
            200,
            ITEM_COUNT + 1,
        ),
        (  # Adding a new data package to a site without uploads
            "./tests/test_data/cube_simple_example.parquet",
            f"/{EXISTING_STUDY}/{NEW_DATA_P}/{NEW_SITE}" f"/{EXISTING_VERSION}/document.parquet",
            f"/{EXISTING_STUDY}/{NEW_DATA_P}/{NEW_SITE}" f"/{EXISTING_VERSION}/document.parquet",
            200,
            ITEM_COUNT + 1,
        ),
        (  # Updating an existing data package
            "./tests/test_data/cube_simple_example.parquet",
            f"/{EXISTING_STUDY}/{EXISTING_DATA_P}/{EXISTING_SITE}"
            f"/{EXISTING_VERSION}/encounter.parquet",
            f"/{EXISTING_STUDY}/{EXISTING_DATA_P}/{EXISTING_SITE}"
            f"/{EXISTING_VERSION}/encounter.parquet",
            200,
            ITEM_COUNT + 1,
        ),
        (  # New version of an existing data package
            "./tests/test_data/cube_simple_example.parquet",
            f"/{EXISTING_STUDY}/{EXISTING_DATA_P}/{EXISTING_SITE}"
            f"/{NEW_VERSION}/encounter.parquet",
            f"/{EXISTING_STUDY}/{EXISTING_DATA_P}/{EXISTING_SITE}"
            f"/{NEW_VERSION}/encounter.parquet",
            200,
            ITEM_COUNT + 1,
        ),
        (  # Upload of a flat file
            "./tests/test_data/cube_simple_example.parquet",
            f"/{EXISTING_STUDY}/{NEW_DATA_P}/{EXISTING_SITE}"
            f"/{EXISTING_VERSION}/document.flat.parquet",
            f"/{EXISTING_STUDY}/{NEW_DATA_P}/{EXISTING_SITE}"
            f"/{EXISTING_VERSION}/document.flat.parquet",
            200,
            ITEM_COUNT + 1,
        ),
        (  # Upload of an archive file (which should be deleted)
            "./tests/test_data/cube_simple_example.parquet",
            f"/{EXISTING_STUDY}/{NEW_DATA_P}/{EXISTING_SITE}"
            f"/{EXISTING_VERSION}/document.archive.parquet",
            f"/{EXISTING_STUDY}/{NEW_DATA_P}/{EXISTING_SITE}"
            f"/{EXISTING_VERSION}/document.archive.parquet",
            200,
            ITEM_COUNT,
        ),
        (  # Non-parquet file
            "./tests/test_data/cube_simple_example.csv",
            f"/{EXISTING_STUDY}/{NEW_DATA_P}/{EXISTING_SITE}" f"/{EXISTING_VERSION}/document.csv",
            f"/{EXISTING_STUDY}/{NEW_DATA_P}/{EXISTING_SITE}" f"/{EXISTING_VERSION}/document.csv",
            500,
            ITEM_COUNT + 1,
        ),
        (  # S3 event dispatched when file is not present
            None,
            None,
            f"/{EXISTING_STUDY}/{EXISTING_DATA_P}/{EXISTING_SITE}"
            f"/{EXISTING_VERSION}/missing.parquet",
            500,
            ITEM_COUNT,
        ),
        (  # Adding metadata data package
            "./tests/test_data/cube_simple_example.parquet",
            (
                f"/{EXISTING_STUDY}/{EXISTING_DATA_P}/{EXISTING_SITE}/"
                f"{EXISTING_VERSION}/document_meta_date.parquet"
            ),
            (
                f"/{EXISTING_STUDY}/{EXISTING_DATA_P}/{EXISTING_SITE}/"
                f"{EXISTING_VERSION}/document_meta_date.parquet"
            ),
            200,
            ITEM_COUNT + 1,
        ),
        (  # Adding discovery data package
            "./tests/test_data/cube_simple_example.parquet",
            (
                f"/{EXISTING_STUDY}/{EXISTING_DATA_P}/{EXISTING_SITE}/"
                f"{EXISTING_VERSION}/discovery__file.parquet"
            ),
            (
                f"/{EXISTING_STUDY}/{EXISTING_DATA_P}/{EXISTING_SITE}/"
                f"{EXISTING_VERSION}/discovery__file.parquet"
            ),
            200,
            ITEM_COUNT + 1,
        ),
    ],
)
def test_process_upload(
    upload_file,
    upload_path,
    event_key,
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
            f"{enums.BucketPath.UPLOAD.value}{upload_path}",
        )
    event = {
        "Records": [
            {
                "awsRegion": "us-east-1",
                "s3": {"object": {"key": f"{enums.BucketPath.UPLOAD.value}{event_key}"}},
            }
        ]
    }

    res = process_upload.process_upload_handler(event, {})
    assert res["statusCode"] == status
    s3_res = s3_client.list_objects_v2(Bucket=TEST_BUCKET)
    assert len(s3_res["Contents"]) == expected_contents
    if event_key.endswith(".archive.parquet"):
        return
    for item in s3_res["Contents"]:
        if item["Key"].endswith("aggregate.parquet"):
            assert item["Key"].startswith(enums.BucketPath.AGGREGATE.value)
        elif item["Key"].endswith("aggregate.csv"):
            assert item["Key"].startswith(enums.BucketPath.CSVAGGREGATE.value)
        elif item["Key"].endswith("transactions.json"):
            assert item["Key"].startswith(enums.BucketPath.META.value)
            if upload_path is not None and "template" not in upload_path:
                metadata = functions.read_metadata(s3_client, TEST_BUCKET)
                if upload_file is not None and upload_path is not None:
                    path_params = upload_path.split("/")
                    study = path_params[1]
                    data_package = path_params[2]
                    site = path_params[3]
                    version = path_params[4]
                    assert (
                        metadata[site][study][data_package][version]["last_upload"]
                        == datetime.now(UTC).isoformat()
                    )
        elif item["Key"].startswith(enums.BucketPath.STUDY_META.value):
            assert any(x in item["Key"] for x in ["_meta_", "/discovery__"])
        else:
            assert (
                item["Key"].startswith(enums.BucketPath.LATEST.value)
                or item["Key"].startswith(enums.BucketPath.LAST_VALID.value)
                or item["Key"].startswith(enums.BucketPath.ERROR.value)
                or item["Key"].startswith(enums.BucketPath.ADMIN.value)
                or item["Key"].startswith(enums.BucketPath.CACHE.value)
                or item["Key"].startswith(enums.BucketPath.FLAT.value)
                or item["Key"].startswith(enums.BucketPath.CSVFLAT.value)
                or item["Key"].startswith(enums.BucketPath.ARCHIVE.value)
                or item["Key"].endswith("study_periods.json")
                or item["Key"].endswith("column_types.json")
            )
