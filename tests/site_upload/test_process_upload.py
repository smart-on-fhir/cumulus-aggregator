import json
from datetime import UTC, datetime

import boto3
import pytest
from freezegun import freeze_time

from src.shared import enums, functions
from src.site_upload.process_upload import process_upload
from tests import mock_utils


@freeze_time("2020-01-01")
@pytest.mark.parametrize(
    "upload_file,upload_path,event_key,status,expected_contents",
    [
        (  # Adding a new data package to a site with uploads
            "./tests/test_data/cube_simple_example.parquet",
            f"/{mock_utils.EXISTING_STUDY}/{mock_utils.NEW_DATA_P}/{mock_utils.EXISTING_SITE}/{mock_utils.EXISTING_VERSION}/document.parquet",
            f"/{mock_utils.EXISTING_STUDY}/{mock_utils.NEW_DATA_P}/{mock_utils.EXISTING_SITE}/{mock_utils.EXISTING_VERSION}/document.parquet",
            200,
            mock_utils.ITEM_COUNT + 1,
        ),
        (  # Adding a new data package to a site without uploads
            "./tests/test_data/cube_simple_example.parquet",
            f"/{mock_utils.EXISTING_STUDY}/{mock_utils.NEW_DATA_P}/{mock_utils.NEW_SITE}/{mock_utils.EXISTING_VERSION}/document.parquet",
            f"/{mock_utils.EXISTING_STUDY}/{mock_utils.NEW_DATA_P}/{mock_utils.NEW_SITE}/{mock_utils.EXISTING_VERSION}/document.parquet",
            200,
            mock_utils.ITEM_COUNT + 1,
        ),
        (  # Updating an existing data package
            "./tests/test_data/cube_simple_example.parquet",
            f"/{mock_utils.EXISTING_STUDY}/{mock_utils.EXISTING_DATA_P}/{mock_utils.EXISTING_SITE}"
            f"/{mock_utils.EXISTING_VERSION}/encounter.parquet",
            f"/{mock_utils.EXISTING_STUDY}/{mock_utils.EXISTING_DATA_P}/{mock_utils.EXISTING_SITE}"
            f"/{mock_utils.EXISTING_VERSION}/encounter.parquet",
            200,
            mock_utils.ITEM_COUNT + 1,
        ),
        (  # New version of an existing data package
            "./tests/test_data/cube_simple_example.parquet",
            f"/{mock_utils.EXISTING_STUDY}/{mock_utils.EXISTING_DATA_P}/{mock_utils.EXISTING_SITE}/{mock_utils.NEW_VERSION}/encounter.parquet",
            f"/{mock_utils.EXISTING_STUDY}/{mock_utils.EXISTING_DATA_P}/{mock_utils.EXISTING_SITE}/{mock_utils.NEW_VERSION}/encounter.parquet",
            200,
            mock_utils.ITEM_COUNT + 1,
        ),
        (  # Upload of a flat file
            "./tests/test_data/cube_simple_example.parquet",
            f"/{mock_utils.EXISTING_STUDY}/{mock_utils.NEW_DATA_P}/{mock_utils.EXISTING_SITE}"
            f"/{mock_utils.EXISTING_VERSION}/document.flat.parquet",
            f"/{mock_utils.EXISTING_STUDY}/{mock_utils.NEW_DATA_P}/{mock_utils.EXISTING_SITE}"
            f"/{mock_utils.EXISTING_VERSION}/document.flat.parquet",
            200,
            mock_utils.ITEM_COUNT + 1,
        ),
        (  # Upload of an archive file (which should be deleted)
            "./tests/test_data/cube_simple_example.parquet",
            f"/{mock_utils.EXISTING_STUDY}/{mock_utils.NEW_DATA_P}/{mock_utils.EXISTING_SITE}"
            f"/{mock_utils.EXISTING_VERSION}/document.archive.parquet",
            f"/{mock_utils.EXISTING_STUDY}/{mock_utils.NEW_DATA_P}/{mock_utils.EXISTING_SITE}"
            f"/{mock_utils.EXISTING_VERSION}/document.archive.parquet",
            200,
            mock_utils.ITEM_COUNT,
        ),
        (  # Non-parquet file
            "./tests/test_data/cube_simple_example.csv",
            f"/{mock_utils.EXISTING_STUDY}/{mock_utils.NEW_DATA_P}/{mock_utils.EXISTING_SITE}/{mock_utils.EXISTING_VERSION}/document.csv",
            f"/{mock_utils.EXISTING_STUDY}/{mock_utils.NEW_DATA_P}/{mock_utils.EXISTING_SITE}/{mock_utils.EXISTING_VERSION}/document.csv",
            500,
            mock_utils.ITEM_COUNT + 1,
        ),
        (  # Sns event dispatched when file is not present
            None,
            None,
            f"/{mock_utils.EXISTING_STUDY}/{mock_utils.EXISTING_DATA_P}/{mock_utils.EXISTING_SITE}"
            f"/{mock_utils.EXISTING_VERSION}/missing.parquet",
            500,
            mock_utils.ITEM_COUNT,
        ),
        (  # Adding metadata data package
            "./tests/test_data/cube_simple_example.parquet",
            (
                f"/{mock_utils.EXISTING_STUDY}/{mock_utils.EXISTING_DATA_P}/{mock_utils.EXISTING_SITE}/"
                f"{mock_utils.EXISTING_VERSION}/document__meta_date.parquet"
            ),
            (
                f"/{mock_utils.EXISTING_STUDY}/{mock_utils.EXISTING_DATA_P}/{mock_utils.EXISTING_SITE}/"
                f"{mock_utils.EXISTING_VERSION}/document__meta_date.parquet"
            ),
            200,
            mock_utils.ITEM_COUNT + 1,
        ),
        (  # Adding discovery data package
            "./tests/test_data/cube_simple_example.parquet",
            (
                f"/{mock_utils.EXISTING_STUDY}/{mock_utils.EXISTING_DATA_P}/{mock_utils.EXISTING_SITE}/"
                f"{mock_utils.EXISTING_VERSION}/discovery__file.parquet"
            ),
            (
                f"/{mock_utils.EXISTING_STUDY}/{mock_utils.EXISTING_DATA_P}/{mock_utils.EXISTING_SITE}/"
                f"{mock_utils.EXISTING_VERSION}/discovery__file.parquet"
            ),
            200,
            mock_utils.ITEM_COUNT + 1,
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
    mock_queue,
):
    s3_client = boto3.client("s3", region_name="us-east-1")
    sqs_client = boto3.client("sqs", region_name="us-east-1")
    if upload_file is not None:
        s3_client.upload_file(
            upload_file,
            mock_utils.TEST_BUCKET,
            f"{enums.BucketPath.UPLOAD.value}{upload_path}",
        )
    event = {
        "Records": [
            {
                "awsRegion": "us-east-1",
                "Sns": {"Message": f"{enums.BucketPath.UPLOAD.value}{event_key}"},
            }
        ]
    }

    res = process_upload.process_upload_handler(event, {})
    assert res["statusCode"] == status
    s3_res = s3_client.list_objects_v2(Bucket=mock_utils.TEST_BUCKET)
    assert len(s3_res["Contents"]) == expected_contents
    if event_key.endswith(".archive.parquet"):
        return
    for item in s3_res["Contents"]:
        if item["Key"].endswith("aggregate.parquet"):
            assert item["Key"].startswith(enums.BucketPath.AGGREGATE.value)
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
                or item["Key"].startswith(enums.BucketPath.ARCHIVE.value)
                or item["Key"].startswith(enums.BucketPath.STUDY_META.value)
                or item["Key"].startswith(enums.BucketPath.META.value)
            )
    if res["statusCode"] == 200:
        sqs_res = sqs_client.receive_message(
            QueueUrl=mock_utils.TEST_METADATA_UPDATE_URL, MaxNumberOfMessages=10
        )
        assert len(sqs_res["Messages"]) == 1
        message = json.loads(sqs_res["Messages"][0]["Body"])
        assert message["key"] == "metadata/transactions.json"
        update = json.loads(message["updates"])
        dp_meta = functions.parse_s3_key(f"{enums.BucketPath.UPLOAD.value}{upload_path}")
        print(update[dp_meta.site][dp_meta.study][dp_meta.data_package].keys())
        assert (
            update[dp_meta.site][dp_meta.study][dp_meta.data_package][
                f"{dp_meta.data_package}__{dp_meta.version}"
            ]["last_upload"]
            == datetime.now(UTC).isoformat()
        )
