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
    "upload_file,study,data_package,site,version,filename,status,expected_contents",
    [
        (  # Adding a new data package to a site with uploads
            "./tests/test_data/cube_simple_example.parquet",
            mock_utils.EXISTING_STUDY,
            mock_utils.NEW_DATA_P,
            mock_utils.EXISTING_SITE,
            mock_utils.EXISTING_VERSION,
            "document.cube.parquet",
            200,
            mock_utils.ITEM_COUNT + 1,
        ),
        (  # Adding a new data package to a site without uploads
            "./tests/test_data/cube_simple_example.parquet",
            mock_utils.EXISTING_STUDY,
            mock_utils.NEW_DATA_P,
            mock_utils.NEW_SITE,
            mock_utils.EXISTING_VERSION,
            "document.cube.parquet",
            200,
            mock_utils.ITEM_COUNT + 1,
        ),
        (  # Updating an existing data package
            "./tests/test_data/cube_simple_example.parquet",
            mock_utils.EXISTING_STUDY,
            mock_utils.EXISTING_DATA_P,
            mock_utils.EXISTING_SITE,
            mock_utils.EXISTING_VERSION,
            "encounter.cube.parquet",
            200,
            mock_utils.ITEM_COUNT + 1,
        ),
        (  # New version of an existing data package
            "./tests/test_data/cube_simple_example.parquet",
            mock_utils.EXISTING_STUDY,
            mock_utils.EXISTING_DATA_P,
            mock_utils.EXISTING_SITE,
            mock_utils.NEW_VERSION,
            "encounter.cube.parquet",
            200,
            mock_utils.ITEM_COUNT + 1,
        ),
        (  # Upload of a flat file
            "./tests/test_data/cube_simple_example.parquet",
            mock_utils.EXISTING_STUDY,
            mock_utils.EXISTING_DATA_P,
            mock_utils.EXISTING_SITE,
            mock_utils.EXISTING_VERSION,
            "document.flat.parquet",
            200,
            mock_utils.ITEM_COUNT + 1,
        ),
        (  # Upload of an archive file (which should be deleted)
            "./tests/test_data/cube_simple_example.parquet",
            mock_utils.EXISTING_STUDY,
            mock_utils.EXISTING_DATA_P,
            mock_utils.NEW_SITE,
            mock_utils.EXISTING_VERSION,
            "encounter.archive.parquet",
            200,
            mock_utils.ITEM_COUNT,
        ),
        (  # Non-parquet file
            "./tests/test_data/cube_simple_example.csv",
            mock_utils.EXISTING_STUDY,
            mock_utils.EXISTING_DATA_P,
            mock_utils.EXISTING_SITE,
            mock_utils.EXISTING_VERSION,
            "encounter.cube.csv",
            500,
            mock_utils.ITEM_COUNT + 1,
        ),
        (  # Sns event dispatched when file is not present
            None,
            mock_utils.EXISTING_STUDY,
            mock_utils.EXISTING_DATA_P,
            mock_utils.EXISTING_SITE,
            mock_utils.EXISTING_VERSION,
            "encounter.cube.parquet",
            500,
            mock_utils.ITEM_COUNT,
        ),
        (  # Adding metadata data package
            "./tests/test_data/cube_simple_example.parquet",
            mock_utils.EXISTING_STUDY,
            mock_utils.EXISTING_DATA_P,
            mock_utils.NEW_SITE,
            mock_utils.EXISTING_VERSION,
            "study__meta_date.parquet",
            200,
            mock_utils.ITEM_COUNT + 1,
        ),
        (  # Adding discovery data package
            "./tests/test_data/cube_simple_example.parquet",
            "discovery",
            mock_utils.EXISTING_DATA_P,
            mock_utils.EXISTING_SITE,
            mock_utils.EXISTING_VERSION,
            "discovery__file.parquet",
            200,
            mock_utils.ITEM_COUNT + 1,
        ),
    ],
)
def test_process_upload(
    upload_file,
    study,
    data_package,
    site,
    version,
    filename,
    status,
    expected_contents,
    mock_bucket,
    mock_notification,
    mock_queue,
):
    s3_client = boto3.client("s3", region_name="us-east-1")
    sqs_client = boto3.client("sqs", region_name="us-east-1")
    upload_key = functions.construct_s3_key(
        subbucket=enums.BucketPath.UPLOAD,
        site=site,
        study=study,
        data_package=data_package,
        version=version,
        filename=filename,
    )
    if upload_file is not None:
        s3_client.upload_file(
            upload_file,
            mock_utils.TEST_BUCKET,
            upload_key,
        )
    event = {
        "Records": [
            {
                "awsRegion": "us-east-1",
                "Sns": {"Message": upload_key},
            }
        ]
    }

    res = process_upload.process_upload_handler(event, {})
    assert res["statusCode"] == status
    s3_res = s3_client.list_objects_v2(Bucket=mock_utils.TEST_BUCKET)
    assert len(s3_res["Contents"]) == expected_contents
    if upload_key.endswith(".archive.parquet"):
        return
    for item in s3_res["Contents"]:
        if item["Key"].endswith("aggregate.parquet"):
            assert item["Key"].startswith(enums.BucketPath.AGGREGATE)
        elif item["Key"].startswith(enums.BucketPath.STUDY_META):
            assert any(x in item["Key"] for x in ["_meta_", "/discovery__"])
        else:
            assert (
                item["Key"].startswith(enums.BucketPath.LATEST)
                or item["Key"].startswith(enums.BucketPath.LAST_VALID)
                or item["Key"].startswith(enums.BucketPath.ERROR)
                or item["Key"].startswith(enums.BucketPath.ADMIN)
                or item["Key"].startswith(enums.BucketPath.CACHE)
                or item["Key"].startswith(enums.BucketPath.FLAT)
                or item["Key"].startswith(enums.BucketPath.LATEST_FLAT)
                or item["Key"].startswith(enums.BucketPath.ARCHIVE)
                or item["Key"].startswith(enums.BucketPath.STUDY_META)
                or item["Key"].startswith(enums.BucketPath.META)
            )
    if res["statusCode"] == 200:
        sqs_res = sqs_client.receive_message(
            QueueUrl=mock_utils.TEST_METADATA_UPDATE_URL, MaxNumberOfMessages=10
        )
        assert len(sqs_res["Messages"]) == 1
        message = json.loads(sqs_res["Messages"][0]["Body"])
        assert message["key"] == "metadata/transactions.json"
        update = json.loads(message["updates"])
        dp_meta = functions.parse_s3_key(upload_key)
        assert (
            update[dp_meta.site][dp_meta.study][dp_meta.data_package][
                f"{dp_meta.data_package}__{dp_meta.version}"
            ]["last_upload"]
            == datetime.now(UTC).isoformat()
        )
