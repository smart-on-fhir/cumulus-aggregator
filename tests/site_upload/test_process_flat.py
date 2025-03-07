from unittest import mock

import boto3

from src.site_upload.process_flat import process_flat
from tests import mock_utils


@mock.patch.object(process_flat.s3_manager.S3Manager, "cache_api")
def test_process_flat(mock_cache, mock_bucket):
    event = {
        "Records": [
            {
                "Sns": {
                    "TopicArn": "arn",
                    "Message": (
                        "latest/study/study__encounter/site/study__encounter__version/file.parquet"
                    ),
                }
            }
        ]
    }
    s3_client = boto3.client("s3")
    files = [
        file["Key"] for file in s3_client.list_objects_v2(Bucket=mock_utils.TEST_BUCKET)["Contents"]
    ]
    s3_client.upload_file(
        Bucket=mock_utils.TEST_BUCKET,
        Key=event["Records"][0]["Sns"]["Message"],
        Filename="./tests/test_data/count_synthea_patient_agg.parquet",
    )
    process_flat.process_flat_handler(event, {})
    files = [
        file["Key"] for file in s3_client.list_objects_v2(Bucket=mock_utils.TEST_BUCKET)["Contents"]
    ]
    assert "flat/study/site/study__encounter__version/study__encounter_site__flat.parquet" in files

    mock_cache.reset_mock()
    s3_client.upload_file(
        Bucket=mock_utils.TEST_BUCKET,
        Key=event["Records"][0]["Sns"]["Message"],
        Filename="./tests/test_data/count_synthea_patient_agg.parquet",
    )
    process_flat.process_flat_handler(event, {})
    assert mock_cache.called
