import boto3

from src.site_upload.process_flat import process_flat
from tests import mock_utils


def test_process_flat(mock_bucket, mock_queue):
    event = {
        "Records": [
            {
                "Sns": {
                    "TopicArn": "arn",
                    "Message": (
                        f"latest/{mock_utils.EXISTING_STUDY}/{mock_utils.EXISTING_STUDY}__"
                        f"{mock_utils.EXISTING_DATA_P}/{mock_utils.EXISTING_SITE}/"
                        f"{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}__"
                        f"{mock_utils.EXISTING_SITE}__{mock_utils.EXISTING_VERSION}/file.parquet"
                    ),
                }
            }
        ]
    }
    s3_client = boto3.client("s3")
    sqs_client = boto3.client("sqs")
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
    assert (
        f"flat/{mock_utils.EXISTING_STUDY}/{mock_utils.EXISTING_SITE}/"
        f"{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}__{mock_utils.EXISTING_SITE}"
        f"__{mock_utils.EXISTING_VERSION}/"
        f"{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}__{mock_utils.EXISTING_SITE}__flat.parquet"
    ) in files

    sqs_res = sqs_client.receive_message(
        QueueUrl=mock_utils.TEST_METADATA_UPDATE_URL, MaxNumberOfMessages=10
    )
    assert len(sqs_res["Messages"]) == 2

    s3_client.upload_file(
        Bucket=mock_utils.TEST_BUCKET,
        Key=event["Records"][0]["Sns"]["Message"],
        Filename="./tests/test_data/count_synthea_patient_agg.parquet",
    )
    process_flat.process_flat_handler(event, {})

    sqs_res = sqs_client.receive_message(
        QueueUrl=mock_utils.TEST_METADATA_UPDATE_URL, MaxNumberOfMessages=10
    )
    assert len(sqs_res["Messages"]) == 2
