import boto3

from src.shared import enums, functions
from src.site_upload.process_flat import process_flat
from tests import mock_utils


def test_process_flat(mock_bucket, mock_queue):
    dp_meta = functions.PackageMetadata(
        study=mock_utils.EXISTING_STUDY,
        site=mock_utils.EXISTING_SITE,
        data_package=mock_utils.EXISTING_DATA_P,
        version=mock_utils.EXISTING_VERSION,
    )
    latest_flat_key = functions.construct_s3_key(
        enums.BucketPath.LATEST_FLAT, dp_meta=dp_meta, filename="file.parquet"
    )
    event = {"Records": [{"Sns": {"TopicArn": "arn", "Message": latest_flat_key}}]}
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
    assert (functions.construct_s3_key(enums.BucketPath.FLAT, dp_meta=dp_meta) in x for x in files)

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
