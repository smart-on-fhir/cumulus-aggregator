""" Lambda for moving data to processing locations """
import os

import boto3

from ..shared.decorators import generic_error_handler
from ..shared.enums import BucketPath, TransactionKeys
from ..shared.functions import (
    http_response,
    move_s3_file,
    read_metadata,
    update_metadata,
    write_metadata,
)


class UnexpectedFileTypeError(Exception):
    pass


def process_upload(s3_client, sns_client, s3_bucket_name: str, s3_key: str) -> None:
    """Moves file from upload path to appropriate subfolder and emits SNS event"""
    last_uploaded_date = s3_client.head_object(Bucket=s3_bucket_name, Key=s3_key)[
        "LastModified"
    ]
    metadata = read_metadata(s3_client, s3_bucket_name)
    path_params = s3_key.split("/")
    study = path_params[1]
    data_package = path_params[2]
    site = path_params[3]
    version = path_params[4]
    # If someone runs an upload on the template study, we'll just move it
    # to archive - we don't care about metadata for this, but can look there to
    # verify transmission if it's a connectivity test
    if study == "template":
        new_key = f"{BucketPath.ARCHIVE.value}/{s3_key.split('/', 1)[-1]}"
        move_s3_file(s3_client, s3_bucket_name, s3_key, new_key)
    elif s3_key.endswith(".parquet"):
        if "__meta_" in s3_key or "/discovery__" in s3_key:
            new_key = f"{BucketPath.STUDY_META.value}/{s3_key.split('/', 1)[-1]}"
            topic_sns_arn = os.environ.get("TOPIC_PROCESS_STUDY_META_ARN")
            sns_subject = "Process study metadata upload event"
        else:
            new_key = f"{BucketPath.LATEST.value}/{s3_key.split('/', 1)[-1]}"
            topic_sns_arn = os.environ.get("TOPIC_PROCESS_COUNTS_ARN")
            sns_subject = "Process counts upload event"
        move_s3_file(s3_client, s3_bucket_name, s3_key, new_key)
        metadata = update_metadata(
            metadata,
            site,
            study,
            data_package,
            version,
            TransactionKeys.LAST_UPLOAD.value,
            last_uploaded_date,
        )
        sns_client.publish(TopicArn=topic_sns_arn, Message=new_key, Subject=sns_subject)
        write_metadata(s3_client, s3_bucket_name, metadata)
    else:
        new_key = f"{BucketPath.ERROR.value}/{s3_key.split('/', 1)[-1]}"
        move_s3_file(s3_client, s3_bucket_name, s3_key, new_key)
        metadata = update_metadata(
            metadata,
            site,
            study,
            data_package,
            version,
            TransactionKeys.LAST_UPLOAD.value,
            last_uploaded_date,
        )
        metadata = update_metadata(
            metadata,
            site,
            study,
            data_package,
            version,
            TransactionKeys.LAST_ERROR.value,
            last_uploaded_date,
        )
        write_metadata(s3_client, s3_bucket_name, metadata)
        raise UnexpectedFileTypeError


@generic_error_handler(msg="Error processing file upload")
def process_upload_handler(event, context):
    """manages event from S3, triggers file processing and merge"""
    del context
    s3_bucket = os.environ.get("BUCKET_NAME")
    s3_client = boto3.client("s3")
    sns_client = boto3.client("sns", region_name=event["Records"][0]["awsRegion"])
    s3_key = event["Records"][0]["s3"]["object"]["key"]
    process_upload(s3_client, sns_client, s3_bucket, s3_key)
    res = http_response(200, "Upload processing successful")
    return res
