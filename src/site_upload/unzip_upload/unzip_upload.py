"""Lambda for moving data to processing locations"""

import datetime
import logging
import os
import zipfile
from io import BytesIO

import boto3

from shared import enums, functions

log_level = os.environ.get("LAMBDA_LOG_LEVEL", "INFO")
logger = logging.getLogger()
logger.setLevel(log_level)


def unzip_upload(s3_client, sns_client, s3_bucket_name: str, s3_key: str) -> None:
    metadata = functions.parse_s3_key(s3_key)
    buffer = BytesIO(s3_client.get_object(Bucket=s3_bucket_name, Key=s3_key)["Body"].read())
    archive = zipfile.ZipFile(buffer)
    files = archive.namelist()
    files.remove("manifest.toml")

    # The manifest will be used as a signal that the extract has finished,
    # so we'll extract it last in all cases.
    # TODO: decide on extract location for manifests
    new_keys = []
    for file_list in [files, ["manifest.toml"]]:
        for file in file_list:
            target_folder = (
                functions.get_folder_from_s3_path(s3_key)
                .replace(
                    f"{enums.BucketPath.UPLOAD_STAGING.value}/", f"{enums.BucketPath.UPLOAD.value}/"
                )
                .replace(f"/{metadata.study}/", f"/{metadata.study}/{file.split('.')[0]}/")
            )
            s3_client.upload_fileobj(
                archive.open(file), Bucket=s3_bucket_name, Key=f"{target_folder}/{file}"
            )
            new_keys.append(f"{target_folder}/{file}")
    archive_key = s3_key.replace(
        f"{enums.BucketPath.UPLOAD_STAGING.value}/", f"{enums.BucketPath.ARCHIVE.value}/"
    )
    functions.move_s3_file(
        s3_client=s3_client,
        s3_bucket_name=s3_bucket_name,
        old_key=s3_key,
        new_key=f"{archive_key}.{datetime.datetime.now(datetime.UTC).isoformat()}",
    )
    topic_sns_arn = os.environ.get("TOPIC_PROCESS_UPLOADS_ARN")
    sns_subject = "Process file unzip event"
    for key in new_keys:
        sns_client.publish(TopicArn=topic_sns_arn, Message=key, Subject=sns_subject)


# @decorators.generic_error_handler(msg="Error processing file upload")
def unzip_upload_handler(event, context):
    """manages event from S3, triggers file processing and merge"""
    del context
    s3_bucket = os.environ.get("BUCKET_NAME")
    s3_client = boto3.client("s3")
    sns_client = boto3.client("sns", region_name=event["Records"][0]["awsRegion"])
    s3_key = event["Records"][0]["s3"]["object"]["key"]
    unzip_upload(s3_client, sns_client, s3_bucket, s3_key)
    res = functions.http_response(200, "Upload processing successful")
    return res
