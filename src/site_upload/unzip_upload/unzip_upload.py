"""Lambda for moving data to processing locations"""

import logging
import os
import zipfile
from io import BytesIO

import boto3

from shared import decorators, enums, functions, s3_manager

log_level = os.environ.get("LAMBDA_LOG_LEVEL", "INFO")
logger = logging.getLogger()
logger.setLevel(log_level)


def unzip_upload(s3_client, sns_client, s3_bucket_name: str, s3_key: str) -> None:
    metadata = functions.parse_s3_key(s3_key)
    buffer = BytesIO(s3_client.get_object(Bucket=s3_bucket_name, Key=s3_key)["Body"].read())
    archive = zipfile.ZipFile(buffer)
    files = archive.namelist()
    files.remove("manifest.toml")

    # We'll update the transaction data with the files we're going to process
    # (we can't use the manifest, because empty tables will not get uploaded),
    # and use this later to check to see if all files have been processed.
    # Since metadata is just copied and not otherwise massaged, we skip it
    manager = s3_manager.S3Manager(site=metadata.site, study=metadata.study)
    transaction = manager.get_transaction()
    for upload_type in [
        enums.UploadTypes.CUBE,
        enums.UploadTypes.FLAT,
        enums.UploadTypes.ANNOTATED_CUBE,
    ]:
        transaction[f"{upload_type}"] = [file for file in files if f".{upload_type}." in file]
        transaction["version"] = metadata.version
    manager.put_file(path=manager.transaction, payload=transaction)

    # The manifest may be used in future cases to handle metadata, so we'll
    # extract it last in all cases
    # TODO: decide on extract location for manifests
    new_keys = []
    for file_list in [files]:
        for file in file_list:
            data_package = file.split(".")[0]
            if "__" in data_package:
                data_package = data_package.split("__")[1]
            key = functions.construct_s3_key(
                subbucket=enums.BucketPath.UPLOAD,
                dp_meta=metadata,
                data_package=data_package,
                filename=file,
            )
            s3_client.upload_fileobj(archive.open(file), Bucket=s3_bucket_name, Key=key)
            new_keys.append(key)
    archive_key = functions.construct_s3_key(
        subbucket=enums.BucketPath.ARCHIVE,
        dp_meta=metadata,
    )
    functions.move_s3_file(
        s3_client=s3_client,
        s3_bucket_name=s3_bucket_name,
        old_key=s3_key,
        new_key=archive_key,
    )
    topic_sns_arn = os.environ.get("TOPIC_PROCESS_UPLOADS_ARN")
    sns_subject = "Process file unzip event"
    for key in new_keys:
        sns_client.publish(TopicArn=topic_sns_arn, Message=key, Subject=sns_subject)


@decorators.generic_error_handler(msg="Error processing file upload")
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
