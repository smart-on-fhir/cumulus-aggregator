"""Lambda for moving data to processing locations"""

import logging
import os

import boto3

from shared import decorators, enums, functions

log_level = os.environ.get("LAMBDA_LOG_LEVEL", "INFO")
logger = logging.getLogger()
logger.setLevel(log_level)


class UnexpectedFileTypeError(Exception):
    pass


def process_upload(s3_client, sns_client, sqs_client, s3_bucket_name: str, s3_key: str) -> None:
    """Moves file from upload path to appropriate subfolder and emits SNS event"""
    last_uploaded_date = s3_client.head_object(Bucket=s3_bucket_name, Key=s3_key)["LastModified"]

    logger.info(f"Proccessing upload at {s3_key}")
    dp_meta = functions.parse_s3_key(s3_key)
    if dp_meta.filename.endswith(".parquet"):
        if (
            s3_key.endswith(f".{enums.UploadTypes.META}.parquet")
            or "/discovery__" in s3_key
            or "/catalog__" in s3_key
            or "__meta_" in s3_key
        ):
            new_key = functions.construct_s3_key(enums.BucketPath.STUDY_META, dp_meta=dp_meta)
            topic_sns_arn = os.environ.get("TOPIC_PROCESS_STUDY_META_ARN")
            sns_subject = "Process study metadata upload event"
        elif s3_key.endswith(f".{enums.UploadTypes.FLAT}.parquet"):
            new_key = functions.construct_s3_key(enums.BucketPath.LATEST_FLAT, dp_meta=dp_meta)
            topic_sns_arn = os.environ.get("TOPIC_PROCESS_FLAT_ARN")
            sns_subject = "Process flat table upload event"
        elif s3_key.endswith(f".{enums.UploadTypes.ARCHIVE}.parquet"):
            # These may contain line level data, and so we just throw them out as a matter
            # of policy
            s3_client.delete_object(Bucket=s3_bucket_name, Key=s3_key)
            logging.info(f"Deleted archive file at {s3_key}")
            return
        else:
            # TODO: Check for .cube.parquet prefix after older versions of the library phase out
            new_key = functions.construct_s3_key(enums.BucketPath.LATEST, dp_meta=dp_meta)
            topic_sns_arn = os.environ.get("TOPIC_PROCESS_COUNTS_ARN")
            sns_subject = "Process counts upload event"
        functions.move_s3_file(s3_client, s3_bucket_name, s3_key, new_key)
        metadata = functions.update_metadata(
            metadata={},
            site=dp_meta.site,
            study=dp_meta.study,
            data_package=dp_meta.data_package,
            version=f"{dp_meta.data_package}__{dp_meta.version}",
            target=enums.TransactionKeys.LAST_UPLOAD,
            dt=last_uploaded_date,
        )
        sns_client.publish(TopicArn=topic_sns_arn, Message=new_key, Subject=sns_subject)
        functions.write_metadata(
            sqs_client=sqs_client, s3_bucket_name=s3_bucket_name, metadata=metadata
        )
    else:
        new_key = functions.construct_s3_key(
            subbucket=enums.BucketPath.ERROR,
            dp_meta=dp_meta,
        )
        functions.move_s3_file(s3_client, s3_bucket_name, s3_key, new_key)
        metadata = functions.update_metadata(
            metadata={},
            site=dp_meta.site,
            study=dp_meta.study,
            data_package=dp_meta.data_package,
            version=f"{dp_meta.data_package}__{dp_meta.version}",
            target=enums.TransactionKeys.LAST_UPLOAD,
            dt=last_uploaded_date,
        )
        metadata = functions.update_metadata(
            metadata=metadata,
            site=dp_meta.site,
            study=f"{dp_meta.site}__{dp_meta.study}",
            data_package=dp_meta.data_package,
            version=f"{dp_meta.data_package}__{dp_meta.version}",
            target=enums.TransactionKeys.LAST_ERROR,
            dt=last_uploaded_date,
        )
        functions.write_metadata(
            sqs_client=sqs_client, s3_bucket_name=s3_bucket_name, metadata=metadata
        )
        raise UnexpectedFileTypeError


@decorators.generic_error_handler(msg="Error processing file upload")
def process_upload_handler(event, context):
    """manages event from S3, triggers file processing and merge"""
    del context
    s3_bucket = os.environ.get("BUCKET_NAME")
    s3_client = boto3.client("s3")
    sns_client = boto3.client("sns", region_name=os.environ.get("AWS_REGION"))
    sqs_client = boto3.client("sqs", region_name=os.environ.get("AWS_REGION"))
    s3_key = event["Records"][0]["Sns"]["Message"]
    process_upload(s3_client, sns_client, sqs_client, s3_bucket, s3_key)
    res = functions.http_response(200, "Upload processing successful")
    return res
