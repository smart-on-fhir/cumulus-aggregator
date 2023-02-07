""" Lambda for generating pre-signed URLs for site upload """
import json
import logging

import boto3
import botocore.exceptions

from src.handlers.site_upload.enums import BucketPath
from src.handlers.site_upload.shared_functions import http_response


def create_presigned_post(
    bucket_name: str, object_name: str, fields=None, conditions=None, expiration=3600
):
    # Generates a secure URL for upload without AWS credentials
    s3_client = boto3.client(
        "s3",
        region_name="us-east-1",
        config=boto3.session.Config(signature_version="s3v4"),
    )
    try:
        response_body = s3_client.generate_presigned_post(
            bucket_name,
            object_name,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=expiration,
        )
        return http_response(200, response_body)
    except botocore.exceptions.ClientError as e:
        logging.error(e)
        return http_response(400, "Error occured presigning url")


def upload_url_handler(event, context):  # pylint: disable=W0613
    # Processes event from API Gateway
    with open(
        "src/handlers/site_upload/site_data/metadata.json", encoding="utf-8"
    ) as metadata:
        metadata_db = json.load(metadata)
    try:
        user = event["headers"]["user"]
        body = json.loads(event["body"])
        res = create_presigned_post(
            "cumulus-aggregator-site-counts",
            f"{BucketPath.UPLOAD.value}/{body['study']}/"
            f"{metadata_db[user]['path']}/{body['filename']}",
        )
    except Exception as e:  # pylint: disable=broad-except
        logging.error(e)
        res = http_response(500, "Error occured presigning url")
    return res
