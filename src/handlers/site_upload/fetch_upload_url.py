""" Lambda for generating pre-signed URLs for site upload """
import json
import logging
import os

import boto3
import botocore.exceptions

from src.handlers.shared.decorators import generic_error_handler
from src.handlers.shared.enums import BucketPath
from src.handlers.shared.functions import get_s3_json_as_dict, http_response


def create_presigned_post(
    bucket_name: str, object_name: str, fields=None, conditions=None, expiration=3600
):
    """Generates a secure URL for upload without AWS credentials"""
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


@generic_error_handler(msg="Error occured presigning url")
def upload_url_handler(event, context):
    """Processes event from API Gateway"""
    del context
    metadata_db = get_s3_json_as_dict(
        os.environ.get("BUCKET_NAME"), f"{BucketPath.ADMIN.value}/metadata.json"
    )
    user = event["requestContext"]["authorizer"]["principalId"]
    body = json.loads(event["body"])
    res = create_presigned_post(
        os.environ.get("BUCKET_NAME"),
        f"{BucketPath.UPLOAD.value}/{body['study']}/{body['data_package']}/"
        f"{int(body['data_package_version']):03d}/{metadata_db[user]['path']}"
        f"/{body['filename']}",
    )
    return res
