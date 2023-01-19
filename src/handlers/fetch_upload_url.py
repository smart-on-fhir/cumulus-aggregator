import boto3
import json
import logging
import os
import botocore.exceptions

from shared_functions import http_response


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


def uploadUrlHandler(event, context):
    # Processes event from API Gateway
    # TODO: route to folders based on study/institution
    try:
        name = json.loads(event["body"])["name"]
        res = create_presigned_post("cumulus-aggregator", "site_uploads/" + name)
    except KeyError as e:
        res = http_response(400, "Error occured presigning url")
    return res
