import boto3
import json
import logging
import os

import botocore.exceptions


def http_response(status: int, body: str):
    return {
        "isBase64Encoded": False,
        "statusCode": status,
        "body": json.dumps(body),
        "headers": {"Content-Type": "application/json"},
    }


def create_presigned_post(
    bucket_name: str, object_name: str, fields=None, conditions=None, expiration=3600
):
    s3_client = boto3.client(
        "s3",
        region_name="us-east-1",
        config=boto3.session.Config(signature_version="s3v4"),
    )
    try:
        print("generate_presigned_post")
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
    try:
        name = json.loads(event["body"])["name"]
        res = create_presigned_post("cumulus-aggregator", name)
    except KeyError as e:
        res = http_response(400, "Error occured presigning url")
    return res
