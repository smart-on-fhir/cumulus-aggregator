"""Lambda for retrieving static json files"""

import json
import os

import boto3

from shared import decorators, enums, functions


@decorators.generic_error_handler(msg="Error retrieving static file")
def static_handler(event, context):
    """Retrieves static json files from S3"""
    del context
    s3_bucket = os.environ.get("BUCKET_NAME")
    s3_client = boto3.client("s3")
    if params := event["pathParameters"]:
        path = params["path"]
    else:
        res = functions.http_response(404, "No path supplied")
        return res
    try:
        res = s3_client.get_object(Bucket=s3_bucket, Key=f"{enums.BucketPath.STATIC.value}/{path}")

        data = json.loads(res["Body"].read())
        res = functions.http_response(200, data)
        return res
    except s3_client.exceptions.NoSuchKey:
        res = functions.http_response(404, f"{path} not found")
        return res
