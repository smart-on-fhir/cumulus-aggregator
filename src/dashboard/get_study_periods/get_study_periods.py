"""Lambda for retrieving all, or some, of the study period information"""

import os

import boto3

from shared import decorators, enums, functions


@decorators.generic_error_handler(msg="Error retrieving study period")
def study_periods_handler(event, context):
    """Retrieves the study period from S3"""
    del context
    s3_bucket = os.environ.get("BUCKET_NAME")
    s3_client = boto3.client("s3")
    metadata = functions.read_metadata(
        s3_client, s3_bucket, meta_type=enums.JsonFilename.STUDY_PERIODS.value
    )
    if params := event["pathParameters"]:
        if "site" in params:
            metadata = metadata[params["site"]]
        if "study" in params:
            metadata = metadata[params["study"]]
    res = functions.http_response(200, metadata)
    return res
