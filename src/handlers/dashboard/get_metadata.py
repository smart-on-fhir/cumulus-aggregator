"""Lambda for retrieving all, or some, of the upload metadata information"""

import os

import boto3

from src.handlers.shared.decorators import generic_error_handler
from src.handlers.shared.functions import http_response, read_metadata


@generic_error_handler(msg="Error retrieving metadata")
def metadata_handler(event, context):
    """Retrieves the upload metadata from S3"""
    del context
    s3_bucket = os.environ.get("BUCKET_NAME")
    s3_client = boto3.client("s3")
    metadata = read_metadata(s3_client, s3_bucket)
    if params := event["pathParameters"]:
        if "site" in params:
            metadata = metadata[params["site"]]
        if "study" in params:
            metadata = metadata[params["study"]]
        if "data_package" in params:
            metadata = metadata[params["data_package"]]
        if "version" in params:
            metadata = metadata[params["version"]]
    res = http_response(200, metadata)
    return res
