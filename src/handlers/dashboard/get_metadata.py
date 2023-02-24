""" Lambda for retrieving all, or some, of the upload metadata information
"""
import logging
import os

import boto3

from src.handlers.shared.functions import http_response, read_metadata


def metadata_handler(event, context):
    """Retrieves the upload metadata from S3"""
    del context
    try:
        s3_bucket = os.environ.get("BUCKET_NAME")
        s3_client = boto3.client("s3")
        metadata = read_metadata(s3_client, s3_bucket)
        if params := event["pathParameters"]:
            if "site" in params:
                metadata = metadata[params["site"]]
            if "study" in params:
                metadata = metadata[params["study"]]
        res = http_response(200, metadata)
        return res
    except Exception as e:  # pylint: disable=broad-except
        logging.error("Error retrieving metadata: %s", str(e))
        res = http_response(500, "Error retrieving metadata")
        return res
