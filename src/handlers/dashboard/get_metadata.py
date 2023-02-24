""" Lambda for retrieving all, or some, of the upload metadata information
"""
import logging
import os

from typing import List, Dict

import boto3

from src.handlers.dashboard.filter_config import get_filter_string
from src.handlers.shared.functions import http_response, read_metadata


def metadata_handler(event, context):  # pylint: disable=unused-argument
    try:
        s3_bucket = os.environ.get("BUCKET_NAME")
        s3_client = boto3.client("s3")
        metadata = read_metadata(s3_client, s3_bucket)
        if event["pathParameters"] is not None:
            if "site" in event["pathParameters"]:
                metadata = metadata[event["pathParameters"]["site"]]
            if "study" in event["pathParameters"]:
                metadata = metadata[event["pathParameters"]["study"]]
        res = http_response(200, metadata)
        return res
    except Exception as e:  # pylint: disable=broad-except
        logging.error("Error retrieving metadata: %s", str(e))
        res = http_response(500, "Error retrieving metadata")
        return res
