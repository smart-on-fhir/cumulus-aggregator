"""Lambda for retrieving site specific information"""

import os

import boto3

from shared import decorators, enums, functions


@decorators.generic_error_handler(msg="Error retrieving metadata")
def sites_handler(event, context):
    """Retrieves the upload metadata from S3"""
    del context
    s3_bucket = os.environ.get("BUCKET_NAME")
    s3_client = boto3.client("s3")
    site_data = functions.read_json_config(
        s3_client,
        s3_bucket,
        subfolder=enums.BucketPath.ADMIN,
        json_type=enums.JsonFilename.SITES,
    )
    res = {}
    # we don't need to return the key, which maps to the login retrieval in secrets manager
    for value in site_data.values():
        res[value["path"]] = value["display"]
    res = functions.http_response(200, res)
    return res
