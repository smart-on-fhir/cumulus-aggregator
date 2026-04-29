"""Lambda for retrieving the study description from the manifest"""

import os

import boto3

from shared import decorators, enums, functions

s3_client = boto3.client("s3")


@decorators.generic_error_handler(msg="Error retrieving study data")
def study_data_handler(event, context):
    """Retrieves the upload metadata from S3"""
    del context
    s3_bucket = os.environ.get("BUCKET_NAME")
    if event["pathParameters"] is not None:
        study = event["pathParameters"].get("study")
        version = event["pathParameters"].get("version")
    else:
        study = None
        version = None

    # This is currently focused on just getting the data out from the cached manifests.
    # In the future, we'll want to cache this data someplace, and hydrate it with other
    # sources, like data package info.
    payload = functions.get_s3_json_as_dict(
        bucket=s3_bucket,
        key=f"{enums.BucketPath.CACHE}/{enums.JsonFilename.STUDIES.value}.json",
        s3_client=s3_client,
    )
    try:
        if study:
            payload = payload[study]
            if version:
                if version == "@latest":
                    payload = payload[sorted(payload.keys())[-1]]
                else:
                    payload = payload[version]
    except KeyError:
        return functions.http_response(404, "Not found", allow_cors=True)
    res = functions.http_response(200, payload, allow_cors=True)
    return res
