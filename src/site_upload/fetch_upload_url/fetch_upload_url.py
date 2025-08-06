"""Lambda for generating pre-signed URLs for site upload"""

import json
import logging
import os

import boto3
import botocore.exceptions

from shared import decorators, enums, errors, functions, s3_manager


def create_presigned_post(
    bucket_name: str,
    object_name: str,
    transaction_id: str,
    fields=None,
    conditions=None,
    expiration=3600,
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
        return functions.http_response(
            200,
            response_body,
            extra_headers={"transaction-id": transaction_id},
        )
    except botocore.exceptions.ClientError as e:
        logging.error(e)
        return functions.http_response(400, "Error occured presigning url")


@decorators.generic_error_handler(msg="Error occured presigning url")
def upload_url_handler(event, context):
    """Processes event from API Gateway"""
    del context
    try:
        metadata_db = functions.get_s3_json_as_dict(
            os.environ.get("BUCKET_NAME"), f"{enums.BucketPath.ADMIN.value}/metadata.json"
        )
    except Exception:
        return functions.http_response(
            500, "Stack configuration error - check bucket name & metadata."
        )
    user = event["requestContext"]["authorizer"]["principalId"]
    body = json.loads(event["body"])
    for key in ["study", "data_package", "filename"]:
        if key not in body.keys() or body[key] is None:
            return functions.http_response(  # test
                400,
                "Malformed data payload. See "
                "https://docs.smarthealthit.org/cumulus/library/sharing-data.html "
                "for more information about uploading data.",
            )
    if "data_package_version" in body:
        version = body["data_package_version"]
    else:
        version = "0"
    try:
        manager = s3_manager.S3Manager(
            event,
            site=metadata_db[user]["path"],
            study=body["study"],
            data_package=body["data_package"],
            version=version,
        )
        transaction_id = manager.request_or_validate_transaction(
            event.get("headers", {}).get("transaction-id")
        )
        res = create_presigned_post(
            os.environ.get("BUCKET_NAME"),
            f"{enums.BucketPath.UPLOAD.value}/{body['study']}/{body['data_package']}/"
            f"{metadata_db[user]['path']}/{int(version):03d}/{body['filename']}",
            transaction_id,
        )
        return res
    except errors.AggregatorStudyProcessingError:
        return functions.http_response(  # test
            401,
            f"Currently processing a previous upload of {body['study']}. Please try again later.",
        )
