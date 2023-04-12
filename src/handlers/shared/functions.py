""" Functions used across different lambdas"""
import io
import logging
import json

from typing import Dict, Optional
from datetime import datetime, timezone

import boto3

from src.handlers.shared.enums import BucketPath

TRANSACTION_METADATA_TEMPLATE = {
    "version": "1.0",
    "last_upload": None,
    "last_data_update": None,
    "last_aggregation": None,
    "last_error": None,
    "deleted": None,
}
STUDY_PERIOD_METADATA_TEMPLATE = {
    "version": "1.0",
    "earliest_date": None,
    "latest_date": None,
    "last_data_update": None,
}


def http_response(status: int, body: str) -> Dict:
    """Generates the payload AWS lambda expects as a return value"""
    return {
        "isBase64Encoded": False,
        "statusCode": status,
        "body": json.dumps(body),
        "headers": {"Content-Type": "application/json"},
    }


# metadata processing


def check_meta_type(meta_type: str) -> None:
    """helper for ensuring specified metadata types"""
    types = ["transactions", "study_periods"]
    if meta_type not in types:
        raise ValueError("invalid metadata type specified")


def read_metadata(
    s3_client, s3_bucket_name: str, meta_type: str = "transactions"
) -> Dict:
    """Reads transaction information from an s3 bucket as a dictionary"""
    check_meta_type(meta_type)
    prefix = f"{BucketPath.META.value}/{meta_type}.json"
    res = s3_client.list_objects_v2(Bucket=s3_bucket_name, Prefix=prefix)
    if "Contents" in res:
        res = s3_client.get_object(Bucket=s3_bucket_name, Key=prefix)
        doc = res["Body"].read()
        return json.loads(doc)
    else:
        return {}


def update_metadata(
    metadata: Dict,
    site: str,
    study: str,
    data_package: str,
    target: str,
    dt: Optional[datetime] = None,
    meta_type: str = "transactions",
):
    """Safely updates items in metadata dictionary"""
    check_meta_type(meta_type)
    if meta_type == "transactions":
        site_metadata = metadata.setdefault(site, {})
        study_metadata = site_metadata.setdefault(study, {})
        data_package_metadata = study_metadata.setdefault(
            data_package, TRANSACTION_METADATA_TEMPLATE
        )
        dt = dt or datetime.now(timezone.utc)
        data_package_metadata[target] = dt.isoformat()
    elif meta_type == "study_periods":
        site_metadata = metadata.setdefault(site, {})
        study_period_metadata = site_metadata.setdefault(
            study, STUDY_PERIOD_METADATA_TEMPLATE
        )
        dt = dt or datetime.now(timezone.utc)
        study_period_metadata[target] = dt.isoformat()
    return metadata


def write_metadata(
    s3_client, s3_bucket_name: str, metadata: Dict, meta_type: str = "transactions"
) -> None:
    """Writes transaction info from ∏a dictionary to an s3 bucket metadata location"""
    check_meta_type(meta_type)
    s3_client.put_object(
        Bucket=s3_bucket_name,
        Key=f"{BucketPath.META.value}/{meta_type}.json",
        Body=json.dumps(metadata),
    )


# S3 data management


class S3UploadError(Exception):
    pass


def move_s3_file(s3_client, s3_bucket_name: str, old_key: str, new_key: str) -> None:
    """Move file to different S3 location"""
    source = {"Bucket": s3_bucket_name, "Key": old_key}
    copy_response = s3_client.copy_object(
        CopySource=source, Bucket=s3_bucket_name, Key=new_key
    )
    if copy_response["ResponseMetadata"]["HTTPStatusCode"] != 200:
        logging.error("error copying file %s to %s", old_key, new_key)
        raise S3UploadError
    delete_response = s3_client.delete_object(Bucket=s3_bucket_name, Key=old_key)
    if delete_response["ResponseMetadata"]["HTTPStatusCode"] != 204:
        logging.error("error deleting file %s", old_key)
        raise S3UploadError


def get_s3_site_filename_suffix(s3_path: str):
    """Extracts site/filename data from s3 path"""
    # The expected s3 path for site data packages looks like:
    #   s3://bucket_name/enum_value/site/study/subscription/file
    # so this is returning subscription/file
    return "/".join(s3_path.split("/")[6:])


def get_s3_json_as_dict(bucket, key: str):
    """reads a json object as dict (typically metadata in this case)"""
    s3_client = boto3.client("s3")
    bytes_buffer = io.BytesIO()
    s3_client.download_fileobj(
        Bucket=bucket,
        Key=key,
        Fileobj=bytes_buffer,
    )
    return json.loads(bytes_buffer.getvalue().decode())
