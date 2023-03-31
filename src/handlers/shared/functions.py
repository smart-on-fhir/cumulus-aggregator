""" Functions used across different lambdas"""
import io
import json

from typing import Dict, Optional
from datetime import datetime, timezone

import boto3

from src.handlers.shared.enums import BucketPath

META_PATH = f"{BucketPath.META.value}/transactions.json"
METADATA_TEMPLATE = {
    "version": "1.0",
    "last_upload": None,
    "last_data_update": None,
    "last_aggregation": None,
    "last_error": None,
    "earliest_data": None,
    "latest_data": None,
    "deleted": None,
}


def http_response(status: int, body: str) -> Dict:
    """Generates the payload AWS lambda expects as a return value"""
    return {
        "isBase64Encoded": False,
        "statusCode": status,
        "body": json.dumps(body),
        "headers": {"Content-Type": "application/json"},
    }


def read_metadata(s3_client, s3_bucket_name: str) -> Dict:
    """Reads transaction information from an s3 bucket as a dictionary"""
    res = s3_client.list_objects_v2(Bucket=s3_bucket_name, Prefix=META_PATH)
    if "Contents" in res:
        res = s3_client.get_object(Bucket=s3_bucket_name, Key=META_PATH)
        return json.loads(res["Body"].read())
    else:
        return {}


def update_metadata(
    metadata: Dict,
    site: str,
    study: str,
    data_package: str,
    target: str,
    dt: Optional[datetime] = None,
):
    """Safely updates items in metadata dictionary"""
    site_metadata = metadata.setdefault(site, {})
    study_metadata = site_metadata.setdefault(study, {})
    data_package_metadata = study_metadata.setdefault(data_package, METADATA_TEMPLATE)
    dt = dt or datetime.now(timezone.utc)
    data_package_metadata[target] = dt.isoformat()
    return metadata


def write_metadata(s3_client, s3_bucket_name: str, metadata: Dict) -> None:
    """Writes transaction info from ∏a dictionary to an s3 bucket metadata location"""
    s3_client.put_object(
        Bucket=s3_bucket_name, Key=META_PATH, Body=json.dumps(metadata)
    )


def get_s3_json_as_dict(bucket, key):
    s3_client = boto3.client("s3")
    bytes_buffer = io.BytesIO()
    s3_client.download_fileobj(
        Bucket=bucket,
        Key=key,
        Fileobj=bytes_buffer,
    )
    return json.loads(bytes_buffer.getvalue().decode())
