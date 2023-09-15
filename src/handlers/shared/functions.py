""" Functions used across different lambdas"""
import io
import json
import logging
from datetime import datetime, timezone
from typing import Optional

import boto3

from src.handlers.shared.enums import BucketPath, JsonFilename

TRANSACTION_METADATA_TEMPLATE = {
    "transaction_format_version": "2",
    "last_upload": None,
    "last_data_update": None,
    "last_aggregation": None,
    "last_error": None,
    "deleted": None,
}
STUDY_PERIOD_METADATA_TEMPLATE = {
    "study_period_format_version": "2",
    "earliest_date": None,
    "latest_date": None,
    "last_data_update": None,
}


def http_response(status: int, body: str, allow_cors: bool = False) -> dict:
    """Generates the payload AWS lambda expects as a return value"""
    headers = {"Content-Type": "application/json"}
    if allow_cors:
        headers.update(
            {
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET",
            }
        )
    return {
        "isBase64Encoded": False,
        "statusCode": status,
        "body": json.dumps(body),
        "headers": headers,
    }


# S3 json processing


def check_meta_type(meta_type: str) -> None:
    """helper for ensuring specified metadata types"""
    types = [item.value for item in JsonFilename]
    if meta_type not in types:
        raise ValueError("invalid metadata type specified")


def read_metadata(
    s3_client, s3_bucket_name: str, meta_type: str = JsonFilename.TRANSACTIONS.value
) -> dict:
    """Reads transaction information from an s3 bucket as a dictionary"""
    check_meta_type(meta_type)
    s3_path = f"{BucketPath.META.value}/{meta_type}.json"
    res = s3_client.list_objects_v2(Bucket=s3_bucket_name, Prefix=s3_path)
    if "Contents" in res:
        res = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_path)
        doc = res["Body"].read()
        return json.loads(doc)
    else:
        return {}


def update_metadata(
    metadata: dict,
    site: str,
    study: str,
    data_package: str,
    version: str,
    target: str,
    dt: Optional[datetime] = None,
    meta_type: str = JsonFilename.TRANSACTIONS.value,
):
    """Safely updates items in metadata dictionary


    It's assumed that, other than the version field itself, every item in one
    of these metadata dicts is a datetime corresponding to an S3 event timestamp
    """
    check_meta_type(meta_type)
    if meta_type == JsonFilename.TRANSACTIONS.value:
        site_metadata = metadata.setdefault(site, {})
        study_metadata = site_metadata.setdefault(study, {})
        data_package_metadata = study_metadata.setdefault(data_package, {})
        data_version_metadata = data_package_metadata.setdefault(
            version, TRANSACTION_METADATA_TEMPLATE
        )
        dt = dt or datetime.now(timezone.utc)
        data_version_metadata[target] = dt.isoformat()
    elif meta_type == JsonFilename.STUDY_PERIODS.value:
        site_metadata = metadata.setdefault(site, {})
        study_period_metadata = site_metadata.setdefault(study, {})
        data_version_metadata = study_period_metadata.setdefault(
            version, STUDY_PERIOD_METADATA_TEMPLATE
        )
        dt = dt or datetime.now(timezone.utc)
        data_version_metadata[target] = dt.isoformat()
    return metadata


def write_metadata(
    s3_client,
    s3_bucket_name: str,
    metadata: dict,
    meta_type: str = JsonFilename.TRANSACTIONS.value,
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
    #   s3://bucket_name/enum_value/site/study/data_package/file
    # so this is returning data_package/file
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


def get_latest_data_package_version(bucket, prefix):
    """Returns the newest version in a data package folder"""
    s3_client = boto3.client("s3")
    if not prefix.endswith("/"):
        prefix = prefix + "/"
    s3_res = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    highest_ver = None
    for item in s3_res["Contents"]:
        ver_str = item["Key"].replace(prefix, "").split("/")[0]
        if ver_str.isdigit():
            if highest_ver is None:
                highest_ver = ver_str
            else:
                if int(highest_ver) < int(ver_str):
                    highest_ver = ver_str
    return highest_ver
