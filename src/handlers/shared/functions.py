""" Functions used across different lambdas"""
import json

from typing import Dict

from src.handlers.shared.enums import BucketPath

META_PATH = f"{BucketPath.META.value}/transactions.json"


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


def write_metadata(s3_client, s3_bucket_name: str, metadata: Dict) -> None:
    """Writes transaction info from ∏a dictionary to an s3 bucket metadata location"""
    s3_client.put_object(
        Bucket=s3_bucket_name, Key=META_PATH, Body=json.dumps(metadata)
    )
