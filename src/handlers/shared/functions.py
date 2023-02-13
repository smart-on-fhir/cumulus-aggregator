""" Functions used across different lambdas"""
import json

from src.handlers.shared.enums import BucketPath

META_PATH = f"{BucketPath.META.value}/transactions.json"


def http_response(status: int, body: str):
    """Generates the payload AWS lambda expects as a return value"""
    return {
        "isBase64Encoded": False,
        "statusCode": status,
        "body": json.dumps(body),
        "headers": {"Content-Type": "application/json"},
    }


def read_metadata(s3_client, s3_bucket_name):
    res = s3_client.list_objects_v2(Bucket=s3_bucket_name, Prefix=META_PATH)
    if "Contents" in res:
        res = s3_client.get_object(Bucket=s3_bucket_name, Key=META_PATH)
        return json.loads(res["Body"].read())
    else:
        return {}


def write_metadata(s3_client, s3_bucket_name, metadata):
    s3_client.put_object(
        Bucket=s3_bucket_name, Key=META_PATH, Body=json.dumps(metadata)
    )
