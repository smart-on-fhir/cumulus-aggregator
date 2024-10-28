import os

import boto3
import botocore
from shared import decorators, enums, functions


def _format_and_validate_key(
    s3_client,
    s3_bucket_name: str,
    study: str,
    data_package: str,
    version: str,
    filename: str,
    site: str | None = None,
):
    """Creates S3 key from url params"""
    if site is not None:
        key = f"last_valid/{study}/{study}__{data_package}/{site}/{version}/{filename}"
    else:
        key = f"csv_aggregates/{study}/{study}__{data_package}/{version}/{filename}"
    try:
        s3_client.head_object(Bucket=s3_bucket_name, Key=key)
        return key
    except botocore.exceptions.ClientError as e:
        raise OSError(f"No object found at key {key}") from e


def _get_column_types(
    s3_client,
    s3_bucket_name: str,
    study: str,
    data_package: str,
    version: str,
    **kwargs,
) -> dict:
    """Gets column types from the metadata store for a given data_package"""
    types_metadata = functions.read_metadata(
        s3_client,
        s3_bucket_name,
        meta_type=enums.JsonFilename.COLUMN_TYPES.value,
    )
    try:
        return types_metadata[study][data_package][version][enums.ColumnTypesKeys.COLUMNS.value]
    except KeyError:
        return {}


@decorators.generic_error_handler(msg="Error retrieving chart data")
def get_csv_handler(event, context):
    """manages event from dashboard api call and creates a temporary URL"""
    del context
    s3_bucket_name = os.environ.get("BUCKET_NAME")
    s3_client = boto3.client("s3")
    key = _format_and_validate_key(s3_client, s3_bucket_name, **event["pathParameters"])
    types = _get_column_types(s3_client, s3_bucket_name, **event["pathParameters"])
    presign_url = s3_client.generate_presigned_url(
        "get_object",
        Params={
            "Bucket": s3_bucket_name,
            "Key": key,
            "ResponseContentType": "text/csv",
        },
        ExpiresIn=600,
    )
    extra_headers = {
        "Location": presign_url,
        "x-column-names": ",".join(key for key in types.keys()),
        "x-column-types": ",".join(key for key in types.values()),
        # TODO: add data to x-column-descriptions once a source for column descriptions
        # has been established
        "x-column-descriptions": "",
    }
    res = functions.http_response(302, "", extra_headers=extra_headers)
    return res


@decorators.generic_error_handler(msg="Error retrieving csv data")
def get_csv_list_handler(event, context):
    """manages event from dashboard api call and creates a temporary URL"""
    del context
    s3_bucket_name = os.environ.get("BUCKET_NAME")
    s3_client = boto3.client("s3")
    if event["path"].startswith("/last-valid"):
        key_prefix = "last_valid"
        url_prefix = "last-valid"
    elif event["path"].startswith("/aggregates"):
        key_prefix = "csv_aggregates"
        url_prefix = "aggregates"
    else:
        raise Exception("Unexpected url encountered")

    urls = []
    s3_objs = s3_client.list_objects_v2(Bucket=s3_bucket_name, Prefix=key_prefix)
    if s3_objs["KeyCount"] == 0:
        return functions.http_response(200, urls)
    while True:
        for obj in s3_objs["Contents"]:
            if not obj["Key"].endswith(".csv"):
                continue
            key_parts = obj["Key"].split("/")
            study = key_parts[1]
            data_package = key_parts[2].split("__")[1]
            version = key_parts[-2]
            filename = key_parts[-1]
            site = key_parts[3] if url_prefix == "last-valid" else None
            url_parts = [url_prefix, study, data_package, version, filename]
            if url_prefix == "last-valid":
                url_parts.insert(3, site)
            urls.append("/".join(url_parts))
        if not s3_objs["IsTruncated"]:
            break
        s3_objs = s3_client.list_objects_v2(
            Bucket=s3_bucket_name,
            Prefix=key_prefix,
            ContinuationToken=s3_objs["NextContinuationToken"],
        )
    res = functions.http_response(200, urls)
    return res
