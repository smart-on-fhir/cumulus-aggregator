"""Util script for regenerating the list of data packages from data"""

import argparse
import io
import json
import os

import boto3
import pandas
from rich import progress

from src.shared import enums, pandas_functions
from src.site_upload.cache_api import cache_api


def _put_s3_data(key: str, bucket_name: str, client, data: dict) -> None:
    """Convenience class for writing a dict to S3"""
    b_data = io.BytesIO(json.dumps(data, indent=2).encode())
    client.upload_fileobj(Bucket=bucket_name, Key=key, Fileobj=b_data)


def update_column_type_metadata(bucket: str, client):
    """creates a new metadata dict for column types.

    By design, this will replaces an existing column type dict if one already exists.
    """
    output = {}
    for subbucket in ["aggregates", "flat"]:
        res = client.list_objects_v2(Bucket=bucket, Prefix=f"{subbucket}/")
        contents = res.get("Contents", [])
        for resource in progress.track(contents, description=f"Processing {subbucket}"):
            dirs = resource["Key"].split("/")
            study = dirs[1]
            if subbucket == "aggregates":
                data_package = dirs[2].split("__")[1]
            elif subbucket == "flat":
                data_package = dirs[3].split("__")[1]
            version = dirs[3]
            bytes_buffer = io.BytesIO()
            client.download_fileobj(Bucket=bucket, Key=resource["Key"], Fileobj=bytes_buffer)
            df = pandas.read_parquet(bytes_buffer)
            type_dict = pandas_functions.get_column_datatypes(df.dtypes)
            output.setdefault(study, {})
            output[study].setdefault(data_package, {})
            output[study][data_package].setdefault(version, {})
            output[study][data_package][version]["column_types_format_version"] = 2
            output[study][data_package][version]["columns"] = type_dict
            output[study][data_package][version]["last_data_update"] = (
                resource["LastModified"].now().isoformat()
            )
            output[study][data_package][version]["s3_path"] = f"s3://{bucket}/{resource['Key']}"
            if subbucket == "aggregates":
                output[study][data_package][version]["total"] = int(df["cnt"][0])
            elif subbucket == "flat":
                output[study][data_package][version]["type"] = "flat"
                output[study][data_package][version]["site"] = dirs[2]
                output[study][data_package][version]["total"] = len(df)
    _put_s3_data("metadata/column_types.json", bucket, client, output)


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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""Creates data package metadata for existing aggregates. """
    )
    parser.add_argument("-b", "--bucket", help="bucket name")
    parser.add_argument("-d", "--db", help="database name")
    args = parser.parse_args()
    s3_client = boto3.client("s3")
    update_column_type_metadata(args.bucket, s3_client)
    env_cache = dict(os.environ)
    try:
        # mock some env vars assumed to be set inside a lambda env
        os.environ["BUCKET_NAME"] = args.bucket
        cache_api.cache_api_data(
            s3_client, args.bucket, args.db, enums.JsonFilename.DATA_PACKAGES.value
        )
    except AttributeError as e:
        print(e)
        print(
            "You may need to hot modify the imports inside of cache_api to "
            "point at the project root."
        )
    finally:
        os.environ.clear()
        os.environ.update(env_cache)
