"""Adds a new metadata type, column_types"""

import argparse
import io
import json

import boto3
import pandas
from rich import progress


def get_csv_column_datatypes(dtypes):
    """helper for generating column type for dashboard API"""
    column_dict = {}
    for column in dtypes.index:
        if column.endswith("year"):
            column_dict[column] = "year"
        elif column.endswith("month"):
            column_dict[column] = "month"
        elif column.endswith("week"):
            column_dict[column] = "week"
        elif column.endswith("day") or str(dtypes[column]) == "datetime64":
            column_dict[column] = "day"
        elif "cnt" in column or str(dtypes[column]) in (
            "Int8",
            "Int16",
            "Int32",
            "Int64",
            "UInt8",
            "UInt16",
            "UInt32",
            "UInt64",
        ):
            column_dict[column] = "integer"
        elif str(dtypes[column]) in ("Float32", "Float64"):
            column_dict[column] = "float"
        elif str(dtypes[column]) == "boolean":
            column_dict[column] = "float"
        else:
            column_dict[column] = "string"
    return column_dict


def _put_s3_data(key: str, bucket_name: str, client, data: dict) -> None:
    """Convenience class for writing a dict to S3"""
    b_data = io.BytesIO(json.dumps(data).encode())
    client.upload_fileobj(Bucket=bucket_name, Key=key, Fileobj=b_data)


def create_column_type_metadata(bucket: str):
    """creates a new metadata dict for column types.

    By design, this will replaces an existing column type dict if one already exists.
    """
    client = boto3.client("s3")
    res = client.list_objects_v2(Bucket=bucket, Prefix="aggregates/")
    contents = res["Contents"]
    output = {}
    for resource in progress.track(contents):
        dirs = resource["Key"].split("/")
        study = dirs[1]
        subscription = dirs[2].split("__")[1]
        version = dirs[3]
        bytes_buffer = io.BytesIO()
        client.download_fileobj(Bucket=bucket, Key=resource["Key"], Fileobj=bytes_buffer)
        df = pandas.read_parquet(bytes_buffer)
        type_dict = get_csv_column_datatypes(df.dtypes)
        filename = f"{resource['Key'].split('/')[-1].split('.')[0]}.csv"
        output.setdefault(study, {})
        output[study].setdefault(subscription, {})
        output[study][subscription].setdefault(version, {})
        output[study][subscription][version]["columns"] = type_dict
        output[study][subscription][version]["filename"] = filename
    # print(json.dumps(output, indent=2))
    _put_s3_data("metadata/column_types.json", bucket, client, output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""Creates column types for existing aggregates. """
    )
    parser.add_argument("-b", "--bucket", help="bucket name")
    args = parser.parse_args()
    create_column_type_metadata(args.bucket)
