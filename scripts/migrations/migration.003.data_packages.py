"""this migration is the basis of reset_data_package_cache - it is kept around
mostly as a historical artifact of how migrations were run until then/utility
for quickly cribbing together new migrations through reuse."""

import argparse
import enum
import io
import json
import os

import awswrangler
import boto3
import pandas
from rich import progress


class JsonFilename(enum.Enum):
    """stores names of expected kinds of persisted S3 JSON files"""

    COLUMN_TYPES = "column_types"
    TRANSACTIONS = "transactions"
    DATA_PACKAGES = "data_packages"
    STUDY_PERIODS = "study_periods"


class BucketPath(enum.Enum):
    """stores root level buckets for managing data processing state"""

    ADMIN = "admin"
    AGGREGATE = "aggregates"
    ARCHIVE = "archive"
    CACHE = "cache"
    CSVAGGREGATE = "csv_aggregates"
    ERROR = "error"
    LAST_VALID = "last_valid"
    LATEST = "latest"
    META = "metadata"
    STUDY_META = "study_metadata"
    UPLOAD = "site_upload"


def get_column_datatypes(dtypes):
    """helper for generating column type for dashboard API"""
    column_dict = {}
    for column in dtypes.index:
        if column.endswith("year"):
            column_dict[column] = "year"
        elif column.endswith("month"):
            column_dict[column] = "month"
        elif column.endswith("week"):
            column_dict[column] = "week"
        elif column.endswith("day") or str(dtypes[column]).lower() == "datetime64":
            column_dict[column] = "day"
        elif column.startswith("cnt") or str(dtypes[column]).lower() in (
            "int8",
            "int16",
            "int32",
            "int64",
            "uint8",
            "uint16",
            "uint32",
            "uint64",
        ):
            column_dict[column] = "integer"
        elif str(dtypes[column]).lower() in ("float32", "float64"):
            column_dict[column] = "float"
        elif str(dtypes[column]) == "boolean":
            column_dict[column] = "boolean"
        elif column in ["median", "average", "std_dev", "percentage"]:
            column_dict[column] = "double"
        else:
            column_dict[column] = "string"
    return column_dict


def _put_s3_data(key: str, bucket_name: str, client, data: dict) -> None:
    """Convenience class for writing a dict to S3"""
    b_data = io.BytesIO(json.dumps(data, indent=2).encode())
    client.upload_fileobj(Bucket=bucket_name, Key=key, Fileobj=b_data)


def update_column_type_metadata(bucket: str):
    """creates a new metadata dict for column types.

    By design, this will replaces an existing column type dict if one already exists.
    """
    client = boto3.client("s3")
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
            type_dict = get_column_datatypes(df.dtypes)
            output.setdefault(study, {})
            output[study].setdefault(data_package, {})
            output[study][data_package].setdefault(version, {})
            output[study][data_package][version]["column_types_format_version"] = "2"
            output[study][data_package][version]["columns"] = type_dict
            output[study][data_package][version]["last_data_update"] = (
                resource["LastModified"].now().isoformat()
            )
            output[study][data_package][version]["s3_path"] = resource["Key"]
            if subbucket == "aggregates":
                output[study][data_package][version]["total"] = int(df["cnt"][0])
            elif subbucket == "flat":
                output[study][data_package][version]["type"] = "flat"
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


def cache_api_data(s3_bucket_name: str, db: str) -> None:
    s3_client = boto3.client("s3")
    df = awswrangler.athena.read_sql_query(
        (
            f"SELECT table_name FROM information_schema.tables "  # noqa: S608
            f"WHERE table_schema = '{db}'"  # nosec
        ),
        database=db,
        s3_output=f"s3://{s3_bucket_name}/awswrangler",
        workgroup=os.environ.get("WORKGROUP_NAME"),
    )
    data_packages = df[df["table_name"].str.contains("__")].iloc[:, 0]
    column_types = get_s3_json_as_dict(
        s3_bucket_name,
        f"{BucketPath.META.value}/{JsonFilename.COLUMN_TYPES.value}.json",
    )
    dp_details = []
    for dp in list(data_packages):
        try:
            study, name, version = dp.split("__")
        except ValueError:
            print("invalid name: ", dp)
            continue
        try:
            matching_col_types = column_types[study][name]
            dp_details.append(
                {
                    "study": study,
                    "name": name,
                    "version": version,
                    **matching_col_types[dp],
                    "id": dp,
                }
            )
        except KeyError as e:
            print("invalid key: ", e)
    s3_client.put_object(
        Bucket=s3_bucket_name,
        Key=f"{BucketPath.CACHE.value}/{JsonFilename.DATA_PACKAGES.value}.json",
        Body=json.dumps(dp_details),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""Creates data package metadata for existing aggregates. """
    )
    parser.add_argument("-b", "--bucket", help="bucket name")
    parser.add_argument("-d", "--db", help="database name")
    args = parser.parse_args()
    update_column_type_metadata(args.bucket)
    cache_api_data(args.bucket, args.db)
