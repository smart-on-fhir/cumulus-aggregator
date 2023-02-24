""" Lambda for performing joins of site count data """
import csv
import logging
import os

from datetime import datetime, timezone

import awswrangler
import boto3
import pandas

from numpy import nan

from src.handlers.shared.enums import BucketPath
from src.handlers.shared.functions import http_response, read_metadata, write_metadata

STUDY_METADATA_TEMPLATE = {
    "version": "1.0",
    "last_upload": None,
    "last_data_update": None,
    "last_aggregation": None,
    "last_error": None,
    "earliest_data": None,
    "latest_data": None,
    "deleted": None,
}


class S3UploadError(Exception):
    pass


def move_s3_file(s3_client, s3_bucket_name: str, old_key: str, new_key: str) -> None:
    """Move file to different S3 location"""
    # TODO: may need to go into shared_functions at some point.
    source = {"Bucket": s3_bucket_name, "Key": old_key}
    copy_response = s3_client.copy_object(
        CopySource=source, Bucket=s3_bucket_name, Key=new_key
    )
    delete_response = s3_client.delete_object(Bucket=s3_bucket_name, Key=old_key)
    if (
        copy_response["ResponseMetadata"]["HTTPStatusCode"] != 200
        or delete_response["ResponseMetadata"]["HTTPStatusCode"] != 204
    ):
        logging.error("error copying file %s to %s", old_key, new_key)
        raise S3UploadError


def process_upload(s3_client, s3_bucket_name: str, s3_key: str) -> None:
    """Moves file from upload path to powerset generation path"""
    last_uploaded_date = s3_client.head_object(Bucket=s3_bucket_name, Key=s3_key)[
        "LastModified"
    ]
    metadata = read_metadata(s3_client, s3_bucket_name)
    path_params = s3_key.split("/")
    study = path_params[1]
    site = path_params[2]
    new_key = f"{BucketPath.LATEST.value}/{s3_key.split('/', 1)[-1]}"
    move_s3_file(s3_client, s3_bucket_name, s3_key, new_key)
    site_metadata = metadata.setdefault(site, {})
    study_metadata = site_metadata.setdefault(study, STUDY_METADATA_TEMPLATE)
    study_metadata["last_upload"] = last_uploaded_date.isoformat()
    write_metadata(s3_client, s3_bucket_name, metadata)


def concat_sets(df: pandas.DataFrame, file_path: str) -> pandas.DataFrame:
    """concats a count dataset in a specified S3 location with in memory dataframe"""
    site_df = awswrangler.s3.read_csv(file_path, na_filter=False)
    data_cols = list(site_df.columns)  # type: ignore[union-attr]
    # There is a baked in assumption with the following line related to the powerset
    # structures, which we will need to handle differently in the future:
    # Specifically, we are assuming the bucket sizes are in a column labeled "cnt",
    # but at some point, we may have different kinds of counts, like "cnt_encounter".
    # We'll need to modify this once we know a bit more about the final design.
    data_cols.remove("cnt")
    return pandas.concat([df, site_df]).groupby(data_cols).sum().reset_index()


def get_site_filename_suffix(s3_path: str):
    # Extracts site/filename data from s3 path
    return "/".join(s3_path.split("/")[5:])


def merge_powersets(s3_client, s3_bucket_name: str, study: str) -> None:
    """Creates an aggregate powerset from all files with a given s3 prefix"""
    # TODO: this should be memory profiled for large datasets. We can use
    # chunking to lower memory usage during merges.
    metadata = read_metadata(s3_client, s3_bucket_name)
    df = pandas.DataFrame()
    latest_csv_list = awswrangler.s3.list_objects(  # type: ignore[call-overload]
        path=f"s3://{s3_bucket_name}/{BucketPath.LATEST.value}/{study}", suffix="csv"
    )
    last_valid_csv_list = awswrangler.s3.list_objects(  # type: ignore[call-overload]
        path=f"s3://{s3_bucket_name}/{BucketPath.LAST_VALID.value}/{study}",
        suffix="csv",
    )
    for s3_path in last_valid_csv_list:
        site_specific_name = get_site_filename_suffix(s3_path)
        site = site_specific_name.split("/", maxsplit=1)[0]
        # If the latest uploads don't include this site, we'll use the last-valid
        # one instead
        if not any(x.endswith(site_specific_name) for x in latest_csv_list):
            df = concat_sets(df, s3_path)
            metadata[site][study]["last_aggregation"] = datetime.now(
                timezone.utc
            ).isoformat()
    for s3_path in latest_csv_list:
        site_specific_name = get_site_filename_suffix(s3_path)
        try:
            df = concat_sets(df, s3_path)
            move_s3_file(
                s3_client,
                s3_bucket_name,
                f"{BucketPath.LATEST.value}/{study}/{site_specific_name}",
                f"{BucketPath.LAST_VALID.value}/{study}/{site_specific_name}",
            )
            site = site_specific_name.split("/", maxsplit=1)[0]
            date_str = datetime.now(timezone.utc).isoformat()
            metadata[site][study]["last_data_update"] = date_str
            metadata[site][study]["last_aggregation"] = date_str
        except Exception as e:  # pylint: disable=broad-except
            logging.error("File %s failed to aggregate: %s", s3_path, str(e))
            move_s3_file(
                s3_client,
                s3_bucket_name,
                f"{BucketPath.LATEST.value}/{study}/{site_specific_name}",
                f"{BucketPath.ERROR.value}/{study}/{site_specific_name}",
            )
            date_str = datetime.now(timezone.utc).isoformat()
            metadata[site][study]["last_error"] = date_str

            # if a new file fails, we want to replace it with the last valid
            # for purposes of aggregation
            if any(x.endswith(site_specific_name) for x in last_valid_csv_list):
                df = concat_sets(
                    df,
                    f"s3://{s3_bucket_name}/{BucketPath.LAST_VALID.value}"
                    f"/{study}/{site_specific_name}",
                )
                metadata[site][study]["last_aggregation"] = date_str
    write_metadata(s3_client, s3_bucket_name, metadata)

    csv_aggregate_path = (
        f"s3://{s3_bucket_name}/{BucketPath.CSVAGGREGATE.value}/"
        f"{study}/{study}_aggregate.csv"
    )
    df = df.apply(lambda x: x.strip() if isinstance(x, str) else x).replace('""', nan)
    awswrangler.s3.to_csv(df, csv_aggregate_path, index=False, quoting=csv.QUOTE_NONE)
    aggregate_path = (
        f"s3://{s3_bucket_name}/{BucketPath.AGGREGATE.value}/"
        f"{study}/{study}_aggregate.parquet"
    )
    # Note: the to_parquet function is noted in the docs as potentially mutating the
    # dataframe it's called on, so this should always be the last action applied
    # to this dataframe, or a deep copy could be made (though mind memory overhead).
    awswrangler.s3.to_parquet(df, aggregate_path, index=False)


def powerset_merge_handler(event, context):
    """manages event from S3, triggers file processing and merge"""
    del context
    try:
        s3_bucket = os.environ.get("BUCKET_NAME")
        s3_client = boto3.client("s3")
        s3_key = event["Records"][0]["s3"]["object"]["key"]
        study = s3_key.split("/")[1]
        process_upload(s3_client, s3_bucket, s3_key)
        merge_powersets(s3_client, s3_bucket, study)
        res = http_response(200, "Merge successful")
    except Exception as e:  # pylint: disable=broad-except
        logging.error("Error processing file %s: %s", s3_key, str(e))
        res = http_response(500, "Error processing file")
    return res
