""" Lambda for performing joins of site count data """
import csv
import logging
import os

from datetime import datetime, timezone

import awswrangler
import boto3
import pandas

from numpy import nan

from src.handlers.shared.decorators import generic_error_handler
from src.handlers.shared.enums import BucketPath
from src.handlers.shared.functions import (
    http_response,
    read_metadata,
    update_metadata,
    write_metadata,
)


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
    subscription = path_params[2]
    site = path_params[3]
    new_key = f"{BucketPath.LATEST.value}/{s3_key.split('/', 1)[-1]}"
    move_s3_file(s3_client, s3_bucket_name, s3_key, new_key)
    metadata = update_metadata(
        metadata, site, study, subscription, "last_uploaded_date", last_uploaded_date
    )
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
    return "/".join(s3_path.split("/")[6:])


def get_file_list(bucket_root, s3_bucket_name, study, subscription, extension="csv"):
    return awswrangler.s3.list_objects(
        path=f"s3://{s3_bucket_name}/{bucket_root}/{study}/{subscription}",
        suffix=extension,
    )


def merge_powersets(
    s3_client, s3_bucket_name: str, study: str, subscription: str
) -> None:
    """Creates an aggregate powerset from all files with a given s3 prefix"""
    # TODO: this should be memory profiled for large datasets. We can use
    # chunking to lower memory usage during merges.
    metadata = read_metadata(s3_client, s3_bucket_name)
    df = pandas.DataFrame()
    latest_csv_list = get_file_list(
        BucketPath.LATEST.value, s3_bucket_name, study, subscription
    )
    last_valid_csv_list = get_file_list(
        BucketPath.LAST_VALID.value, s3_bucket_name, study, subscription
    )
    for s3_path in last_valid_csv_list:
        site_specific_name = get_site_filename_suffix(s3_path)
        site = site_specific_name.split("/", maxsplit=1)[0]

        # If the latest uploads don't include this site, we'll use the last-valid
        # one instead
        if not any(x.endswith(site_specific_name) for x in latest_csv_list):

            df = concat_sets(df, s3_path)
            metadata = update_metadata(
                metadata, site, study, subscription, "last_uploaded_date"
            )
    for s3_path in latest_csv_list:
        site_specific_name = get_site_filename_suffix(s3_path)
        subbucket_path = f"{study}/{subscription}/{site_specific_name}"
        date_str = datetime.now(timezone.utc).isoformat()
        timestamped_name = f".{date_str}.".join(site_specific_name.split("."))
        timestamped_path = f"{study}/{subscription}/{timestamped_name}"
        try:
            # if we're going to replace a file in last_valid, archive the old data
            if any(x.endswith(site_specific_name) for x in last_valid_csv_list):
                source = {
                    "Bucket": s3_bucket_name,
                    "Key": f"{BucketPath.LAST_VALID.value}/{subbucket_path}",
                }
                s3_client.copy_object(
                    CopySource=source,
                    Bucket=s3_bucket_name,
                    Key=f"{BucketPath.ARCHIVE.value}/{timestamped_path}",
                )
            df = concat_sets(df, s3_path)
            move_s3_file(
                s3_client,
                s3_bucket_name,
                f"{BucketPath.LATEST.value}/{subbucket_path}",
                f"{BucketPath.LAST_VALID.value}/{subbucket_path}",
            )
            site = site_specific_name.split("/", maxsplit=1)[0]
            metadata = update_metadata(
                metadata, site, study, subscription, "last_data_update"
            )
            metadata = update_metadata(
                metadata, site, study, subscription, "last_aggregation"
            )
        except Exception as e:  # pylint: disable=broad-except
            logging.error("File %s failed to aggregate: %s", s3_path, str(e))
            move_s3_file(
                s3_client,
                s3_bucket_name,
                f"{BucketPath.LATEST.value}/{subbucket_path}",
                f"{BucketPath.ERROR.value}/{subbucket_path}",
            )
            metadata = update_metadata(
                metadata, site, study, subscription, "last_error"
            )
            # if a new file fails, we want to replace it with the last valid
            # for purposes of aggregation
            if any(x.endswith(site_specific_name) for x in last_valid_csv_list):
                df = concat_sets(
                    df,
                    f"s3://{s3_bucket_name}/{BucketPath.LAST_VALID.value}"
                    f"/{subbucket_path}",
                )
                metadata = update_metadata(
                    metadata, site, study, subscription, "last_aggregation"
                )
    write_metadata(s3_client, s3_bucket_name, metadata)
    csv_aggregate_path = (
        f"s3://{s3_bucket_name}/{BucketPath.CSVAGGREGATE.value}/"
        f"{study}/{study}_{subscription}/{study}_{subscription}_aggregate.csv"
    )
    df = df.apply(lambda x: x.strip() if isinstance(x, str) else x).replace('""', nan)
    awswrangler.s3.to_csv(df, csv_aggregate_path, index=False, quoting=csv.QUOTE_NONE)
    aggregate_path = (
        f"s3://{s3_bucket_name}/{BucketPath.AGGREGATE.value}/"
        f"{study}/{study}_{subscription}/{study}_{subscription}_aggregate.parquet"
    )
    # Note: the to_parquet function is noted in the docs as potentially mutating the
    # dataframe it's called on, so this should always be the last action applied
    # to this dataframe, or a deep copy could be made (though mind memory overhead).
    awswrangler.s3.to_parquet(df, aggregate_path, index=False)


@generic_error_handler(msg="Error processing file")
def powerset_merge_handler(event, context):
    """manages event from S3, triggers file processing and merge"""
    del context
    s3_bucket = os.environ.get("BUCKET_NAME")
    s3_client = boto3.client("s3")
    s3_key = event["Records"][0]["s3"]["object"]["key"]
    s3_key_array = s3_key.split("/")
    study = s3_key_array[1]
    subscription = s3_key_array[2]
    process_upload(s3_client, s3_bucket, s3_key)
    merge_powersets(s3_client, s3_bucket, study, subscription)
    res = http_response(200, "Merge successful")
    return res
