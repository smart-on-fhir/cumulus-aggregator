""" Lambda for performing joins of site count data """
import csv
import logging
import os

from datetime import datetime, timezone

import awswrangler
import boto3
import pandas

from numpy import nan
from pandas.core.indexes.range import RangeIndex

from src.handlers.shared.decorators import generic_error_handler
from src.handlers.shared.enums import BucketPath
from src.handlers.shared.awswrangler_functions import get_s3_data_package_list
from src.handlers.shared.functions import (
    get_s3_site_filename_suffix,
    http_response,
    move_s3_file,
    read_metadata,
    update_metadata,
    write_metadata,
)


def get_static_string_series(static_str: str, index: RangeIndex):
    """Helper for the verbose way of defining a pandas string series"""
    return pandas.Series([static_str] * len(index)).astype("string")


def expand_and_concat_sets(
    df: pandas.DataFrame, file_path: str, site_name: str
) -> pandas.DataFrame:
    """Processes and joins dataframes containing powersets.
    :param df: A dataframe to merge with
    :param file_path: An S3 location of an uploaded dataframe
    :param site: The site name used by the aggregator, for convenience
    :return: expanded and merged dataframe

    This function has two steps in terms of business logic:
    - For a powerset we load from S3, we need to add a new column for site name,
        and then duplicate that data and populate the site into the cloned copy,
        so that we will still have a valid powerset
    - We need to take that new powerset and merge it via unique hash of non-count
        columns with the provided in-memory dataframe. We need to preserve N/A
        values since the powerset, by definition, contains lots of them.

    """
    #
    site_df = awswrangler.s3.read_parquet(file_path)
    df_copy = site_df.copy()
    site_df["site"] = get_static_string_series(None, site_df.index)
    df_copy["site"] = get_static_string_series(site_name, df_copy.index)
    # concating in this way adds a new column we want to explictly drop
    # from the final set
    site_df = pandas.concat([site_df, df_copy]).reset_index().drop("index", axis=1)
    data_cols = list(site_df.columns)  # type: ignore[union-attr]
    # There is a baked in assumption with the following line related to the powerset
    # structures, which we will need to handle differently in the future:
    # Specifically, we are assuming the bucket sizes are in a column labeled "cnt",
    # but at some point, we may have different kinds of counts, like "cnt_encounter".
    # We'll need to modify this once we know a bit more about the final design.
    data_cols.remove("cnt")
    agg_df = (
        pandas.concat([df, site_df])
        .groupby(data_cols, dropna=False)
        .sum()
        .sort_values(by=["cnt", "site"], ascending=False, na_position="first")
        .reset_index()
        # this last line makes "cnt" the first column in the set, matching the
        # library style
        .filter(["cnt"] + data_cols)
    )
    return agg_df


def write_parquet(
    df: pandas.DataFrame, path: str, sns_client, is_new_data_package: bool
) -> None:
    awswrangler.s3.to_parquet(df, path, index=False)
    if is_new_data_package:
        topic_sns_arn = os.environ.get("TOPIC_CACHE_API_ARN")
        sns_client.publish(
            TopicArn=topic_sns_arn, Message="data_packages", Subject="data_packages"
        )


def write_csv(df: pandas.DataFrame, path) -> None:
    df = df.apply(lambda x: x.strip() if isinstance(x, str) else x).replace('""', nan)
    df = df.replace(to_replace=r",", value="", regex=True)
    awswrangler.s3.to_csv(df, path, index=False, quoting=csv.QUOTE_NONE)


def merge_powersets(
    s3_client, sns_client, s3_bucket_name: str, site: str, study: str, data_package: str
) -> None:
    """Creates an aggregate powerset from all files with a given s3 prefix"""
    # TODO: this should be memory profiled for large datasets. We can use
    # chunking to lower memory usage during merges.
    metadata = read_metadata(s3_client, s3_bucket_name)
    df = pandas.DataFrame()
    latest_file_list = get_s3_data_package_list(
        BucketPath.LATEST.value, s3_bucket_name, study, data_package
    )
    last_valid_file_list = get_s3_data_package_list(
        BucketPath.LAST_VALID.value, s3_bucket_name, study, data_package
    )
    for s3_path in last_valid_file_list:
        site_specific_name = get_s3_site_filename_suffix(s3_path)
        last_valid_site = site_specific_name.split("/", maxsplit=1)[0]

        # If the latest uploads don't include this site, we'll use the last-valid
        # one instead
        if not any(x.endswith(site_specific_name) for x in latest_file_list):
            df = expand_and_concat_sets(df, s3_path, last_valid_site)
            metadata = update_metadata(
                metadata, last_valid_site, study, data_package, "last_uploaded_date"
            )
    for s3_path in latest_file_list:
        site_specific_name = get_s3_site_filename_suffix(s3_path)
        subbucket_path = f"{study}/{data_package}/{site_specific_name}"
        date_str = datetime.now(timezone.utc).isoformat()
        timestamped_name = f".{date_str}.".join(site_specific_name.split("."))
        timestamped_path = f"{study}/{data_package}/{timestamped_name}"
        try:
            is_new_data_package = False
            # if we're going to replace a file in last_valid, archive the old data
            if any(x.endswith(site_specific_name) for x in last_valid_file_list):
                source = {
                    "Bucket": s3_bucket_name,
                    "Key": f"{BucketPath.LAST_VALID.value}/{subbucket_path}",
                }
                s3_client.copy_object(
                    CopySource=source,
                    Bucket=s3_bucket_name,
                    Key=f"{BucketPath.ARCHIVE.value}/{timestamped_path}",
                )

            # otherwise, this is the first instance - after it's in the database,
            # we'll generate a new list of valid tables for the dashboard
            else:
                is_new_data_package = True
            df = expand_and_concat_sets(df, s3_path, site)
            move_s3_file(
                s3_client,
                s3_bucket_name,
                f"{BucketPath.LATEST.value}/{subbucket_path}",
                f"{BucketPath.LAST_VALID.value}/{subbucket_path}",
            )
            latest_site = site_specific_name.split("/", maxsplit=1)[0]
            metadata = update_metadata(
                metadata, latest_site, study, data_package, "last_data_update"
            )
            metadata = update_metadata(
                metadata, latest_site, study, data_package, "last_aggregation"
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
                metadata, site, study, data_package, "last_error"
            )
            # if a new file fails, we want to replace it with the last valid
            # for purposes of aggregation
            if any(x.endswith(site_specific_name) for x in last_valid_file_list):
                df = expand_and_concat_sets(
                    df,
                    f"s3://{s3_bucket_name}/{BucketPath.LAST_VALID.value}"
                    f"/{subbucket_path}",
                    site,
                )
                metadata = update_metadata(
                    metadata, site, study, data_package, "last_aggregation"
                )
    write_metadata(s3_client, s3_bucket_name, metadata)

    # In this section, we are trying to accomplish two things:
    #   - Prepare a csv that can be loaded manually into the dashboard (requiring no
    #     quotes, which means removing commas from strings)
    #   - Make a parquet file from the dataframe, which may mutate the dataframe
    # So we're making a deep copy to isolate these two mutation paths from each other.

    parquet_aggregate_path = (
        f"s3://{s3_bucket_name}/{BucketPath.AGGREGATE.value}/"
        f"{study}/{study}__{data_package}/{study}__{data_package}__aggregate.parquet"
    )
    write_parquet(
        df.copy(deep=True), parquet_aggregate_path, sns_client, is_new_data_package
    )
    csv_aggregate_path = (
        f"s3://{s3_bucket_name}/{BucketPath.CSVAGGREGATE.value}/"
        f"{study}/{study}__{data_package}/{study}__{data_package}__aggregate.csv"
    )
    write_csv(df, csv_aggregate_path)


@generic_error_handler(msg="Error merging powersets")
def powerset_merge_handler(event, context):
    """manages event from SNS, triggers file processing and merge"""
    del context
    s3_bucket = os.environ.get("BUCKET_NAME")
    s3_client = boto3.client("s3")
    s3_key = event["Records"][0]["Sns"]["Message"]
    s3_key_array = s3_key.split("/")
    site = s3_key_array[3]
    study = s3_key_array[1]
    data_package = s3_key_array[2]
    sns_client = boto3.client("sns")
    merge_powersets(s3_client, sns_client, s3_bucket, site, study, data_package)
    res = http_response(200, "Merge successful")
    return res
