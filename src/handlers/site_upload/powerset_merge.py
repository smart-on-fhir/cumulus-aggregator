""" Lambda for performing joins of site count data """
import csv
import logging
import os
import traceback

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


class MergeError(ValueError):
    def __init__(self, message, filename):
        super().__init__(message)
        self.filename = filename


class S3Manager:
    """Convenience class for managing S3 Access"""

    def __init__(self, event):
        self.s3_bucket_name = os.environ.get("BUCKET_NAME")
        self.s3_client = boto3.client("s3")
        self.sns_client = boto3.client(
            "sns", region_name=self.s3_client.meta.region_name
        )

        s3_key = event["Records"][0]["Sns"]["Message"]
        s3_key_array = s3_key.split("/")
        self.site = s3_key_array[3]
        self.study = s3_key_array[1]
        self.data_package = s3_key_array[2]

        self.metadata = read_metadata(self.s3_client, self.s3_bucket_name)

    # S3 Filesystem operations
    def get_data_package_list(self, path) -> list:
        """convenience wrapper for get_s3_data_package_list"""
        return get_s3_data_package_list(
            path, self.s3_bucket_name, self.study, self.data_package
        )

    def move_file(self, from_path: str, to_path: str) -> None:
        """convenience wrapper for move_s3_file"""
        move_s3_file(self.s3_client, self.s3_bucket_name, from_path, to_path)

    def copy_file(self, from_path: str, to_path: str) -> None:
        """convenience wrapper for copy_s3_file"""
        source = {
            "Bucket": self.s3_bucket_name,
            "Key": from_path,
        }
        self.s3_client.copy_object(
            CopySource=source,
            Bucket=self.s3_bucket_name,
            Key=to_path,
        )

    # parquet/csv output creation
    def write_parquet(self, df: pandas.DataFrame, is_new_data_package: bool) -> None:
        """writes dataframe as parquet to s3 and sends an SNS notification if new"""
        parquet_aggregate_path = (
            f"s3://{self.s3_bucket_name}/{BucketPath.AGGREGATE.value}/"
            f"{self.study}/{self.study}__{self.data_package}/"
            f"{self.study}__{self.data_package}__aggregate.parquet"
        )
        awswrangler.s3.to_parquet(df, parquet_aggregate_path, index=False)
        if is_new_data_package:
            topic_sns_arn = os.environ.get("TOPIC_CACHE_API_ARN")
            self.sns_client.publish(
                TopicArn=topic_sns_arn, Message="data_packages", Subject="data_packages"
            )

    def write_csv(self, df: pandas.DataFrame) -> None:
        """writes dataframe as csv to s3"""
        csv_aggregate_path = (
            f"s3://{self.s3_bucket_name}/{BucketPath.CSVAGGREGATE.value}/"
            f"{self.study}/{self.study}__{self.data_package}/"
            f"{self.study}__{self.data_package}__aggregate.csv"
        )
        df = df.apply(lambda x: x.strip() if isinstance(x, str) else x).replace(
            '""', nan
        )
        df = df.replace(to_replace=r",", value="", regex=True)
        awswrangler.s3.to_csv(
            df, csv_aggregate_path, index=False, quoting=csv.QUOTE_NONE
        )

    # metadata
    def update_local_metadata(self, key, site=None):
        """convenience wrapper for update_metadata"""
        if site is None:
            site = self.site
        self.metadata = update_metadata(
            self.metadata, site, self.study, self.data_package, key
        )

    def write_local_metadata(self):
        """convenience wrapper for write_metadata"""
        write_metadata(self.s3_client, self.s3_bucket_name, self.metadata)

    def merge_error_handler(
        self,
        s3_path: str,
        subbucket_path: str,
        error: Exception,
    ) -> None:
        """Helper for logging errors and moving files"""
        logging.error("File %s failed to aggregate: %s", s3_path, str(error))
        logging.error(traceback.print_exc())
        self.move_file(
            s3_path.replace(f"s3://{self.s3_bucket_name}/", ""),
            f"{BucketPath.ERROR.value}/{subbucket_path}",
        )
        self.update_local_metadata("last_error")


def get_static_string_series(static_str: str, index: RangeIndex) -> pandas.Series:
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
    site_df = awswrangler.s3.read_parquet(file_path)
    if site_df.empty:
        raise MergeError("Uploaded data file is empty", filename=file_path)
    df_copy = site_df.copy()
    site_df["site"] = get_static_string_series(None, site_df.index)
    df_copy["site"] = get_static_string_series(site_name, df_copy.index)

    # TODO: we should introduce some kind of data versioning check to see if datasets
    # are generated from the same vintage. This naive approach will cause a decent
    # amount of data churn we'll have to manage in the interim.
    print(df.head)
    print(site_df.head)
    if df.empty is False and set(site_df.columns) != set(df.columns):
        raise MergeError(
            "Uploaded data has a different schema than last aggregate",
            filename=file_path,
        )

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
        .sum(numeric_only=False)
        .sort_values(by=["cnt", "site"], ascending=False, na_position="first")
        .reset_index()
        # this last line makes "cnt" the first column in the set, matching the
        # library style
        .filter(["cnt"] + data_cols)
    )
    return agg_df


def merge_powersets(manager: S3Manager) -> None:
    """Creates an aggregate powerset from all files with a given s3 prefix"""
    # TODO: this should be memory profiled for large datasets. We can use
    # chunking to lower memory usage during merges.

    # initializing this early in case an empty file causes us to never set it
    is_new_data_package = False
    df = pandas.DataFrame()
    latest_file_list = manager.get_data_package_list(BucketPath.LATEST.value)
    last_valid_file_list = manager.get_data_package_list(BucketPath.LAST_VALID.value)
    for last_valid_path in last_valid_file_list:
        site_specific_name = get_s3_site_filename_suffix(last_valid_path)
        subbucket_path = f"{manager.study}/{manager.data_package}/{site_specific_name}"
        last_valid_site = site_specific_name.split("/", maxsplit=1)[0]
        # If the latest uploads don't include this site, we'll use the last-valid
        # one instead
        try:
            if not any(x.endswith(site_specific_name) for x in latest_file_list):
                df = expand_and_concat_sets(df, last_valid_path, last_valid_site)
                manager.update_local_metadata(
                    "last_uploaded_date", site=last_valid_site
                )
        except MergeError as e:  # pylint: disable=broad-except
            # This is expected to trigger if there's an issue in expand_and_concat_sets;
            # this usually means there's a data problem.
            manager.merge_error_handler(
                e.filename,
                subbucket_path,
                e,
            )
    for latest_path in latest_file_list:
        site_specific_name = get_s3_site_filename_suffix(latest_path)
        subbucket_path = f"{manager.study}/{manager.data_package}/{site_specific_name}"
        date_str = datetime.now(timezone.utc).isoformat()
        timestamped_name = f".{date_str}.".join(site_specific_name.split("."))
        timestamped_path = f"{manager.study}/{manager.data_package}/{timestamped_name}"
        try:
            is_new_data_package = False
            # if we're going to replace a file in last_valid, archive the old data
            if any(x.endswith(site_specific_name) for x in last_valid_file_list):
                manager.copy_file(
                    f"{BucketPath.LAST_VALID.value}/{subbucket_path}",
                    f"{BucketPath.ARCHIVE.value}/{timestamped_path}",
                )

            # otherwise, this is the first instance - after it's in the database,
            # we'll generate a new list of valid tables for the dashboard
            else:
                is_new_data_package = True
            df = expand_and_concat_sets(df, latest_path, manager.site)
            manager.move_file(
                f"{BucketPath.LATEST.value}/{subbucket_path}",
                f"{BucketPath.LAST_VALID.value}/{subbucket_path}",
            )
            latest_site = site_specific_name.split("/", maxsplit=1)[0]
            manager.update_local_metadata("last_data_update", site=latest_site)
            manager.update_local_metadata("last_aggregation", site=latest_site)
        except Exception as e:  # pylint: disable=broad-except
            manager.merge_error_handler(
                latest_path,
                subbucket_path,
                e,
            )
            # if a new file fails, we want to replace it with the last valid
            # for purposes of aggregation
            if any(x.endswith(site_specific_name) for x in last_valid_file_list):
                df = expand_and_concat_sets(
                    df,
                    f"s3://{manager.s3_bucket_name}/{BucketPath.LAST_VALID.value}"
                    f"/{subbucket_path}",
                    manager.site,
                )
                manager.update_local_metadata("last_aggregation")
    manager.write_local_metadata()

    # In this section, we are trying to accomplish two things:
    #   - Prepare a csv that can be loaded manually into the dashboard (requiring no
    #     quotes, which means removing commas from strings)
    #   - Make a parquet file from the dataframe, which may mutate the dataframe
    # So we're making a deep copy to isolate these two mutation paths from each other.
    manager.write_parquet(df.copy(deep=True), is_new_data_package)
    manager.write_csv(df)


@generic_error_handler(msg="Error merging powersets")
def powerset_merge_handler(event, context):
    """manages event from SNS, triggers file processing and merge"""
    del context
    manager = S3Manager(event)
    merge_powersets(manager)
    res = http_response(200, "Merge successful")
    return res
