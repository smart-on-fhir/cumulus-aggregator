"""Lambda for performing joins of site count data"""

import csv
import datetime
import logging
import os
import traceback

import awswrangler
import boto3
import numpy
import pandas
from pandas.core.indexes.range import RangeIndex

from src.handlers.shared import (
    awswrangler_functions,
    decorators,
    enums,
    functions,
    pandas_functions,
)

log_level = os.environ.get("LAMBDA_LOG_LEVEL", "INFO")
logger = logging.getLogger()
logger.setLevel(log_level)


class MergeError(ValueError):
    def __init__(self, message, filename):
        super().__init__(message)
        self.filename = filename


class S3Manager:
    """Convenience class for managing S3 Access"""

    def __init__(self, event):
        self.s3_bucket_name = os.environ.get("BUCKET_NAME")
        self.s3_client = boto3.client("s3")
        self.sns_client = boto3.client("sns", region_name=self.s3_client.meta.region_name)

        self.s3_key = event["Records"][0]["Sns"]["Message"]
        s3_key_array = self.s3_key.split("/")
        self.study = s3_key_array[1]
        self.data_package = s3_key_array[2].split("__")[1]
        self.site = s3_key_array[3]
        self.version = s3_key_array[4][-3:]
        self.metadata = functions.read_metadata(self.s3_client, self.s3_bucket_name)
        self.types_metadata = functions.read_metadata(
            self.s3_client,
            self.s3_bucket_name,
            meta_type=enums.JsonFilename.COLUMN_TYPES.value,
        )
        self.csv_aggerate_path = (
            f"s3://{self.s3_bucket_name}/{enums.BucketPath.CSVAGGREGATE.value}/"
            f"{self.study}/{self.study}__{self.data_package}/"
            f"{self.version}/"
            f"{self.study}__{self.data_package}__aggregate.csv"
        )

    # S3 Filesystem operations
    def get_data_package_list(self, path) -> list:
        """convenience wrapper for get_s3_data_package_list"""
        return awswrangler_functions.get_s3_data_package_list(
            path, self.s3_bucket_name, self.study, self.data_package
        )

    def move_file(self, from_path: str, to_path: str) -> None:
        """convenience wrapper for move_s3_file"""
        functions.move_s3_file(self.s3_client, self.s3_bucket_name, from_path, to_path)

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
            f"s3://{self.s3_bucket_name}/{enums.BucketPath.AGGREGATE.value}/"
            f"{self.study}/{self.study}__{self.data_package}/"
            f"{self.study}__{self.data_package}_{self.version}/"
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
        df = df.apply(lambda x: x.strip() if isinstance(x, str) else x).replace('""', numpy.nan)
        df = df.replace(to_replace=r",", value="", regex=True)
        awswrangler.s3.to_csv(df, self.csv_aggerate_path, index=False, quoting=csv.QUOTE_NONE)

    # metadata
    def update_local_metadata(
        self,
        key,
        *,
        site=None,
        value=None,
        metadata: dict | None = None,
        meta_type: str | None = enums.JsonFilename.TRANSACTIONS.value,
        extra_items: dict | None = None,
    ):
        """convenience wrapper for update_metadata"""
        # We are excluding COLUMN_TYPES explicitly from this first check because,
        # by design, it should never have a site field in it - the column types
        # are tied to the study version, not a specific site's data
        if extra_items is None:
            extra_items = {}
        if site is None and meta_type != enums.JsonFilename.COLUMN_TYPES.value:
            site = self.site
        if metadata is None:
            metadata = self.metadata
        metadata = functions.update_metadata(
            metadata=metadata,
            site=site,
            study=self.study,
            data_package=self.data_package,
            version=self.version,
            target=key,
            value=value,
            meta_type=meta_type,
            extra_items=extra_items,
        )

    def write_local_metadata(self, metadata: dict | None = None, meta_type: str | None = None):
        """convenience wrapper for write_metadata"""
        metadata = metadata or self.metadata
        meta_type = meta_type or enums.JsonFilename.TRANSACTIONS.value
        functions.write_metadata(
            s3_client=self.s3_client,
            s3_bucket_name=self.s3_bucket_name,
            metadata=metadata,
            meta_type=meta_type,
        )

    def merge_error_handler(
        self,
        s3_path: str,
        subbucket_path: str,
        error: Exception,
    ) -> None:
        """Helper for logging errors and moving files"""
        logger.error("File %s failed to aggregate: %s", s3_path, str(error))
        logger.error(traceback.print_exc())
        self.move_file(
            s3_path.replace(f"s3://{self.s3_bucket_name}/", ""),
            f"{enums.BucketPath.ERROR.value}/{subbucket_path}",
        )
        self.update_local_metadata(enums.TransactionKeys.LAST_ERROR.value)


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

    # Did we change the schema without updating the version?
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
        .filter(["cnt", *data_cols])
    )
    return agg_df


def generate_csv_from_parquet(bucket_name: str, bucket_root: str, subbucket_path: str):
    """Convenience function for generating csvs for dashboard upload

    TODO: Remove on dashboard parquet/API support"""
    last_valid_df = awswrangler.s3.read_parquet(
        f"s3://{bucket_name}/{bucket_root}" f"/{subbucket_path}"
    )
    last_valid_df = last_valid_df.apply(lambda x: x.strip() if isinstance(x, str) else x).replace(
        '""', numpy.nan
    )
    awswrangler.s3.to_csv(
        last_valid_df,
        (f"s3://{bucket_name}/{bucket_root}/{subbucket_path}".replace(".parquet", ".csv")),
        index=False,
        quoting=csv.QUOTE_MINIMAL,
    )


def merge_powersets(manager: S3Manager) -> None:
    """Creates an aggregate powerset from all files with a given s3 prefix"""
    # TODO: this should be memory profiled for large datasets. We can use
    # chunking to lower memory usage during merges.

    # initializing this early in case an empty file causes us to never set it
    logger.info(f"Proccessing data package at {manager.s3_key}")
    is_new_data_package = False
    df = pandas.DataFrame()
    latest_file_list = manager.get_data_package_list(enums.BucketPath.LATEST.value)
    last_valid_file_list = manager.get_data_package_list(enums.BucketPath.LAST_VALID.value)
    for last_valid_path in last_valid_file_list:
        if manager.version not in last_valid_path:
            continue
        site_specific_name = functions.get_s3_site_filename_suffix(last_valid_path)
        subbucket_path = f"{manager.study}/{manager.data_package}/{site_specific_name}"
        last_valid_site = site_specific_name.split("/", maxsplit=1)[0]
        # If the latest uploads don't include this site, we'll use the last-valid
        # one instead
        try:
            if not any(x.endswith(site_specific_name) for x in latest_file_list):
                df = expand_and_concat_sets(df, last_valid_path, last_valid_site)
                manager.update_local_metadata(
                    enums.TransactionKeys.LAST_AGGREGATION.value, site=last_valid_site
                )
        except MergeError as e:
            # This is expected to trigger if there's an issue in expand_and_concat_sets;
            # this usually means there's a data problem.
            manager.merge_error_handler(
                e.filename,
                subbucket_path,
                e,
            )
    for latest_path in latest_file_list:
        if manager.version not in latest_path:
            continue
        site_specific_name = functions.get_s3_site_filename_suffix(latest_path)
        subbucket_path = (
            f"{manager.study}/{manager.study}__{manager.data_package}" f"/{site_specific_name}"
        )
        date_str = datetime.datetime.now(datetime.UTC).isoformat()
        timestamped_name = f".{date_str}.".join(site_specific_name.split("."))
        timestamped_path = (
            f"{manager.study}/{manager.study}__{manager.data_package}" f"/{timestamped_name}"
        )
        try:
            is_new_data_package = False
            # if we're going to replace a file in last_valid, archive the old data
            if any(x.endswith(site_specific_name) for x in last_valid_file_list):
                manager.copy_file(
                    f"{enums.BucketPath.LAST_VALID.value}/{subbucket_path}",
                    f"{enums.BucketPath.ARCHIVE.value}/{timestamped_path}",
                )
            # otherwise, this is the first instance - after it's in the database,
            # we'll generate a new list of valid tables for the dashboard
            else:
                is_new_data_package = True
            df = expand_and_concat_sets(df, latest_path, manager.site)
            manager.move_file(
                f"{enums.BucketPath.LATEST.value}/{subbucket_path}",
                f"{enums.BucketPath.LAST_VALID.value}/{subbucket_path}",
            )

            ####################
            # For now, we'll create a csv of the file we just put in last valid.
            # This is used for uploading to the dashboard.
            # TODO: remove as soon as we support either parquet upload or
            # the API is supported by the dashboard
            generate_csv_from_parquet(
                manager.s3_bucket_name,
                enums.BucketPath.LAST_VALID.value,
                subbucket_path,
            )
            ####################

            latest_site = site_specific_name.split("/", maxsplit=1)[0]
            manager.update_local_metadata(
                enums.TransactionKeys.LAST_DATA_UPDATE.value, site=latest_site
            )
            manager.update_local_metadata(
                enums.TransactionKeys.LAST_AGGREGATION.value, site=latest_site
            )
        except Exception as e:
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
                    f"s3://{manager.s3_bucket_name}/{enums.BucketPath.LAST_VALID.value}"
                    f"/{subbucket_path}",
                    manager.site,
                )
                manager.update_local_metadata(enums.TransactionKeys.LAST_AGGREGATION.value)

    if df.empty:
        raise OSError("File not found")

    manager.write_local_metadata()

    # Updating the typing dict for the column type API
    column_dict = pandas_functions.get_column_datatypes(df.dtypes)
    manager.update_local_metadata(
        enums.ColumnTypesKeys.COLUMNS.value,
        value=column_dict,
        metadata=manager.types_metadata,
        meta_type=enums.JsonFilename.COLUMN_TYPES.value,
        extra_items={"total": int(df["cnt"][0]), "s3_path": manager.csv_aggerate_path},
    )
    manager.update_local_metadata(
        enums.ColumnTypesKeys.LAST_DATA_UPDATE.value,
        value=column_dict,
        metadata=manager.types_metadata,
        meta_type=enums.JsonFilename.COLUMN_TYPES.value,
    )
    manager.write_local_metadata(
        metadata=manager.types_metadata,
        meta_type=enums.JsonFilename.COLUMN_TYPES.value,
    )

    # In this section, we are trying to accomplish two things:
    #   - Prepare a csv that can be loaded manually into the dashboard (requiring no
    #     quotes, which means removing commas from strings)
    #   - Make a parquet file from the dataframe, which may mutate the dataframe
    # So we're making a deep copy to isolate these two mutation paths from each other.
    manager.write_parquet(df.copy(deep=True), is_new_data_package)
    manager.write_csv(df)


@decorators.generic_error_handler(msg="Error merging powersets")
def powerset_merge_handler(event, context):
    """manages event from SNS, triggers file processing and merge"""
    del context
    manager = S3Manager(event)
    merge_powersets(manager)
    res = functions.http_response(200, "Merge successful")
    return res
