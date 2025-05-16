"""Lambda for performing joins of site count data"""

import datetime
import logging
import os

import awswrangler
import pandas
from pandas.core.indexes.range import RangeIndex

from shared import decorators, enums, functions, pandas_functions, s3_manager

log_level = os.environ.get("LAMBDA_LOG_LEVEL", "INFO")
logger = logging.getLogger()
logger.setLevel(log_level)


class MergeError(ValueError):
    def __init__(self, message, filename):
        super().__init__(message)
        self.filename = filename


def get_static_string_series(static_str: str, index: RangeIndex) -> pandas.Series:
    """Helper for the verbose way of defining a pandas string series"""
    return pandas.Series([static_str] * len(index)).astype("string")


def expand_and_concat_powersets(
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


def merge_powersets(manager: s3_manager.S3Manager) -> None:
    """Creates an aggregate powerset from all files with a given s3 prefix"""
    # TODO: this should be memory profiled for large datasets. We can use
    # chunking to lower memory usage during merges.

    logger.info(f"Proccessing data package at {manager.s3_key}")
    # initializing this early in case an empty file causes us to never set it
    is_new_data_package = False
    df = pandas.DataFrame()
    latest_file_list = manager.get_data_package_list(enums.BucketPath.LATEST.value)
    last_valid_file_list = manager.get_data_package_list(enums.BucketPath.LAST_VALID.value)
    for last_valid_path in last_valid_file_list:
        if manager.version not in last_valid_path:
            continue
        last_valid_metadata = functions.parse_s3_key(last_valid_path)
        subbucket_path = (
            f"{manager.study}/{manager.data_package}/{last_valid_metadata.site}/"
            f"{manager.study}__{manager.data_package}__{manager.version}"
        )
        # If the latest uploads don't include this site, we'll use the last-valid
        # one instead
        try:
            if not any(subbucket_path in x for x in latest_file_list):
                df = expand_and_concat_powersets(df, last_valid_path, last_valid_metadata.site)
                manager.update_local_metadata(
                    enums.TransactionKeys.LAST_AGGREGATION.value, site=last_valid_metadata.site
                )
        except MergeError as e:
            # This is expected to trigger if there's an issue in expand_and_concat_powersets;
            # this usually means there's a data problem.
            manager.error_handler(
                e.filename,
                subbucket_path,
                e,
            )
    for latest_path in latest_file_list:
        if manager.version not in latest_path:
            continue
        latest_metadata = functions.parse_s3_key(latest_path)

        # Noting since this introduced a bug previously:
        # the latest/last_data paths reflect the date format used by the upload script,
        # which uses a bare version. This is different than the aggregates, which have
        # a compound version that's really more of an ID.
        # TODO: move path handling for the various subbuckets to a centralized location,
        # this is a brittle pattern right now

        subbucket_path = (
            f"{manager.study}/{manager.study}__{manager.data_package}/{latest_metadata.site}/"
            f"{manager.version}"
        )
        archived_files = []
        try:
            is_new_data_package = False
            # if we're going to replace a file in last_valid, archive the old data
            date_str = datetime.datetime.now(datetime.UTC).isoformat()
            for match in filter(lambda x: subbucket_path in x, last_valid_file_list):
                match_filename = functions.get_filename_from_s3_path(match)
                match_timestamped_filename = f"{date_str}.{match_filename}"
                archive_target = (
                    f"{enums.BucketPath.ARCHIVE.value}/{subbucket_path}/"
                    f"{match_timestamped_filename}"
                )
                manager.move_file(match, archive_target)
                archived_files.append((archive_target, match))
            # otherwise, this is the first instance - after it's in the database,
            # we'll generate a new list of valid tables for the dashboard
            else:
                is_new_data_package = True
            df = expand_and_concat_powersets(df, latest_path, manager.site)
            filename = functions.get_filename_from_s3_path(latest_path)
            manager.move_file(
                functions.get_s3_key_from_path(latest_path),
                f"{enums.BucketPath.LAST_VALID.value}/{subbucket_path}/{filename}",
            )

            manager.update_local_metadata(
                enums.TransactionKeys.LAST_DATA_UPDATE.value, site=latest_metadata.site
            )
            manager.update_local_metadata(
                enums.TransactionKeys.LAST_AGGREGATION.value, site=latest_metadata.site
            )
        except Exception as e:
            manager.error_handler(
                latest_path,
                subbucket_path,
                e,
            )
            # Undo any archiving we tried to do
            for archive in archived_files:
                manager.move_file(archive[0], archive[1])
            # if a new file fails, we want to replace it with the last valid
            # for purposes of aggregation
            for match in filter(lambda x: subbucket_path in x, last_valid_file_list):
                df = expand_and_concat_powersets(
                    df,
                    match,
                    manager.site,
                )
                manager.update_local_metadata(enums.TransactionKeys.LAST_AGGREGATION.value)

    if df.empty:
        raise OSError("File not found")

    manager.write_local_metadata()

    # Updating the typing dict for the column type API
    column_dict = pandas_functions.get_column_datatypes(df)
    manager.update_local_metadata(
        enums.ColumnTypesKeys.COLUMNS.value,
        value=column_dict,
        metadata=manager.types_metadata,
        meta_type=enums.JsonFilename.COLUMN_TYPES.value,
        extra_items={"total": int(df["cnt"][0]), "s3_path": manager.parquet_aggregate_path},
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

    manager.write_parquet(df, is_new_data_package)


@decorators.generic_error_handler(msg="Error merging powersets")
def powerset_merge_handler(event, context):
    """manages event from SNS, triggers file processing and merge"""
    del context
    manager = s3_manager.S3Manager(event)
    merge_powersets(manager)
    res = functions.http_response(200, "Merge successful")
    return res
