import logging
import os
import traceback

import awswrangler
import boto3
import pandas

from shared import (
    awswrangler_functions,
    enums,
    functions,
)

log_level = os.environ.get("LAMBDA_LOG_LEVEL", "INFO")
logger = logging.getLogger()
logger.setLevel(log_level)


class S3Manager:
    """Class for managing S3 paramaters/access from data in an AWS SNS event.

    This is generally intended as a one stop shop for the data processing phase
    of the aggregator pipeline, providing commmon file paths/sns event parsing helpers/
    stripped down write methods. Consider adding utility functions here instead of using
    raw awswrangler/shared functions to try and make those processes simpler.
    """

    def __init__(self, event):
        self.s3_bucket_name = os.environ.get("BUCKET_NAME")
        self.s3_client = boto3.client("s3")
        self.sns_client = boto3.client("sns", region_name=self.s3_client.meta.region_name)
        self.event_source = event["Records"][0]["Sns"]["TopicArn"]
        self.s3_key = event["Records"][0]["Sns"]["Message"]
        dp_meta = functions.parse_s3_key(self.s3_key)
        self.study = dp_meta.study
        self.data_package = dp_meta.data_package
        self.site = dp_meta.site
        self.version = dp_meta.version
        self.metadata = functions.read_metadata(
            self.s3_client, self.s3_bucket_name, meta_type=enums.JsonFilename.TRANSACTIONS.value
        )
        self.types_metadata = functions.read_metadata(
            self.s3_client,
            self.s3_bucket_name,
            meta_type=enums.JsonFilename.COLUMN_TYPES.value,
        )
        self.parquet_aggregate_path = (
            f"s3://{self.s3_bucket_name}/{enums.BucketPath.AGGREGATE.value}/"
            f"{self.study}/{self.study}__{self.data_package}/"
            f"{self.study}__{self.data_package}__{self.version}/"
            f"{self.study}__{self.data_package}__aggregate.parquet"
        )

        # TODO: Taking out a folder layer to match the depth of non-site aggregates
        # Revisit when targeted crawling is implemented
        self.parquet_flat_key = (
            f"{enums.BucketPath.FLAT.value}/"
            f"{self.study}/{self.site}/"  # {self.study}__{self.data_package}/"
            f"{self.study}__{self.data_package}__{self.site}__{self.version}/"
            f"{self.study}__{self.data_package}__{self.site}__flat.parquet"
        )

    def error_handler(
        self,
        s3_path: str,
        subbucket_path: str,
        error: Exception,
    ) -> None:
        """Logs errors and moves files to the error folder

        :param s3_path: the path of the file generating the S3 error
        :param subbucket_path: the path to write the file to inside the root error folder
        """
        logger.error("Error processing file %s: %s", s3_path, str(error))
        logger.error(traceback.print_exc())
        self.move_file(
            s3_path.replace(f"s3://{self.s3_bucket_name}/", ""),
            f"{enums.BucketPath.ERROR.value}/{subbucket_path}",
        )
        self.update_local_metadata(enums.TransactionKeys.LAST_ERROR.value)

    # S3 Filesystem operations
    def copy_file(self, from_path_or_key: str, to_path_or_key: str) -> None:
        """Copies a file from one location to another in S3.

        This function is agnostic to being provided an S3 path versus an S3 key.

        :param from_path_or_key: the data source
        :param to_path_or_key: the data destination.
        """
        if from_path_or_key.startswith("s3"):
            from_path_or_key = from_path_or_key.split("/", 3)[-1]
        if to_path_or_key.startswith("s3"):
            to_path_or_key = to_path_or_key.split("/", 3)[-1]
        source = {
            "Bucket": self.s3_bucket_name,
            "Key": from_path_or_key,
        }
        self.s3_client.copy_object(
            CopySource=source,
            Bucket=self.s3_bucket_name,
            Key=to_path_or_key,
        )

    def get_data_package_list(self, bucket_root) -> list:
        """Gets a list of data packages associated with the study from the SNS event payload.

        :param bucket_root: the top level directory name in the root of the S3 bucket
        :returns: a list of full s3 file paths
        """
        return awswrangler_functions.get_s3_data_package_list(
            bucket_root, self.s3_bucket_name, self.study, self.data_package
        )

    def move_file(self, from_path_or_key: str, to_path_or_key: str) -> None:
        """moves file from one location to another in s3

        This function is agnostic to being provided an S3 path versus an S3 key.

        :param from_path_or_key: the data source
        :param to_path_or_key: the data destination.

        """
        if from_path_or_key.startswith("s3"):
            from_path_or_key = from_path_or_key.split("/", 3)[-1]
        if to_path_or_key.startswith("s3"):
            to_path_or_key = to_path_or_key.split("/", 3)[-1]
        functions.move_s3_file(
            self.s3_client, self.s3_bucket_name, from_path_or_key, to_path_or_key
        )

    # parquet output creation
    def cache_api(self):
        """Sends an SNS cache event"""
        topic_sns_arn = os.environ.get("TOPIC_CACHE_API_ARN")
        self.sns_client.publish(
            TopicArn=topic_sns_arn, Message="data_packages", Subject="data_packages"
        )

    def write_parquet(self, df: pandas.DataFrame, is_new_data_package: bool, path=None) -> None:
        """Writes a dataframe as parquet to s3 and sends an SNS cache event if new

        :param df: pandas dataframe
        :param is_new_data_package: if true, will dispatch a cache SNS event after copy is completed
        :param path: an S3 path to write to (default: aggregate path)"""
        if path is None:
            path = self.parquet_aggregate_path
        awswrangler.s3.to_parquet(df, path, index=False)
        if is_new_data_package:
            self.cache_api()

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
        """Updates the local cache of a json metadata dictionary

        :param key: the key of the parameter to update
        :keyword site: If provided, the site to update
        :keyword value: If provided, a specific value to assign to the key parameter
            (only used by ColumnTypes)
        :keyword metadata: the specific metadata type to update. default: Transactions
        :keyword meta_type: The enum representing the name of the metadata type.
            Default: Transactions
        :keyword extra_items: A dictionary of items to append to the metadata

        """
        # We are excluding COLUMN_TYPES explicitly from this first check because,
        # by design, it should never have a site field in it - the column types
        # are tied to the study version, not a specific site's data
        if extra_items is None:
            extra_items = {}
        if site is None and (
            meta_type != enums.JsonFilename.COLUMN_TYPES.value
            or extra_items.get("type", "") == "flat"
        ):
            site = self.site
        if extra_items.get("type", "") == "flat":
            version = f"{self.study}__{self.data_package}__{self.site}__{self.version}"
        else:
            version = f"{self.study}__{self.data_package}__{self.version}"
        if metadata is None:
            metadata = self.metadata
        functions.update_metadata(
            metadata=metadata,
            site=site,
            study=self.study,
            data_package=self.data_package,
            version=version,
            target=key,
            value=value,
            meta_type=meta_type,
            extra_items=extra_items,
        )

    def write_local_metadata(self, metadata: dict | None = None, meta_type: str | None = None):
        """Writes a cache of the local metadata back to S3

        :param metadata: the specific dictionary to write. Default: transactions
        :param meta_type: The enum representing the name of the metadata type. Default: Transactions
        """
        metadata = metadata or self.metadata
        meta_type = meta_type or enums.JsonFilename.TRANSACTIONS.value
        functions.write_metadata(
            s3_client=self.s3_client,
            s3_bucket_name=self.s3_bucket_name,
            metadata=metadata,
            meta_type=meta_type,
        )
