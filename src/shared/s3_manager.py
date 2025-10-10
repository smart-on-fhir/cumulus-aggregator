import datetime
import json
import logging
import os
import traceback
import uuid

import awswrangler
import boto3
import botocore
import pandas

from shared import (
    awswrangler_functions,
    enums,
    errors,
    functions,
)

log_level = os.environ.get("LAMBDA_LOG_LEVEL", "INFO")
logger = logging.getLogger()
logger.setLevel(log_level)


class S3Manager:
    """Class for managing S3 paramaters/access from AWS events, or manual definition.

    This is generally intended as a one stop shop for the data processing phase
    of the aggregator pipeline, providing commmon file paths/sns event parsing helpers/
    stripped down write methods. Consider adding utility functions here instead of using
    raw awswrangler/shared functions to try and make those processes simpler.
    """

    def __init__(
        self,
        event: dict | None = None,
        study: str | None = None,
        site: str | None = None,
        data_package: str | None = None,
        version: str | None = None,
    ):
        self.s3_bucket_name = os.environ.get("BUCKET_NAME")
        self.s3_client = boto3.client("s3")
        self.sns_client = boto3.client("sns", region_name=self.s3_client.meta.region_name)
        self.sqs_client = boto3.client("sqs", region_name=self.s3_client.meta.region_name)
        self.site = None
        self.study = None
        self.data_package = None
        self.version = None
        self.transaction = None
        self.dp_meta = None
        # If the event is an SNS type event, we're in the aggregation pipeline and set up
        # some convenience values.
        if event is not None and "Records" in event and "Sns" in event["Records"][0]:
            self.event_source = event["Records"][0]["Sns"]["TopicArn"]
            self.s3_key = event["Records"][0]["Sns"]["Message"]
            self.dp_meta = functions.parse_s3_key(self.s3_key)
            self.study = self.dp_meta.study
            self.data_package = self.dp_meta.data_package
            self.site = self.dp_meta.site
            self.version = self.dp_meta.version
            self.metadata = functions.read_metadata(
                self.s3_client, self.s3_bucket_name, meta_type=enums.JsonFilename.TRANSACTIONS
            )
            self.types_metadata = functions.read_metadata(
                self.s3_client,
                self.s3_bucket_name,
                meta_type=enums.JsonFilename.COLUMN_TYPES,
            )
            # These two dictionaries should be shadow copies of the metadata dicts,
            # containing only the changes made in the lambda lifecycle.
            self.metadata_delta = {}
            self.types_metadata_delta = {}

            self.parquet_aggregate_key = functions.construct_s3_key(
                subbucket=enums.BucketPath.AGGREGATE,
                dp_meta=self.dp_meta,
                filename=self.dp_meta.get_filename(enums.BucketPath.AGGREGATE),
            )
            self.parquet_flat_key = functions.construct_s3_key(
                subbucket=enums.BucketPath.FLAT,
                dp_meta=self.dp_meta,
                filename=self.dp_meta.get_filename(enums.BucketPath.FLAT),
            )
        if study:
            self.study = study
        if site:
            self.site = site
        if data_package:
            self.data_package = data_package
        if version:
            self.version = version
        if self.site and self.study:
            self.transaction = (
                f"{enums.BucketPath.META}/transactions/{self.site}__{self.study}.json"
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
        # Note - this can take either latest or last valid, and may be from
        # another site, so don't replace this with functions.construct_s3_key
        self.move_file(
            s3_path.replace(f"s3://{self.s3_bucket_name}/", ""),
            f"{enums.BucketPath.ERROR}/{subbucket_path}",
        )
        self.update_local_metadata(enums.TransactionKeys.LAST_ERROR)

    # S3 Filesystem operations
    def put_file(self, path: str, payload: str | dict) -> None:
        """Writes the variable in payload to s3.

        :param path: the path of the requested file
        :param payload: the variable to write to the file in S3
        """
        path = functions.get_s3_key_from_path(path)
        functions.put_s3_file(
            s3_client=self.s3_client, s3_bucket_name=self.s3_bucket_name, key=path, payload=payload
        )

    def copy_file(self, from_path: str, to_path: str) -> None:
        """Copies a file from one location to another in S3.

        :param from_path: the data source S3 path or key
        :param to_path: the data destinationS3 path or key
        """
        from_path = functions.get_s3_key_from_path(from_path)
        to_path = functions.get_s3_key_from_path(to_path)
        source = {
            "Bucket": self.s3_bucket_name,
            "Key": from_path,
        }
        self.s3_client.copy_object(
            CopySource=source,
            Bucket=self.s3_bucket_name,
            Key=to_path,
        )

    def write_data_to_file(self, data: str, path: str) -> None:
        """Creates a file in s3 from a string

        :param data: The string to write
        :param path: The key or full S3 path
        """
        path = functions.get_s3_key_from_path(path)
        data = data.encode(encoding="utf-8")
        self.s3_client.put_object(Bucket=self.s3_bucket_name, Key=path, Body=data)

    def move_file(self, from_path: str, to_path: str) -> None:
        """Moves file from one location to another in s3

        :param from_path: the data source S3 path or key
        :param to_path: the data destination S3 path or key

        """
        from_path = functions.get_s3_key_from_path(from_path)
        to_path = functions.get_s3_key_from_path(to_path)
        functions.move_s3_file(self.s3_client, self.s3_bucket_name, from_path, to_path)

    def delete_file(self, path: str) -> None:
        """Deletes a file at the speicified location in S3

        :param path: the data S3 path or key

        """
        path = functions.get_s3_key_from_path(path)
        functions.delete_s3_file(self.s3_client, self.s3_bucket_name, path)

    def get_last_modified_timestamp(self, path: str):
        path = functions.get_s3_key_from_path(path)
        return self.s3_client.head_object(Bucket=self.s3_bucket_name, Key=path)["LastModified"]

    def get_presigned_download_url(self, path: str, expiration=3600) -> dict | None:
        """Generates a secure URL for upload without AWS credentials

        :param path: an object s3 path or key
        :param expiration: Time in seconds for url to be valid
        :returns: A url, or None if no object at the location
        """
        path = functions.get_s3_key_from_path(path)
        try:
            res = self.s3_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.s3_bucket_name, "Key": path},
                ExpiresIn=expiration,
            )
            return res
        except botocore.exceptions.ClientError as e:  # pragma: no cover
            logging.error(e)
            return None

    def get_data_package_list(self, bucket_root) -> list:
        """Gets a list of data packages associated with the study from the SNS event payload.

        :param bucket_root: the top level directory name in the root of the S3 bucket
        :returns: a list of full s3 file paths
        """
        return awswrangler_functions.get_s3_data_package_list(
            bucket_root, self.s3_bucket_name, self.study, self.data_package
        )

    # parquet output creation
    def cache_api(self):
        """Sends an SNS cache event"""
        topic_sns_arn = os.environ.get("TOPIC_COMPLETENESS_ARN")
        self.sns_client.publish(
            TopicArn=topic_sns_arn,
            Message=json.dumps({"site": self.site, "study": self.study}),
            Subject="check_completeness",
        )

    def write_parquet(self, df: pandas.DataFrame, key=None) -> None:
        """Writes a dataframe as parquet to s3 and sends an SNS cache event if new

        :param df: pandas dataframe
        :param is_new_data_package: if true, will dispatch a cache SNS event after copy is completed
        :param path: an S3 path to write to (default: aggregate path)"""
        if key is None:
            key = self.parquet_aggregate_key
        awswrangler.s3.to_parquet(df, f"s3://{self.s3_bucket_name}/{key}", index=False)
        self.cache_api()

    # metadata
    def update_local_metadata(
        self,
        key,
        *,
        site=None,
        value=None,
        meta_type: str | None = enums.JsonFilename.TRANSACTIONS,
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
            meta_type != enums.JsonFilename.COLUMN_TYPES or extra_items.get("type", "") == "flat"
        ):
            site = self.site
        if extra_items.get("type", "") == "flat":
            version = f"{self.study}__{self.data_package}__{self.site}__{self.version}"
        else:
            version = f"{self.study}__{self.data_package}__{self.version}"
        match meta_type:
            case enums.JsonFilename.TRANSACTIONS:
                metadata = self.metadata
                meta_delta = self.metadata_delta
            case enums.JsonFilename.COLUMN_TYPES:
                metadata = self.types_metadata
                meta_delta = self.types_metadata_delta
        for meta_dict in (metadata, meta_delta):
            functions.update_metadata(
                metadata=meta_dict,
                site=site,
                study=self.study,
                data_package=self.data_package,
                version=version,
                target=key,
                value=value,
                meta_type=meta_type,
                extra_items=extra_items,
            )

    def write_local_metadata(self, meta_type: str | None = None):
        """Writes a cache of the local metadata back to S3

        :param metadata: the specific dictionary to write. Default: transactions
        :param meta_type: The enum representing the name of the metadata type. Default: Transactions
        """
        meta_type = meta_type or enums.JsonFilename.TRANSACTIONS
        match meta_type:
            case enums.JsonFilename.TRANSACTIONS:
                metadata = self.metadata_delta
            case enums.JsonFilename.COLUMN_TYPES:
                metadata = self.types_metadata_delta

        functions.write_metadata(
            sqs_client=self.sqs_client,
            s3_bucket_name=self.s3_bucket_name,
            metadata=metadata,
            meta_type=meta_type,
        )

    # transaction management
    def request_or_validate_transaction(self, transaction_id: str | None = None):
        try:
            transaction = functions.get_s3_json_as_dict(
                bucket=self.s3_bucket_name, key=self.transaction
            )
            if transaction_id != transaction["id"]:
                raise errors.AggregatorStudyProcessingError

        except botocore.exceptions.ClientError:
            if transaction_id is not None:
                # are we requesting a transaction that no longer exists for some reason?
                raise errors.AggregatorStudyProcessingError
            # if the transaction doesn't exist, we'll make one
            transaction_id = str(uuid.uuid4())
            transaction = {"id": transaction_id, "uploaded_at": datetime.datetime.now(datetime.UTC)}
            sqs_client = boto3.client("sqs")
            sqs_client.send_message(
                QueueUrl=os.environ.get("QUEUE_TRANSACTION_CLEANUP"),
                MessageBody=json.dumps({"site": self.site, "study": self.study}),
            )
            self.s3_client.put_object(
                Bucket=self.s3_bucket_name,
                Key=self.transaction,
                Body=json.dumps(transaction, default=str, indent=2),
                IfNoneMatch="*",
            )
        return transaction_id

    def get_transaction(self):
        return json.loads(
            self.s3_client.get_object(Bucket=self.s3_bucket_name, Key=self.transaction)[
                "Body"
            ].read()
        )

    def delete_transaction(self):
        self.delete_file(self.transaction)
