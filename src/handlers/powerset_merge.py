""" Lambda for performing joins of site count data """
import csv
import logging

import awswrangler
import boto3
import pandas


from src.handlers.enums import BucketPath
from src.handlers.shared_functions import http_response


class S3UploadError(Exception):
    pass


def move_s3_file(s3_client, s3_bucket_name, old_key, new_key):
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


def process_upload(s3_client, s3_bucket_name, s3_key):
    # Moves file from upload path to to powerset generation path
    # TODO: this should be updated to log file metadata
    new_key = f"{BucketPath.LATEST.value}/{s3_key.split('/', 1)[1]}"
    move_s3_file(s3_client, s3_bucket_name, s3_key, new_key)


def concat_sets(df, file_path):
    # concats a count dataset in a specified S3 location with the in memory dataframe
    site_df = awswrangler.s3.read_csv(file_path, na_filter=False)
    data_cols = list(site_df.columns)
    # This is from the semantics of how the datasets are generated, but
    # we may want to have this be more flexible in the future.
    data_cols.remove("cnt")
    return pandas.concat([df, site_df]).groupby(data_cols).sum().reset_index()


def get_site_filename_suffix(s3_path):
    # if s3_path.startswith('s3'):
    #    return '/'.join(s3_path.split('/')[7:])
    return "/".join(s3_path.split("/")[5:])


def merge_powersets(s3_client, s3_bucket_name, study):
    # Creates an aggregate powerset from all files with a given s3 prefix
    # TODO: this should be memory profiled for large datasets. We can use
    # chunking to lower memory usage during merges.
    df = pandas.DataFrame()
    latest_csv_list = awswrangler.s3.list_objects(
        f"s3://{s3_bucket_name}/{BucketPath.LATEST.value}/{study}", suffix="csv"
    )
    last_valid_csv_list = awswrangler.s3.list_objects(
        f"s3://{s3_bucket_name}/{BucketPath.LAST_VALID.value}/{study}", suffix="csv"
    )
    for s3_path in last_valid_csv_list:
        site_specific_name = get_site_filename_suffix(s3_path)
        if (
            len(list(filter(lambda x, n=site_specific_name: n in x, latest_csv_list)))
            == 0
        ):
            df = concat_sets(df, s3_path)
    for s3_path in latest_csv_list:
        try:
            site_specific_name = get_site_filename_suffix(s3_path)
            df = concat_sets(df, s3_path)
            move_s3_file(
                s3_client,
                s3_bucket_name,
                f"{BucketPath.LATEST.value}/{study}/{site_specific_name}",
                f"{BucketPath.LAST_VALID.value}/{study}/{site_specific_name}",
            )
        except Exception as e:
            logging.error("File %s failed to aggregate", s3_path)
            logging.error(e)
            move_s3_file(
                s3_client,
                s3_bucket_name,
                f"{BucketPath.LATEST.value}/{study}/{site_specific_name}",
                f"{BucketPath.ERROR.value}/{study}/{site_specific_name}",
            )
            raise S3UploadError  # pylint: disable=raise-missing-from
    aggregate_path = (
        f"s3://{s3_bucket_name}/{BucketPath.AGGREGATE.value}/{study}/aggregate.csv"
    )
    awswrangler.s3.to_csv(df, aggregate_path, index=False, quoting=csv.QUOTE_NONNUMERIC)


def powerset_merge_handler(event, context):  # pylint: disable=W0613
    # manages event from S3, triggers file processing and merge
    try:
        s3_bucket = "cumulus-aggregator-site-counts"
        s3_client = boto3.client("s3")
        s3_key = event["Records"][0]["s3"]["object"]["key"]
        study = s3_key.split("/")[1]
        process_upload(s3_client, s3_bucket, s3_key)
        merge_powersets(s3_client, s3_bucket, study)
        res = http_response(200, "Merge successful")
    except Exception as e:  # pylint: disable=W0703
        logging.error(e)
        res = http_response(500, "Error processing file")
    return res
