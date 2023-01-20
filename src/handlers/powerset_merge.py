import awswrangler
import boto3
import json
import os
import pandas

from src.handlers.shared_functions import http_response


class S3UploadError(Exception):
    pass


def processUpload(s3_client, s3_bucket_name, s3_key):
    # Moves file from upload path to to powerset generation path
    # TODO: this should be replaced by a dedicated lambda that can handle
    # uploads from multiple sites, multiple studies and metadata logging
    new_key = "latest_data/" + s3_key.split("/")[-1]
    source = {"Bucket": s3_bucket_name, "Key": s3_key}
    copy_response = s3_client.copy_object(
        CopySource=source, Bucket=s3_bucket_name, Key=new_key
    )
    delete_response = s3_client.delete_object(Bucket=s3_bucket_name, Key=s3_key)
    if (
        copy_response["ResponseMetadata"]["HTTPStatusCode"] != 200
        or delete_response["ResponseMetadata"]["HTTPStatusCode"] != 204
    ):
        raise S3UploadError


def mergePowersets(s3_client, s3_bucket_name, s3_prefix):
    # Creates an aggregate powerset from all files with a given s3 prefix
    # TODO: this should be memory profiled for large datasets. We can use
    # chunking to lower memory usage during merges.
    aggregate_path = "s3://" + s3_bucket_name + "/" + s3_prefix + "/aggregate.csv"
    try:
        awswrangler.s3.delete_objects(aggregate_path)
    # This except only happens the first time data is merged in a folder
    except:
        pass
    df = pandas.DataFrame()
    csv_list = awswrangler.s3.list_objects(
        "s3://" + s3_bucket_name + "/" + s3_prefix, suffix="csv"
    )
    for csv_file in csv_list:
        site_df = awswrangler.s3.read_csv(csv_file, na_filter=False)
        data_cols = list(site_df.columns)
        # This is from the semantics of how the datasets are generated, but
        # we may want to have this be more flexible in the future.
        data_cols.remove("cnt")
        df = pandas.concat([df, site_df]).groupby(data_cols).sum().reset_index()
    print(df)
    awswrangler.s3.to_csv(df, aggregate_path)


def powersetMergeHandler(event, context):
    # manages event from S3, triggers file processing and merge
    try:
        s3_bucket = "cumulus-aggregator-site-counts"
        s3_client = boto3.client("s3")
        s3_key = event["Records"][0]["s3"]["object"]["key"]
        processUpload(s3_client, s3_bucket, s3_key)
        mergePowersets(s3_client, s3_bucket, "latest_data")
        res = http_response(200, "Merge successful")
    except Exception as e:
        res = http_response(500, "Error processing file")
    return res
