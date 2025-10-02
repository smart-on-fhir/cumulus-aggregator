import datetime
import json
import logging
import os
from time import sleep

import awswrangler
import boto3
import botocore

from shared import decorators, enums, functions

logger = logging.getLogger()
logger.setLevel("INFO")
s3_client = boto3.client("s3")
sns_client = boto3.client("sns")
g_client = boto3.client("glue")


def check_if_complete(message) -> (bool, dict):
    transaction = functions.get_s3_json_as_dict(
        bucket=os.environ.get("BUCKET_NAME"),
        key=(
            f"{enums.BucketPath.META.value}/transactions/{message['site']}__{message['study']}.json"
        ),
        s3_client=s3_client,
    )
    upload_time = datetime.datetime.fromisoformat(transaction["uploaded_at"])
    for filetype, source, suffix in [
        ("cube", enums.BucketPath.AGGREGATE.value, "aggregate"),
        ("annotated_cube", enums.BucketPath.AGGREGATE.value, "aggregate"),
        ("flat", enums.BucketPath.FLAT.value, "flat"),
    ]:
        filenames = transaction.get(filetype, [])
        for filename in filenames:
            dp = filename.split("__")[1].split(".")[0]
            if source == enums.BucketPath.FLAT.value:
                key = (
                    f"{source}/{message['study']}/{message['site']}/{message['study']}__{dp}__{message['site']}__{transaction['version']}/"
                    f"{message['study']}__{dp}__{message['site']}__{suffix}.parquet"
                )
            else:
                key = (
                    f"{source}/{message['study']}/{message['study']}__{dp}/{message['study']}__{dp}__{transaction['version']}/"
                    f"{message['study']}__{dp}__{suffix}.parquet"
                )
            try:
                head = s3_client.head_object(Bucket=os.environ.get("BUCKET_NAME"), Key=key)
            except s3_client.exceptions.ClientError:
                return False, None
            delta = head["LastModified"] - upload_time
            # an average time to process from an upload to all the files being
            # processed is a minute, and since this is largely parallel it shouldn't
            # change by much; we'll set a large buffer just in case, which should
            # still exclude dangling files from error states
            if upload_time > head["LastModified"] or (delta.seconds >= 300 and delta.seconds < 0):
                return False, None
    return True, transaction


def has_new_packages(message, transaction) -> bool:
    db = os.environ.get("GLUE_DB_NAME")
    s3_bucket_name = os.environ.get("BUCKET_NAME")
    tables = awswrangler.athena.read_sql_query(
        (
            f"SELECT table_name FROM information_schema.tables "  # noqa: S608
            f"WHERE table_schema = '{db}' "  # nosec
            f"AND regexp_like(table_name, '^{message['study']}__')"
        ),
        database=db,
        s3_output=f"s3://{s3_bucket_name}/awswrangler",
        workgroup=os.environ.get("WORKGROUP_NAME"),
    )["table_name"].values.tolist()
    for dp_type in ["cube", "annotated_cube", "flat"]:
        filenames = transaction.get(dp_type, [])

        for filename in filenames:
            dp = filename.split("__")[1].split(".")[0]
            if dp_type == "flat":
                name = f"{message['study']}__{dp}__{message['site']}__{transaction['version']}"
            else:
                name = f"{message['study']}__{dp}__{transaction['version']}"
            if name not in tables:
                return True
    return False


def cleanup_transaction(message) -> bool:
    key = f"{enums.BucketPath.META.value}/transactions/{message['site']}__{message['study']}.json"
    try:
        s3_client.head_object(Bucket=os.environ.get("BUCKET_NAME"), Key=key)
    except botocore.exceptions.ClientError:
        return False
    s3_client.delete_object(Bucket=os.environ.get("BUCKET_NAME"), Key=key)
    return True


@decorators.generic_error_handler(msg="Error processing metadata events")
def check_if_complete_handler(event, context):
    del context
    message = json.loads(event["Records"][0]["Sns"]["Message"])
    completed, transaction = check_if_complete(message)
    if not completed:
        return functions.http_response(200, "Processing not completed")
    new = has_new_packages(message, transaction)

    if new:
        attempts = 0
        while attempts < 10:
            crawler = g_client.get_crawler(Name=os.environ.get("GLUE_CRAWLER_NAME"))["Crawler"]
            if crawler["State"] == "READY":
                cleanup_performed = cleanup_transaction(message)
                if not cleanup_performed:
                    return functions.http_response(
                        200, f"Processing request for {message!s} already sent"
                    )
                if "LastCrawl" in crawler and crawler["LastCrawl"][
                    "StartTime"
                ] > datetime.datetime.fromisoformat(event["Records"][0]["Sns"]["Timestamp"]):
                    return functions.http_response(
                        200, f"Recrawl request for {message!s} covered by subsequent request"
                    )
                g_client.start_crawler(Name=os.environ.get("GLUE_CRAWLER_NAME"))
                return functions.http_response(200, f"Crawl for {message!s} initiated")
            attempts += 1
            sleep(60)
        return functions.http_response(500, "Error requesting crawl")  # pragma: no cover
    else:
        cleanup_performed = cleanup_transaction(message)
        if not cleanup_performed:
            return functions.http_response(200, f"Processing request for {message!s} already sent")
        topic_sns_arn = os.environ.get("TOPIC_CACHE_API_ARN")
        sns_client.publish(
            TopicArn=topic_sns_arn,
            Message=message["study"],
            Subject=enums.JsonFilename.DATA_PACKAGES.value,
        )
        return functions.http_response(
            200, f"Crawl for {message!s} not required, directly invoked caching"
        )
