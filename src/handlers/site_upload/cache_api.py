""" Lambda for running and caching query results """

import json
import os

import awswrangler
import boto3

from ..shared.decorators import generic_error_handler
from ..shared.enums import BucketPath, JsonFilename
from ..shared.functions import http_response


def cache_api_data(s3_client, s3_bucket_name: str, db: str, target: str) -> None:
    """Performs caching of API data

    In the future, this will be used for more than one type of data caching.
    """
    if target == JsonFilename.DATA_PACKAGES.value:
        df = awswrangler.athena.read_sql_query(
            (
                f"SELECT table_name FROM information_schema.tables "
                f"WHERE table_schema = '{db}'"  # nosec
            ),
            database=db,
            s3_output=f"s3://{s3_bucket_name}/awswrangler",
            workgroup=os.environ.get("WORKGROUP_NAME"),
        )
    else:
        raise KeyError("Invalid API caching target")
    data_packages = df.iloc[:, 0].to_json(orient="values")
    s3_client.put_object(
        Bucket=s3_bucket_name,
        Key=f"{BucketPath.CACHE.value}/{JsonFilename.DATA_PACKAGES.value}.json",
        Body=json.dumps(data_packages),
    )


@generic_error_handler(msg="Error caching API responses")
def cache_api_handler(event, context):
    """manages event from SNS, executes queries and stashes cache in S#"""
    del context
    s3_bucket_name = os.environ.get("BUCKET_NAME")
    s3_client = boto3.client("s3")
    db = os.environ.get("GLUE_DB_NAME")
    target = event["Records"][0]["Sns"]["Subject"]
    cache_api_data(s3_client, s3_bucket_name, db, target)
    res = http_response(200, "Study period update successful")
    return res
