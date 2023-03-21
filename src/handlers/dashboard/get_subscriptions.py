import os

import awswrangler
import boto3

from src.handlers.shared.decorators import generic_error_handler
from src.handlers.shared.functions import http_response


@generic_error_handler(msg="Error retrieving subscriptions")
def subscriptions_handler(event, context):
    """Retrieves list of subscriptions in Athena DB."""
    del context
    boto3.setup_default_session(region_name="us-east-1")
    df = awswrangler.athena.read_sql_query(
        "show tables",
        database=os.environ.get("DB_NAME"),
        s3_output="s3://cumulus-aggregator-site-counts/awswrangler",
    )
    print(df)
    res = http_response(200, "df")
    return res
