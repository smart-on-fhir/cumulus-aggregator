import os

import awswrangler
import boto3

from src.handlers.shared.decorators import generic_error_handler
from src.handlers.shared.functions import http_response


# @generic_error_handler(msg="Error retrieving subscriptions")
def subscriptions_handler(event, context):
    """Retrieves list of subscriptions in Athena DB."""
    del context
    boto3.setup_default_session(region_name="us-east-1")
    db = os.environ.get("GLUE_DB_NAME")
    df = awswrangler.athena.read_sql_query(
        (
            f"SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{db}'"
        ),
        database=db,
        s3_output=f"s3://{os.environ.get('BUCKET_NAME')}/awswrangler",
        workgroup=os.environ.get("WORKGROUP_NAME"),
    )
    res = http_response(200, df.iloc[:, 0].to_json(orient="values"))
    return res
