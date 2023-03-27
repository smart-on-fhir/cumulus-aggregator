""" Lambda for performing joins of site count data.
This is intended to provide an implementation of the logic described in docs/api.md
"""
import os

from typing import List, Dict

import awswrangler
import boto3
import pandas

from src.handlers.dashboard.filter_config import get_filter_string
from src.handlers.shared.decorators import generic_error_handler
from src.handlers.shared.enums import BucketPath
from src.handlers.shared.functions import http_response


def _get_table_cols(table_name: str) -> List:
    """Returns the columns associated with a table.

    Since running an athena query takes a decent amount of time due to queueing
    a query with the execution engine, and we already have this data at the top
    of a CSV, we're getting table cols directly from S3 for speed reasons.
    """
    s3_bucket_name = os.environ.get("BUCKET_NAME")
    s3_key = (
        f"{BucketPath.CSVAGGREGATE.value}/{table_name.split('__')[0]}"
        f"/{table_name}/{table_name}__aggregate.csv"
    )
    s3_client = boto3.client("s3")
    s3_iter = s3_client.get_object(
        Bucket=s3_bucket_name, Key=s3_key  # type: ignore[arg-type]
    )["Body"].iter_lines()
    return next(s3_iter).decode().split(",")


def _build_query(query_params: Dict, filters: List, path_params: Dict) -> str:
    """Creates a query from the dashboard API spec"""
    table = path_params["subscription_name"]
    columns = _get_table_cols(table)
    filter_str = get_filter_string(filters)
    if filter_str != "":
        filter_str = f"AND {filter_str}"
    count_col = [c for c in columns if c.startswith("cnt")][0]
    columns.remove(count_col)
    select_str = f"{query_params['column']}, sum({count_col}) as {count_col}"
    group_str = f"{query_params['column']}"
    # the 'if in' check is meant to handle the case where the selected column is also
    # present in the filter logic and has already been removed
    if query_params["column"] in columns:
        columns.remove(query_params["column"])
    if "stratifier" in query_params.keys():
        select_str = f"{query_params['stratifier']}, {select_str}"
        group_str = f"{query_params['stratifier']}, {group_str}"
        columns.remove(query_params["stratifier"])
    if len(columns) > 0:
        coalesce_str = f"WHERE COALESCE ({','.join(columns)}) IS NOT Null AND"
    else:
        coalesce_str = "WHERE"
    query_str = (
        f"SELECT {select_str} "  # nosec
        f"FROM \"{os.environ.get('GLUE_DB_NAME')}\".\"{table}\" "
        f"{coalesce_str} "
        f"{query_params['column']} IS NOT Null {filter_str} "
        f"GROUP BY {group_str}"
    )
    return query_str


def _format_payload(df: pandas.DataFrame, query_params: Dict, filters: List) -> Dict:
    """Coerces query results into the return format defined by the dashboard"""
    payload = {}
    payload["column"] = query_params["column"]
    payload["filters"] = filters
    payload["rowCount"] = int(df.shape[0])
    payload["totalCount"] = int(df["cnt"].sum())
    if "stratifier" in query_params.keys():
        payload["stratifier"] = query_params["stratifier"]
        data = []
        for unique_val in df[query_params["stratifier"]]:
            df_slice = df[df[query_params["stratifier"]] == unique_val]
            df_slice = df_slice.drop(columns=[query_params["stratifier"]])
            rows = df_slice.values.tolist()
            data.append({"stratifier": unique_val, "rows": rows})
        payload["data"] = data
    else:
        rows = df.values.tolist()
        payload["data"] = [{"rows": rows}]

    return payload


@generic_error_handler(msg="Error retrieving chart data")
def chart_data_handler(event, context):
    """manages event from dashboard api call and retrieves data"""
    del context
    query_params = event["queryStringParameters"]
    filters = event["multiValueQueryStringParameters"].get("filter", [])
    path_params = event["pathParameters"]
    boto3.setup_default_session(region_name="us-east-1")
    query = _build_query(query_params, filters, path_params)
    df = awswrangler.athena.read_sql_query(
        query,
        database=os.environ.get("GLUE_DB_NAME"),
        s3_output=f"s3://{os.environ.get('BUCKET_NAME')}/awswrangler",
        workgroup=os.environ.get("WORKGROUP_NAME"),
    )
    res = _format_payload(df, query_params, filters)
    res = http_response(200, res)
    return res
