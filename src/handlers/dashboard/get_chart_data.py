"""Lambda for performing joins of site count data.
This is intended to provide an implementation of the logic described in docs/api.md
"""

import logging
import os

import awswrangler
import boto3
import pandas

from src.handlers.dashboard import filter_config
from src.handlers.shared import decorators, enums, errors, functions

log_level = os.environ.get("LAMBDA_LOG_LEVEL", "INFO")
logger = logging.getLogger()
logger.setLevel(log_level)


def _get_table_cols(dp_id: str, version: str | None = None) -> list:
    """Returns the columns associated with a table.

    Since running an athena query takes a decent amount of time due to queueing
    a query with the execution engine, and we already have this data at the top
    of a CSV, we're getting table cols directly from S3 for speed reasons.
    """

    s3_bucket_name = os.environ.get("BUCKET_NAME")
    dp_name = dp_id.rsplit("__", 1)[0]
    prefix = f"{enums.BucketPath.CSVAGGREGATE.value}/{dp_id.split('__')[0]}/{dp_name}"
    if version is None:
        version = functions.get_latest_data_package_version(s3_bucket_name, prefix)
    s3_key = f"{prefix}/{version}/{dp_name}__aggregate.csv"
    s3_client = boto3.client("s3")
    try:
        s3_iter = s3_client.get_object(
            Bucket=s3_bucket_name,
            Key=s3_key,
        )["Body"].iter_lines()
        return next(s3_iter).decode().split(",")
    except Exception:
        raise errors.AggregatorS3Error


def _build_query(query_params: dict, filters: list, path_params: dict) -> str:
    """Creates a query from the dashboard API spec"""
    dp_id = path_params["data_package_id"]
    columns = _get_table_cols(dp_id)
    filter_str = filter_config.get_filter_string(filters)
    if filter_str != "":
        filter_str = f"AND {filter_str} "
    count_col = next(c for c in columns if c.startswith("cnt"))
    columns.remove(count_col)
    select_str = f"{query_params['column']}, sum({count_col}) as {count_col}"
    strat_str = ""
    group_str = f"{query_params['column']}"
    # the 'if in' check is meant to handle the case where the selected column is also
    # present in the filter logic and has already been removed
    if query_params["column"] in columns:
        columns.remove(query_params["column"])
    if "stratifier" in query_params.keys():
        select_str = f"{query_params['stratifier']}, {select_str}"
        group_str = f"{query_params['stratifier']}, {group_str}"
        columns.remove(query_params["stratifier"])
        strat_str = f'AND {query_params["stratifier"]} IS NOT NULL '
    if len(columns) > 0:
        coalesce_str = (
            f"WHERE COALESCE (cast({' AS VARCHAR), cast('.join(columns)} AS VARCHAR)) "
            "IS NOT NULL AND "
        )
    else:
        coalesce_str = "WHERE "
    query_str = (
        f"SELECT {select_str} "  # nosec  # noqa: S608
        f"FROM \"{os.environ.get('GLUE_DB_NAME')}\".\"{dp_id}\" "
        f"{coalesce_str}"
        f"{query_params['column']} IS NOT NULL "
        f"{filter_str}"
        f"{strat_str}"
        f"GROUP BY {group_str} "
    )
    if "stratifier" in query_params.keys():
        query_str += f"ORDER BY {query_params['stratifier']}, {query_params['column']}"
    else:
        query_str += f"ORDER BY {query_params['column']}"
    return query_str, count_col


def _format_payload(
    df: pandas.DataFrame, query_params: dict, filters: list, count_col: str
) -> dict:
    """Coerces query results into the return format defined by the dashboard"""
    payload = {}
    payload["column"] = query_params["column"]
    payload["filters"] = filters
    payload["rowCount"] = int(df.shape[0])
    payload["totalCount"] = int(df["cnt"].sum())
    if "stratifier" in query_params.keys():
        payload["stratifier"] = query_params["stratifier"]
        counts = {}
        for unique_val in df[query_params["column"]]:
            df_slice = df[df[query_params["column"]] == unique_val]
            df_slice = df_slice.drop(columns=[query_params["stratifier"], query_params["column"]])
            counts[unique_val] = int(df_slice[count_col].sum())
        payload["counts"] = counts
        data = []
        for unique_strat in df[query_params["stratifier"]].unique():
            df_slice = df[df[query_params["stratifier"]] == unique_strat]
            df_slice = df_slice.drop(columns=[query_params["stratifier"]])
            rows = df_slice.values.tolist()
            data.append({"stratifier": unique_strat, "rows": rows})
        payload["data"] = data

    else:
        rows = df.values.tolist()
        payload["data"] = [{"rows": rows}]

    return payload


@decorators.generic_error_handler(msg="Error retrieving chart data")
def chart_data_handler(event, context):
    """manages event from dashboard api call and retrieves data"""
    del context
    query_params = event["queryStringParameters"]
    filters = event["multiValueQueryStringParameters"].get("filter", [])
    if "filter" in query_params and filters == []:
        filters = [query_params["filter"]]
    path_params = event["pathParameters"]
    boto3.setup_default_session(region_name="us-east-1")
    try:
        query, count_col = _build_query(query_params, filters, path_params)
        df = awswrangler.athena.read_sql_query(
            query,
            database=os.environ.get("GLUE_DB_NAME"),
            s3_output=f"s3://{os.environ.get('BUCKET_NAME')}/awswrangler",
            workgroup=os.environ.get("WORKGROUP_NAME"),
        )
        res = _format_payload(df, query_params, filters, count_col)
        res = functions.http_response(200, res)
    except errors.AggregatorS3Error:
        # while the API is publicly accessible, we've been asked to not pass
        # helpful error messages back. revisit when dashboard is in AWS.
        res = functions.http_response(
            404, f"Aggregate for {path_params['data_package_id']} not found"
        )

    return res
