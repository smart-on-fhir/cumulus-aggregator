""" Lambda for performing joins of site count data.
This is intended to provide an implementation of the logic described in docs/api.md
"""
import logging

from typing import List, Dict

import awswrangler
import boto3
import pandas

from src.handlers.dashboard.filter_config import get_filter_string
from src.handlers.site_upload.shared_functions import http_response


def _get_table_name(subscription_id: str) -> str:  # pylint: disable=unused-argument
    """returns the table name associated with a subscription.
    TODO: this is hard coded for now, pending creation of subscription persistence
    """
    return "covid"


def _get_table_cols(table_name: str) -> List:  # pylint: disable=unused-argument
    """returns the columns associated with a table.
    TODO: this is hard coded for now, pending creation of subscription persistence
    """
    return [
        "cnt",
        "covid_icd10",
        "covid_pcr_result",
        "covid_symptom",
        "symptom_icd10_display",
        "variant_era",
        "author_week",
        "gender",
        "age_group",
    ]


def _build_query(query_params: Dict, filters: List, path_params: Dict) -> str:
    """Creates a query from the dashboard API spec"""
    table = _get_table_name(path_params["subscription_id"])
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
        coalesce_str = f"WHERE COALESCE ({','.join(columns)}) = '' AND"
    else:
        coalesce_str = "WHERE"
    query_str = (
        f"SELECT {select_str} FROM {table} "
        f"{coalesce_str} "
        f"{query_params['column']} != '' {filter_str} "
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


def chart_data_handler(event, context):  # pylint: disable=unused-argument
    """manages event from dashboard api call and retrieves data"""
    query_params = event["queryStringParameters"]
    filters = event["multiValueQueryStringParameters"].get("filter", [])
    path_params = event["pathParameters"]
    query = _build_query(query_params, filters, path_params)
    try:
        boto3.setup_default_session(region_name="us-east-1")
        df = awswrangler.athena.read_sql_query(
            query,
            database="cumulus-aggregator-db",
            s3_output="s3://cumulus-aggregator-site-counts/awswrangler",
        )
        res = _format_payload(df, query_params, filters)
        res = http_response(200, res)
        return res
    except Exception as e:  # pylint: disable=broad-except
        logging.error("Error retrieving chart data: %s", str(e))
        res = http_response(500, "Error retrieving chart data")
        return res