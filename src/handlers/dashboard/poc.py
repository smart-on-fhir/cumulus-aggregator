""" Lambda for performing joins of site count data """
import csv
import logging

import awswrangler
import boto3
import pandas

from src.handlers.dashboard.filter_config import get_filter_string
from src.handlers.site_upload.enums import BucketPath
from src.handlers.site_upload.shared_functions import http_response


def _get_table_name(subscription_id):
    """returns the table name associated with a subscription.
    TODO: this is hard coded for now, pending creation of subscription persistence
    """
    return "aggregates"


def _get_table_cols(table_name):
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


def _build_query(query_params, filters, path_params):
    """Creates a query from the dashboard API spec"""
    table = _get_table_name(path_params["subscription_id"])
    columns = _get_table_cols(table)
    filter_str = get_filter_string(filters)
    count_col = [c for c in columns if c.startswith("cnt")][0]
    columns.remove(count_col)
    select_str = f"{query_params['column']}, sum({count_col}) as {count_col}"
    group_str = f"{query_params['column']}"
    columns.remove(query_params["column"])
    if "stratifier" in query_params.keys():
        select_str = f"{query_params['stratifier']}, {select_str}"
        group_str = f"{query_params['stratifier']}, {group_str}"
        columns.remove(query_params["stratifier"])
    query_str = (
        f"SELECT {select_str} FROM {table} "
        f"WHERE COALESCE ({','.join(columns)}) = '' "
        f"GROUP BY {group_str}"
    )
    return query_str


def _format_payload(df, query_params, filters):
    print(query_params)
    if "stratifier" in query_params.keys():
        df = df.groupby(query_params["stratifier"], group_keys=True).apply(lambda x: x)
    return df.to_string()


def lambda_handler(event, context):
    query_params = event["queryStringParameters"]
    filters = event["multiValueQueryStringParameters"].get("filter", [])
    path_params = event["pathParameters"]
    query = _build_query(query_params, filters, path_params)
    boto3.setup_default_session(region_name="us-east-1")
    df = awswrangler.athena.read_sql_query(
        query,
        database="cumulus-aggregator",
        s3_output="s3://cumulus-aggregator-site-counts/awswrangler",
    )
    res = http_response(200, _format_payload(df, query_params, filters))
    return res
