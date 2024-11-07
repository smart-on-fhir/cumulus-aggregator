"""Lambda for performing joins of site count data.
This is intended to provide an implementation of the logic described in docs/api.md
"""

import logging
import os
import pathlib

import awswrangler
import boto3
import jinja2
import pandas
from shared import decorators, enums, errors, functions

log_level = os.environ.get("LAMBDA_LOG_LEVEL", "INFO")
logger = logging.getLogger()
logger.setLevel(log_level)

# These constants are specified by the dashboard API and should not be changed
# unless a corresponding change is made on that side.
INLINE_FILTERS = (
    # string filters
    "strEq",
    "strContains",
    "strStartsWith",
    "strEndsWith",
    "matches",
    "strEqCI",
    "strContainsCI",
    "strStartsWithCI",
    "strEndsWithCI",
    "strMatchesCI",
    "strNotEq",
    "strNotContains",
    "strNotStartsWith",
    "strNotEndsWith",
    "notMatches",
    "strNotEqCI",
    "strNotContainsCI",
    "strNotStartsWithCI",
    "strNotEndsWithCI",
    "notMatchesCI",
    # date filters
    "sameDay",
    "sameWeek",
    "sameMonth",
    "sameYear",
    "sameDayOrBefore",
    "sameWeekOrBefore",
    "sameMonthOrBefore",
    "sameYearOrBefore",
    "sameDayOrAfter",
    "sameWeekOrAfter",
    "sameYearOrAfter",
    "beforeDay",
    "beforeWeek",
    "beforeMonth",
    "beforeYear",
    "afterDay",
    "afterMonth",
    "afterYear",
    # Boolean filters (one param only)
    "isTrue",
    "isNotTrue",
    "isFalse",
    "isNotFalse",
    # null filters (one param only)
    "isNull",
    "isNotNull",
    # numeric filters
    "eq",
    "ne",
    "gt",
    "gte",
    "lt",
    "lte",
)

# This set of filters (i.e. all the string/null filters) are suitable to be run against
# table slices where we are looking at explict source nulls without having to worry
# about type issues
NONE_FILTERS = (
    "strEq",
    "strContains",
    "strStartsWith",
    "strEndsWith",
    "matches",
    "strEqCI",
    "strContainsCI",
    "strStartsWithCI",
    "strEndsWithCI",
    "strMatchesCI",
    "strNotEq",
    "strNotContains",
    "strNotStartsWith",
    "strNotEndsWith",
    "notMatches",
    "strNotEqCI",
    "strNotContainsCI",
    "strNotStartsWithCI",
    "strNotEndsWithCI",
    "notMatchesCI",
    "isNull",
    "isNotNull",
)


def _get_table_cols(dp_id: str) -> list:
    """Returns the columns associated with a table.

    Since running an athena query takes a decent amount of time due to queueing
    a query with the execution engine, and we already have this data at the top
    of a CSV, we're getting table cols directly from S3 for speed reasons.
    """

    s3_bucket_name = os.environ.get("BUCKET_NAME")
    study, name, version = dp_id.split("__")
    prefix = f"{enums.BucketPath.CSVAGGREGATE.value}/{study}/{study}__{name}"
    if version is None:
        version = functions.get_latest_data_package_version(s3_bucket_name, prefix)
    s3_key = f"{prefix}/{version}/{study}__{name}__aggregate.csv"
    s3_client = boto3.client("s3")
    try:
        s3_iter = s3_client.get_object(
            Bucket=s3_bucket_name,
            Key=s3_key,
        )["Body"].iter_lines()
        return next(s3_iter).decode().split(",")
    except Exception:
        raise errors.AggregatorS3Error


def _build_query(query_params: dict, filter_groups: list, path_params: dict) -> str:
    """Creates a query from the dashboard API spec
    :arg queryparams: All arguments passed to the endpoint
    :arg filter_groups: Filter params used to generate filtering logic.
        These should look like ['col:arg:optionalvalue,...','']
    :path path_params: URL specific arguments, as a convenience
    """
    dp_id = path_params["data_package_id"]
    columns = _get_table_cols(dp_id)

    inline_configs = []
    none_configs = []
    for filter_group in filter_groups:
        # in this block we are trying to do the following things:
        # - create a config for where the selected column is not `cumulus__none`
        # - create a config for where the selected column is `cumulus__none`, removing
        #   filters that would cause type cast errors
        filter_group = filter_group.split(",")
        config_params = []
        none_params = []
        for filter_config in filter_group:
            filter_config = filter_config.split(":")
            if filter_config[1] in INLINE_FILTERS:
                params = {"data": filter_config[0], "filter_type": filter_config[1]}
                if len(filter_config) == 3:
                    params["bound"] = filter_config[2]
                config_params.append(params)
                if filter_config[1] in NONE_FILTERS or filter_config[0] != query_params["column"]:
                    if params.get("bound", "").casefold() == "none":
                        params["bound"] = "cumulus__none"
                    none_params.append(params)
            else:
                raise errors.AggregatorFilterError(
                    f"Invalid filter type {filter_config[1]} requested."
                )

        inline_configs.append(config_params)
        none_configs.append(none_params)
    count_col = next(c for c in columns if c.startswith("cnt"))
    columns.remove(count_col)
    # these 'if in' checks is meant to handle the case where the selected column is also
    # present in the filter logic and has already been removed
    if query_params["column"] in columns:
        columns.remove(query_params["column"])
    if query_params.get("stratifier") in columns:
        columns.remove(query_params["stratifier"])
    with open(pathlib.Path(__file__).parent / "templates/get_chart_data.sql.jinja") as file:
        template = file.read()
        loader = jinja2.FileSystemLoader(pathlib.Path(__file__).parent / "templates/")
        env = jinja2.Environment(loader=loader).from_string(template)  # noqa: S701
        query_str = env.render(
            data_column=query_params["column"],
            stratifier_column=query_params.get("stratifier", None),
            count_columns=[count_col],
            schema=os.environ.get("GLUE_DB_NAME"),
            data_package_id=path_params["data_package_id"],
            coalesce_columns=columns,
            inline_configs=inline_configs,
            none_configs=none_configs,
        )
    return query_str, count_col


def _format_payload(
    df: pandas.DataFrame, query_params: dict, filter_groups: list, count_col: str
) -> dict:
    """Coerces query results into the return format defined by the dashboard

    :arg queryparams: All arguments passed to the endpoint
    :arg filter_groups: Filter params used to generate filtering logic.
        These should look like ['col:arg:optionalvalue,...','']
    :path count_col: column to use as the primary count column"""
    payload = {}
    payload["column"] = query_params["column"]
    payload["filters"] = filter_groups
    payload["rowCount"] = int(df.shape[0])
    payload["totalCount"] = int(df["cnt"].sum())
    if "stratifier" in query_params.keys():
        payload["stratifier"] = query_params["stratifier"]
        counts = {}
        for unique_val in df[query_params["column"]].unique():
            column_mask = df[query_params["column"]] == unique_val
            df_slice = df[column_mask]
            df_slice = df_slice.drop(columns=[query_params["stratifier"], query_params["column"]])
            counts[unique_val] = int(df_slice[count_col].sum())
        payload["counts"] = counts
        data = []
        for unique_strat in df[query_params["stratifier"]].unique():
            strat_mask = df[query_params["stratifier"]] == unique_strat
            df_slice = df[strat_mask]
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
    filter_groups = event["multiValueQueryStringParameters"].get("filter", [])
    if "filter" in query_params and filter_groups == []:
        filter_groups = [query_params["filter"]]
    path_params = event["pathParameters"]
    boto3.setup_default_session(region_name="us-east-1")
    try:
        query, count_col = _build_query(query_params, filter_groups, path_params)
        df = awswrangler.athena.read_sql_query(
            query,
            database=os.environ.get("GLUE_DB_NAME"),
            s3_output=f"s3://{os.environ.get('BUCKET_NAME')}/awswrangler",
            workgroup=os.environ.get("WORKGROUP_NAME"),
        )
        res = _format_payload(df, query_params, filter_groups, count_col)
        res = functions.http_response(200, res)
    except errors.AggregatorS3Error:
        # while the API is publicly accessible, we've been asked to not pass
        # helpful error messages back. revisit when dashboard is in AWS.
        res = functions.http_response(
            404, f"Aggregate for {path_params['data_package_id']} not found"
        )

    return res
