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
    "matchesCI",
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
    "sameMonthOrAfter",
    "sameYearOrAfter",
    "beforeDay",
    "beforeWeek",
    "beforeMonth",
    "beforeYear",
    "afterDay",
    "afterWeek",
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
    "isNone",  # we treat this as an alias for "strEqCI"
    "isNotNone",  # we treat this as an alias for "strEqCI"
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
    "matchesCI",
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
    "isNotNull",
    "isNull",
)


def _get_table_cols(dp_id: str) -> list:
    """Returns the columns associated with a table.

    Since running an athena query takes a decent amount of time due to queueing
    a query with the execution engine, and we already have this data in the
    column_types metadata, we're getting table cols directly from S3 for speed reasons.

    TODO: Read from column_types instead
    """

    s3_bucket_name = os.environ.get("BUCKET_NAME")
    s3_client = boto3.client("s3")
    column_types = functions.read_metadata(
        s3_client, s3_bucket_name, meta_type=enums.JsonFilename.COLUMN_TYPES.value
    )
    for study in column_types.keys():
        if study in dp_id:
            for data_package in column_types[study].keys():
                if dp_id in column_types[study][data_package].keys():
                    details = column_types[study][data_package][dp_id]
                    return list(details["columns"].keys())
    raise errors.AggregatorS3Error


def _build_query(query_params: dict, filter_groups: list, path_params: dict) -> str:
    """Creates a query from the dashboard API spec
    :arg queryparams: All arguments passed to the endpoint
    :arg filter_groups: Filter params used to generate filtering logic.
        These should look like ['col:arg:optionalvalue,...','']
    :path path_params: URL specific arguments, as a convenience
    """
    ungraphed_stratifier = False
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
                    if params.get("bound", "").casefold() == "none" or filter_config[1] in [
                        "isNone",
                        "isNotNone",
                    ]:
                        params["bound"] = "cumulus__none"
                    none_params.append(params)
                if filter_config[0] in columns:
                    columns.remove(filter_config[0])
            else:
                raise errors.AggregatorFilterError(
                    f"Invalid filter type {filter_config[1]} requested."
                )
        if config_params != []:
            inline_configs.append(config_params)
        if none_params != []:
            none_configs.append(none_params)
    count_col = next((c for c in columns if c.startswith("cnt")), False)
    if count_col:
        columns.remove(count_col)
    else:  # pragma: no cover
        count_col = "cnt"
    # these 'if in' checks is meant to handle the case where the selected column is also
    # present in the filter logic and has already been removed
    if query_params.get("column") in columns:
        columns.remove(query_params["column"])
    if query_params.get("stratifier") in columns:
        columns.remove(query_params["stratifier"])
    else:
        ungraphed_stratifier = True

    with open(pathlib.Path(__file__).parent / "templates/get_chart_data.sql.jinja") as file:
        template = file.read()
        loader = jinja2.FileSystemLoader(pathlib.Path(__file__).parent / "templates/")
        env = jinja2.Environment(loader=loader).from_string(template)  # noqa: S701
        query_str = env.render(
            data_column=query_params.get("column"),
            stratifier_column=query_params.get("stratifier", None),
            count_columns=[count_col],
            schema=os.environ.get("GLUE_DB_NAME"),
            data_package_id=path_params["data_package_id"],
            coalesce_columns=columns,
            inline_configs=inline_configs,
            none_configs=none_configs,
            ungraphed_stratifier=ungraphed_stratifier,
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

        counts = df.groupby(query_params["column"]).agg({count_col: ["sum"]})
        # If the column index is a numpy type, json serialization breaks,
        # so let's convert it to a python primitive
        counts.index = counts.index.astype(str)
        payload["counts"] = counts.to_dict()[(count_col, "sum")]

        data = []

        # We are combining two values into a pandas index. This means that we've got
        # two different ways the data is represented:
        # 1. in the dataframe itself, where pandas does some intelligent things
        #   about how the data is represented (for example, truncating python
        #   datetimes to dates if they are all have a timestamp of zero)
        # 2. the string representation of the underlying numpy data type converted
        #   to a string, which in this case is the full datetime representation
        # So, we're going to adjust the values in the multiindex to match the
        # values in the dataframe itself, fixing the following kinds of issues:
        # - dates getting encoded as timestamps
        # - numerics becoming objects, when we expect them to be string-like
        #   after adding `cumulus_none`
        # - Booleans being converted to strings after adding `cumulus_none`
        for column in [query_params["column"], query_params["stratifier"]]:
            if pandas.api.types.is_datetime64_ns_dtype(df.dtypes[column]):
                df[column] = df[column].dt.strftime("%Y-%m-%d")
            elif pandas.api.types.is_object_dtype(
                df.dtypes[column]
            ) or pandas.api.types.is_bool_dtype(df.dtypes[column]):
                df[column] = df[column].astype("string")

        stratifiers = df[query_params["stratifier"]].unique()
        df = df.groupby([query_params["stratifier"], query_params["column"]]).agg(
            {count_col: ["sum"]}
        )
        for stratifier in stratifiers:
            # We have a multiindex dataframe here, so we're going to get the slice
            # corresponding to the individual stratifier. The second part of the index
            # contains all our data labels. We'll then join the index and the values
            # into a list of two element lists for the dashboard payload
            df_slice = df.loc[stratifier, :]
            df_slice = [df_slice.columns.tolist()], *df_slice.reset_index().values.tolist()

            data.append(
                {
                    "stratifier": stratifier,
                    # The first element here is the columns of the dataframe, which we don't need
                    "rows": list(df_slice[1:]),
                }
            )
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
        res = functions.http_response(200, res, alt_log="Chart data succesfully retrieved")
    except errors.AggregatorS3Error:  # pragma: no cover
        # while the API is publicly accessible, we've been asked to not pass
        # helpful error messages back. revisit when dashboard is in AWS.
        res = functions.http_response(
            404, f"Aggregate for {path_params['data_package_id']} not found"
        )

    return res
