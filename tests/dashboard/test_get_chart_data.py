import json
from unittest import mock

import botocore
import pandas
import pytest

from src.dashboard.get_chart_data import get_chart_data
from tests.mock_utils import (
    EXISTING_DATA_P,
    EXISTING_STUDY,
    EXISTING_VERSION,
    TEST_GLUE_DB,
)


def mock_data_frame(query_params, filter_groups):
    df = pandas.read_csv("tests/test_data/mock_cube_col_types.csv", na_filter=False)
    expected_columns = {query_params["column"], "cnt"}
    if "stratifier" in query_params:
        expected_columns.add(query_params["stratifier"])
    if filter_groups != []:
        # for purposes of mocking, we are assuming filters are of an equality comparison type
        for filter_param in filter_groups:
            filter_param = filter_param.split(":")
            df = df[df[filter_param[0]] == filter_param[2]]
    for column in df.columns:
        if column not in expected_columns:
            df = df.loc[df[column].notnull()]
            df = df.drop([column], axis=1)
    return df


@pytest.mark.parametrize(
    "query_params,filter_groups,expected_payload",
    [
        (
            {"column": "nato"},
            [],
            json.load(open("tests/test_data/cube_response.json")),
        ),
        (
            {"column": "nato", "stratifier": "bool"},
            [],
            json.load(open("tests/test_data/cube_response_stratified.json")),
        ),
        (
            {"column": "nato"},
            ["greek:strEq:alpha"],
            json.load(open("tests/test_data/cube_response_filtered.json")),
        ),
        (
            {"column": "nato", "stratifier": "bool"},
            ["greek:strEq:alpha"],
            json.load(open("tests/test_data/cube_response_filtered_stratified.json")),
        ),
    ],
)
def test_format_payload(query_params, filter_groups, expected_payload):
    df = mock_data_frame(query_params, filter_groups)
    payload = get_chart_data._format_payload(df, query_params, filter_groups, "cnt")
    print(payload)
    assert payload == expected_payload


def test_format_payload_date_coercion():
    df = pandas.DataFrame(
        {
            "a": ["C", "D"],
            "b": [
                pandas.to_datetime("2020-01-01 00:00:00"),
                pandas.to_datetime("2020-01-01 00:00:00"),
            ],
            "cnt": [20, 15],
        }
    )
    payload = get_chart_data._format_payload(df, {"column": "a", "stratifier": "b"}, [], "cnt")
    assert payload["data"] == [{"stratifier": "2020-01-01", "rows": [["C", 20], ["D", 15]]}]


def test_get_data_cols(mock_bucket):
    table_id = f"{EXISTING_STUDY}__{EXISTING_DATA_P}__{EXISTING_VERSION}"
    res = get_chart_data._get_table_cols(table_id)
    cols = pandas.read_csv("./tests/test_data/count_synthea_patient_agg.csv").columns
    assert res == list(cols)


@mock.patch("botocore.client")
def test_get_data_cols_err(mock_client):
    mock_clientobj = mock_client.ClientCreator.return_value.create_client.return_value
    mock_clientobj.get_object.side_effect = [
        None,
        botocore.exceptions.ClientError({}, {}),
    ]
    with pytest.raises(Exception):
        table_id = f"{EXISTING_STUDY}__{EXISTING_DATA_P}__{EXISTING_VERSION}"
        get_chart_data._get_table_cols(table_id)


@pytest.mark.parametrize(
    "event,expected",
    [
        (
            {
                "queryStringParameters": {"column": "nato"},
                "multiValueQueryStringParameters": {},
                "pathParameters": {"data_package_id": "test__cube__001"},
            },
            {
                "column": "nato",
                "filters": [],
                "rowCount": 2,
                "totalCount": 60,
                "data": [{"rows": [["alfa", 50.0], ["bravo", 10.0]]}],
            },
        ),
        (
            {
                "queryStringParameters": {"column": "nato", "filter": "greek:strEq:alpha"},
                "multiValueQueryStringParameters": {},
                "pathParameters": {"data_package_id": "test__cube__001"},
            },
            {
                "column": "nato",
                "filters": ["greek:strEq:alpha"],
                "rowCount": 2,
                "totalCount": 50,
                "data": [{"rows": [["alfa", 40.0], ["bravo", 10.0]]}],
            },
        ),
        (
            {
                "queryStringParameters": {"column": "nato", "stratifier": "bool"},
                "multiValueQueryStringParameters": {},
                "pathParameters": {"data_package_id": "test__cube__001"},
            },
            {
                "column": "nato",
                "filters": [],
                "rowCount": 3,
                "totalCount": 60,
                "stratifier": "bool",
                "counts": {"alfa": 50, "bravo": 10},
                "data": [
                    {"stratifier": "False", "rows": [["alfa", 40], ["bravo", 10]]},
                    {"stratifier": "True", "rows": [["alfa", 10]]},
                ],
            },
        ),
        (
            {
                "queryStringParameters": {"column": "nato", "stratifier": "bool"},
                "multiValueQueryStringParameters": {"filter": ["greek:strEq:alpha"]},
                "pathParameters": {"data_package_id": "test__cube__001"},
            },
            {
                "column": "nato",
                "filters": ["greek:strEq:alpha"],
                "rowCount": 3,
                "totalCount": 50,
                "stratifier": "bool",
                "counts": {"alfa": 40.0, "bravo": 10.0},
                "data": [
                    {"stratifier": "False", "rows": [["alfa", 30.0], ["bravo", 10.0]]},
                    {"stratifier": "True", "rows": [["alfa", 10.0]]},
                ],
            },
        ),
        (
            {
                "queryStringParameters": {"column": "nato", "stratifier": "bool"},
                "multiValueQueryStringParameters": {"filter": ["greek:strEq:alpha,numeric:eq:1.1"]},
                "pathParameters": {"data_package_id": "test__cube__001"},
            },
            {
                "column": "nato",
                "filters": ["greek:strEq:alpha,numeric:eq:1.1"],
                "rowCount": 3,
                "totalCount": 40,
                "stratifier": "bool",
                "counts": {"alfa": 30.0, "bravo": 10.0},
                "data": [
                    {"stratifier": "False", "rows": [["alfa", 20.0], ["bravo", 10.0]]},
                    {"stratifier": "True", "rows": [["alfa", 10.0]]},
                ],
            },
        ),
        (
            {
                "queryStringParameters": {"column": "nato", "stratifier": "bool"},
                "multiValueQueryStringParameters": {
                    "filter": ["greek:strEq:alpha", "numeric:eq:1.1"]
                },
                "pathParameters": {"data_package_id": "test__cube__001"},
            },
            {
                "column": "nato",
                "filters": ["greek:strEq:alpha", "numeric:eq:1.1"],
                "rowCount": 3,
                "totalCount": 160,
                "stratifier": "bool",
                "counts": {"alfa": 130.0, "bravo": 30.0},
                "data": [
                    {"stratifier": "False", "rows": [["alfa", 100.0], ["bravo", 30.0]]},
                    {"stratifier": "True", "rows": [["alfa", 30.0]]},
                ],
            },
        ),
        # `cumulus__none` filtering cases
        (
            {
                "queryStringParameters": {"column": "greek"},
                "multiValueQueryStringParameters": {"filter": ["greek:isNotNone"]},
                "pathParameters": {"data_package_id": "test__cube__001"},
            },
            {
                "column": "greek",
                "filters": ["greek:isNotNone"],
                "rowCount": 2,
                "totalCount": 60,
                "data": [{"rows": [["alpha", 50], ["beta", 10]]}],
            },
        ),
        (
            {
                "queryStringParameters": {"column": "greek"},
                "multiValueQueryStringParameters": {"filter": ["greek:isNone"]},
                "pathParameters": {"data_package_id": "test__cube__001"},
            },
            {
                "column": "greek",
                "filters": ["greek:isNone"],
                "rowCount": 1,
                "totalCount": 5,
                "data": [{"rows": [["cumulus__none", 5]]}],
            },
        ),
        (
            {
                "queryStringParameters": {"column": "nato", "stratifier": "greek"},
                "multiValueQueryStringParameters": {"filter": ["greek:isNotNone"]},
                "pathParameters": {"data_package_id": "test__cube__001"},
            },
            {
                "column": "nato",
                "filters": ["greek:isNotNone"],
                "rowCount": 3,
                "totalCount": 60,
                "stratifier": "greek",
                "counts": {"alfa": 50, "bravo": 10},
                "data": [
                    {"stratifier": "alpha", "rows": [["alfa", 40], ["bravo", 10]]},
                    {"stratifier": "beta", "rows": [["alfa", 10]]},
                ],
            },
        ),
        (
            {
                "queryStringParameters": {"column": "nato", "stratifier": "greek"},
                "multiValueQueryStringParameters": {"filter": ["greek:isNone"]},
                "pathParameters": {"data_package_id": "test__cube__001"},
            },
            {
                "column": "nato",
                "filters": ["greek:isNone"],
                "rowCount": 1,
                "totalCount": 5,
                "stratifier": "greek",
                "counts": {"alfa": 5},
                "data": [{"stratifier": "cumulus__none", "rows": [["alfa", 5]]}],
            },
        ),
    ],
)
@mock.patch("src.dashboard.get_chart_data.get_chart_data._get_table_cols")
@mock.patch(
    "awswrangler.athena",
)
def test_handler(mock_athena, mock_get_cols, mock_db, mock_bucket, event, expected):
    file = "./tests/test_data/mock_cube_col_types.parquet"
    mock_db.execute(f'CREATE TABLE test__cube__001 AS SELECT * FROM read_parquet("{file}")')

    def mock_read(query, database, s3_output, workgroup):
        return mock_db.execute(query.replace(TEST_GLUE_DB, "main")).df()

    mock_athena.read_sql_query = mock_read
    mock_get_cols.return_value = list(pandas.read_parquet(file).columns)
    res = get_chart_data.chart_data_handler(event, {})
    print(res)
    assert json.loads(res["body"]) == expected


def mock_get_table_cols_results(name):
    return ["cnt", "nato", "greek", "numeric", "timestamp", "bool"]


@pytest.mark.parametrize(
    "query_params,filter_groups,expected",
    [
        # filtering on display column
        ({"column": "nato"}, ["nato:strEq:alfa"], [("alfa", 50)]),
        # General check on joins with non-included columns
        ({"column": "nato"}, ["nato:strEq:alfa,greek:strEq:alpha"], [("alfa", 40)]),
        ({"column": "nato"}, ["nato:strEq:alfa,greek:strEq:beta"], [("alfa", 10)]),
        # filtering on non-included columns only
        ({"column": "nato"}, ["greek:strEq:beta"], [("alfa", 10)]),
        # checking joins on AND/OR
        (
            {"column": "nato"},
            ["greek:strEq:alpha,numeric:eq:2.2", "greek:strEq:beta,numeric:eq:1.1"],
            [("alfa", 20)],
        ),
        # validating all potential filter types
        # strings
        ({"column": "nato"}, ["nato:strEq:bravo"], [("bravo", 10)]),
        ({"column": "nato"}, ["nato:strContains:bravo"], [("bravo", 10)]),
        ({"column": "nato"}, ["nato:strStartsWith:bravo"], [("bravo", 10)]),
        ({"column": "nato"}, ["nato:strEndsWith:bravo"], [("bravo", 10)]),
        ({"column": "nato"}, ["nato:matches:bravo"], [("bravo", 10)]),
        ({"column": "nato"}, ["nato:strEqCI:bravo"], [("bravo", 10)]),
        ({"column": "nato"}, ["nato:strContainsCI:bravo"], [("bravo", 10)]),
        ({"column": "nato"}, ["nato:strStartsWithCI:bravo"], [("bravo", 10)]),
        ({"column": "nato"}, ["nato:strEndsWithCI:bravo"], [("bravo", 10)]),
        ({"column": "nato"}, ["nato:matchesCI:bravo"], [("bravo", 10)]),
        ({"column": "nato"}, ["nato:strNotEq:alfa"], [("bravo", 10)]),
        ({"column": "nato"}, ["nato:strNotContains:alfa"], [("bravo", 10)]),
        ({"column": "nato"}, ["nato:strNotStartsWith:alfa"], [("bravo", 10)]),
        ({"column": "nato"}, ["nato:strNotEndsWith:alfa"], [("bravo", 10)]),
        ({"column": "nato"}, ["nato:notMatches:alfa"], [("bravo", 10)]),
        ({"column": "nato"}, ["nato:strNotEqCI:alfa"], [("bravo", 10)]),
        ({"column": "nato"}, ["nato:strNotContainsCI:alfa"], [("bravo", 10)]),
        ({"column": "nato"}, ["nato:strNotStartsWithCI:alfa"], [("bravo", 10)]),
        ({"column": "nato"}, ["nato:strNotEndsWithCI:alfa"], [("bravo", 10)]),
        ({"column": "nato"}, ["nato:notMatchesCI:alfa"], [("bravo", 10)]),
        # Date handling
        ({"column": "nato"}, ["timestamp:sameDay:2022-02-02"], [("alfa", 10)]),
        ({"column": "nato"}, ["timestamp:sameWeek:2022-02-03"], [("alfa", 10)]),
        ({"column": "nato"}, ["timestamp:sameMonth:2022-02-21"], [("alfa", 10)]),
        ({"column": "nato"}, ["timestamp:sameYear:2022-03-03"], [("alfa", 10)]),
        (
            {"column": "nato"},
            ["timestamp:sameDayOrBefore:2022-02-02"],
            [("alfa", 50), ("bravo", 10)],
        ),
        (
            {"column": "nato"},
            ["timestamp:sameWeekOrBefore:2022-02-03"],
            [("alfa", 50), ("bravo", 10)],
        ),
        (
            {"column": "nato"},
            ["timestamp:sameMonthOrBefore:2022-02-21"],
            [("alfa", 50), ("bravo", 10)],
        ),
        (
            {"column": "nato"},
            ["timestamp:sameYearOrBefore:2022-03-03"],
            [("alfa", 50), ("bravo", 10)],
        ),
        ({"column": "nato"}, ["timestamp:sameDayOrAfter:2022-02-02"], [("alfa", 10)]),
        ({"column": "nato"}, ["timestamp:sameWeekOrAfter:2022-02-03"], [("alfa", 10)]),
        ({"column": "nato"}, ["timestamp:sameMonthOrAfter:2022-02-21"], [("alfa", 10)]),
        ({"column": "nato"}, ["timestamp:sameYearOrAfter:2022-03-03"], [("alfa", 10)]),
        ({"column": "nato"}, ["timestamp:beforeDay:2022-02-02"], [("alfa", 40), ("bravo", 10)]),
        ({"column": "nato"}, ["timestamp:beforeWeek:2022-02-03"], [("alfa", 40), ("bravo", 10)]),
        ({"column": "nato"}, ["timestamp:beforeMonth:2022-02-21"], [("alfa", 40), ("bravo", 10)]),
        ({"column": "nato"}, ["timestamp:beforeYear:2022-03-03"], [("alfa", 40), ("bravo", 10)]),
        ({"column": "nato"}, ["timestamp:afterDay:2022-02-01"], [("alfa", 10)]),
        ({"column": "nato"}, ["timestamp:afterWeek:2022-01-20"], [("alfa", 10)]),
        ({"column": "nato"}, ["timestamp:afterMonth:2022-01-01"], [("alfa", 10)]),
        ({"column": "nato"}, ["timestamp:afterYear:2021-03-03"], [("alfa", 10)]),
        # numeric
        ({"column": "nato"}, ["numeric:eq:2.2"], [("alfa", 10)]),
        ({"column": "nato"}, ["numeric:ne:1.1"], [("alfa", 10)]),
        ({"column": "nato"}, ["numeric:gt:2.1"], [("alfa", 10)]),
        ({"column": "nato"}, ["numeric:gte:2.2"], [("alfa", 10)]),
        ({"column": "nato"}, ["numeric:lt:2.2"], [("alfa", 40), ("bravo", 10)]),
        ({"column": "nato"}, ["numeric:lte:2.2"], [("alfa", 50), ("bravo", 10)]),
        # Boolean
        ({"column": "nato"}, ["bool:isTrue"], [("alfa", 10)]),
        ({"column": "nato"}, ["bool:isNotTrue"], [("alfa", 40), ("bravo", 10)]),
        ({"column": "nato"}, ["bool:isNotFalse"], [("alfa", 10)]),
        ({"column": "nato"}, ["bool:isFalse"], [("alfa", 40), ("bravo", 10)]),
        ({"column": "nato"}, ["bool:isNull"], [("alfa", 50), ("bravo", 10)]),
        ({"column": "nato"}, ["bool:isNotNull"], [("alfa", 50), ("bravo", 10)]),
        ({"column": "nato"}, ["greek:isNone"], [("alfa", 5)]),
        ({"column": "nato"}, ["greek:isNotNone"], [("alfa", 50), ("bravo", 10)]),
    ],
)
@mock.patch(
    "src.dashboard.get_chart_data.get_chart_data._get_table_cols", mock_get_table_cols_results
)
def test_query_results(mock_db, mock_bucket, query_params, filter_groups, expected):
    mock_db.execute(f'CREATE SCHEMA "{TEST_GLUE_DB}"')
    mock_db.execute(
        'CREATE TABLE "cumulus-aggregator-test-db"."test__cube__001" AS SELECT * FROM '
        'read_parquet("./tests/test_data/mock_cube_col_types.parquet")'
    )
    query, _ = get_chart_data._build_query(
        query_params, filter_groups, {"data_package_id": "test__cube__001"}
    )
    res = mock_db.execute(query).fetchall()
    assert len(res) == len(expected)
    for i in range(0, len(res)):
        assert res[i] == expected[i]


# while duckdb can handle boolean to string equality comparisons, athena does not.
# So, we'll validate that casts show up for cumulus__none checks.
@mock.patch(
    "src.dashboard.get_chart_data.get_chart_data._get_table_cols", mock_get_table_cols_results
)
def test_cast_filter(mock_db, mock_bucket):
    query, _ = get_chart_data._build_query(
        {"column": "nato"}, ["bool:isTrue"], {"data_package_id": "test__cube__001"}
    )
    assert """cast("nato" AS VARCHAR) != 'cumulus__none'""" in query
    assert """cast("nato" AS VARCHAR) = 'cumulus__none'""" in query
