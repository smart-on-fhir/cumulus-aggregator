import json
import os
from contextlib import nullcontext as does_not_raise
from unittest import mock

import botocore
import pandas
import pytest

from src.dashboard.get_chart_data import get_chart_data
from tests.mock_utils import (
    EXISTING_DATA_P,
    EXISTING_STUDY,
    EXISTING_VERSION,
    MOCK_ENV,
    TEST_GLUE_DB,
)


def mock_get_table_cols(name):
    return ["cnt", "gender", "race"]


def mock_data_frame(filter_param):
    df = pandas.read_csv("tests/test_data/cube_simple_example.csv", na_filter=False)
    if filter_param != []:
        df = df[df["gender"] == "female"]
    return df


@mock.patch("src.dashboard.get_chart_data.get_chart_data._get_table_cols", mock_get_table_cols)
@mock.patch.dict(os.environ, MOCK_ENV)
@pytest.mark.parametrize(
    "query_params,filter_groups,path_params,query_str, raises",
    [
        (
            {"column": "gender"},
            [],
            {"data_package_id": "test_study"},
            f"""SELECT
    "gender",
    "cnt"

FROM "{TEST_GLUE_DB}"."test_study"
WHERE COALESCE(
            CAST("race" AS VARCHAR)
    ) IS NULL
    AND "gender" IS NOT NULL
    AND (
        (
            gender != 'cumulus__none'
        )
        OR (
            gender = 'cumulus__none'
        )
    )
ORDER BY
    "gender\"""",
            does_not_raise(),
        ),
        (
            {"column": "gender", "stratifier": "race"},
            [],
            {"data_package_id": "test_study"},
            f"""SELECT
        "race",
    "gender",
    "cnt"

FROM "{TEST_GLUE_DB}"."test_study"
WHERE "gender" IS NOT NULL
    AND (
        (
            gender != 'cumulus__none'
        )
        OR (
            gender = 'cumulus__none'
        )
    )
        AND "race" IS NOT NULL
ORDER BY
        "race", "gender\"""",
            does_not_raise(),
        ),
        (
            {"column": "gender"},
            ["gender:strEq:female"],
            {"data_package_id": "test_study"},
            f"""SELECT
    "gender",
    "cnt"

FROM "{TEST_GLUE_DB}"."test_study"
WHERE COALESCE(
            CAST("race" AS VARCHAR)
    ) IS NULL
    AND "gender" IS NOT NULL
    AND (
        (
            gender != 'cumulus__none'
                AND
                (
                    
        (
            CAST("gender" AS VARCHAR) LIKE 'female'
                    )
                )
        )
        OR (
            gender = 'cumulus__none'
                AND
                (
                    
        (
            CAST("gender" AS VARCHAR) LIKE 'female'
                    )
                )
        )
    )
ORDER BY
    "gender\"""",
            does_not_raise(),
        ),
        (
            {"column": "gender", "stratifier": "race"},
            ["gender:strEq:none"],
            {"data_package_id": "test_study"},
            f"""SELECT
        "race",
    "gender",
    "cnt"

FROM "{TEST_GLUE_DB}"."test_study"
WHERE "gender" IS NOT NULL
    AND (
        (
            gender != 'cumulus__none'
                AND
                (
                    
        (
            CAST("gender" AS VARCHAR) LIKE 'cumulus__none'
                    )
                )
        )
        OR (
            gender = 'cumulus__none'
                AND
                (
                    
        (
            CAST("gender" AS VARCHAR) LIKE 'cumulus__none'
                    )
                )
        )
    )
        AND "race" IS NOT NULL
ORDER BY
        "race", "gender\"""",
            does_not_raise(),
        ),
        (
            {"column": "gender", "stratifier": "race"},
            ["cnt:gt:2", "cnt:lt:10"],
            {"data_package_id": "test_study"},
            f"""SELECT
        "race",
    "gender",
    "cnt"

FROM "{TEST_GLUE_DB}"."test_study"
WHERE "gender" IS NOT NULL
    AND (
        (
            gender != 'cumulus__none'
                AND
                (
                    
        (
            "cnt" > 2
                    )
        OR (
            "cnt" < 10
                    )
                )
        )
        OR (
            gender = 'cumulus__none'
                AND
                (
                    
        (
            "cnt" > 2
                    )
        OR (
            "cnt" < 10
                    )
                )
        )
    )
        AND "race" IS NOT NULL
ORDER BY
        "race", "gender\"""",
            does_not_raise(),
        ),
        (
            {"column": "gender", "stratifier": "race"},
            ["gender:strEqCI:foo"],
            {"data_package_id": "test_study"},
            f"""SELECT
        "race",
    "gender",
    "cnt"

FROM "{TEST_GLUE_DB}"."test_study"
WHERE "gender" IS NOT NULL
    AND (
        (
            gender != 'cumulus__none'
                AND
                (
                    
        (
            regexp_like(CAST("gender" AS VARCHAR), '(?i)^foo$')
                    )
                )
        )
        OR (
            gender = 'cumulus__none'
                AND
                (
                    
        (
            regexp_like(CAST("gender" AS VARCHAR), '(?i)^foo$')
                    )
                )
        )
    )
        AND "race" IS NOT NULL
ORDER BY
        "race", "gender\"""",
            does_not_raise(),
        ),
        (
            {"column": "gender", "stratifier": "race"},
            [
                "gender:matches:a",
                "gender:matches:e,gender:matches:m",
            ],
            {"data_package_id": "test_study"},
            f"""SELECT
        "race",
    "gender",
    "cnt"

FROM "{TEST_GLUE_DB}"."test_study"
WHERE "gender" IS NOT NULL
    AND (
        (
            gender != 'cumulus__none'
                AND
                (
                    
        (
            regexp_like(CAST("gender" AS VARCHAR), 'a')
                    )
        OR (
            regexp_like(CAST("gender" AS VARCHAR), 'e')
            AND regexp_like(CAST("gender" AS VARCHAR), 'm')
                    )
                )
        )
        OR (
            gender = 'cumulus__none'
                AND
                (
                    
        (
            regexp_like(CAST("gender" AS VARCHAR), 'a')
                    )
        OR (
            regexp_like(CAST("gender" AS VARCHAR), 'e')
            AND regexp_like(CAST("gender" AS VARCHAR), 'm')
                    )
                )
        )
    )
        AND "race" IS NOT NULL
ORDER BY
        "race", "gender\"""",
            does_not_raise(),
        ),
        (
            {"column": "gender", "stratifier": "race"},
            [
                "gender:invalid:a",
            ],
            {"data_package_id": "test_study"},
            "",
            # The deployed class vs testing module approach makes getting
            # the actual error raised here fussy.
            pytest.raises(Exception),
        ),
    ],
)
def test_build_query(query_params, filter_groups, path_params, query_str, raises):
    with raises:
        query, _ = get_chart_data._build_query(query_params, filter_groups, path_params)
        assert query == query_str


@pytest.mark.parametrize(
    "query_params,filter_groups,expected_payload",
    [
        (
            {"column": "gender"},
            [],
            json.load(open("tests/test_data/cube_response.json")),
        ),
        (
            {"column": "gender", "stratifier": "race"},
            [],
            json.load(open("tests/test_data/cube_response_stratified.json")),
        ),
        (
            {"column": "gender"},
            ["gender:strEq:female"],
            json.load(open("tests/test_data/cube_response_filtered.json")),
        ),
        (
            {"column": "gender", "stratifier": "race"},
            ["gender:strEq:female"],
            json.load(open("tests/test_data/cube_response_filtered_stratified.json")),
        ),
    ],
)
def test_format_payload(query_params, filter_groups, expected_payload):
    df = mock_data_frame(filter_groups)
    payload = get_chart_data._format_payload(df, query_params, filter_groups, "cnt")
    assert payload == expected_payload


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


@mock.patch(
    "src.dashboard.get_chart_data.get_chart_data._build_query",
    lambda query_params, filter_groups, path_params: (
        (
            "SELECT gender, sum(cnt) as cnt"
            f'FROM "{TEST_GLUE_DB}"."test_study" '
            "WHERE COALESCE (race) IS NOT NULL AND gender IS NOT NULL "
            "AND gender LIKE 'female' "
            "GROUP BY gender",
            "cnt",
        )
    ),
)
@mock.patch(
    "awswrangler.athena.read_sql_query",
    lambda query, database, s3_output, workgroup: pandas.DataFrame(
        data={"gender": ["male", "female"], "cnt": [10, 10]}
    ),
)
def test_handler():
    event = {
        "queryStringParameters": {"column": "gender"},
        "multiValueQueryStringParameters": {"filter": ["gender:strEq:female"]},
        "pathParameters": {},
    }
    res = get_chart_data.chart_data_handler(event, {})
    assert res["body"] == (
        '{"column": "gender", "filters": ["gender:strEq:female"], '
        '"rowCount": 2, "totalCount": 20, "data": [{"rows": [["male", 10], '
        '["female", 10]]}]}'
    )
    event = {
        "queryStringParameters": {"column": "gender", "filter": "gender:strEq:female"},
        "multiValueQueryStringParameters": {},
        "pathParameters": {},
    }
    res = get_chart_data.chart_data_handler(event, {})
    assert res["body"] == (
        '{"column": "gender", "filters": ["gender:strEq:female"], '
        '"rowCount": 2, "totalCount": 20, "data": [{"rows": [["male", 10], '
        '["female", 10]]}]}'
    )


def mock_get_table_cols_results(name):
    return ["cnt", "nato", "greek", "numeric", "timestamp", "bool"]


@pytest.mark.parametrize(
    "query_params,filter_groups,expected",
    [
        # flitering on display column
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
            [("alfa", 10), ("alfa", 10)],
        ),
        # validating all potential filter types
        ## strings
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
        ({"column": "nato"}, ["nato:notMatchesCI:alfa"], [("bravo", 10)]),
        # Date handling
        ({"column": "nato"}, ["timestamp:sameDay:2022-02-02"], [("alfa", 10)]),
        ({"column": "nato"}, ["timestamp:sameWeek:2022-02-03"], [("alfa", 10)]),
        ({"column": "nato"}, ["timestamp:sameMonth:2022-02-21"], [("alfa", 10)]),
        ({"column": "nato"}, ["timestamp:sameYear:2022-03-03"], [("alfa", 10)]),
        (
            {"column": "nato"},
            ["timestamp:sameDayOrBefore:2022-02-02"],
            [("alfa", 40), ("alfa", 10), ("bravo", 10)],
        ),
        (
            {"column": "nato"},
            ["timestamp:sameWeekOrBefore:2022-02-03"],
            [("alfa", 40), ("alfa", 10), ("bravo", 10)],
        ),
        (
            {"column": "nato"},
            ["timestamp:sameMonthOrBefore:2022-02-21"],
            [("alfa", 40), ("alfa", 10), ("bravo", 10)],
        ),
        (
            {"column": "nato"},
            ["timestamp:sameYearOrBefore:2022-03-03"],
            [("alfa", 40), ("alfa", 10), ("bravo", 10)],
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
        ({"column": "nato"}, ["numeric:lte:2.2"], [("alfa", 40), ("alfa", 10), ("bravo", 10)]),
        # Boolean
        ({"column": "nato"}, ["bool:isTrue"], [("alfa", 10)]),
        ({"column": "nato"}, ["bool:isNotTrue"], [("alfa", 40), ("bravo", 10)]),
        ({"column": "nato"}, ["bool:isNotFalse"], [("alfa", 10)]),
        ({"column": "nato"}, ["bool:isFalse"], [("alfa", 40), ("bravo", 10)]),
        ({"column": "nato"}, ["bool:isNull"], [("alfa", 50), ("bravo", 10)]),
        ({"column": "nato"}, ["bool:isNotNull"], [("alfa", 40), ("alfa", 10), ("bravo", 10)]),
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
    query, count_col = get_chart_data._build_query(
        query_params, filter_groups, {"data_package_id": "test__cube__001"}
    )
    res = mock_db.execute(query).fetchall()

    assert len(res) == len(expected)
    for i in range(0, len(res)):
        assert res[i] == expected[i]
