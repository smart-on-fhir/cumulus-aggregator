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
            "gender" LIKE 'female'
                    )
                )
        )
        OR (
            gender = 'cumulus__none'
                AND
                (
                    
        (
            "gender" LIKE 'female'
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
            "gender" LIKE 'cumulus__none'
                    )
                )
        )
        OR (
            gender = 'cumulus__none'
                AND
                (
                    
        (
            "gender" LIKE 'cumulus__none'
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
            regexp_like("gender", 'a')
                    )
        OR (
            regexp_like("gender", 'e')
            AND regexp_like("gender", 'm')
                    )
                )
        )
        OR (
            gender = 'cumulus__none'
                AND
                (
                    
        (
            regexp_like("gender", 'a')
                    )
        OR (
            regexp_like("gender", 'e')
            AND regexp_like("gender", 'm')
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
