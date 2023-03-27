import json
import os

import pandas
import pytest

from unittest import mock

from src.handlers.dashboard import get_chart_data
from tests.utils import MOCK_ENV, TEST_BUCKET, TEST_GLUE_DB, TEST_WORKGROUP


def mock_get_table_cols(name):
    return ["cnt", "gender", "race"]


def mock_data_frame(filter):
    df = pandas.read_csv("tests/test_data/cube_simple_example.csv", na_filter=False)
    if filter != []:
        df = df[df["gender"] == "female"]
    return df


@mock.patch(
    "src.handlers.dashboard.get_chart_data._get_table_cols", mock_get_table_cols
)
@mock.patch.dict(os.environ, MOCK_ENV)
@pytest.mark.parametrize(
    "query_params,filters,path_params,query_str",
    [
        (
            {"column": "gender"},
            [],
            {"subscription_name": "test_study"},
            f'SELECT gender, sum(cnt) as cnt FROM "{TEST_GLUE_DB}"."test_study" '
            "WHERE COALESCE (race) IS NOT Null AND gender IS NOT Null  "
            "GROUP BY gender",
        ),
        (
            {"column": "gender", "stratifier": "race"},
            [],
            {"subscription_name": "test_study"},
            f'SELECT race, gender, sum(cnt) as cnt FROM "{TEST_GLUE_DB}"."test_study" '
            "WHERE gender IS NOT Null  "
            "GROUP BY race, gender",
        ),
        (
            {"column": "gender"},
            ["gender:strEq:female"],
            {"subscription_name": "test_study"},
            f'SELECT gender, sum(cnt) as cnt FROM "{TEST_GLUE_DB}"."test_study" '
            "WHERE COALESCE (race) IS NOT Null AND gender IS NOT Null "
            "AND gender LIKE 'female' "
            "GROUP BY gender",
        ),
        (
            {"column": "gender", "stratifier": "race"},
            ["gender:strEq:female"],
            {"subscription_name": "test_study"},
            f'SELECT race, gender, sum(cnt) as cnt FROM "{TEST_GLUE_DB}"."test_study" '
            "WHERE gender IS NOT Null "
            "AND gender LIKE 'female' "
            "GROUP BY race, gender",
        ),
    ],
)
def test_build_query(query_params, filters, path_params, query_str):
    query = get_chart_data._build_query(query_params, filters, path_params)
    assert query == query_str


@pytest.mark.parametrize(
    "query_params,filters,expected_payload",
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
def test_format_payload(query_params, filters, expected_payload):
    df = mock_data_frame(filters)
    payload = get_chart_data._format_payload(df, query_params, filters)
    assert payload == expected_payload
