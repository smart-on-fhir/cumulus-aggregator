import json
import os
from unittest import mock

import pandas
import pytest

from src.handlers.dashboard import get_chart_data
from tests.mock_utils import (
    EXISTING_DATA_P,
    EXISTING_STUDY,
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
            {"data_package": "test_study"},
            f'SELECT gender, sum(cnt) as cnt FROM "{TEST_GLUE_DB}"."test_study" '
            "WHERE COALESCE (race) IS NOT Null AND gender IS NOT Null  "
            "GROUP BY gender",
        ),
        (
            {"column": "gender", "stratifier": "race"},
            [],
            {"data_package": "test_study"},
            f'SELECT race, gender, sum(cnt) as cnt FROM "{TEST_GLUE_DB}"."test_study" '
            "WHERE gender IS NOT Null  "
            "GROUP BY race, gender",
        ),
        (
            {"column": "gender"},
            ["gender:strEq:female"],
            {"data_package": "test_study"},
            f'SELECT gender, sum(cnt) as cnt FROM "{TEST_GLUE_DB}"."test_study" '
            "WHERE COALESCE (race) IS NOT Null AND gender IS NOT Null "
            "AND gender LIKE 'female' "
            "GROUP BY gender",
        ),
        (
            {"column": "gender", "stratifier": "race"},
            ["gender:strEq:female"],
            {"data_package": "test_study"},
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


def test_get_data_cols(mock_bucket):
    table_name = f"{EXISTING_STUDY}__{EXISTING_DATA_P}"
    res = get_chart_data._get_table_cols(table_name)
    cols = pandas.read_csv("./tests/test_data/count_synthea_patient_agg.csv").columns
    assert res == list(cols)
