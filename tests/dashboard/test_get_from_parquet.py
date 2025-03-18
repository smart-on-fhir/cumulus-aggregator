import json
import os
from unittest import mock

import pytest

from src.dashboard.get_from_parquet import get_from_parquet
from tests.mock_utils import MOCK_ENV


def mock_event(target, payload_type):
    if target is None:
        return {}
    payload = {"queryStringParameters": {"s3_path": target}}
    if payload_type:
        payload["queryStringParameters"]["type"] = payload_type
    return payload


S3_PATH = (
    "aggregates/study/study__encounter/study__encounter__099/study__encounter__aggregate.parquet"
)


@pytest.mark.parametrize(
    "target,payload_type,code,length,first,last,schema",
    [
        (None, None, 404, [], None, None, None),
        (
            S3_PATH,
            None,
            200,
            506,
            {"cnt": 1103, "gender": None, "age": None, "race_display": None, "site": None},
            {
                "cnt": 10,
                "gender": None,
                "age": 78.0,
                "race_display": "Not Hispanic or Latino",
                "site": "princeton_plainsboro_teaching_hospital",
            },
            [
                {"name": "cnt", "type": "integer", "extDtype": "Int64"},
                {"name": "gender", "type": "any", "extDtype": "string"},
                {"name": "age", "type": "integer", "extDtype": "Int64"},
                {"name": "race_display", "type": "any", "extDtype": "string"},
                {"name": "site", "type": "any", "extDtype": "string"},
            ],
        ),
        (
            S3_PATH,
            "json",
            200,
            506,
            {"cnt": 1103, "gender": None, "age": None, "race_display": None, "site": None},
            {
                "cnt": 10,
                "gender": None,
                "age": 78.0,
                "race_display": "Not Hispanic or Latino",
                "site": "princeton_plainsboro_teaching_hospital",
            },
            [
                {"name": "cnt", "type": "integer", "extDtype": "Int64"},
                {"name": "gender", "type": "any", "extDtype": "string"},
                {"name": "age", "type": "integer", "extDtype": "Int64"},
                {"name": "race_display", "type": "any", "extDtype": "string"},
                {"name": "site", "type": "any", "extDtype": "string"},
            ],
        ),
        (
            S3_PATH,
            "csv",
            200,
            507,
            "cnt,gender,age,race_display,site",
            "10,,78,Not Hispanic or Latino,princeton_plainsboro_teaching_hospital",
            None,
        ),
        (
            S3_PATH,
            "tsv",
            200,
            507,
            "cnt|gender|age|race_display|site",
            "10||78|Not Hispanic or Latino|princeton_plainsboro_teaching_hospital",
            None,
        ),
    ],
)
@mock.patch.dict(os.environ, MOCK_ENV)
def test_get_data_packages(mock_bucket, target, payload_type, code, length, first, last, schema):
    payload = mock_event(target, payload_type)
    res = get_from_parquet.from_parquet_handler(payload, {})
    assert (res["statusCode"]) == code
    if code == 200:
        if payload_type is None or payload_type == "json":
            assert res["headers"] == {"Content-Type": "application/json"}
            res = json.loads(res["body"])
            assert res["schema"]["fields"] == [
                {"name": "cnt", "type": "integer", "extDtype": "Int64"},
                {"name": "gender", "type": "any", "extDtype": "string"},
                {"name": "age", "type": "integer", "extDtype": "Int64"},
                {"name": "race_display", "type": "any", "extDtype": "string"},
                {"name": "site", "type": "any", "extDtype": "string"},
            ]
            res = res["data"]
        else:
            assert res["headers"] == {
                "Content-Type": "text/csv",
                "Content-disposition": "attachment; filename=study__encounter__aggregate.csv",
                "Content-Length": len(res["body"].encode("utf-8")),
            }
            res = res["body"].split("\n")[:-1]
        assert len(res) == length
        assert res[0] == first
        assert res[-1] == last
