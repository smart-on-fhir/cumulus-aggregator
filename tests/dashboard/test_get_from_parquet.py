import json
import os
from unittest import mock

import boto3
import pytest

from src.dashboard.get_from_parquet import get_from_parquet
from tests.mock_utils import MOCK_ENV, TEST_BUCKET


def mock_event(target, payload_type):
    if target is None:
        return {}
    payload = {"queryStringParameters": {"s3_path": target}}
    if payload_type:
        payload["queryStringParameters"]["type"] = payload_type
    return payload


S3_KEY = (
    "aggregates/study/study__encounter/study__encounter__099/study__encounter__aggregate.parquet"
)
S3_PATH = f"s3://{TEST_BUCKET}/{S3_KEY}"
S3_TEMP_KEY = f"temp/{S3_KEY}"
S3_TEMP_PATH = f"https://{TEST_BUCKET}.s3.amazonaws.com/"


@pytest.mark.parametrize(
    "target,payload_type,code,length,first,last,schema",
    [
        (None, None, 404, [], None, None, None),
        (
            S3_PATH,
            None,
            302,
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
            302,
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
            302,
            507,
            "cnt,gender,age,race_display,site",
            "10,,78,Not Hispanic or Latino,princeton_plainsboro_teaching_hospital",
            None,
        ),
        (
            S3_PATH,
            "tsv",
            302,
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
    if code == 302:
        url = json.loads(res["body"])["location"]
        assert url.startswith(S3_TEMP_PATH)
        s3_client = boto3.client("s3", region_name="us-east-1")
        res = s3_client.get_object(Bucket=TEST_BUCKET, Key=S3_TEMP_KEY)
        file = res["Body"].read().decode("utf-8")
        if payload_type is None or payload_type == "json":
            file = json.loads(file)
            assert file["schema"]["fields"] == [
                {"name": "cnt", "type": "integer", "extDtype": "Int64"},
                {"name": "gender", "type": "any", "extDtype": "string"},
                {"name": "age", "type": "integer", "extDtype": "Int64"},
                {"name": "race_display", "type": "any", "extDtype": "string"},
                {"name": "site", "type": "any", "extDtype": "string"},
            ]
            file = file["data"]
        else:
            file = file.split("\n")[:-1]
        assert len(file) == length
        assert file[0] == first
        assert file[-1] == last
