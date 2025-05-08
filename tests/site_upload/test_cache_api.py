import json
import os
from unittest import mock

import boto3
import pandas
import pytest

from src.shared import enums
from src.site_upload.cache_api import cache_api
from tests.mock_utils import MOCK_ENV, get_mock_data_packages_cache


def mock_data_packages(*args, **kwargs):
    return pandas.DataFrame(get_mock_data_packages_cache(), columns=["table_name"])


# This may seem like overkill for now, but eventually we will have multiple
# cache types
@pytest.mark.parametrize(
    "subject,message,mock_result,status",
    [
        ("data_packages", "", mock_data_packages, 200),
        ("nonexistant", "endpoint", lambda: None, 500),
    ],
)
def test_cache_api_handler(mocker, mock_bucket, subject, message, mock_result, status):
    mock_query_result = mocker.patch("awswrangler.athena.read_sql_query")
    mock_query_result.side_effect = mock_result
    event = {"Records": [{"Sns": {"Subject": subject, "Message": message}}]}
    res = cache_api.cache_api_handler(event, {})
    assert res["statusCode"] == status


@mock.patch(
    "awswrangler.athena.read_sql_query",
    lambda query, database, s3_output, workgroup: pandas.DataFrame(
        data=pandas.DataFrame(
            {
                "table_name": [
                    "study__encounter__098",
                    "study__encounter__099",
                    "nonexistent_study__encounter__099",
                ]
            }
        )
    ),
)
def test_cache_api_data(mock_bucket):
    s3_bucket_name = os.environ.get("BUCKET_NAME")
    s3_client = boto3.client("s3")
    cache_api.cache_api_data(
        s3_client, s3_bucket_name, MOCK_ENV["GLUE_DB_NAME"], enums.JsonFilename.DATA_PACKAGES.value
    )
    cache = json.loads(
        s3_client.get_object(
            Bucket=s3_bucket_name,
            Key=f"{enums.BucketPath.CACHE.value}/{enums.JsonFilename.DATA_PACKAGES.value}.json",
        )["Body"]
        .read()
        .decode("UTF-8")
    )
    assert cache == [
        {
            "study": "study",
            "name": "encounter",
            "column_types_format_version": "2",
            "columns": {
                "cnt": "integer",
                "gender": "string",
                "age": "integer",
                "race_display": "string",
                "site": "string",
            },
            "last_data_update": "2023-02-24T15:08:07.771080+00:00",
            "s3_path": (
                "aggregates/study/study__encounter/study__encounter__099"
                "/study__encounter__aggregate.parquet"
            ),
            "total": 1000,
            "version": "099",
            "id": "study__encounter__099",
        }
    ]
