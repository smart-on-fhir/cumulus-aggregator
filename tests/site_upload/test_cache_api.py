import json
import os
from unittest import mock

import boto3
import pandas
import pytest

from src.shared import enums, functions
from src.site_upload.cache_api import cache_api
from tests import mock_utils


def mock_data_packages(*args, **kwargs):
    return pandas.DataFrame(mock_utils.get_mock_data_packages_cache(), columns=["table_name"])


def mock_event(source, subject, message):
    if source == "sns":
        return {"Records": [{"Sns": {"Subject": subject, "Message": message}}]}
    elif source == "eventbridge":
        return {"detail-type": message}
    raise Exception("invalid event mock")


@pytest.mark.parametrize(
    "subject,source,message,mock_result,status",
    [
        ("data_packages", "sns", "", mock_data_packages, 200),
        ("nonexistant", "sns", "endpoint", lambda: None, 500),
        ("", "eventbridge", "Glue Crawler State Change", mock_data_packages, 200),
    ],
)
def test_cache_api_handler(mocker, mock_bucket, subject, source, message, mock_result, status):
    mock_query_result = mocker.patch("awswrangler.athena.read_sql_query")
    mock_query_result.side_effect = mock_result
    event = mock_event(source, subject, message)
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
        s3_client,
        s3_bucket_name,
        mock_utils.MOCK_ENV["GLUE_DB_NAME"],
        enums.JsonFilename.DATA_PACKAGES.value,
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
            "column_types_format_version": "3",
            "columns": {
                "cnt": {"type": "integer"},
                "gender": {"type": "string", "distinct_values_count": 10},
                "age": {"type": "integer", "distinct_values_count": 10},
                "race_display": {"type": "string", "distinct_values_count": 10},
                "site": {"type": "string", "distinct_values_count": 10},
            },
            "last_data_update": "2023-02-24T15:08:07.771080+00:00",
            "s3_path": (
                "aggregates/study/study__encounter/study__encounter__099/study__encounter__aggregate.parquet"
            ),
            "total": 1000,
            "id": "study__encounter__099",
            "version": "099",
        }
    ]


def test_cache_study_data(mock_bucket):
    s3_bucket_name = os.environ.get("BUCKET_NAME")
    s3_client = boto3.client("s3")
    functions.put_s3_file(
        s3_client=s3_client,
        s3_bucket_name=s3_bucket_name,
        key=(
            f"{enums.BucketPath.MANIFEST.value}/{mock_utils.EXISTING_STUDY}/"
            f"{mock_utils.NEW_VERSION}/manifest.json"
        ),
        payload={
            "study_prefix": "test",
            "description": "latest version",
            "stages": {
                "default": [
                    {
                        "type": "export:counts",
                        "tables": [
                            {"name": "test_table", "description": "A test table"},
                        ],
                    }
                ]
            },
            "study_owner": "princeton_plainsboro_teaching_hospital",
        },
    )
    cache_api.cache_study_data(
        s3_client,
        s3_bucket_name,
        mock_utils.MOCK_ENV["GLUE_DB_NAME"],
    )
    cache = json.loads(
        s3_client.get_object(
            Bucket=s3_bucket_name,
            Key=f"{enums.BucketPath.CACHE.value}/{enums.JsonFilename.STUDIES.value}.json",
        )["Body"]
        .read()
        .decode("UTF-8")
    )
    assert cache == {
        "other_study": {
            "099": {
                "study_owner": "st_elsewhere",
                "study_prefix": "other_study",
                "description": "version 099 of other_study",
                "tables": {},
            }
        },
        "study": {
            "099": {
                "study_owner": "princeton_plainsboro_teaching_hospital",
                "study_prefix": "study",
                "description": "version 099 of study",
                "tables": {},
            },
            "100": {
                "study_prefix": "test",
                "description": "latest version",
                "study_owner": "princeton_plainsboro_teaching_hospital",
                "tables": {"test_table": {"description": "A test table"}},
            },
        },
    }
