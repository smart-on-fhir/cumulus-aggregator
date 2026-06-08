import json
import os

import boto3
import pytest

from src.dashboard.get_study_data import get_study_data
from src.shared import enums, functions
from tests import mock_utils

MOCK_STUDY_JSON_WITH_NEW = {
    "other_study": {
        "099": {
            "study_owner": "st_elsewhere",
            "study_prefix": "other_study",
            "study_owner_display": "Saint Elsewhere",
            "description": "version 099 of other_study",
            "tables": {"test_table_1": {"description": "A test table"}},
            "data_dictionary": [{"name": "foo"}],
        }
    },
    "study": {
        "099": {
            "study_owner": "princeton_plainsboro_teaching_hospital",
            "study_owner_display": "Princeton Plainsboro Teaching Hospital",
            "study_prefix": "study",
            "description": "version 099 of study",
            "data_dictionary": [{"name": "bar"}],
            "tables": {"test_table_2": {"description": "A test table"}},
        },
        "100": {
            "study_prefix": "test",
            "description": "latest version",
            "study_owner": "princeton_plainsboro_teaching_hospital",
            "study_owner_display": "Princeton Plainsboro Teaching Hospital",
            "data_dictionary": [{"name": "baz"}],
            "tables": {"test_table_3": {"description": "A test table"}},
        },
    },
}


@pytest.mark.parametrize(
    "path,params,status,expected",
    [
        ("/", None, 200, MOCK_STUDY_JSON_WITH_NEW),
        (
            f"/studies/{mock_utils.EXISTING_STUDY}",
            {"study": mock_utils.EXISTING_STUDY},
            200,
            MOCK_STUDY_JSON_WITH_NEW[mock_utils.EXISTING_STUDY],
        ),
        (
            f"/studies/{mock_utils.EXISTING_STUDY}/versions/{mock_utils.EXISTING_VERSION}",
            {"study": mock_utils.EXISTING_STUDY, "version": mock_utils.EXISTING_VERSION},
            200,
            MOCK_STUDY_JSON_WITH_NEW[mock_utils.EXISTING_STUDY][mock_utils.EXISTING_VERSION],
        ),
        (
            f"/studies/{mock_utils.EXISTING_STUDY}/versions/{mock_utils.NEW_VERSION}",
            {"study": mock_utils.EXISTING_STUDY, "version": mock_utils.NEW_VERSION},
            200,
            MOCK_STUDY_JSON_WITH_NEW[mock_utils.EXISTING_STUDY][mock_utils.NEW_VERSION],
        ),
        (
            f"/studies/{mock_utils.EXISTING_STUDY}/versions/@latest",
            {"study": mock_utils.EXISTING_STUDY, "version": "@latest"},
            200,
            MOCK_STUDY_JSON_WITH_NEW[mock_utils.EXISTING_STUDY][mock_utils.NEW_VERSION],
        ),
        (
            f"/studies/{mock_utils.EXISTING_STUDY}/versions/@latest/dictionary",
            {"study": mock_utils.EXISTING_STUDY, "version": "@latest"},
            200,
            MOCK_STUDY_JSON_WITH_NEW[mock_utils.EXISTING_STUDY][mock_utils.NEW_VERSION][
                "data_dictionary"
            ],
        ),
        (
            f"/studies/{mock_utils.EXISTING_STUDY}/versions/@latest/tables",
            {"study": mock_utils.EXISTING_STUDY, "version": "@latest"},
            200,
            MOCK_STUDY_JSON_WITH_NEW[mock_utils.EXISTING_STUDY][mock_utils.NEW_VERSION]["tables"],
        ),
        (
            f"/studies/{mock_utils.EXISTING_STUDY}/versions/@latest/tables/test_table_3",
            {"study": mock_utils.EXISTING_STUDY, "version": "@latest", "table": "test_table_3"},
            200,
            MOCK_STUDY_JSON_WITH_NEW[mock_utils.EXISTING_STUDY][mock_utils.NEW_VERSION]["tables"][
                "test_table_3"
            ],
        ),
        (
            f"/studies/{mock_utils.NEW_STUDY}/versions/{mock_utils.EXISTING_VERSION}",
            {"study": mock_utils.NEW_STUDY, "version": mock_utils.EXISTING_VERSION},
            404,
            None,
        ),
        (
            f"/studies/{mock_utils.NEW_STUDY}/versions/bad_version",
            {"study": mock_utils.EXISTING_STUDY, "version": "bad_version"},
            404,
            None,
        ),
    ],
)
def test_get_study_data(mock_bucket, path, params, status, expected):
    s3_bucket_name = os.environ.get("BUCKET_NAME")
    s3_client = boto3.client("s3")
    functions.put_s3_file(
        s3_client=s3_client,
        s3_bucket_name=s3_bucket_name,
        key=f"{enums.BucketPath.CACHE.value}/{enums.JsonFilename.STUDIES.value}.json",
        payload=MOCK_STUDY_JSON_WITH_NEW,
    )
    event = {"pathParameters": params, "path": path}
    res = get_study_data.study_data_handler(event, {})
    assert res["statusCode"] == status
    if status == 200:
        assert json.loads(res["body"]) == expected
