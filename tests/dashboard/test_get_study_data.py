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


@pytest.mark.parametrize(
    "params,status,expected",
    [
        (None, 200, MOCK_STUDY_JSON_WITH_NEW),
        (
            {"study": mock_utils.EXISTING_STUDY},
            200,
            MOCK_STUDY_JSON_WITH_NEW[mock_utils.EXISTING_STUDY],
        ),
        (
            {"study": mock_utils.EXISTING_STUDY, "version": mock_utils.EXISTING_VERSION},
            200,
            MOCK_STUDY_JSON_WITH_NEW[mock_utils.EXISTING_STUDY][mock_utils.EXISTING_VERSION],
        ),
        (
            {"study": mock_utils.EXISTING_STUDY, "version": mock_utils.NEW_VERSION},
            200,
            MOCK_STUDY_JSON_WITH_NEW[mock_utils.EXISTING_STUDY][mock_utils.NEW_VERSION],
        ),
        (
            {"study": mock_utils.EXISTING_STUDY, "version": "@latest"},
            200,
            MOCK_STUDY_JSON_WITH_NEW[mock_utils.EXISTING_STUDY][mock_utils.NEW_VERSION],
        ),
        ({"study": mock_utils.NEW_STUDY, "version": mock_utils.EXISTING_VERSION}, 404, None),
        ({"study": mock_utils.EXISTING_STUDY, "version": "bad_version"}, 404, None),
    ],
)
def test_get_study_data(mock_bucket, params, status, expected):
    s3_bucket_name = os.environ.get("BUCKET_NAME")
    s3_client = boto3.client("s3")
    functions.put_s3_file(
        s3_client=s3_client,
        s3_bucket_name=s3_bucket_name,
        key=f"{enums.BucketPath.CACHE.value}/{enums.JsonFilename.STUDIES.value}.json",
        payload=MOCK_STUDY_JSON_WITH_NEW,
    )
    event = {"pathParameters": params}
    res = get_study_data.study_data_handler(event, {})
    assert res["statusCode"] == status
    if status == 200:
        assert json.loads(res["body"]) == expected
