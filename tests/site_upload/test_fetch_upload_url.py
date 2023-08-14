import json
import os
from unittest import mock

import boto3
import pytest

from src.handlers.shared.enums import BucketPath
from src.handlers.site_upload.fetch_upload_url import upload_url_handler
from tests.utils import (
    EXISTING_DATA_P,
    EXISTING_SITE,
    EXISTING_STUDY,
    EXISTING_VERSION,
    TEST_BUCKET,
    get_mock_metadata,
)


@pytest.mark.parametrize(
    "body,status",
    [
        (
            {
                "study": EXISTING_STUDY,
                "data_package": EXISTING_DATA_P,
                "filename": "encounter.parquet",
                "data_package_version": EXISTING_VERSION,
            },
            200,
        ),
        ({}, 500),
    ],
)
def test_fetch_upload_url(body, status, mock_bucket):
    context = {
        "authorizer": {
            "principalId": "ppth",
        }
    }

    response = upload_url_handler(
        {"body": json.dumps(body), "requestContext": context}, None
    )
    assert response["statusCode"] == status
    if response["statusCode"] == 200:
        res_body = json.loads(response["body"])
        assert res_body["fields"]["key"] == (
            f"{BucketPath.UPLOAD.value}/{body['study']}/{body['data_package']}/"
            f"{EXISTING_SITE}/{body['data_package_version']}/encounter.parquet"
        )

    assert "Access-Control-Allow-Origin" not in response["headers"]
