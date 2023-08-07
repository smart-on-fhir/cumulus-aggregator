import json
import os
from unittest import mock

import boto3
import pytest

from src.handlers.site_upload.fetch_upload_url import upload_url_handler
from tests.utils import (
    EXISTING_DATA_P,
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
                "version": EXISTING_VERSION,
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
    print(response)
    assert response["statusCode"] == status
    assert "Access-Control-Allow-Origin" not in response["headers"]
