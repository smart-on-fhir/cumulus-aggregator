import boto3
import json
import os
import pytest

from unittest import mock

from src.handlers.site_upload.fetch_upload_url import upload_url_handler
from tests.utils import TEST_BUCKET, get_mock_metadata


@pytest.mark.parametrize(
    "body,status",
    [
        (
            {
                "study": "covid",
                "data_package": "encounter",
                "filename": "encounter.parquet",
            },
            200,
        ),
        ({}, 500),
    ],
)
def test_fetch_upload_url(body, status, mock_bucket):
    context = {
        "authorizer": {
            "principalId": "general",
        }
    }
    response = upload_url_handler(
        {"body": json.dumps(body), "requestContext": context}, None
    )
    print(response)
    assert response["statusCode"] == status
