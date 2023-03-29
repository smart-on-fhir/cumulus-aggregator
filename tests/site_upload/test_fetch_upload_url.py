import boto3
import json
import os
import pytest

from moto import mock_s3
from unittest import TestCase, mock

from src.handlers.site_upload.fetch_upload_url import upload_url_handler
from tests.utils import TEST_BUCKET, get_mock_metadata

builtin_open = open


def mock_open(*args, **kwargs):
    if args[0] == "src/handlers/site_upload/site_data/metadata.json":
        return mock.mock_open()(*args, **kwargs)
    return builtin_open(*args, **kwargs)


def mock_json_load(*args):
    return {"general": {"path": f"s3://{TEST_BUCKET}/testpath"}}


@mock.patch.dict(os.environ, {"BUCKET_NAME": TEST_BUCKET})
@mock.patch("builtins.open", mock_open)
@mock.patch("json.load", mock_json_load)
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
def test_fetch_upload_url(body, status):
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
