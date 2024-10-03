import json
from unittest import mock

import botocore
import pytest

from src.handlers.shared import enums
from src.handlers.site_upload import fetch_upload_url
from tests.mock_utils import (
    EXISTING_DATA_P,
    EXISTING_SITE,
    EXISTING_STUDY,
    EXISTING_VERSION,
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

    response = fetch_upload_url.upload_url_handler(
        {"body": json.dumps(body), "requestContext": context}, None
    )
    assert response["statusCode"] == status
    if response["statusCode"] == 200:
        res_body = json.loads(response["body"])
        assert res_body["fields"]["key"] == (
            f"{enums.BucketPath.UPLOAD.value}/{body['study']}/{body['data_package']}/"
            f"{EXISTING_SITE}/{body['data_package_version']}/encounter.parquet"
        )

    assert "Access-Control-Allow-Origin" not in response["headers"]


@mock.patch("boto3.client")
def test_create_presigned_post_error(mock_client):
    mock_client.return_value.generate_presigned_post.side_effect = botocore.exceptions.ClientError(
        error_response={}, operation_name="op"
    )
    res = fetch_upload_url.create_presigned_post("bucket", "obj")
    assert res["statusCode"] == 400
