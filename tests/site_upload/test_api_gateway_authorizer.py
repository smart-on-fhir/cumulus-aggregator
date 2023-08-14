import json
import os
from contextlib import nullcontext as does_not_raise
from unittest import mock

import pytest
from pytest_mock import MockerFixture

from src.handlers.site_upload.api_gateway_authorizer import lambda_handler
from tests.utils import TEST_BUCKET, get_mock_auth


@pytest.mark.parametrize(
    "auth,expects",
    [
        (f"Basic {list(get_mock_auth().keys())[0]}", does_not_raise()),
        ("Basic other_auth", pytest.raises(Exception)),
        (None, pytest.raises(Exception)),
    ],
)
def test_validate_pw(auth, expects, mock_bucket):
    mock_headers = {"Authorization": auth}
    event = {
        "headers": mock_headers,
        "methodArn": "arn:aws:execute-api:us-east-1:11223:123/Prod/post/lambda",
    }
    with expects:
        res = lambda_handler(event, {})
