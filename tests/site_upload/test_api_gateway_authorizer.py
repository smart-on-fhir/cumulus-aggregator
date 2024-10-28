import json
from contextlib import nullcontext as does_not_raise
from unittest import mock

import pytest

from src.site_upload.api_gateway_authorizer import api_gateway_authorizer
from tests import mock_utils


@pytest.mark.parametrize(
    "auth,expects",
    [
        (f"Basic {next(iter(mock_utils.get_mock_auth().keys()))}", does_not_raise()),
        ("Basic other_auth", pytest.raises(api_gateway_authorizer.AuthError)),
        (None, pytest.raises(AttributeError)),
    ],
)
@mock.patch("botocore.client")
def test_validate_pw(mock_client, auth, expects):
    mock_secret_client = mock_client.ClientCreator.return_value.create_client.return_value
    mock_secret_client.get_secret_value.return_value = {
        "SecretString": json.dumps(mock_utils.get_mock_auth())
    }
    mock_headers = {"Authorization": auth}
    event = {
        "headers": mock_headers,
        "methodArn": "arn:aws:execute-api:us-east-1:11223:123/Prod/post/lambda",
    }
    with expects:
        res = api_gateway_authorizer.lambda_handler(event, {})
        assert mock_client.is_called()
        assert res["policyDocument"]["Statement"][0]["Effect"] == "Allow"
