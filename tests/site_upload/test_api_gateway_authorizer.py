from contextlib import nullcontext as does_not_raise

import pytest

from src.handlers.site_upload import api_gateway_authorizer
from tests import utils


@pytest.mark.parametrize(
    "auth,expects",
    [
        (f"Basic {next(iter(utils.get_mock_auth().keys()))}", does_not_raise()),
        ("Basic other_auth", pytest.raises(api_gateway_authorizer.AuthError)),
        (None, pytest.raises(AttributeError)),
    ],
)
def test_validate_pw(auth, expects, mock_bucket):
    mock_headers = {"Authorization": auth}
    event = {
        "headers": mock_headers,
        "methodArn": "arn:aws:execute-api:us-east-1:11223:123/Prod/post/lambda",
    }
    with expects:
        res = api_gateway_authorizer.lambda_handler(event, {})
        assert res["policyDocument"]["Statement"][0]["Effect"] == "Allow"
