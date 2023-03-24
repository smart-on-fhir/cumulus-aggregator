import json
import pytest

from contextlib import nullcontext as does_not_raise

from src.handlers.site_upload.api_gateway_authorizer import lambda_handler
from tests.utils import get_mock_auth


@pytest.fixture
def mocker_site_json(mocker):
    mocked_site_json = mocker.mock_open(read_data=json.dumps(get_mock_auth()))
    mocker.patch("builtins.open", mocked_site_json)


@pytest.mark.parametrize(
    "auth,expects",
    [
        (f"Basic {list(get_mock_auth().keys())[0]}", does_not_raise()),
        ("Basic other_auth", pytest.raises(Exception)),
        (None, pytest.raises(Exception)),
    ],
)
def test_validate_pw(auth, expects, mock_bucket, mocker_site_json):
    mock_headers = {"Authorization": auth}
    event = {
        "headers": mock_headers,
        "methodArn": "arn:aws:execute-api:us-east-1:11223:123/Prod/post/lambda",
    }
    with expects:
        res = lambda_handler(event, {})
