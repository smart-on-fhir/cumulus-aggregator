import pytest

from contextlib import nullcontext as does_not_raise

from src.handlers.site_upload.api_gateway_authorizer import lambda_handler


@pytest.fixture
def mocker_site_json(mocker):
    mocked_site_json = mocker.mock_open(
        read_data="""{
            "testauth":{
                "site":"general"
            }
        }"""
    )
    mocker.patch("builtins.open", mocked_site_json)


@pytest.mark.parametrize(
    "auth,expects",
    [
        ("Basic testauth", does_not_raise()),
        ("Basic testauth2", pytest.raises(Exception)),
        (None, pytest.raises(Exception)),
    ],
)
def test_validate_pw(auth, expects, mocker_site_json):
    mock_headers = {"Authorization": auth}
    event = {
        "headers": mock_headers,
        "methodArn": "arn:aws:execute-api:us-east-1:11223:123/Prod/post/lambda",
    }
    with expects:
        auth = lambda_handler(event, {})
