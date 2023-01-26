import pytest

from contextlib import nullcontext as does_not_raise

from src.handlers.api_gateway_authorizer import lambda_handler


@pytest.fixture
def mocker_site_json(mocker):
    mocked_site_json = mocker.mock_open(
        read_data="""{
            "test":{
                "secret":"$2b$12$zUWNDX.zhLDcgTFpHMOZsuFJzVCHpZ8B8RqZTKAk8YhXKGsqcFZPm",
                "path":"testpath"
            }
        }"""
    )
    mocker.patch("builtins.open", mocked_site_json)


@pytest.mark.parametrize(
    "user,pw,expects",
    [
        ("test", "testpw", does_not_raise()),
        ("test", "testpw2", pytest.raises(Exception)),
        ("test2", "testpw", pytest.raises(Exception)),
        ("test2", None, pytest.raises(Exception)),
    ],
)
def test_validate_pw(user, pw, expects, mocker_site_json):
    mock_headers = {"user": user, "Authorization": pw}
    event = {
        "headers": mock_headers,
        "methodArn": "arn:aws:execute-api:us-east-1:11223:123/Prod/post/lambda",
    }
    with expects:
        auth = lambda_handler(event, {})
