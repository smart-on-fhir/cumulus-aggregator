import os
from unittest import mock

from src.shared import heartbeat


@mock.patch.dict(os.environ, {"DISPLAY_NAME": "Unit test", "DOC_URL": "http://test.com"})
def test_heartbeat():
    response = heartbeat.heartbeat_handler({}, {})
    assert response["statusCode"] == 200
    assert response["headers"]["Content-Type"] == "text/html"
    assert "Unit test" in response["body"]
    assert "test.com" in response["body"]
