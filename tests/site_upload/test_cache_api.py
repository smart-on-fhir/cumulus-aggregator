import os
import pytest

from unittest import mock

import awswrangler
import pandas

from src.handlers.site_upload.cache_api import cache_api_handler
from tests.utils import get_mock_data_packages_cache, MOCK_ENV


def mock_data_packages(*args, **kwargs):
    return pandas.DataFrame(get_mock_data_packages_cache())


@mock.patch.dict(os.environ, MOCK_ENV)
# This may seem like overkill for now, but eventually we will have multiple
# cache types
@pytest.mark.parametrize(
    "subject,message,mock_result,status",
    [
        ("data_packages", "", mock_data_packages, 200),
        ("nonexistant", "endpoint", lambda: None, 500),
    ],
)
def test_cache_api(mocker, mock_bucket, subject, message, mock_result, status):

    mock_query_result = mocker.patch("awswrangler.athena.read_sql_query")
    mock_query_result.side_effect = mock_result
    event = {"Records": [{"Sns": {"Subject": subject, "Message": message}}]}
    res = cache_api_handler(event, {})
    assert res["statusCode"] == status
