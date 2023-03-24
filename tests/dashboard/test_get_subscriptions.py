import os

from unittest import mock

import awswrangler
import pandas

from src.handlers.dashboard.get_subscriptions import subscriptions_handler
from tests.utils import get_mock_metadata, get_mock_env, SUBSCRIPTION_COUNT


def mock_response(*args, **kwargs):
    meta = get_mock_metadata()
    table_names = []
    for site in meta.keys():
        for study in meta[site].keys():
            for subscription in meta[site][study].keys():
                table_names.append(f"{study}__{subscription}")
    return pandas.DataFrame.from_dict({"0": list(set(table_names))})


@mock.patch.dict(os.environ, get_mock_env())
def test_get_subscriptions(mocker):
    mocker_read = mocker.patch("awswrangler.athena.read_sql_query")
    mocker_read.side_effect = mock_response
    res = subscriptions_handler({}, {})
    assert res["statusCode"] == 200
    assert SUBSCRIPTION_COUNT == 2
