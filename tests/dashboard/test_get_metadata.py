import json

import pytest

from src.handlers.dashboard.get_metadata import metadata_handler
from tests.utils import (
    EXISTING_DATA_P,
    EXISTING_SITE,
    EXISTING_STUDY,
    NEW_SITE,
    NEW_STUDY,
    get_mock_metadata,
)


@pytest.mark.parametrize(
    "params,status,expected",
    [
        (None, 200, get_mock_metadata()),
        (
            {"site": EXISTING_SITE},
            200,
            get_mock_metadata()[EXISTING_SITE],
        ),
        (
            {"site": EXISTING_SITE, "study": EXISTING_STUDY},
            200,
            get_mock_metadata()[EXISTING_SITE][EXISTING_STUDY],
        ),
        (
            {
                "site": EXISTING_SITE,
                "study": EXISTING_STUDY,
                "data_package": EXISTING_DATA_P,
            },
            200,
            get_mock_metadata()[EXISTING_SITE][EXISTING_STUDY][EXISTING_DATA_P],
        ),
        ({"site": NEW_SITE, "study": EXISTING_STUDY}, 500, None),
        ({"site": EXISTING_SITE, "study": NEW_STUDY}, 500, None),
    ],
)
def test_get_metadata(mock_bucket, params, status, expected):
    event = {"pathParameters": params}

    res = metadata_handler(event, {})
    assert res["statusCode"] == status
    if status == 200:
        assert json.loads(res["body"]) == expected
