import json

import pytest

from src.handlers.dashboard.get_study_periods import study_periods_handler
from tests.mock_utils import (
    EXISTING_SITE,
    EXISTING_STUDY,
    NEW_SITE,
    NEW_STUDY,
    get_mock_study_metadata,
)


@pytest.mark.parametrize(
    "params,status,expected",
    [
        (None, 200, get_mock_study_metadata()),
        (
            {"site": EXISTING_SITE},
            200,
            get_mock_study_metadata()[EXISTING_SITE],
        ),
        (
            {"site": EXISTING_SITE, "study": EXISTING_STUDY},
            200,
            get_mock_study_metadata()[EXISTING_SITE][EXISTING_STUDY],
        ),
        ({"site": NEW_SITE, "study": EXISTING_STUDY}, 500, None),
        ({"site": EXISTING_SITE, "study": NEW_STUDY}, 500, None),
    ],
)
def test_get_study_periods(mock_bucket, params, status, expected):
    event = {"pathParameters": params}
    res = study_periods_handler(event, {})
    assert res["statusCode"] == status
    if status == 200:
        assert json.loads(res["body"]) == expected
