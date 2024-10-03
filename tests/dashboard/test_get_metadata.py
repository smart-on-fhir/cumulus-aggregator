import json

import pytest

from src.handlers.dashboard.get_metadata import metadata_handler
from tests import mock_utils


@pytest.mark.parametrize(
    "params,status,expected",
    [
        (None, 200, mock_utils.get_mock_metadata()),
        (
            {"site": mock_utils.EXISTING_SITE},
            200,
            mock_utils.get_mock_metadata()[mock_utils.EXISTING_SITE],
        ),
        (
            {"site": mock_utils.EXISTING_SITE, "study": mock_utils.EXISTING_STUDY},
            200,
            mock_utils.get_mock_metadata()[mock_utils.EXISTING_SITE][mock_utils.EXISTING_STUDY],
        ),
        (
            {
                "site": mock_utils.EXISTING_SITE,
                "study": mock_utils.EXISTING_STUDY,
                "data_package": mock_utils.EXISTING_DATA_P,
            },
            200,
            mock_utils.get_mock_metadata()[mock_utils.EXISTING_SITE][mock_utils.EXISTING_STUDY][
                mock_utils.EXISTING_DATA_P
            ],
        ),
        (
            {
                "site": mock_utils.EXISTING_SITE,
                "study": mock_utils.EXISTING_STUDY,
                "data_package": mock_utils.EXISTING_DATA_P,
                "version": mock_utils.EXISTING_VERSION,
            },
            200,
            mock_utils.get_mock_metadata()[mock_utils.EXISTING_SITE][mock_utils.EXISTING_STUDY][
                mock_utils.EXISTING_DATA_P
            ][mock_utils.EXISTING_VERSION],
        ),
        ({"site": mock_utils.NEW_SITE, "study": mock_utils.EXISTING_STUDY}, 500, None),
        ({"site": mock_utils.EXISTING_SITE, "study": mock_utils.NEW_STUDY}, 500, None),
    ],
)
def test_get_metadata(mock_bucket, params, status, expected):
    event = {"pathParameters": params}

    res = metadata_handler(event, {})
    assert res["statusCode"] == status
    if status == 200:
        assert json.loads(res["body"]) == expected
