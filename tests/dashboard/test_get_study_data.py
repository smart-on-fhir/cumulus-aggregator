import json

import pytest

from src.dashboard.get_study_data import get_study_data
from tests import mock_utils


@pytest.mark.parametrize(
    "params,status,expected",
    [
        (None, 200, mock_utils.get_mock_study_data()),
        (
            {"study": mock_utils.EXISTING_STUDY},
            200,
            mock_utils.get_mock_study_data()[mock_utils.EXISTING_STUDY],
        ),
        (
            {"study": mock_utils.EXISTING_STUDY, "version": mock_utils.EXISTING_VERSION},
            200,
            mock_utils.get_mock_study_data()[mock_utils.EXISTING_STUDY][
                mock_utils.EXISTING_VERSION
            ],
        ),
        ({"study": mock_utils.NEW_STUDY, "version": mock_utils.EXISTING_VERSION}, 500, None),
        ({"study": mock_utils.EXISTING_STUDY, "version": mock_utils.NEW_VERSION}, 500, None),
    ],
)
def test_get_study_data(mock_bucket, params, status, expected):
    event = {"pathParameters": params}
    res = get_study_data.study_data_handler(event, {})
    assert res["statusCode"] == status
    if status == 200:
        assert json.loads(res["body"]) == expected
