import json
import os
from unittest import mock

from src.handlers.dashboard.get_data_packages import data_packages_handler
from tests.mock_utils import DATA_PACKAGE_COUNT, MOCK_ENV


@mock.patch.dict(os.environ, MOCK_ENV)
def test_get_data_packages(mock_bucket):
    res = data_packages_handler({}, {})
    assert res["statusCode"] == 200
    assert DATA_PACKAGE_COUNT == 2
    assert "Access-Control-Allow-Origin" in res["headers"]
    assert json.loads(res["body"]) == {
        "study_valid": {
            "table": {
                "001": {
                    "column_types_format_version": "1",
                    "columns": {
                        "cnt": "integer",
                        "class_display": "string",
                        "servicetype_display": "string",
                        "period_start_month": "month",
                        "site": "string",
                    },
                    "last_data_update": "2024-09-25T20:12:45.193296+00:00",
                    "total": 31669,
                },
                "002": {
                    "column_types_format_version": "1",
                    "columns": {
                        "cnt": "integer",
                        "enc_class_display": "string",
                        "enc_service_display": "string",
                        "start_month": "month",
                        "site": "string",
                    },
                    "last_data_update": "2024-10-01T18:12:13.463978+00:00",
                    "total": 7695176,
                },
            }
        }
    }
