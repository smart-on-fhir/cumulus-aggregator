import json
import os
from contextlib import nullcontext as does_not_raise
from unittest import mock

import boto3
import pytest

from src.handlers.dashboard import get_csv
from src.handlers.shared import enums
from tests import mock_utils

# data matching these params is created via conftest
site = mock_utils.EXISTING_SITE
study = mock_utils.EXISTING_STUDY
data_package = mock_utils.EXISTING_DATA_P
version = mock_utils.EXISTING_VERSION
filename = filename = f"{study}__{data_package}__aggregate.csv"


def _mock_last_valid():
    bucket = os.environ["BUCKET_NAME"]
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.upload_file(
        "./tests/test_data/count_synthea_patient_agg.csv",
        bucket,
        f"{enums.BucketPath.LAST_VALID.value}/{study}/"
        f"{study}__{data_package}/{site}/{version}/{filename}",
    )


@pytest.mark.parametrize(
    "params,status,expected",
    [
        (
            {
                "site": None,
                "study": study,
                "data_package": data_package,
                "version": version,
                "filename": filename,
            },
            302,
            None,
        ),
        (
            {
                "site": site,
                "study": study,
                "data_package": data_package,
                "version": version,
                "filename": filename,
            },
            302,
            None,
        ),
        (
            {
                "study": study,
                "data_package": data_package,
                "version": version,
                "filename": filename,
            },
            302,
            None,
        ),
        (
            {
                "site": site,
                "study": study,
                "data_package": data_package,
                "version": version,
                "filename": "foo",
            },
            500,
            None,
        ),
        (
            {
                "site": None,
                "study": None,
                "data_package": None,
                "version": None,
                "filename": None,
            },
            500,
            None,
        ),
    ],
)
@mock.patch.dict(os.environ, mock_utils.MOCK_ENV)
def test_get_csv(mock_bucket, params, status, expected):
    event = {"pathParameters": params}
    if "site" in params and params["site"] is not None:
        _mock_last_valid()
    res = get_csv.get_csv_handler(event, {})
    assert res["statusCode"] == status
    if status == 302:
        if "site" not in params or params["site"] is None:
            url = (
                "https://cumulus-aggregator-site-counts-test.s3.amazonaws.com/csv_aggregates/"
                f"{study}/{study}__{data_package}/{version}/{filename}"
            )
        else:
            url = (
                "https://cumulus-aggregator-site-counts-test.s3.amazonaws.com/last_valid/"
                f"{study}/{study}__{data_package}/{site}/{version}/{filename}"
            )
        assert res["headers"]["x-column-types"] == "integer,string,integer,string,string"
        assert res["headers"]["x-column-names"] == "cnt,gender,age,race_display,site"
        assert res["headers"]["x-column-descriptions"] == ""
        assert res["headers"]["Location"].startswith(url)


@pytest.mark.parametrize(
    "path,status,expected,raises",
    [
        (
            "/aggregates",
            200,
            [
                "aggregates/other_study/encounter/099/other_study__encounter__aggregate.csv",
                "aggregates/study/encounter/099/study__encounter__aggregate.csv",
            ],
            does_not_raise(),
        ),
        (
            "/last-valid",
            200,
            [
                "last_valid/study/encounter/princeton_plainsboro_teaching_hospital/099/study__encounter__aggregate.csv"
            ],
            does_not_raise(),
        ),
        ("/some_other_endpoint", 500, [], does_not_raise()),
    ],
)
@mock.patch.dict(os.environ, mock_utils.MOCK_ENV)
def test_get_csv_list(mock_bucket, path, status, expected, raises):
    with raises:
        if path.startswith("/last-valid"):
            _mock_last_valid()
        event = {"path": path}
        res = get_csv.get_csv_list_handler(event, {})
        keys = json.loads(res["body"])
        assert res["statusCode"] == status
        if status == 200:
            assert keys == expected
