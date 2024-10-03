from contextlib import nullcontext as does_not_raise
from unittest import mock

import pandas
import pytest

from src.handlers.shared import functions


@pytest.mark.parametrize(
    "copy_res,del_res,raises",
    [
        (200, 204, does_not_raise()),
        (400, 204, pytest.raises(functions.S3UploadError)),
        (200, 400, pytest.raises(functions.S3UploadError)),
    ],
)
def test_move_s3_file(copy_res, del_res, raises):
    s3_client = mock.MagicMock()
    s3_client.copy_object.return_value = {"ResponseMetadata": {"HTTPStatusCode": copy_res}}
    s3_client.delete_object.return_value = {"ResponseMetadata": {"HTTPStatusCode": del_res}}
    with raises:
        functions.move_s3_file(
            s3_client=s3_client, s3_bucket_name="bucket", old_key="old", new_key="new"
        )


@pytest.mark.parametrize(
    "meta_type,raises",
    [("column_types", does_not_raise()), ("wrong_value", pytest.raises(ValueError))],
)
def test_check_meta_type(meta_type, raises):
    with raises:
        functions.check_meta_type(meta_type)


def test_column_datatypes():
    df = pandas.DataFrame(
        data={
            "study_year": ["2020-01-01"],
            "study_month": ["2020-01-01"],
            "study_week": ["2020-01-01"],
            "study_day": ["2020-01-01"],
            "cnt_item": [10],
            "int": pandas.Series([10], dtype="Int32"),
            "float": pandas.Series([10.1], dtype="Float32"),
            "bool": pandas.Series([True], dtype="boolean"),
            "string": ["string"],
        }
    )
    col_types = functions.get_column_datatypes(df.dtypes)
    assert col_types == {
        "study_year": "year",
        "study_month": "month",
        "study_week": "week",
        "study_day": "day",
        "cnt_item": "integer",
        "int": "integer",
        "float": "float",
        "bool": "boolean",
        "string": "string",
    }
