import boto3
import os

import pytest
from datetime import datetime, timezone
from freezegun import freeze_time

from src.handlers.shared.enums import BucketPath
from src.handlers.shared.functions import read_metadata, write_metadata
from src.handlers.site_upload.powerset_merge import powerset_merge_handler

from tests.utils import get_mock_metadata, TEST_BUCKET, ITEM_COUNT


@freeze_time("2020-01-01")
@pytest.mark.parametrize(
    "site,upload_file,upload_path,event_key,archives,status,expected_contents",
    [
        (  # Adding a new data package to a site with uploads
            "general_hospital",
            "./tests/test_data/count_synthea_patient.parquet",
            "/covid/encounter/general_hospital/patient.parquet",
            "/covid/encounter/general_hospital/patient.parquet",
            False,
            200,
            ITEM_COUNT + 3,
        ),
        (  # Adding a new data package to a site without uploads
            "chicago_hope",
            "./tests/test_data/count_synthea_patient.parquet",
            "/covid/encounter/chicago_hope/patient.parquet",
            "/covid/encounter/chicago_hope/patient.parquet",
            False,
            200,
            ITEM_COUNT + 3,
        ),
        (  # Updating an existing data package
            "general_hospital",
            "./tests/test_data/count_synthea_patient.parquet",
            "/covid/encounter/general_hospital/encounter.parquet",
            "/covid/encounter/general_hospital/encounter.parquet",
            True,
            200,
            ITEM_COUNT + 4,
        ),
        (  # Invalid parquet file
            "general_hospital",
            "./tests/site_upload/test_powerset_merge.py",
            "/covid/encounter/general_hospital/patient.parquet",
            "/covid/encounter/general_hospital/patient.parquet",
            False,
            500,
            ITEM_COUNT + 1,
        ),
    ],
)
def test_powerset_merge(
    site,
    upload_file,
    upload_path,
    event_key,
    archives,
    status,
    expected_contents,
    mock_bucket,
):
    s3_client = boto3.client("s3", region_name="us-east-1")
    if upload_file is not None:
        s3_client.upload_file(
            upload_file,
            TEST_BUCKET,
            f"{BucketPath.LATEST.value}{upload_path}",
        )
    if archives:
        s3_client.upload_file(
            upload_file,
            TEST_BUCKET,
            f"{BucketPath.LAST_VALID.value}{upload_path}",
        )
    event = {"Records": [{"Sns": {"Message": f"{BucketPath.LATEST.value}{event_key}"}}]}

    res = powerset_merge_handler(event, {})
    assert res["statusCode"] == status
    s3_res = s3_client.list_objects_v2(Bucket=TEST_BUCKET)
    assert len(s3_res["Contents"]) == expected_contents
    for item in s3_res["Contents"]:
        if item["Key"].endswith("aggregate.parquet"):
            assert item["Key"].startswith(BucketPath.AGGREGATE.value)
        elif item["Key"].endswith("aggregate.csv"):
            assert item["Key"].startswith(BucketPath.CSVAGGREGATE.value)
        elif item["Key"].endswith("transactions.json"):
            assert item["Key"].startswith(BucketPath.META.value)
            metadata = read_metadata(s3_client, TEST_BUCKET)
            if res["statusCode"] == 200:
                assert (
                    metadata[site]["covid"]["encounter"]["last_aggregation"]
                    == datetime.now(timezone.utc).isoformat()
                )
            else:
                assert (
                    metadata[site]["covid"]["encounter"]["last_aggregation"]
                    == get_mock_metadata()[site]["covid"]["encounter"][
                        "last_aggregation"
                    ]
                )
        else:
            assert (
                item["Key"].startswith(BucketPath.LAST_VALID.value)
                or item["Key"].startswith(BucketPath.ARCHIVE.value)
                or item["Key"].startswith(BucketPath.ERROR.value)
                or item["Key"].startswith(BucketPath.ADMIN.value)
                or item["Key"].endswith("study_periods.json")
            )
    if archives:
        keys = []
        for resource in s3_res["Contents"]:
            keys.append(resource["Key"])
        date_str = datetime.now(timezone.utc).isoformat()
        archive_path = f".{date_str}.".join(upload_path.split("."))
        assert f"{BucketPath.ARCHIVE.value}{archive_path}" in keys
