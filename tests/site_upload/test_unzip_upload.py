import boto3
import pytest
from freezegun import freeze_time

from src.shared import enums
from src.site_upload.unzip_upload import unzip_upload
from tests import mock_utils


@freeze_time("2020-01-01")
@pytest.mark.parametrize(
    "upload_file,upload_path,status,expected_contents",
    [
        (  # Adding a new data package to a site with uploads
            "./tests/test_data/exports/upload.zip",
            f"/{mock_utils.EXISTING_STUDY}/{mock_utils.EXISTING_SITE}/{mock_utils.EXISTING_VERSION}/upload.zip",
            200,
            [
                "manifest.toml",
                "upload__count_synthea_patient.cube.parquet",
                "upload__meta_date.meta.parquet",
                "upload__meta_version.meta.parquet",
            ],
        ),
    ],
)
def test_unzip_upload(
    upload_file,
    upload_path,
    status,
    expected_contents,
    mock_bucket,
    mock_notification,
    mock_env,
):
    s3_client = boto3.client("s3", region_name="us-east-1")
    mock_utils.put_mock_transaction(
        s3_client=s3_client,
        site=mock_utils.EXISTING_SITE,
        study=mock_utils.EXISTING_STUDY,
        transaction=mock_utils.get_mock_transaction(),
    )
    if upload_file is not None:
        s3_client.upload_file(
            upload_file,
            mock_utils.TEST_BUCKET,
            f"{enums.BucketPath.UPLOAD_STAGING.value}{upload_path}",
        )
    event = {
        "Records": [
            {
                "awsRegion": "us-east-1",
                "s3": {"object": {"key": f"{enums.BucketPath.UPLOAD_STAGING.value}{upload_path}"}},
            }
        ]
    }

    res = unzip_upload.unzip_upload_handler(event, {})
    assert res["statusCode"] == status
    s3_res = s3_client.list_objects_v2(Bucket=mock_utils.TEST_BUCKET)
    assert len(s3_res["Contents"]) == mock_utils.ITEM_COUNT + len(expected_contents) + 2
    s3_res = s3_client.list_objects_v2(
        Bucket=mock_utils.TEST_BUCKET, Prefix=enums.BucketPath.UPLOAD.value
    )
    for file in s3_res["Contents"]:
        assert any(file["Key"].endswith(expected) for expected in expected_contents)
    s3_res = s3_client.list_objects_v2(
        Bucket=mock_utils.TEST_BUCKET, Prefix=enums.BucketPath.ARCHIVE.value
    )
    assert s3_res["Contents"][0]["Key"] == (
        f"{enums.BucketPath.ARCHIVE.value}{upload_path}.2020-01-01T00:00:00+00:00"
    )
