import boto3
import pytest
from freezegun import freeze_time

from src.shared import enums, functions
from src.site_upload.unzip_upload import unzip_upload
from tests import mock_utils


@freeze_time("2020-01-01")
@pytest.mark.parametrize(
    "upload_file,site,study,version,status,expected_contents",
    [
        (  # Adding a new data package to a site with uploads
            "./tests/test_data/exports/upload.zip",
            mock_utils.EXISTING_SITE,
            mock_utils.EXISTING_STUDY,
            mock_utils.EXISTING_VERSION,
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
    site,
    study,
    version,
    status,
    expected_contents,
    mock_bucket,
    mock_notification,
    mock_env,
):
    s3_client = boto3.client("s3", region_name="us-east-1")
    upload_key = functions.construct_s3_key(
        enums.BucketPath.UPLOAD_STAGING,
        study=mock_utils.EXISTING_STUDY,
        site=mock_utils.EXISTING_SITE,
        version=mock_utils.EXISTING_VERSION,
        filename="upload.zip",
    )
    archive_key = functions.construct_s3_key(
        enums.BucketPath.ARCHIVE,
        study=mock_utils.EXISTING_STUDY,
        site=mock_utils.EXISTING_SITE,
        version=mock_utils.EXISTING_VERSION,
        filename="upload.zip",
    )
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
            upload_key,
        )
    event = {
        "Records": [
            {
                "awsRegion": "us-east-1",
                "s3": {"object": {"key": upload_key}},
            }
        ]
    }

    res = unzip_upload.unzip_upload_handler(event, {})
    assert res["statusCode"] == status
    s3_res = s3_client.list_objects_v2(Bucket=mock_utils.TEST_BUCKET)
    assert len(s3_res["Contents"]) == mock_utils.ITEM_COUNT + len(expected_contents) + 1
    s3_res = s3_client.list_objects_v2(
        Bucket=mock_utils.TEST_BUCKET, Prefix=enums.BucketPath.UPLOAD
    )
    for file in s3_res["Contents"]:
        assert any(file["Key"].endswith(expected) for expected in expected_contents)
        dp_meta = functions.parse_s3_key(file["Key"])
        assert dp_meta.site == site
        assert dp_meta.study == study
        assert dp_meta.version == version

    s3_res = s3_client.list_objects_v2(
        Bucket=mock_utils.TEST_BUCKET, Prefix=enums.BucketPath.ARCHIVE
    )
    assert s3_res["Contents"][0]["Key"] == archive_key
