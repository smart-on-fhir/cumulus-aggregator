import json

import boto3
import pytest
import toml

from src.shared import enums, functions
from src.site_upload.process_manifest import process_manifest
from tests import mock_utils


@pytest.mark.parametrize(
    "filename,study,site,version,expected",
    [
        (
            "manifest.toml",
            mock_utils.EXISTING_STUDY,
            mock_utils.EXISTING_SITE,
            mock_utils.EXISTING_VERSION,
            {
                "study_prefix": mock_utils.EXISTING_STUDY,
                "description": "test",
                "study_owner": mock_utils.EXISTING_SITE,
            },
        ),
        (
            "manifest.toml",
            mock_utils.NEW_STUDY,
            mock_utils.NEW_SITE,
            mock_utils.NEW_VERSION,
            {
                "study_prefix": mock_utils.NEW_STUDY,
                "description": "test",
                "study_owner": mock_utils.NEW_SITE,
            },
        ),
        # This should get discarded as a non-owning site upload, so the
        # description should match the mock data.
        (
            "manifest.toml",
            mock_utils.EXISTING_STUDY,
            mock_utils.NEW_SITE,
            mock_utils.EXISTING_VERSION,
            {
                "study_prefix": mock_utils.EXISTING_STUDY,
                "description": "version 099 of study",
                "study_owner": mock_utils.EXISTING_SITE,
            },
        ),
        # This should get discarded as a non-manifest, so the
        # description should match the mock data.
        (
            "workflow.toml",
            mock_utils.EXISTING_STUDY,
            mock_utils.EXISTING_SITE,
            mock_utils.EXISTING_VERSION,
            {
                "study_prefix": mock_utils.EXISTING_STUDY,
                "description": "version 099 of study",
                "study_owner": mock_utils.EXISTING_SITE,
            },
        ),
    ],
)
def test_process_manifest(mock_bucket, tmp_path, filename, study, site, version, expected):
    print(tmp_path)
    dp_meta = functions.PackageMetadata(
        study=study,
        site=site,
        version=version,
    )
    with open(tmp_path / filename, "w") as f:
        if filename == "manifest.toml":
            toml.dump({"study_prefix": study, "description": "test"}, f)
        else:
            toml.dump({"config_type": "file_upload"}, f)
    manifest_key = functions.construct_s3_key(
        enums.BucketPath.MANIFEST, dp_meta=dp_meta, filename=filename
    )
    event = {"Records": [{"Sns": {"TopicArn": "arn", "Message": manifest_key}}]}
    s3_client = boto3.client("s3")
    files = [
        file["Key"] for file in s3_client.list_objects_v2(Bucket=mock_utils.TEST_BUCKET)["Contents"]
    ]
    s3_client.upload_file(
        Bucket=mock_utils.TEST_BUCKET,
        Key=event["Records"][0]["Sns"]["Message"],
        Filename=f"{tmp_path}/{filename}",
    )
    process_manifest.process_manifest_handler(event, {})
    files = [
        file["Key"] for file in s3_client.list_objects_v2(Bucket=mock_utils.TEST_BUCKET)["Contents"]
    ]
    assert f"{enums.BucketPath.MANIFEST}/{study}/{version}/manifest.json" in files
    manifest = json.load(
        s3_client.get_object(
            Bucket=mock_utils.TEST_BUCKET,
            Key=f"{enums.BucketPath.MANIFEST}/{study}/{version}/manifest.json",
        )["Body"]
    )
    assert manifest == expected
