import json

from src.shared import enums, s3_manager
from src.site_upload.lockfile_cleanup import lockfile_cleanup
from tests import mock_utils


def test_lockfile_cleanup(mock_bucket, mock_queue):
    manager = s3_manager.S3Manager(
        {}, site=mock_utils.EXISTING_SITE, study=mock_utils.EXISTING_STUDY
    )
    manager.request_or_validate_lock()
    assert (
        len(
            manager.s3_client.list_objects_v2(
                Bucket=manager.s3_bucket_name, Prefix=f"{enums.BucketPath.META.value}/lockfiles/"
            )["Contents"]
        )
        == 1
    )
    event = {
        "Records": [
            {
                "body": json.dumps(
                    {"site": mock_utils.EXISTING_SITE, "study": mock_utils.EXISTING_STUDY}
                )
            }
        ]
    }
    res = lockfile_cleanup.lockfile_cleanup_handler(event, {})
    assert res["statusCode"] == 200
    assert (
        "Contents"
        not in manager.s3_client.list_objects_v2(
            Bucket=manager.s3_bucket_name, Prefix=f"{enums.BucketPath.META.value}/lockfiles/"
        ).keys()
    )
