import json
from datetime import UTC, datetime

import boto3
import pytest
from freezegun import freeze_time

from src.shared import enums, functions
from src.site_upload.study_period import study_period
from tests import mock_utils


@freeze_time("2020-01-01")
@pytest.mark.parametrize(
    "upload_file,upload_path,event_key,multiple_files,status",
    [
        (  # Adding a new study to an existing site
            "./tests/test_data/meta_date.parquet",
            (
                f"/{mock_utils.NEW_STUDY}/{mock_utils.NEW_STUDY}__{mock_utils.EXISTING_DATA_P}/{mock_utils.EXISTING_SITE}"
                f"/{mock_utils.EXISTING_VERSION}/test_meta_date.parquet"
            ),
            (
                f"/{mock_utils.NEW_STUDY}/{mock_utils.NEW_STUDY}__{mock_utils.EXISTING_DATA_P}/{mock_utils.EXISTING_SITE}"
                f"/{mock_utils.EXISTING_VERSION}/test_meta_date.parquet"
            ),
            False,
            200,
        ),
        (  # Adding a new study to a new site
            "./tests/test_data/meta_date.parquet",
            f"/{mock_utils.NEW_STUDY}/{mock_utils.NEW_STUDY}__{mock_utils.EXISTING_DATA_P}/{mock_utils.NEW_SITE}"
            f"/{mock_utils.EXISTING_VERSION}/test_meta_date.parquet",
            f"/{mock_utils.NEW_STUDY}/{mock_utils.NEW_STUDY}__{mock_utils.EXISTING_DATA_P}/{mock_utils.NEW_SITE}"
            f"/{mock_utils.EXISTING_VERSION}/test_meta_date.parquet",
            False,
            200,
        ),
        (  # newer version of existing study
            "./tests/test_data/meta_date.parquet",
            (
                f"/{mock_utils.EXISTING_STUDY}/{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}"
                f"/{mock_utils.EXISTING_SITE}/{mock_utils.NEW_VERSION}/test_meta_date.parquet"
            ),
            (
                f"/{mock_utils.EXISTING_STUDY}/{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}"
                f"/{mock_utils.EXISTING_SITE}/{mock_utils.NEW_VERSION}/test_meta_date.parquet"
            ),
            False,
            200,
        ),
        (  # updating an existing study
            "./tests/test_data/meta_date.parquet",
            (
                f"/{mock_utils.EXISTING_STUDY}/{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}"
                f"/{mock_utils.EXISTING_SITE}/{mock_utils.EXISTING_VERSION}/test_meta_date.parquet"
            ),
            (
                f"/{mock_utils.EXISTING_STUDY}/{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}"
                f"/{mock_utils.EXISTING_SITE}/{mock_utils.EXISTING_VERSION}/test_meta_date.parquet"
            ),
            False,
            200,
        ),
        (  # updating an existing study
            "./tests/test_data/meta_date.parquet",
            (
                f"/{mock_utils.EXISTING_STUDY}/{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}"
                f"/{mock_utils.EXISTING_SITE}/{mock_utils.EXISTING_VERSION}/test_meta_date.parquet"
            ),
            (
                f"/{mock_utils.EXISTING_STUDY}/{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}"
                f"/{mock_utils.EXISTING_SITE}/{mock_utils.EXISTING_VERSION}/test_meta_date.parquet"
            ),
            False,
            200,
        ),
        (  # updating an existing study with a differently named file
            "./tests/test_data/meta_date.parquet",
            (
                f"/{mock_utils.EXISTING_STUDY}/{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}"
                f"/{mock_utils.EXISTING_SITE}/{mock_utils.EXISTING_VERSION}/test_meta_date.parquet"
            ),
            (
                f"/{mock_utils.EXISTING_STUDY}/{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}"
                f"/{mock_utils.EXISTING_SITE}/{mock_utils.EXISTING_VERSION}/test_meta_date.parquet"
            ),
            True,
            200,
        ),
        (  # invalid file
            "./tests/test_data/meta_date.parquet",
            None,
            (
                f"/{mock_utils.EXISTING_STUDY}/{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}"
                f"/{mock_utils.EXISTING_SITE}/{mock_utils.EXISTING_VERSION}/wrong.parquet"
            ),
            False,
            500,
        ),
    ],
)
def test_process_upload(
    upload_file,
    upload_path,
    event_key,
    multiple_files,
    status,
    mock_bucket,
    mock_queue,
):
    s3_client = boto3.client("s3", region_name="us-east-1")
    sqs_client = boto3.client("sqs", region_name="us-east-1")
    # we'll get rid of the fixture data in study_meta because we care
    # about timestamps in this case
    # TODO - consider having a intermediate position for files to land so
    # we don't have to do timestamp processing
    s3_client.delete_object(
        Bucket=mock_utils.TEST_BUCKET,
        Key=(
            "study_metadata/study/study__encounter/princeton_plainsboro_teaching_hospital/"
            "099/study__meta_date.parquet"
        ),
    )
    if multiple_files:
        s3_client.upload_file(
            upload_file,
            mock_utils.TEST_BUCKET,
            (
                f"{enums.BucketPath.STUDY_META.value}"
                f"/{mock_utils.EXISTING_STUDY}/{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}"
                f"/{mock_utils.EXISTING_SITE}/{mock_utils.EXISTING_VERSION}/test_old_meta_date.parquet"
            ),
        )
    if upload_file is not None:
        s3_client.upload_file(
            upload_file,
            mock_utils.TEST_BUCKET,
            f"{enums.BucketPath.STUDY_META.value}{upload_path}",
        )
    event = {"Records": [{"Sns": {"Message": f"{enums.BucketPath.STUDY_META.value}{event_key}"}}]}
    res = study_period.study_period_handler(event, {})
    assert res["statusCode"] == status
    if upload_file is not None and upload_path is not None:
        sqs_res = sqs_client.receive_message(
            QueueUrl=mock_utils.TEST_METADATA_UPDATE_URL, MaxNumberOfMessages=10
        )
        assert len(sqs_res["Messages"]) == 1
        message = json.loads(sqs_res["Messages"][0]["Body"])
        assert message["key"] == "metadata/study_periods.json"
        update = json.loads(message["updates"])
        print(update)
        dp_meta = functions.parse_s3_key(f"{enums.BucketPath.STUDY_META.value}{event_key}")
        assert (
            update[dp_meta.site][dp_meta.study][dp_meta.version]["earliest_date"]
            == "2016-06-01T00:00:00"
        )
        assert (
            update[dp_meta.site][dp_meta.study][dp_meta.version]["latest_date"]
            == "2023-04-09T00:00:00"
        )
        assert (
            update[dp_meta.site][dp_meta.study][dp_meta.version]["last_data_update"]
            == datetime.now(UTC).isoformat()
        )
    if multiple_files:
        res = s3_client.list_objects_v2(
            Bucket=mock_utils.TEST_BUCKET,
            Prefix=(
                f"{enums.BucketPath.STUDY_META.value}"
                f"/{mock_utils.EXISTING_STUDY}/{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}"
                f"/{mock_utils.EXISTING_SITE}/{mock_utils.EXISTING_VERSION}"
            ),
        )
        assert len(res["Contents"]) == 1
        assert res["Contents"][0]["Key"] == f"{enums.BucketPath.STUDY_META.value}{upload_path}"
        assert res["Contents"][0]["LastModified"] == datetime(2020, 1, 1, tzinfo=UTC)
