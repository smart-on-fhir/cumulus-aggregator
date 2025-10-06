"""
The intent of this test is to check the most common front to back workflow in the
aggregator, i.e. given a file has been uploaded, is it available to be retrieved
from the dashboard chart API. Failure cases are left to the various unit tests,
this is intended to cover interactions across the various lambdas.

We have to mock a lot of AWS infrastructure to do this, since we can't do event
processing or athena queries locally.
"""

import datetime
import json
import pathlib
import zipfile
from unittest import mock

import boto3
import pandas
import pytest
import toml
from freezegun import freeze_time
from moto.core import DEFAULT_ACCOUNT_ID
from moto.sns import sns_backends

from scripts import reset_data_package_cache
from src.dashboard.get_chart_data import get_chart_data
from src.dashboard.get_data_packages import get_data_packages
from src.dashboard.get_from_parquet import get_from_parquet
from src.shared import enums, functions
from src.site_upload.cache_api import cache_api
from src.site_upload.powerset_merge import powerset_merge
from src.site_upload.process_flat import process_flat
from src.site_upload.process_upload import process_upload
from src.site_upload.unzip_upload import unzip_upload
from src.site_upload.update_metadata import update_metadata
from tests import mock_utils

CURRENT_COL_TYPES_VERSION = "3"


@freeze_time("2025-06-06")
@pytest.mark.parametrize(
    "upload_file,upload_type,study,site,data_package,version,existing,run_migration",
    [
        (
            pathlib.Path(__file__).parent / "test_data/mock_cube_col_types.parquet",
            "cube",
            mock_utils.NEW_STUDY,
            mock_utils.NEW_SITE,
            mock_utils.NEW_DATA_P,
            mock_utils.NEW_VERSION,
            False,
            False,
        ),
        (
            pathlib.Path(__file__).parent / "test_data/mock_cube_col_types.parquet",
            "cube",
            mock_utils.NEW_STUDY,
            mock_utils.NEW_SITE,
            mock_utils.NEW_DATA_P,
            mock_utils.NEW_VERSION,
            False,
            True,
        ),
        (
            pathlib.Path(__file__).parent / "test_data/count_synthea_patient.parquet",
            "cube",
            mock_utils.EXISTING_STUDY,
            mock_utils.EXISTING_SITE,
            mock_utils.EXISTING_DATA_P,
            mock_utils.EXISTING_VERSION,
            True,
            False,
        ),
        (
            pathlib.Path(__file__).parent / "test_data/count_synthea_patient.parquet",
            "cube",
            mock_utils.EXISTING_STUDY,
            mock_utils.EXISTING_SITE,
            mock_utils.EXISTING_DATA_P,
            mock_utils.EXISTING_VERSION,
            True,
            True,
        ),
        (
            pathlib.Path(__file__).parent / "test_data/flat_synthea_q_date_recent.parquet",
            "flat",
            mock_utils.NEW_STUDY,
            mock_utils.NEW_SITE,
            mock_utils.NEW_FLAT_DATA_P,
            mock_utils.NEW_VERSION,
            False,
            False,
        ),
        (
            pathlib.Path(__file__).parent / "test_data/flat_synthea_q_date_recent.parquet",
            "flat",
            mock_utils.NEW_STUDY,
            mock_utils.NEW_SITE,
            mock_utils.NEW_FLAT_DATA_P,
            mock_utils.NEW_VERSION,
            False,
            True,
        ),
        (
            pathlib.Path(__file__).parent / "test_data/flat_synthea_q_date_recent.parquet",
            "flat",
            mock_utils.EXISTING_STUDY,
            mock_utils.EXISTING_SITE,
            mock_utils.EXISTING_FLAT_DATA_P,
            mock_utils.EXISTING_VERSION,
            True,
            False,
        ),
        (
            pathlib.Path(__file__).parent / "test_data/flat_synthea_q_date_recent.parquet",
            "flat",
            mock_utils.EXISTING_STUDY,
            mock_utils.EXISTING_SITE,
            mock_utils.EXISTING_FLAT_DATA_P,
            mock_utils.EXISTING_VERSION,
            True,
            True,
        ),
    ],
)
def test_integration(
    tmp_path,
    mock_bucket,
    mock_notification,
    mock_queue,
    upload_file,
    upload_type,
    study,
    site,
    data_package,
    version,
    existing,
    run_migration,
):
    s3_client = boto3.client("s3", region_name="us-east-1")
    sns_backend = sns_backends[DEFAULT_ACCOUNT_ID]["us-east-1"]
    sqs_client = boto3.client("sqs", region_name="us-east-1")
    # Upload prep
    # TODO: consider importing the library and using it to create a manifest
    with open(tmp_path / "manifest.toml", "w") as manifest_file:
        manifest = {}
        manifest["study_prefix"] = study
        manifest["export_config"] = {}
        if upload_type == "flat":
            manifest["export_config"]["flat_list"] = [data_package]
        else:
            manifest["export_config"]["count_list"] = [data_package]
        toml.dump(manifest, manifest_file)
    with zipfile.ZipFile(tmp_path / "upload.zip", "w") as upload_zip:
        upload_zip.write(upload_file, arcname=f"{data_package}.{upload_type}.parquet")
        upload_zip.write(tmp_path / "manifest.toml", arcname="manifest.toml")
        upload_zip.write(
            pathlib.Path(__file__).parent / "test_data/meta_date.parquet",
            arcname="meta_date.parquet",
        )
    upload_key = f"{enums.BucketPath.UPLOAD_STAGING.value}/{study}/{site}/{version}/upload.zip"
    s3_client.put_object(
        Bucket=mock_utils.TEST_BUCKET,
        Key=f"{enums.BucketPath.META.value}/transactions/{site}__{study}.json",
        Body=json.dumps(
            {"id": "12345", "uploaded_at": datetime.datetime.now(datetime.UTC).isoformat()}
        ).encode("UTF-8"),
    )

    # Expected tables based on the mock bucket configuration
    tables = [
        f"{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}__{mock_utils.EXISTING_VERSION}",
        f"{mock_utils.OTHER_STUDY}__{mock_utils.EXISTING_DATA_P}__{mock_utils.EXISTING_VERSION}",
        f"{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_FLAT_DATA_P}__{mock_utils.EXISTING_SITE}__{mock_utils.EXISTING_VERSION}",
        f"{mock_utils.OTHER_STUDY}__{mock_utils.EXISTING_FLAT_DATA_P}__{mock_utils.EXISTING_SITE}__{mock_utils.EXISTING_VERSION}",
    ]

    # Get a copy of the uploaded file into memory for reference later
    reference_df = pandas.read_parquet(upload_file)

    # Make sure that the migration is not introducing any unusual behavior,
    # or if not running, ensure that the test bucket configuration is behaving
    # the same way
    if run_migration:
        with mock.patch(
            "awswrangler.athena.read_sql_query",
            lambda query, database, s3_output, workgroup: pandas.DataFrame(
                data=pandas.DataFrame({"table_name": tables})
            ),
        ):
            reset_data_package_cache.reset_data_package_cache(mock_utils.TEST_BUCKET, "mock_db")

    # grab the data packages list before modifications for later validation
    dp_before_event = {
        "queryStringParameters": [],
        "multiValueQueryStringParameters": {},
        "pathParameters": [],
    }
    # TODO: supress this info warning?
    dp_before = get_data_packages.data_packages_handler(dp_before_event, [])

    # Mock the upload and the event kicking off the aggregator processing pipeline
    s3_client.upload_file(tmp_path / "upload.zip", mock_utils.TEST_BUCKET, upload_key)
    upload_event = {"Records": [{"awsRegion": "us-east-1", "s3": {"object": {"key": upload_key}}}]}
    unzip_res = unzip_upload.unzip_upload_handler(upload_event, {})
    assert unzip_res["statusCode"] == 200

    # Mock running the upload processing for the unzipped files
    files = s3_client.list_objects_v2(
        Bucket=mock_utils.TEST_BUCKET, Prefix=enums.BucketPath.UPLOAD.value
    )
    keys = list(file["Key"] for file in files["Contents"])
    for key in keys:
        if "manifest.toml" not in key:
            upload_event = {
                "Records": [
                    {
                        "awsRegion": "us-east-1",
                        "Sns": {"Message": key},
                    }
                ]
            }
            upload_res = process_upload.process_upload_handler(upload_event, {})
            assert upload_res["statusCode"] == 200

    # We'll need to construct events from the moto sns mock message backend, which looks like this:
    # [(event_id, s3_key, event id, unused field, unused field)]
    # Then we'll pass to the appropriate handler based on type

    if upload_type == enums.UploadTypes.CUBE.value:
        counts_sns = sns_backend.topics[mock_utils.TEST_PROCESS_COUNTS_ARN].sent_notifications
        counts_event = {
            "Records": [
                {
                    "Sns": {
                        "TopicArn": mock_utils.TEST_PROCESS_COUNTS_ARN,
                        "Message": counts_sns[0][1],
                    }
                }
            ]
        }
        merge_res = powerset_merge.powerset_merge_handler(counts_event, {})
        assert merge_res["statusCode"] == 200

    elif upload_type == enums.UploadTypes.FLAT.value:
        counts_sns = sns_backend.topics[mock_utils.TEST_PROCESS_FLAT_ARN].sent_notifications
        counts_event = {
            "Records": [
                {"Sns": {"TopicArn": mock_utils.TEST_PROCESS_FLAT_ARN, "Message": counts_sns[0][1]}}
            ]
        }
        merge_res = process_flat.process_flat_handler(counts_event, {})
        assert merge_res["statusCode"] == 200

    # we have some items in the FIFO queue we'll need to process to get into the caches,
    # so we'll grab the queue contents and run them through the update processor
    res = sqs_client.receive_message(
        QueueUrl=mock_utils.TEST_METADATA_UPDATE_URL, MaxNumberOfMessages=10
    )
    sqs_event = {"Records": res["Messages"]}
    update_res = update_metadata.update_metadata_handler(sqs_event, {})
    assert update_res["statusCode"] == 200

    # The cache is triggered by an S3 event set up in the cloudformation template. We'll have
    # to mock this event as well by expected file path
    cache_event = {"Records": [{"Sns": {"Subject": enums.JsonFilename.DATA_PACKAGES.value}}]}
    # Add an extra table to the list if we've created one, and mock the table list for cache_api
    if upload_type == enums.UploadTypes.CUBE.value:
        if f"{study}__{data_package}__{version}" not in tables:
            tables.append(f"{study}__{data_package}__{version}")
    elif upload_type == enums.UploadTypes.FLAT.value:
        if f"{study}__{data_package}__{site}__{version}" not in tables:
            tables.append(f"{study}__{data_package}__{site}__{version}")
    with mock.patch(
        "awswrangler.athena.read_sql_query",
        lambda query, database, s3_output, workgroup: pandas.DataFrame(
            data=pandas.DataFrame({"table_name": tables})
        ),
    ):
        cache_res = cache_api.cache_api_handler(cache_event, {})
        assert cache_res["statusCode"] == 200

    # Then do some comparisons to the pre-processed version to make sure things ended up
    # in the right place
    dp_after = json.loads(get_data_packages.data_packages_handler({}, [])["body"])
    if existing:
        len(dp_after) == len(dp_before)
    else:
        len(dp_after) == len(dp_before) + 1
        match upload_type:
            case enums.UploadTypes.CUBE.value:
                expected_id = f"{study}__{data_package}__{version}"
            case enums.UploadTypes.FLAT.value:
                expected_id = f"{study}__{data_package}__{site}__{version}"
        if not any(x["id"] == expected_id for x in dp_after):
            raise KeyError("Expected data package id not found")
    for dp in dp_after:
        assert CURRENT_COL_TYPES_VERSION == dp["column_types_format_version"]
        assert len(dp["id"].split("__")) == 3 or len(dp["id"].split("__")) == 4
        if (
            upload_type == enums.UploadTypes.CUBE.value
            and dp["id"] == f"{study}__{data_package}__{version}"
        ) or (
            upload_type == enums.UploadTypes.FLAT.value
            and dp["id"] == f"{study}__{data_package}__{site}__{version}"
        ):
            assert version == dp["version"]
            assert study == dp["study"]
            match upload_type:
                case enums.UploadTypes.CUBE.value:
                    assert data_package == dp["name"]
                    assert (reference_df["cnt"].max()) == dp["total"]
                case enums.UploadTypes.FLAT.value:
                    assert data_package == f"{dp['name'].split('__')[0]}"
            for column in dp["columns"]:
                assert dp["columns"][column]["type"] in (
                    "year",
                    "month",
                    "week",
                    "day",
                    "integer",
                    "float",
                    "double",
                    "boolean",
                    "string",
                )
                if (
                    not (column.startswith("cnt") or column == "site")
                    and upload_type == enums.UploadTypes.CUBE.value
                ):
                    assert (
                        reference_df[column].nunique()
                        == dp["columns"][column]["distinct_values_count"]
                    )
            assert "2025-06-06T00:00:00+00:00" == dp["last_data_update"]
            chart_cols = dp["columns"]
            s3_path = dp["s3_path"]

    # We'll download the post-processing file from the mock bucket to fake query responses
    # from athena for getting individual chart data.
    # The mock bucket get_object doesn't implement seek, so we'll write a copy to disk
    with open(tmp_path / "processed.parquet", "wb") as f:
        f.write(
            s3_client.get_object(
                Bucket=mock_utils.TEST_BUCKET, Key=functions.get_s3_key_from_path(s3_path)
            )["Body"].read()
        )
    post_process_df = pandas.read_parquet(tmp_path / "processed.parquet")

    # ----------------
    # Some of our test data uses types we no longer expect (i.e. integers)
    # which collide with 'cumulus__none'.
    # Changing this breaks a lot of tests, so for now we'll paper over it.
    # TODO: update test data
    if "age" in post_process_df.columns:
        post_process_df = post_process_df.astype({"age": object})
    # ----------------

    # We'll approximate a sql query by slicing and returning unique values from
    # the post_process df
    def parse_select(query, database, s3_output, workgroup):
        cols = query.split("SELECT")[1].split("FROM")[0].replace('"', "").split(",")
        cols = [x.strip() for x in cols]
        selected = post_process_df[cols].drop_duplicates()
        na_fills = {}
        for column in selected.columns:
            if selected[column].dtype == "boolean":
                na_fills[column] = False
            else:
                na_fills[column] = "cumulus__none"
        return post_process_df[cols].drop_duplicates().fillna(na_fills)

    # Finally, we'll check the default endpoint once, and then iterate
    # through all the stratifier combinations to make sure we have valid
    # looking column/stratifier names
    match upload_type:
        case enums.UploadTypes.CUBE.value:
            chart_event = {
                "queryStringParameters": {"column": "site"},
                "multiValueQueryStringParameters": {},
                "pathParameters": {"data_package_id": f"{study}__{data_package}__{version}"},
            }
            with mock.patch("awswrangler.athena.read_sql_query", parse_select):
                chart_res = get_chart_data.chart_data_handler(chart_event, {})
            assert chart_res["statusCode"] == 200
            if upload_type == enums.UploadTypes.FLAT.value:
                return
            for column in chart_cols:
                if column == "cnt":
                    continue
                for stratifier in chart_cols:
                    if column == stratifier or stratifier == "cnt":
                        continue
                    chart_event = {
                        "queryStringParameters": {"column": column, "stratifier": stratifier},
                        "multiValueQueryStringParameters": {},
                        "pathParameters": {
                            "data_package_id": f"{study}__{data_package}__{version}"
                        },
                    }
                    with mock.patch("awswrangler.athena.read_sql_query", parse_select):
                        chart_res = get_chart_data.chart_data_handler(chart_event, {})
                    assert chart_res["statusCode"] == 200
                    body = json.loads(chart_res["body"])
                    expected_vals = body["counts"].keys()
                    for section in body["data"]:
                        for row in section["rows"]:
                            assert row[0] in expected_vals

        # Or, if it's flat, we'll just check to make sure that it's the originally uploaded file
        case enums.UploadTypes.FLAT.value:
            parquet_event = {
                "queryStringParameters": {"type": "csv", "s3_path": s3_path},
                "multiValueQueryStringParameters": {},
                "pathParameters": {},
            }
            csv_res = get_from_parquet.from_parquet_handler(parquet_event, {})
            assert csv_res["statusCode"] == 302
            url = csv_res["headers"]["location"]
            key = url.split("?")[0].replace(
                "https://cumulus-aggregator-site-counts-test.s3.amazonaws.com/", ""
            )
            res = s3_client.get_object(Bucket=mock_utils.TEST_BUCKET, Key=key)
            file = res["Body"].read().decode("utf-8")
            with open(tmp_path / "flat.csv", "w") as f:
                f.write(file)
            csv_df = pandas.read_csv(tmp_path / "flat.csv")
            assert reference_df.compare(csv_df).empty
