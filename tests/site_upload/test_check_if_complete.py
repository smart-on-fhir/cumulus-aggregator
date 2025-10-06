import json
import time
from datetime import datetime
from unittest import mock

import boto3
import pandas
from freezegun import freeze_time

from src.shared import enums
from src.site_upload.check_if_complete import check_if_complete
from tests import mock_utils

transaction_key = (
    f"{enums.BucketPath.META.value}/transactions/"
    f"{mock_utils.EXISTING_SITE}__{mock_utils.NEW_STUDY}.json"
)

expected_tables = [
    f"{mock_utils.NEW_STUDY}__{mock_utils.NEW_DATA_P}__{mock_utils.EXISTING_VERSION}",
    f"{mock_utils.NEW_STUDY}__{mock_utils.EXISTING_DATA_P}__{mock_utils.EXISTING_VERSION}",
    (
        f"{mock_utils.NEW_STUDY}__{mock_utils.EXISTING_DATA_P}__"
        f"{mock_utils.EXISTING_SITE}__{mock_utils.EXISTING_VERSION}"
    ),
]


def upload_file(filename, version, s3_client):
    dp = filename.split("__")[1].split(".")[0]
    if ".flat." in filename:
        key = (
            f"{enums.BucketPath.FLAT.value}/{mock_utils.NEW_STUDY}/{mock_utils.EXISTING_SITE}/"
            f"{mock_utils.NEW_STUDY}__{dp}__{mock_utils.EXISTING_SITE}__{version}/"
            f"{mock_utils.NEW_STUDY}__{dp}__{mock_utils.EXISTING_SITE}__flat.parquet"
        )
    else:
        key = (
            f"{enums.BucketPath.AGGREGATE.value}/{mock_utils.NEW_STUDY}/{mock_utils.NEW_STUDY}__{dp}/"
            f"{mock_utils.NEW_STUDY}__{dp}__{version}/{mock_utils.NEW_STUDY}__{dp}__aggregate.parquet"
        )

    s3_client.put_object(
        Bucket=mock_utils.TEST_BUCKET,
        Key=key,
        Body=json.dumps("foo").encode("UTF-8"),
    )


def reset_state(s3_client, transaction):
    s3_client.put_object(
        Bucket=mock_utils.TEST_BUCKET,
        Key=transaction_key,
        Body=json.dumps(transaction).encode("UTF-8"),
    )


def delete_transaction():
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.delete_object(Bucket=mock_utils.TEST_BUCKET, Key=transaction_key)


@freeze_time("2020-01-01 12:00:00")
@mock.patch("src.site_upload.check_if_complete.check_if_complete.sleep", returns=time.sleep(1))
def test_check_if_complete(mock_wait, mock_bucket, mock_notification, mock_glue):
    s3_client = boto3.client("s3", region_name="us-east-1")
    g_client = boto3.client("glue", region_name="us-east-1")
    transaction = {
        "id": "124e2e63-a28d-4d7c-85ba-afc84e5bc648",
        "uploaded_at": "2020-01-01 11:59:00+00:00",  # replace w timedelta
        "cube": [
            f"{mock_utils.NEW_STUDY}__{mock_utils.EXISTING_DATA_P}.cube.parquet",
            f"{mock_utils.NEW_STUDY}__{mock_utils.NEW_DATA_P}.cube.parquet",
        ],
        "flat": [f"{mock_utils.NEW_STUDY}__{mock_utils.EXISTING_DATA_P}.flat.parquet"],
        "annotated_cube": [],
        "version": mock_utils.EXISTING_VERSION,
    }
    reset_state(s3_client, transaction)
    event = {
        "Records": [
            {
                "Sns": {
                    "Message": json.dumps(
                        {
                            "site": mock_utils.EXISTING_SITE,
                            "study": mock_utils.NEW_STUDY,
                        }
                    ),
                    "TopicArn": mock_utils.TEST_COMPLETENESS_ARN,
                    "Timestamp": datetime.now().isoformat(),
                },
            }
        ]
    }

    # No finished aggregates present
    res = check_if_complete.check_if_complete_handler(event, {})
    assert res["body"] == '"Processing not completed"'

    # Upload a prior run of a study, but without the columns metadata - this function as
    # if a study has never been crawled before
    with freeze_time("2019-12-12"):
        for file in transaction["cube"]:
            upload_file(file, mock_utils.EXISTING_VERSION, s3_client)
    res = check_if_complete.check_if_complete_handler(event, {})
    assert res["body"] == '"Processing not completed"'

    # one aggregate has finished, but another has not
    with freeze_time("2020-01-01 12:01:00"):
        upload_file(transaction["cube"][0], mock_utils.EXISTING_VERSION, s3_client)
    res = check_if_complete.check_if_complete_handler(event, {})
    assert res["body"] == '"Processing not completed"'

    # All aggregates have finished, they are not in the db, so kick off a crawl
    with freeze_time("2020-01-01 12:01:00"):
        for file in transaction["cube"] + transaction["flat"]:
            upload_file(file, mock_utils.EXISTING_VERSION, s3_client)
    with freeze_time("2020-01-01 12:02:00"):
        with mock.patch("awswrangler.athena.read_sql_query") as query:
            query.return_value = pandas.DataFrame(data={"table_name": []})
            res = check_if_complete.check_if_complete_handler(event, {})
    body = (
        "Crawl for {"
        f"'site': '{mock_utils.EXISTING_SITE}', 'study': '{mock_utils.NEW_STUDY}'"
        "} initiated"
    )
    assert res["body"] == f'"{body}"'

    # Throw an error when the crawler is stuck in the running state.
    reset_state(s3_client, transaction)
    with freeze_time("2020-01-01 12:02:00"):
        with mock.patch("awswrangler.athena.read_sql_query") as query:
            query.return_value = pandas.DataFrame(data={"table_name": []})
            res = check_if_complete.check_if_complete_handler(event, {})
    body = "Error requesting crawl"
    assert res["body"] == f'"{body}"'
    # All aggregates have finished, they are not in the db, but a crawl has already started

    g_client.list_crawls(CrawlerName="cumulus-aggregator-test-crawler")
    with freeze_time("2020-01-01 12:02:00"):
        with mock.patch("awswrangler.athena.read_sql_query") as query:
            with mock.patch(
                "src.site_upload.check_if_complete.check_if_complete.mock_entrypoint"
            ) as entrypoint:
                entrypoint.side_effect = delete_transaction
                query.return_value = pandas.DataFrame(data={"table_name": []})
                res = check_if_complete.check_if_complete_handler(event, {})
    body = (
        "Processing request for {"
        f"'site': '{mock_utils.EXISTING_SITE}', 'study': '{mock_utils.NEW_STUDY}'"
        "} already sent"
    )
    assert res["body"] == f'"{body}"'

    g_client.list_crawls(CrawlerName="cumulus-aggregator-test-crawler")

    # The tables are now in the DB, so no crawl is required
    reset_state(s3_client, transaction)
    with freeze_time("2020-01-01 12:02:00"):
        with mock.patch("awswrangler.athena.read_sql_query") as query:
            query.return_value = pandas.DataFrame(data={"table_name": expected_tables})
            res = check_if_complete.check_if_complete_handler(event, {})
    body = (
        "Crawl for {"
        f"'site': '{mock_utils.EXISTING_SITE}', 'study': '{mock_utils.NEW_STUDY}'"
        "} not required, directly invoked caching"
    )
    assert res["body"] == f'"{body}"'

    # A crawl started that covers this data, so we don't need to invoke another
    # we invoke the list_crawls method here as a way of triggering a state change in
    # moto's mocking of crawlers
    reset_state(s3_client, transaction)
    g_client.list_crawls(CrawlerName="cumulus-aggregator-test-crawler")
    with mock.patch("awswrangler.athena.read_sql_query") as query:
        query.return_value = pandas.DataFrame(data={"table_name": []})
        res = check_if_complete.check_if_complete_handler(event, {})
    body = (
        "Recrawl request for {"
        f"'site': '{mock_utils.EXISTING_SITE}', 'study': '{mock_utils.NEW_STUDY}'"
        "} covered by subsequent request"
    )
    assert res["body"] == f'"{body}"'

    # There are no new data packages, so we can skip crawling.
    reset_state(s3_client, transaction)
    with freeze_time("2020-01-01 12:03:00"):
        with mock.patch("awswrangler.athena.read_sql_query") as query:
            query.return_value = pandas.DataFrame(data={"table_name": expected_tables})
            res = check_if_complete.check_if_complete_handler(event, {})
    body = (
        "Crawl for {"
        f"'site': '{mock_utils.EXISTING_SITE}', 'study': '{mock_utils.NEW_STUDY}'"
        "} not required, directly invoked caching"
    )
    assert res["body"] == f'"{body}"'

    # No new packages and transaction deleted, so this has already been queued by a parallel task.

    reset_state(s3_client, transaction)

    with freeze_time("2020-01-01 12:03:00"):
        with mock.patch("awswrangler.athena.read_sql_query") as query:
            with mock.patch(
                "src.site_upload.check_if_complete.check_if_complete.mock_entrypoint"
            ) as entrypoint:
                entrypoint.side_effect = delete_transaction
                query.return_value = pandas.DataFrame(data={"table_name": expected_tables})
                res = check_if_complete.check_if_complete_handler(event, {})
    body = (
        "Processing request for {"
        f"'site': '{mock_utils.EXISTING_SITE}', 'study': '{mock_utils.NEW_STUDY}'"
        "} already sent"
    )
    assert res["body"] == f'"{body}"'
