"""Storage for state variables/methods shared by test modules"""

import calendar
import datetime
import json

from src.shared import enums, functions

TEST_BUCKET = "cumulus-aggregator-site-counts-test"
TEST_WORKGROUP = "cumulus-aggregator-test-wg"
TEST_GLUE_DB = "cumulus-aggregator-test-db"
TEST_PROCESS_COUNTS_ARN = "arn:aws:sns:us-east-1:123456789012:test-counts"
TEST_PROCESS_FLAT_ARN = "arn:aws:sns:us-east-1:123456789012:test-flat"
TEST_PROCESS_STUDY_META_ARN = "arn:aws:sns:us-east-1:123456789012:test-meta"
TEST_CACHE_API_ARN = "arn:aws:sns:us-east-1:123456789012:test-cache"
TEST_PROCESS_UPLOADS_ARN = "arn:aws:sns:us-east-1:123456789012:test-uploads"
TEST_TRANSACTION_CLEANUP_URL = (
    "https://sqs.us-east-1.amazonaws.com/123456789012/test-transaction-cleanup"
)
TEST_TRANSACTION_CLEANUP_ARN = "arn:aws:sqs:us-east-1:123456789012:test-metadata-update"
TEST_METADATA_UPDATE_URL = "https://sqs.us-east-1.amazonaws.com/123456789012/test-metadata-update"
TEST_METADATA_UPDATE_ARN = "arn:aws:sqs:us-east-1:123456789012:test-metadata-update"
ITEM_COUNT = 11
DATA_PACKAGE_COUNT = 3

EXISTING_SITE = "princeton_plainsboro_teaching_hospital"
NEW_SITE = "chicago_hope"
OTHER_SITE = "st_elsewhere"
NEW_STUDY = "new_study"
EXISTING_STUDY = "study"
OTHER_STUDY = "other_study"
EXISTING_DATA_P = "encounter"
NEW_DATA_P = "document"
EXISTING_FLAT_DATA_P = "c_encounter"
NEW_FLAT_DATA_P = "c_document"
EXISTING_VERSION = "099"
NEW_VERSION = "100"

# This is a convenience for loading into os.environ with mock.patch.dict.
# Other cases should probably use the getter version below.
MOCK_ENV = {
    "BUCKET_NAME": TEST_BUCKET,
    "GLUE_DB_NAME": TEST_GLUE_DB,
    "WORKGROUP_NAME": TEST_WORKGROUP,
    "TOPIC_PROCESS_COUNTS_ARN": TEST_PROCESS_COUNTS_ARN,
    "TOPIC_PROCESS_FLAT_ARN": TEST_PROCESS_FLAT_ARN,
    "TOPIC_PROCESS_STUDY_META_ARN": TEST_PROCESS_STUDY_META_ARN,
    "TOPIC_CACHE_API_ARN": TEST_CACHE_API_ARN,
    "TOPIC_PROCESS_UPLOADS_ARN": TEST_PROCESS_UPLOADS_ARN,
    "QUEUE_TRANSACTION_CLEANUP": TEST_TRANSACTION_CLEANUP_URL,
    "QUEUE_METADATA_UPDATE": TEST_METADATA_UPDATE_URL,
}


def get_mock_metadata():
    return {
        EXISTING_SITE: {
            EXISTING_STUDY: {
                EXISTING_DATA_P: {
                    EXISTING_VERSION: {
                        "transaction_format_version": "2",
                        "last_upload": "2023-02-24T15:03:34+00:00",
                        "last_data_update": "2023-02-24T15:03:40.657583+00:00",
                        "last_aggregation": "2023-02-24T15:08:07.504595+00:00",
                        "last_error": None,
                        "deleted": None,
                    }
                },
                EXISTING_FLAT_DATA_P: {
                    EXISTING_VERSION: {
                        "transaction_format_version": "2",
                        "last_upload": "2023-02-24T15:03:34+00:00",
                        "last_data_update": "2023-02-24T15:03:40.657583+00:00",
                        "last_aggregation": "2023-02-24T15:08:07.504595+00:00",
                        "last_error": None,
                        "deleted": None,
                    }
                },
            },
            OTHER_STUDY: {
                EXISTING_DATA_P: {
                    EXISTING_VERSION: {
                        "transaction_format_version": "2",
                        "last_upload": "2023-02-24T15:43:57+00:00",
                        "last_data_update": "2023-02-24T15:44:03.861574+00:00",
                        "last_aggregation": "2023-02-24T15:44:03.861574+00:00",
                        "last_error": None,
                        "deleted": None,
                    }
                }
            },
        },
        OTHER_SITE: {
            EXISTING_STUDY: {
                EXISTING_DATA_P: {
                    EXISTING_VERSION: {
                        "transaction_format_version": "2",
                        "last_upload": "2023-02-24T15:08:06+00:00",
                        "last_data_update": "2023-02-24T15:08:07.771080+00:00",
                        "last_aggregation": "2023-02-24T15:08:07.771080+00:00",
                        "last_error": None,
                        "deleted": None,
                    }
                }
            }
        },
    }


def get_mock_study_metadata():
    return {
        EXISTING_SITE: {
            EXISTING_STUDY: {
                EXISTING_VERSION: {
                    "study_period_format_version": "2",
                    "last_data_update": "2023-02-24T15:03:40.657583+00:00",
                    "earliest_data": "2020-02-24T15:03:40.657583+00:00",
                    "latest_data": "2023-02-24T15:03:40.657583+00:00",
                }
            },
            OTHER_STUDY: {
                EXISTING_VERSION: {
                    "study_period_format_version": "2",
                    "last_data_update": "2023-02-24T15:44:03.861574+00:00",
                    "earliest_data": "2020-02-24T15:03:40.657583+00:00",
                    "latest_data": "2023-02-24T15:03:40.657583+00:00",
                }
            },
        },
        OTHER_SITE: {
            EXISTING_STUDY: {
                EXISTING_VERSION: {
                    "study_period_format_version": "2",
                    "last_data_update": "2023-02-24T15:08:07.771080+00:00",
                    "earliest_data": "2020-02-24T15:03:40.657583+00:00",
                    "latest_data": "2023-02-24T15:03:40.657583+00:00",
                }
            }
        },
    }


def get_mock_column_types_metadata():
    return {
        EXISTING_STUDY: {
            EXISTING_DATA_P: {
                f"{EXISTING_STUDY}__{EXISTING_DATA_P}__{EXISTING_VERSION}": {
                    "column_types_format_version": "3",
                    "columns": {
                        "cnt": {
                            "type": "integer",
                        },
                        "gender": {"type": "string", "distinct_values_count": 10},
                        "age": {"type": "integer", "distinct_values_count": 10},
                        "race_display": {"type": "string", "distinct_values_count": 10},
                        "site": {"type": "string", "distinct_values_count": 10},
                    },
                    "last_data_update": "2023-02-24T15:08:07.771080+00:00",
                    "s3_path": (
                        f"aggregates/{EXISTING_STUDY}/{EXISTING_STUDY}__{EXISTING_DATA_P}/"
                        f"{EXISTING_STUDY}__{EXISTING_DATA_P}__{EXISTING_VERSION}/"
                        f"{EXISTING_STUDY}__{EXISTING_DATA_P}__aggregate.parquet"
                    ),
                    "total": 1000,
                    "id": f"{EXISTING_STUDY}__{EXISTING_DATA_P}__{EXISTING_VERSION}",
                },
            },
            f"{EXISTING_FLAT_DATA_P}__{EXISTING_SITE}": {
                f"{EXISTING_STUDY}__{EXISTING_FLAT_DATA_P}__{EXISTING_SITE}__{EXISTING_VERSION}": {
                    "column_types_format_version": "3",
                    "columns": {
                        "resource": {
                            "type": "string",
                        },
                        "subgroup": {
                            "type": "string",
                        },
                        "numerator": {
                            "type": "integer",
                        },
                        "denominator": {
                            "type": "double",
                        },
                        "percentage": {
                            "type": "double",
                        },
                    },
                    "last_data_update": "2023-02-24T15:08:07.771080+00:00",
                    "s3_path": (
                        f"flat/{EXISTING_STUDY}/{EXISTING_SITE}/"
                        f"{EXISTING_STUDY}__{EXISTING_FLAT_DATA_P}__{EXISTING_SITE}__{EXISTING_VERSION}/"
                        f"{EXISTING_STUDY}__{EXISTING_FLAT_DATA_P}__flat.parquet"
                    ),
                    "site": EXISTING_SITE,
                    "id": (
                        f"{EXISTING_STUDY}__{EXISTING_FLAT_DATA_P}__"
                        f"{EXISTING_VERSION}__{EXISTING_SITE}"
                    ),
                    "type": "flat",
                },
            },
        },
        OTHER_STUDY: {
            EXISTING_DATA_P: {
                f"{OTHER_STUDY}__{EXISTING_DATA_P}__{EXISTING_VERSION}": {
                    "column_types_format_version": "3",
                    "columns": {
                        "cnt": {
                            "type": "integer",
                        },
                        "gender": {"type": "string", "distinct_values_count": 10},
                        "age": {"type": "integer", "distinct_values_count": 10},
                        "race_display": {"type": "string", "distinct_values_count": 10},
                        "site": {"type": "string", "distinct_values_count": 10},
                    },
                    "last_data_update": "2023-02-24T15:08:07.771080+00:00",
                    "s3_path": (
                        f"aggregates/{OTHER_STUDY}/{OTHER_STUDY}__{EXISTING_DATA_P}/"
                        f"{OTHER_STUDY}__{EXISTING_DATA_P}__{EXISTING_VERSION}/"
                        f"{OTHER_STUDY}__{EXISTING_DATA_P}__aggregate.parquet"
                    ),
                    "total": 2000,
                    "id": f"{OTHER_STUDY}__{EXISTING_DATA_P}__{EXISTING_VERSION}/",
                }
            },
            f"{EXISTING_FLAT_DATA_P}__{EXISTING_SITE}": {
                f"{OTHER_STUDY}__{EXISTING_FLAT_DATA_P}__{EXISTING_SITE}__{EXISTING_VERSION}": {
                    "column_types_format_version": "3",
                    "columns": {
                        "resource": {
                            "type": "string",
                        },
                        "subgroup": {
                            "type": "string",
                        },
                        "numerator": {
                            "type": "integer",
                        },
                        "denominator": {
                            "type": "double",
                        },
                        "percentage": {
                            "type": "double",
                        },
                    },
                    "last_data_update": "2023-02-24T15:08:07.771080+00:00",
                    "s3_path": (
                        f"flat/{OTHER_STUDY}/{EXISTING_SITE}/"
                        f"{OTHER_STUDY}__{EXISTING_FLAT_DATA_P}__{EXISTING_SITE}__{EXISTING_VERSION}/"
                        f"{OTHER_STUDY}__{EXISTING_FLAT_DATA_P}__flat.parquet"
                    ),
                    "site": EXISTING_SITE,
                    "id": (
                        f"{OTHER_STUDY}__{EXISTING_FLAT_DATA_P}__"
                        f"{EXISTING_VERSION}__{EXISTING_SITE}"
                    ),
                    "type": "flat",
                },
            },
        },
    }


def get_mock_auth():
    return {
        # u/a: ppth_1 test_1
        "cHB0aF8xOnRlc3RfMQ==": {"site": "ppth"},
        # u/a: elsewhere_2 test_2
        "ZWxzZXdoZXJlXzI6dGVzdF8y": {"site": "elsewhere"},
        # u/a: hope_3 test_3
        "aG9wZV8zOnRlc3RfMw==": {"site": "hope"},
    }


def get_mock_data_packages_cache():
    return ["study__encounter", "other_study__encounter"]


def get_mock_env():
    return MOCK_ENV


def get_mock_transaction(uploaded_at: str | None = None, transaction_id: str | None = None):
    if uploaded_at is None:
        uploaded_at = datetime.datetime.now(datetime.UTC).isoformat()
    if transaction_id is None:
        transaction_id = "12345678-90ab-cdef-1234-567890abcdef"
    return {"id": transaction_id, "uploaded_at": uploaded_at}


def put_mock_transaction(s3_client, site: str, study: str, transaction: dict):
    functions.put_s3_file(
        s3_client=s3_client,
        s3_bucket_name=TEST_BUCKET,
        key=f"{enums.BucketPath.META.value}/transactions/{site}__{study}.json",
        payload=transaction,
    )


def get_mock_sqs_event_record(
    body: dict, timestamp: datetime.datetime, source: str = TEST_METADATA_UPDATE_ARN
):
    """Generates an event record for mocking an SQS message

    Note: when using this, one or more records should be appended to the
    Records key in a dict like this:

    { 'Records':[mock_event_1, mock_event_2...]}

    A FIFO queue will generate blocks of no more than 10 messages at a time
    """
    return {
        "messageId": "01234567-89ab-cdef-0123-4656789abcdef",
        "receiptHandle": "ABCDEFGHIJKLMNOPQR123457890...",
        "body": json.dumps(body),
        "attributes": {
            "ApproximateReceiveCount": "1",
            "SentTimestamp": calendar.timegm(timestamp.timetuple()),
            "SenderId": "ABCDEFGHIJKLMNOPQR",
            "ApproximateFirstReceiveTimestamp": calendar.timegm(timestamp.timetuple()),
        },
        # note: this is included as an example - we won't be using it in all likelihood,
        # but if this changes, we'll need to allow a way for a user to pass in
        # attribute values
        "messageAttributes": {
            "myAttribute": {
                "stringValue": "myValue",
                "stringListValues": [],
                "binaryListValues": [],
                "dataType": "String",
            }
        },
        "md5OfBody": "0123456789abcdef012345678",
        "eventSource": "aws:sqs",
        "eventSourceARN": source,
        "awsRegion": "us-east-1",
    }
