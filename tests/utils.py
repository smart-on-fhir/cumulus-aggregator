"""Storage for state variables/methods shared by test modules"""

TEST_BUCKET = "cumulus-aggregator-site-counts-test"
TEST_WORKGROUP = "cumulus-aggregator-test-wg"
TEST_GLUE_DB = "cumulus-aggregator-test-db"
TEST_PROCESS_COUNTS_ARN = "arn:aws:sns:us-east-1:123456789012:test-counts"
TEST_PROCESS_STUDY_META_ARN = "arn:aws:sns:us-east-1:123456789012:test-meta"
TEST_CACHE_API_ARN = "arn:aws:sns:us-east-1:123456789012:test-cache"
ITEM_COUNT = 9
DATA_PACKAGE_COUNT = 2

EXISTING_SITE = "princeton_plainsboro_teaching_hospital"
NEW_SITE = "chicago_hope"
OTHER_SITE = "st_elsewhere"
NEW_STUDY = "new_study"
EXISTING_STUDY = "study"
OTHER_STUDY = "other_study"
EXISTING_DATA_P = "encounter"
NEW_DATA_P = "document"
EXISTING_VERSION = "099"
NEW_VERSION = "100"

# This is a convenience for loading into os.environ with mock.patch.dict.
# Other cases should probably use the getter version below.
MOCK_ENV = {
    "BUCKET_NAME": TEST_BUCKET,
    "GLUE_DB_NAME": TEST_GLUE_DB,
    "WORKGROUP_NAME": TEST_WORKGROUP,
    "TOPIC_PROCESS_COUNTS_ARN": TEST_PROCESS_COUNTS_ARN,
    "TOPIC_PROCESS_STUDY_META_ARN": TEST_PROCESS_STUDY_META_ARN,
    "TOPIC_CACHE_API_ARN": TEST_CACHE_API_ARN,
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
                }
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
