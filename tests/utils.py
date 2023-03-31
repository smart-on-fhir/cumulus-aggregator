"""Storage for state variables/methods shared by test modules"""

TEST_BUCKET = "cumulus-aggregator-site-counts-test"
TEST_WORKGROUP = "cumulus-aggregator-test-wg"
TEST_GLUE_DB = "cumulus-aggregator-test-db"
ITEM_COUNT = 9
SUBSCRIPTION_COUNT = 2

# This is a convenience for loading into os.environ with mock.patch.dict.
# Other cases should probably use the getter version below.
MOCK_ENV = {
    "BUCKET_NAME": TEST_BUCKET,
    "GLUE_DB_NAME": TEST_GLUE_DB,
    "WORKGROUP_NAME": TEST_WORKGROUP,
}


def get_mock_metadata():
    return {
        "general_hospital": {
            "covid": {
                "encounter": {
                    "version": "1.0",
                    "last_upload": "2023-02-24T15:03:34+00:00",
                    "last_data_update": "2023-02-24T15:03:40.657583+00:00",
                    "last_aggregation": "2023-02-24T15:08:07.504595+00:00",
                    "last_error": None,
                    "earliest_data": None,
                    "latest_data": None,
                    "deleted": None,
                }
            },
            "lyme": {
                "encounter": {
                    "version": "1.0",
                    "last_upload": "2023-02-24T15:43:57+00:00",
                    "last_data_update": "2023-02-24T15:44:03.861574+00:00",
                    "last_aggregation": "2023-02-24T15:44:03.861574+00:00",
                    "last_error": None,
                    "earliest_data": None,
                    "latest_data": None,
                    "deleted": None,
                }
            },
        },
        "st_elsewhere": {
            "covid": {
                "encounter": {
                    "version": "1.0",
                    "last_upload": "2023-02-24T15:08:06+00:00",
                    "last_data_update": "2023-02-24T15:08:07.771080+00:00",
                    "last_aggregation": "2023-02-24T15:08:07.771080+00:00",
                    "last_error": None,
                    "earliest_data": None,
                    "latest_data": None,
                    "deleted": None,
                }
            }
        },
    }


def get_mock_auth():
    return {
        # u/a: general_1 test_1
        "Z2VuZXJhbF8xOnRlc3RfMQ==": {"site": "general"},
        # u/a: elsewhere_2 test_2
        "ZWxzZXdoZXJlXzI6dGVzdF8y": {"site": "elsewhere"},
        # u/a: hope_3 test_3
        "aG9wZV8zOnRlc3RfMw==": {"site": "hope"},
    }


def get_mock_env():
    return MOCK_ENV
