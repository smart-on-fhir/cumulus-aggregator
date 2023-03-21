"""Storage for state variables/methods shared by test moduless"""
TEST_BUCKET = "cumulus-aggregator-site-counts-dev"
ITEM_COUNT = 7


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
        # u/a: general test1
        "Z2VuZXJhbDp0ZXN0MQ==": {"site": "general_hospital"},
        # u/a: elsewhere test2
        "ZWxzZXdoZXJlOnRlc3Qy": {"site": "st_elsewhere"},
        # u/a: hope test3
        "aG9wZTp0ZXN0Mw==": {"site": "chicago_hope"},
    }
