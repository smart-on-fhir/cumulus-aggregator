import json
from datetime import UTC, datetime

import pytest

from src.shared import enums, functions
from src.site_upload.update_metadata import update_metadata
from tests import mock_utils


@pytest.mark.parametrize(
    "messages,assertions",
    [
        # add a new event
        (
            [
                {
                    mock_utils.NEW_SITE: {
                        mock_utils.EXISTING_STUDY: {
                            mock_utils.EXISTING_DATA_P: {
                                mock_utils.EXISTING_VERSION: {
                                    "transaction_format_version": 2,
                                    "last_upload": "new_val",
                                    "last_data_update": None,
                                    "last_aggregation": None,
                                    "last_error": None,
                                    "deleted": None,
                                }
                            }
                        }
                    },
                    "dest": enums.JsonFilename.TRANSACTIONS.value,  #
                },
            ],
            [
                (
                    enums.JsonFilename.TRANSACTIONS.value,
                    [
                        mock_utils.NEW_SITE,
                        mock_utils.EXISTING_STUDY,
                        mock_utils.EXISTING_DATA_P,
                        mock_utils.EXISTING_VERSION,
                        "last_upload",
                    ],
                    "new_val",
                ),
                (
                    enums.JsonFilename.TRANSACTIONS.value,
                    [
                        mock_utils.NEW_SITE,
                        mock_utils.EXISTING_STUDY,
                        mock_utils.EXISTING_DATA_P,
                        mock_utils.EXISTING_VERSION,
                        "last_data_update",
                    ],
                    None,
                ),
                (
                    enums.JsonFilename.TRANSACTIONS.value,
                    [
                        mock_utils.EXISTING_SITE,
                        mock_utils.EXISTING_STUDY,
                        mock_utils.EXISTING_DATA_P,
                        mock_utils.EXISTING_VERSION,
                        "last_data_update",
                    ],
                    "2023-02-24T15:03:40.657583+00:00",
                ),
            ],
        ),
        # update an existing event (but don't overwrite non-null values with nulls)
        (
            [
                {
                    mock_utils.EXISTING_SITE: {
                        mock_utils.EXISTING_STUDY: {
                            mock_utils.EXISTING_DATA_P: {
                                mock_utils.EXISTING_VERSION: {
                                    "transaction_format_version": 2,
                                    "last_upload": "new_val",
                                    "last_data_update": None,
                                    "last_aggregation": None,
                                    "last_error": None,
                                    "deleted": None,
                                }
                            }
                        }
                    },
                    "dest": enums.JsonFilename.TRANSACTIONS.value,
                },
            ],
            [
                (
                    enums.JsonFilename.TRANSACTIONS.value,
                    [
                        mock_utils.EXISTING_SITE,
                        mock_utils.EXISTING_STUDY,
                        mock_utils.EXISTING_DATA_P,
                        mock_utils.EXISTING_VERSION,
                        "last_upload",
                    ],
                    "new_val",
                ),
                (
                    enums.JsonFilename.TRANSACTIONS.value,
                    [
                        mock_utils.EXISTING_SITE,
                        mock_utils.EXISTING_STUDY,
                        mock_utils.EXISTING_DATA_P,
                        mock_utils.EXISTING_VERSION,
                        "last_data_update",
                    ],
                    "2023-02-24T15:03:40.657583+00:00",
                ),
            ],
        ),
        # multiple updates to the same metadata
        (
            [
                {
                    mock_utils.EXISTING_SITE: {
                        mock_utils.EXISTING_STUDY: {
                            mock_utils.EXISTING_DATA_P: {
                                mock_utils.EXISTING_VERSION: {
                                    "transaction_format_version": 2,
                                    "last_upload": "new_val",
                                    "last_data_update": None,
                                    "last_aggregation": None,
                                    "last_error": None,
                                    "deleted": None,
                                }
                            }
                        }
                    },
                    "dest": enums.JsonFilename.TRANSACTIONS.value,
                },
                {
                    mock_utils.EXISTING_SITE: {
                        mock_utils.EXISTING_STUDY: {
                            mock_utils.EXISTING_DATA_P: {
                                mock_utils.EXISTING_VERSION: {
                                    "transaction_format_version": 2,
                                    "last_upload": "newer_val",
                                    "last_data_update": None,
                                    "last_aggregation": None,
                                    "last_error": None,
                                    "deleted": None,
                                }
                            }
                        }
                    },
                    "dest": enums.JsonFilename.TRANSACTIONS.value,
                },
            ],
            [
                (
                    enums.JsonFilename.TRANSACTIONS.value,
                    [
                        mock_utils.EXISTING_SITE,
                        mock_utils.EXISTING_STUDY,
                        mock_utils.EXISTING_DATA_P,
                        mock_utils.EXISTING_VERSION,
                        "last_upload",
                    ],
                    "newer_val",
                ),
            ],
        ),
        # updates to different metadata
        (
            [
                {
                    mock_utils.EXISTING_SITE: {
                        mock_utils.EXISTING_STUDY: {
                            mock_utils.EXISTING_DATA_P: {
                                mock_utils.EXISTING_VERSION: {
                                    "transaction_format_version": 2,
                                    "last_upload": "new_val",
                                    "last_data_update": None,
                                    "last_aggregation": None,
                                    "last_error": None,
                                    "deleted": None,
                                }
                            }
                        }
                    },
                    "dest": enums.JsonFilename.TRANSACTIONS.value,
                },
                {
                    mock_utils.EXISTING_STUDY: {
                        mock_utils.EXISTING_DATA_P: {
                            (
                                f"{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}"
                                f"__{mock_utils.EXISTING_VERSION}"
                            ): {
                                "column_types_format_version": "3",
                                "last_data_update": "cols_update",
                            }
                        }
                    },
                    "dest": enums.JsonFilename.COLUMN_TYPES.value,
                },
                {
                    mock_utils.EXISTING_SITE: {
                        mock_utils.EXISTING_STUDY: {
                            mock_utils.EXISTING_DATA_P: {
                                mock_utils.EXISTING_VERSION: {
                                    "transaction_format_version": 2,
                                    "last_upload": "newer_val",
                                    "last_data_update": None,
                                    "last_aggregation": None,
                                    "last_error": None,
                                    "deleted": None,
                                }
                            }
                        }
                    },
                    "dest": enums.JsonFilename.TRANSACTIONS.value,
                },
            ],
            [
                (
                    enums.JsonFilename.TRANSACTIONS.value,
                    [
                        mock_utils.EXISTING_SITE,
                        mock_utils.EXISTING_STUDY,
                        mock_utils.EXISTING_DATA_P,
                        mock_utils.EXISTING_VERSION,
                        "last_upload",
                    ],
                    "newer_val",
                ),
                (
                    enums.JsonFilename.COLUMN_TYPES.value,
                    [
                        mock_utils.EXISTING_STUDY,
                        mock_utils.EXISTING_DATA_P,
                        f"{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}__{mock_utils.EXISTING_VERSION}",
                        "last_data_update",
                    ],
                    "cols_update",
                ),
            ],
        ),
        # updates to different parts of the same metadata
        (
            [
                {
                    mock_utils.EXISTING_SITE: {
                        mock_utils.EXISTING_STUDY: {
                            mock_utils.EXISTING_DATA_P: {
                                mock_utils.EXISTING_VERSION: {
                                    "transaction_format_version": 2,
                                    "last_upload": "new_val",
                                    "last_data_update": None,
                                    "last_aggregation": None,
                                    "last_error": None,
                                    "deleted": None,
                                }
                            }
                        }
                    },
                    "dest": enums.JsonFilename.TRANSACTIONS.value,
                },
                {
                    mock_utils.EXISTING_SITE: {
                        mock_utils.EXISTING_STUDY: {
                            mock_utils.EXISTING_DATA_P: {
                                mock_utils.EXISTING_VERSION: {
                                    "transaction_format_version": 2,
                                    "last_upload": None,
                                    "last_data_update": "other_val",
                                    "last_aggregation": None,
                                    "last_error": None,
                                    "deleted": None,
                                }
                            }
                        }
                    },
                    "dest": enums.JsonFilename.TRANSACTIONS.value,
                },
            ],
            [
                (
                    enums.JsonFilename.TRANSACTIONS.value,
                    [
                        mock_utils.EXISTING_SITE,
                        mock_utils.EXISTING_STUDY,
                        mock_utils.EXISTING_DATA_P,
                        mock_utils.EXISTING_VERSION,
                        "last_upload",
                    ],
                    "new_val",
                ),
                (
                    enums.JsonFilename.TRANSACTIONS.value,
                    [
                        mock_utils.EXISTING_SITE,
                        mock_utils.EXISTING_STUDY,
                        mock_utils.EXISTING_DATA_P,
                        mock_utils.EXISTING_VERSION,
                        "last_data_update",
                    ],
                    "other_val",
                ),
            ],
        ),
    ],
)
def test_update_metadata(mock_bucket, mock_env, mock_queue, messages, assertions):
    records = []
    for message in messages:
        dest = message["dest"]
        del message["dest"]
        records.append(
            mock_utils.get_mock_sqs_event_record(
                {
                    "key": f"{enums.BucketPath.META.value}/{dest}.json",
                    "updates": json.dumps(message),
                },
                datetime.now(UTC),
            )
        )
    sqs_event = {"Records": records}
    update_metadata.update_metadata_handler(sqs_event, {})
    for assertion in assertions:
        metadata = functions.get_s3_json_as_dict(
            mock_utils.TEST_BUCKET, f"{enums.BucketPath.META.value}/{assertion[0]}.json"
        )
        for key in assertion[1]:
            metadata = metadata.get(key, {})
        assert metadata == assertion[2]
