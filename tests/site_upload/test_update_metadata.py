import json
from datetime import UTC, datetime

import boto3
import pytest

from src.shared import consts, enums, functions
from src.site_upload.update_metadata import update_metadata
from tests import mock_utils


@pytest.mark.parametrize(
    "messages,assertions,delete",
    [
        # add the first event
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
                    {},
                ),
            ],
            True,
        ),
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
            False,
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
            False,
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
            False,
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
            False,
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
            False,
        ),
    ],
)
def test_update_metadata(mock_bucket, mock_env, mock_queue, messages, assertions, delete):
    records = []
    if delete:
        s3_client = boto3.client("s3")
        s3_client.delete_object(
            Bucket=mock_utils.TEST_BUCKET,
            Key=f"{enums.BucketPath.META.value}/{enums.JsonFilename.TRANSACTIONS.value}.json",
        )
    for message in messages:
        dest = message["dest"]
        del message["dest"]
        records.append(
            mock_utils.get_mock_sqs_event_record(
                {
                    "key": f"{enums.BucketPath.META.value}/{dest}.json",
                    "updates": json.dumps(message),
                    "version": None,
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


def test_remove_stale_study_metadata_transactions_scopes_to_study():
    primary_study_dev_key = (
        f"{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}__{consts.RESERVED_DEV_VERSION}"
    )
    primary_study_existing_key = (
        f"{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}__{mock_utils.EXISTING_VERSION}"
    )
    other_study_dev_key = (
        f"{mock_utils.OTHER_STUDY}__{mock_utils.EXISTING_DATA_P}__{consts.RESERVED_DEV_VERSION}"
    )
    other_study_existing_key = (
        f"{mock_utils.OTHER_STUDY}__{mock_utils.EXISTING_DATA_P}__{mock_utils.EXISTING_VERSION}"
    )
    metadata = {
        mock_utils.EXISTING_SITE: {
            mock_utils.EXISTING_STUDY: {
                mock_utils.EXISTING_DATA_P: {
                    primary_study_dev_key: {"last_upload": "dev"},
                    primary_study_existing_key: {"last_upload": "primary"},
                }
            }
        },
        mock_utils.OTHER_SITE: {
            mock_utils.EXISTING_STUDY: {
                mock_utils.EXISTING_DATA_P: {
                    primary_study_existing_key: {"last_upload": "other_existing"},
                }
            },
            mock_utils.OTHER_STUDY: {
                mock_utils.EXISTING_DATA_P: {
                    other_study_dev_key: {"last_upload": "other_dev"},
                    other_study_existing_key: {"last_upload": "other_existing"},
                }
            },
        },
    }
    update_metadata.remove_stale_study_metadata(
        metadata, mock_utils.EXISTING_STUDY, consts.RESERVED_DEV_VERSION
    )
    assert metadata[mock_utils.EXISTING_SITE][mock_utils.EXISTING_STUDY][
        mock_utils.EXISTING_DATA_P
    ] == {primary_study_dev_key: {"last_upload": "dev"}}
    assert metadata[mock_utils.OTHER_SITE][mock_utils.EXISTING_STUDY] == {}
    other_study = metadata[mock_utils.OTHER_SITE][mock_utils.OTHER_STUDY][
        mock_utils.EXISTING_DATA_P
    ]
    assert set(other_study.keys()) == {other_study_dev_key, other_study_existing_key}


def test_remove_stale_study_metadata_column_types_scopes_to_study():
    dp = mock_utils.EXISTING_DATA_P
    dev_key = f"{mock_utils.EXISTING_STUDY}__{dp}__{consts.RESERVED_DEV_VERSION}"
    primary_existing_key = f"{mock_utils.EXISTING_STUDY}__{dp}__{mock_utils.EXISTING_VERSION}"
    other_existing_key = (
        f"{mock_utils.EXISTING_STUDY}__{dp}__{mock_utils.OTHER_SITE}__{mock_utils.EXISTING_VERSION}"
    )
    metadata = {
        mock_utils.EXISTING_STUDY: {
            dp: {
                dev_key: {"last_upload": "dev"},
                primary_existing_key: {"last_upload": "existing"},
            },
            f"{dp}__{mock_utils.OTHER_SITE}": {
                other_existing_key: {"last_upload": "other_existing"}
            },
            f"{dp}_flat": "dev",
        },
        mock_utils.OTHER_STUDY: {
            dp: {f"{mock_utils.OTHER_STUDY}__{dp}__{mock_utils.EXISTING_VERSION}": {"x": 1}},
        },
    }
    update_metadata.remove_stale_study_metadata(
        metadata, mock_utils.EXISTING_STUDY, consts.RESERVED_DEV_VERSION
    )
    assert metadata[mock_utils.EXISTING_STUDY][dp] == {dev_key: {"last_upload": "dev"}}
    assert f"{dp}__{mock_utils.OTHER_SITE}" not in metadata[mock_utils.EXISTING_STUDY]
    assert metadata[mock_utils.OTHER_STUDY][dp] != {}


def test_process_event_queue_dev_removes_at_study_level(mock_bucket, mock_env, mock_queue):
    dev_update = {
        mock_utils.EXISTING_SITE: {
            mock_utils.EXISTING_STUDY: {
                mock_utils.EXISTING_DATA_P: {
                    consts.RESERVED_DEV_VERSION: {
                        "transaction_format_version": 2,
                        "last_upload": "dev_upload",
                        "last_data_update": None,
                        "last_aggregation": None,
                        "last_error": None,
                        "deleted": None,
                    }
                }
            }
        }
    }
    records = [
        mock_utils.get_mock_sqs_event_record(
            {
                "key": (
                    f"{enums.BucketPath.META.value}/{enums.JsonFilename.TRANSACTIONS.value}.json"
                ),
                "updates": json.dumps(dev_update),
                "meta_type": enums.JsonFilename.TRANSACTIONS.value,
                "version": consts.RESERVED_DEV_VERSION,
                "study": mock_utils.EXISTING_STUDY,
            },
            datetime.now(UTC),
        ),
    ]
    update_metadata.update_metadata_handler({"Records": records}, {})

    metadata = functions.get_s3_json_as_dict(
        mock_utils.TEST_BUCKET,
        f"{enums.BucketPath.META.value}/{enums.JsonFilename.TRANSACTIONS.value}.json",
    )

    removed = metadata[mock_utils.EXISTING_SITE][mock_utils.EXISTING_STUDY][
        mock_utils.EXISTING_DATA_P
    ]
    assert list(removed.keys()) == [consts.RESERVED_DEV_VERSION]

    assert (
        mock_utils.EXISTING_VERSION
        in (metadata[mock_utils.EXISTING_SITE][mock_utils.OTHER_STUDY][mock_utils.EXISTING_DATA_P])
    )

    assert metadata[mock_utils.OTHER_SITE][mock_utils.EXISTING_STUDY] == {}
