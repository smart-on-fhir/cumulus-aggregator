"""Enums shared across lambda functions"""
from enum import Enum


class BucketPath(Enum):
    """stores root level buckets for managing data processing state"""

    ADMIN = "admin"
    AGGREGATE = "aggregates"
    ARCHIVE = "archive"
    CACHE = "cache"
    CSVAGGREGATE = "csv_aggregates"
    ERROR = "error"
    LAST_VALID = "last_valid"
    LATEST = "latest"
    META = "metadata"
    STUDY_META = "study_metadata"
    UPLOAD = "site_upload"


class JsonFilename(Enum):
    """stores names of expected kinds of persisted S3 JSON files"""

    TRANSACTIONS = "transactions"
    DATA_PACKAGES = "data_packages"
    STUDY_PERIODS = "study_periods"


class TransactionKeys(Enum):
    """stores names of expected keys in the transaction dictionary"""

    TRANSACTION_FORMAT_VERSION = "transaction_format_version"
    LAST_UPLOAD = "last_upload"
    LAST_DATA_UPDATE = "last_data_update"
    LAST_AGGREGATION = "last_aggregation"
    LAST_ERROR = "last_error"
    DELETED = "deleted"


class StudyPeriodMetadataKeys(Enum):
    """stores names of expected keys in the study period metadata dictionary"""

    STUDY_PERIOD_FORMAT_VERSION = "study_period_format_version"
    EARLIEST_DATE = "earliest_date"
    LATEST_DATE = "latest_date"
    LAST_DATA_UPDATE = "last_data_update"
