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
