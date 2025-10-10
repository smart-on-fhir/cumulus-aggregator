"""Enums shared across lambda functions"""

import enum


class BucketPath(enum.StrEnum):
    """stores root level subbuckets for managing data processing state"""

    ADMIN = "admin"
    AGGREGATE = "aggregates"
    ARCHIVE = "archive"
    CACHE = "cache"
    ERROR = "error"
    FLAT = "flat"
    LAST_VALID = "last_valid"
    LATEST_FLAT = "latest_flat"
    LATEST = "latest"
    META = "metadata"
    STATIC = "static"
    STUDY_META = "study_metadata"
    TEMP = "temp"
    UPLOAD = "site_upload"
    UPLOAD_STAGING = "upload_staging"


class ColumnTypesKeys(enum.StrEnum):
    """stores names of expected keys in the study period metadata dictionary"""

    COLUMN_TYPES_FORMAT_VERSION = "column_types_format_version"
    COLUMNS = "columns"
    LAST_DATA_UPDATE = "last_data_update"


class JsonFilename(enum.StrEnum):
    """stores names of expected kinds of persisted S3 JSON files"""

    COLUMN_TYPES = "column_types"
    TRANSACTIONS = "transactions"
    DATA_PACKAGES = "data_packages"
    FLAT_PACKAGES = "flat_packages"
    STUDY_PERIODS = "study_periods"


class StudyPeriodMetadataKeys(enum.StrEnum):
    """stores names of expected keys in the study period metadata dictionary"""

    STUDY_PERIOD_FORMAT_VERSION = "study_period_format_version"
    EARLIEST_DATE = "earliest_date"
    LATEST_DATE = "latest_date"
    LAST_DATA_UPDATE = "last_data_update"


class TransactionKeys(enum.StrEnum):
    """stores names of expected keys in the transaction dictionary"""

    TRANSACTION_FORMAT_VERSION = "transaction_format_version"
    LAST_UPLOAD = "last_upload"
    LAST_DATA_UPDATE = "last_data_update"
    LAST_AGGREGATION = "last_aggregation"
    LAST_ERROR = "last_error"
    DELETED = "deleted"


class UploadTypes(enum.StrEnum):
    """stores names of different expected upload formats"""

    # archive is not expected to be uploaded, but is one of the generated file types
    # in the library
    ARCHIVE = "archive"
    ANNOTATED_CUBE = "annotated_cube"
    CUBE = "cube"
    FLAT = "flat"
    META = "meta"
