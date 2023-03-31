"""Enums shared across lambda functions"""
from enum import Enum


class BucketPath(Enum):
    ADMIN = "admin"
    AGGREGATE = "aggregates"
    ARCHIVE = "archive"
    CSVAGGREGATE = "csv_aggregates"
    ERROR = "error"
    LAST_VALID = "last_valid"
    LATEST = "latest"
    META = "site_metadata"
    UPLOAD = "site_upload"
