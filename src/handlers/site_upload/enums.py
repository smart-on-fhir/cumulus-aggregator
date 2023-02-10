"""Enums shared across lambda functions"""
from enum import Enum


class BucketPath(Enum):
    UPLOAD = "site_upload"
    LATEST = "latest"
    LAST_VALID = "last_valid"
    ERROR = "error"
    AGGREGATE = "aggregates"
    CSVAGGREGATE = "csv_aggregates"
