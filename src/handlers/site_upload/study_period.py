""" Lambda for updating date ranges associated with studies """

import os
from datetime import datetime, timezone

import awswrangler
import boto3

from src.handlers.shared.awswrangler_functions import get_s3_study_meta_list
from src.handlers.shared.decorators import generic_error_handler
from src.handlers.shared.enums import BucketPath, JsonFilename
from src.handlers.shared.functions import (
    http_response,
    read_metadata,
    update_metadata,
    write_metadata,
)


def update_study_period(s3_client, s3_bucket, site, study, data_package, version):
    """gets earliest/latest date from study metadata files"""
    path = get_s3_study_meta_list(
        BucketPath.STUDY_META.value, s3_bucket, study, data_package, site
    )
    if len(path) != 1:
        raise KeyError("Unique date path not found")
    df = awswrangler.s3.read_parquet(path[0])
    study_meta = read_metadata(
        s3_client, s3_bucket, meta_type=JsonFilename.STUDY_PERIODS.value
    )

    study_meta = update_metadata(
        study_meta,
        site,
        study,
        data_package,
        version,
        "earliest_date",
        df["min_date"][0],
        meta_type=JsonFilename.STUDY_PERIODS.value,
    )
    study_meta = update_metadata(
        study_meta,
        site,
        study,
        data_package,
        version,
        "latest_date",
        df["max_date"][0],
        meta_type=JsonFilename.STUDY_PERIODS.value,
    )
    study_meta = update_metadata(
        study_meta,
        site,
        study,
        data_package,
        version,
        "last_data_update",
        datetime.now(timezone.utc),
        meta_type=JsonFilename.STUDY_PERIODS.value,
    )
    write_metadata(
        s3_client, s3_bucket, study_meta, meta_type=JsonFilename.STUDY_PERIODS.value
    )


@generic_error_handler(msg="Error updating study period")
def study_period_handler(event, context):
    """manages event from SNS, triggers file processing and merge"""
    del context
    s3_bucket = os.environ.get("BUCKET_NAME")
    s3_client = boto3.client("s3")
    s3_key = event["Records"][0]["Sns"]["Message"]
    path_params = s3_key.split("/")
    study = path_params[1]
    data_package = path_params[2]
    site = path_params[3]
    version = path_params[4]
    update_study_period(s3_client, s3_bucket, site, study, data_package, version)
    res = http_response(200, "Study period update successful")
    return res
