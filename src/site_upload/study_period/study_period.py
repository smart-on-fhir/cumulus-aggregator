"""Lambda for updating date ranges associated with studies"""

import os
from datetime import UTC, datetime

import awswrangler
import boto3

from shared import awswrangler_functions, decorators, enums, functions


def update_study_period(s3_client, s3_bucket, site, study, data_package, version):
    """gets earliest/latest date from study metadata files"""
    path = awswrangler_functions.get_s3_study_meta_list(
        s3_bucket, study, data_package, site, version
    )
    if len(path) != 1:
        raise KeyError("Unique date path not found")
    df = awswrangler.s3.read_parquet(path[0])
    study_meta = functions.read_metadata(
        s3_client, s3_bucket, meta_type=enums.JsonFilename.STUDY_PERIODS.value
    )
    study_meta = functions.update_metadata(
        metadata=study_meta,
        site=site,
        study=study,
        data_package=data_package,
        version=version,
        target=enums.StudyPeriodMetadataKeys.EARLIEST_DATE.value,
        dt=df["min_date"][0],
        meta_type=enums.JsonFilename.STUDY_PERIODS.value,
    )
    study_meta = functions.update_metadata(
        metadata=study_meta,
        site=site,
        study=study,
        data_package=data_package,
        version=version,
        target=enums.StudyPeriodMetadataKeys.LATEST_DATE.value,
        dt=df["max_date"][0],
        meta_type=enums.JsonFilename.STUDY_PERIODS.value,
    )
    study_meta = functions.update_metadata(
        metadata=study_meta,
        site=site,
        study=study,
        data_package=data_package,
        version=version,
        target=enums.StudyPeriodMetadataKeys.LAST_DATA_UPDATE.value,
        dt=datetime.now(UTC),
        meta_type=enums.JsonFilename.STUDY_PERIODS.value,
    )
    functions.write_metadata(
        s3_client=s3_client,
        s3_bucket_name=s3_bucket,
        metadata=study_meta,
        meta_type=enums.JsonFilename.STUDY_PERIODS.value,
    )


@decorators.generic_error_handler(msg="Error updating study period")
def study_period_handler(event, context):
    """manages event from SNS, triggers file processing and merge"""
    del context
    s3_bucket = os.environ.get("BUCKET_NAME")
    s3_client = boto3.client("s3")
    s3_key = event["Records"][0]["Sns"]["Message"]
    path_params = s3_key.split("/")
    study = path_params[1]
    data_package = path_params[2].split("__")[1]
    site = path_params[3]
    version = path_params[4]
    update_study_period(s3_client, s3_bucket, site, study, data_package, version)
    res = functions.http_response(200, "Study period update successful")
    return res
