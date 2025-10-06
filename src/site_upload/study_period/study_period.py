"""Lambda for updating date ranges associated with studies"""

import os
from datetime import UTC, datetime

import awswrangler
import boto3

from shared import awswrangler_functions, decorators, enums, functions


def update_study_period(s3_client, sqs_client, s3_bucket, site, study, data_package, version):
    """gets earliest/latest date from study metadata files"""
    paths = awswrangler_functions.get_s3_study_meta_list(
        s3_bucket, study, data_package, site, version
    )
    if len(paths) > 1:

        def modified_time(path):
            key = functions.get_s3_key_from_path(path)
            res = s3_client.head_object(Bucket=s3_bucket, Key=key)
            return res["LastModified"]

        latest_path = max(paths, key=modified_time)
        for path in paths:
            if latest_path != path:
                s3_client.delete_object(Bucket=s3_bucket, Key=functions.get_s3_key_from_path(path))
        paths = [latest_path]
    df = awswrangler.s3.read_parquet(paths[0])
    study_meta = {}
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
        sqs_client=sqs_client,
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
    sqs_client = boto3.client("sqs")
    s3_key = event["Records"][0]["Sns"]["Message"]
    dp_meta = functions.parse_s3_key(s3_key)
    update_study_period(
        s3_client,
        sqs_client,
        s3_bucket,
        dp_meta.site,
        dp_meta.study,
        dp_meta.data_package,
        dp_meta.version,
    )
    res = functions.http_response(200, "Study period update successful")
    return res
