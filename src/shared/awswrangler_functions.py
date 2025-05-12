"""functions specifically requiring AWSWranger, which requires a lambda layer"""

import awswrangler

from shared import enums, errors


def get_s3_data_package_list(
    bucket_root: str,
    s3_bucket_name: str,
    study: str,
    data_package: str,
    *,
    extension: str = "parquet",
    version: str | None = None,
    site: str | None = None,
):
    """Retrieves a list of data packages for a given S3 path post-upload processing"""
    if bucket_root == enums.BucketPath.FLAT.value:
        path = f"s3://{s3_bucket_name}/{bucket_root}/{study}/"
        if site:
            path += f"{site}/"
            if version:
                path += f"{version}/"
    elif bucket_root in [
        enums.BucketPath.AGGREGATE.value,
        enums.BucketPath.LATEST.value,
        enums.BucketPath.LAST_VALID.value,
        enums.BucketPath.UPLOAD.value,
    ]:
        path = f"s3://{s3_bucket_name}/{bucket_root}/{study}/{study}__{data_package}/"
        if version:
            path += f"{version}/"
    else:
        raise errors.AggregatorS3Error(f"{bucket_root} does not contain data packages")
    return awswrangler.s3.list_objects(
        path=path,
        suffix=extension,
    )


def get_s3_study_meta_list(
    bucket_root: str,
    study: str,
    data_package: str,
    site: str,
    version: str,
    extension: str = "parquet",
):
    """Retrieves metadata associated with a given upload"""
    path = (
        f"s3://{bucket_root}/{enums.BucketPath.STUDY_META.value}/{study}/"
        f"{study}__{data_package}/{site}/{version}"
    )
    return awswrangler.s3.list_objects(
        path=path,
        suffix=extension,
    )
