""" functions specifically requiring AWSWranger, which requires a lambda layer"""
import awswrangler

from .enums import BucketPath


def get_s3_data_package_list(
    bucket_root: str,
    s3_bucket_name: str,
    study: str,
    data_package: str,
    extension: str = "parquet",
):
    """Retrieves a list of data packages for a given S3 path post-upload processing"""
    return awswrangler.s3.list_objects(
        path=f"s3://{s3_bucket_name}/{bucket_root}/{study}/{study}__{data_package}/",
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
    return awswrangler.s3.list_objects(
        path=(
            f"s3://{bucket_root}/{BucketPath.STUDY_META.value}/{study}/"
            f"{study}__{data_package}/{site}/{version}"
        ),
        suffix=extension,
    )
