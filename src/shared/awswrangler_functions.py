"""functions specifically requiring AWSWranger, which requires a lambda layer"""

import csv

import awswrangler
import numpy

from .enums import BucketPath


def get_s3_data_package_list(
    bucket_root: str,
    s3_bucket_name: str,
    study: str,
    data_package: str,
    extension: str = "parquet",
    version: str | None = None,
    site: str | None = None,
):
    """Retrieves a list of data packages for a given S3 path post-upload processing"""
    if bucket_root in [BucketPath.FLAT.value, BucketPath.CSVFLAT.value]:
        return awswrangler.s3.list_objects(
            path=f"s3://{s3_bucket_name}/{bucket_root}/{study}/{site}/",
            suffix=extension,
        )
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


def generate_csv_from_parquet(
    bucket_name: str, bucket_root: str, subbucket_path: str, to_path: str | None = None
):
    """Convenience function for generating csvs for dashboard upload

    TODO: Remove on dashboard parquet/API support"""
    if to_path is None:
        to_path = f"s3://{bucket_name}/{bucket_root}/{subbucket_path}".replace(".parquet", ".csv")
    last_valid_df = awswrangler.s3.read_parquet(
        f"s3://{bucket_name}/{bucket_root}" f"/{subbucket_path}"
    )
    last_valid_df = last_valid_df.apply(lambda x: x.strip() if isinstance(x, str) else x).replace(
        '""', numpy.nan
    )
    awswrangler.s3.to_csv(
        last_valid_df,
        to_path,
        index=False,
        quoting=csv.QUOTE_MINIMAL,
    )
