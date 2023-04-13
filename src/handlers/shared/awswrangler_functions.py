""" functions specifically requiring AWSWranger, which requires a lambda layer"""
import awswrangler


def get_s3_data_package_list(
    bucket_root: str,
    s3_bucket_name: str,
    study: str,
    data_package: str,
    extension: str = "parquet",
):
    """Retrieves a list of data packages for a given S3 path post-upload proceesing"""
    return awswrangler.s3.list_objects(
        path=f"s3://{s3_bucket_name}/{bucket_root}/{study}/{data_package}",
        suffix=extension,
    )


def get_s3_study_meta_list(
    bucket_root: str,
    s3_bucket_name: str,
    study: str,
    data_package: str,
    site: str,
    extension: str = "parquet",
):
    """Retrieves a list of data packages for a given S3 path post-upload proceesing"""
    return awswrangler.s3.list_objects(
        path=f"s3://{s3_bucket_name}/{bucket_root}/{study}/{data_package}/{site}",
        suffix=extension,
    )
