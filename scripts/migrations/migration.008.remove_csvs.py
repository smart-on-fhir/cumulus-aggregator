import argparse
import enum

import boto3
from rich import progress


class BucketPath(enum.Enum):
    """stores root level buckets for managing data processing state"""

    ADMIN = "admin"
    AGGREGATE = "aggregates"
    ARCHIVE = "archive"
    CACHE = "cache"
    CSVAGGREGATE = "csv_aggregates"
    CSVFLAT = "csv_flat"
    ERROR = "error"
    FLAT = "flat"
    LAST_VALID = "last_valid"
    LATEST = "latest"
    META = "metadata"
    STUDY_META = "study_metadata"
    UPLOAD = "site_upload"


def remove_csvs(bucket):
    client = boto3.client("s3")
    # We specifically want to look at just the BucketPath folders since much of the
    # raw athena data is stored in csvs, and we don't want to touch that
    for base_folder in progress.track(BucketPath, description="Cleaning csvs..."):
        res = client.list_objects_v2(Bucket=bucket, Prefix=base_folder.value)
        for file in res.get("Contents", []):
            if file["Key"].endswith(".csv"):
                client.delete_object(Bucket=bucket, Key=file["Key"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="""Removes aggregator generated csvs. """)
    parser.add_argument("-b", "--bucket", help="bucket name")
    args = parser.parse_args()
    remove_csvs(args.bucket)
