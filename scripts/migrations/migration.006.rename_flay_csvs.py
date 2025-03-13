import argparse

import boto3
from rich import progress


def rename_csvs(bucket):
    client = boto3.client("s3")
    res = client.list_objects_v2(Bucket=bucket, Prefix="csv_flat")
    for file in progress.track(res["Contents"], description="Renaming files"):
        if file["Key"].endswith(".parquet"):
            client.copy(
                {"Bucket": bucket, "Key": file["Key"]},
                bucket,
                file["Key"].replace(".parquet", ".csv"),
            )
            client.delete_object(Bucket=bucket, Key=file["Key"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""Removes artifacts from a site for a given study. """
    )
    parser.add_argument("-b", "--bucket", help="bucket name")
    parser.add_argument("-d", "--db", help="database name")
    args = parser.parse_args()
    rename_csvs(args.bucket)
