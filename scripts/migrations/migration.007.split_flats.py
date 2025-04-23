import argparse

import boto3
from rich import progress


def split_flats(bucket):
    client = boto3.client("s3")
    for prefix in ["flat", "csv_flat"]:
        res = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        for file in progress.track(res.get("Contents", []), description=f"moving {prefix}s..."):
            split_key = file["Key"].split("/")
            package = split_key[3].split("__")[1]
            site = split_key[4].split("__")[1].replace(f"{package}_", "")
            split_key[3] = split_key[3].split("__")
            split_key[3].insert(2, site)
            split_key[3] = "__".join(split_key[3])

            client.copy(
                {"Bucket": bucket, "Key": file["Key"]},
                bucket,
                "/".join(split_key),
            )
            client.delete_object(Bucket=bucket, Key=file["Key"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="""Adds site-level splits to flat files in s3. """)
    parser.add_argument("-b", "--bucket", help="bucket name")
    args = parser.parse_args()
    split_flats(args.bucket)
