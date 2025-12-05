import argparse

import boto3
from rich import console, progress, table

from src.shared import enums

site_artifacts = [enums.BucketPath.LAST_VALID.value]
aggregates = [enums.BucketPath.AGGREGATE.value]


def get_subbucket_contents(client, bucket, prefix):
    res = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" not in res.keys():
        return []
    files = [x["Key"] for x in res["Contents"]]
    while res["IsTruncated"]:
        res = client.list_objects_v2(
            Bucket=bucket, Prefix=prefix, ContinuationToken=res["NextContinuationToken"]
        )
        files += [x["Key"] for x in res["Contents"]]
    return files


def remove_aggregate_data(bucket: str, target: str, version: str):
    client = boto3.client("s3")
    aggregates = get_subbucket_contents(
        client, bucket, f"{enums.BucketPath.AGGREGATE.value}/{target}"
    )
    if version:
        aggregates = [a for a in aggregates if a.split("/")[3].endswith(f"__{version}")]
    c = console.Console()
    if len(aggregates) == 0:
        c.print(f"No data found for {target}.")
        exit()
    t = table.Table(title="Data cleanup summary")
    t.add_column("Study")
    t.add_column("Version")
    t.add_column("Deletes")
    t.add_row(
        target,
        version if version else "All",
        str(len(aggregates)),
    )
    c.print(t)
    c.print("Proceed with cleanup? Y to proceed, D for details, any other value to quit.")
    response = input()
    if response.lower() == "d":
        t = table.Table()
        t.add_column("File to remove")
        for file in aggregates:
            t.add_row(file.split("/")[3])
        c.print(t)
        c.print("Proceed with cleanup? Y to proceed, any other value to quit.")
        response = input()
    if response.lower() != "y":
        c.print("Skipping cleanup")
        exit()
    for file in progress.track(aggregates, description="Deleting objects..."):
        client.delete_object(Bucket=bucket, Key=file)
    c.print("""Cleanup complete.

You may need to run this again due to bucket backup policy reasons.
Don't forget to rerun the glue crawler.""")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="""Removes aggregates for a given study. """)
    parser.add_argument("-b", "--bucket", help="bucket name", required=True)
    parser.add_argument("-t", "--target", help="target study", required=True)
    parser.add_argument("-v", "--version", help="Specific version of data to remove (optional)")
    args = parser.parse_args()
    remove_aggregate_data(args.bucket, args.target, args.version)
