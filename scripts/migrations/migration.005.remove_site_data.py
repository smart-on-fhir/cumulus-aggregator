"""this migration is the basis of delete_site_data - it is kept around
mostly as a historical artifact of how deletions were run until then/utility
for quickly cribbing together new migrations through reuse."""

import argparse
import enum
from collections import defaultdict

import boto3
from rich.console import Console
from rich.table import Table


class BucketPath(enum.StrEnum):
    """stores root level buckets for managing data processing state"""

    ADMIN = "admin"
    AGGREGATE = "aggregates"
    ARCHIVE = "archive"
    CACHE = "cache"
    CSVAGGREGATE = "csv_aggregates"
    ERROR = "error"
    LAST_VALID = "last_valid"
    LATEST = "latest"
    META = "metadata"
    STUDY_META = "study_metadata"
    UPLOAD = "site_upload"


site_artifacts = [BucketPath.LAST_VALID]
aggregates = [BucketPath.AGGREGATE, BucketPath.CSVAGGREGATE]


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


def cleanup_target(tree: dict, target: str, table: str, site: str, version: str):
    other_sites = list(tree[table].keys())
    other_sites.remove(site)
    for site in other_sites:
        for data_version in tree[table][site].keys():
            if data_version == version:
                return tree[table][site][data_version], None
    return None, f"aggregates/{target}/{table}/{table}__{version}/{table}__aggregate.parquet"


def remove_site_data(bucket: str, target: str, site: str, version: str):
    client = boto3.client("s3")
    contents = get_subbucket_contents(client, bucket, BucketPath.LAST_VALID)
    tree = defaultdict(lambda: defaultdict(dict))
    for path in contents:
        s3_key = path.split("/")
        if target == s3_key[1]:
            tree[s3_key[2]][s3_key[3]][s3_key[4]] = path
    tables_to_prune = []
    regen_targets = []
    delete_targets = []
    for table in tree.keys():
        if site in tree[table].keys():
            for data_version in tree[table][site].keys():
                if version is None or data_version == version:
                    regen_target, delete_target = cleanup_target(
                        tree, target, table, site, data_version
                    )
                    if regen_target:
                        regen_targets.append(regen_target)
                        tables_to_prune.append(((tree[table][site][data_version]), "Regen"))
                    if delete_target:
                        delete_targets.append(delete_target)
                        tables_to_prune.append(((tree[table][site][data_version]), "Delete"))
    console = Console()
    if len(tables_to_prune) == 0:
        console.print(f"No data found for {site} {target}.")
        exit()
    table = Table(title="Data cleanup summary")
    table.add_column("Site")
    table.add_column("Study")
    table.add_column("Version")
    table.add_column("Removals")
    table.add_column("Regens")
    table.add_column("Deletes")
    table.add_row(
        site,
        target,
        version if version else "All",
        str(len(tables_to_prune)),
        str(len(regen_targets)),
        str(len(delete_targets)),
    )
    console.print(table)
    console.print("Proceed with cleanup? Y to proceed, D for details, any other value to quit.")
    response = input()
    if response.lower() == "d":
        table = Table()
        table.add_column("File to remove")
        table.add_column("Mititgation strategy")
        for file in tables_to_prune:
            table.add_row(file[0], file[1])
        console.print(table)
        console.print("Proceed with cleanup? Y to proceed, any other value to quit.")
        response = input()
    if response.lower() != "y":
        console.print("Skipping cleanup")
        exit()
    for file in tables_to_prune:
        client.delete_object(Bucket=bucket, Key=file[0])
    for key in regen_targets:
        client.copy(
            CopySource={"Bucket": bucket, "Key": key},
            Bucket=bucket,
            Key=key.replace(BucketPath.LAST_VALID, BucketPath.UPLOAD, 1),
        )
    for key in delete_targets:
        client.delete_object(Bucket=bucket, Key=key)
    console.print("Cleanup complete. Rerun the glue crawler.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""Removes artifacts from a site for a given study. """
    )
    parser.add_argument("-b", "--bucket", help="bucket name", required=True)
    parser.add_argument("-t", "--target", help="target study", required=True)
    parser.add_argument("-s", "--site", help="site data to remove", required=True)
    parser.add_argument("-v", "--version", help="Specific version of data to remove (optional)")
    args = parser.parse_args()
    remove_site_data(args.bucket, args.target, args.site, args.version)
