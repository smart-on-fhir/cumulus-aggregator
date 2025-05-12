import argparse
from collections import defaultdict

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


def cleanup_target(tree: dict, target: str, data_packages: str, site: str, version: str):
    other_sites = list(tree[data_packages].keys())
    other_sites.remove(site)
    for site in other_sites:
        for data_version in tree[data_packages][site].keys():
            if data_version == version:
                return tree[data_packages][site][data_version], None
    return None, (
        f"aggregates/{target}/{data_packages}/{data_packages}__{version}/"
        f"{data_packages}__aggregate.parquet"
    )


def remove_site_data(bucket: str, target: str, site: str, version: str):
    client = boto3.client("s3")
    contents = get_subbucket_contents(client, bucket, enums.BucketPath.LAST_VALID.value)
    tree = defaultdict(lambda: defaultdict(dict))
    for path in contents:
        s3_key = path.split("/")
        if target == s3_key[1]:
            tree[s3_key[2]][s3_key[3]][s3_key[4]] = path
    data_packages_to_prune = []
    regen_targets = []
    delete_targets = []
    for data_packages in tree.keys():
        if site in tree[data_packages].keys():
            for data_version in tree[data_packages][site].keys():
                if version is None or data_version == version:
                    regen_target, delete_target = cleanup_target(
                        tree, target, data_packages, site, data_version
                    )
                    if regen_target:
                        regen_targets.append(regen_target)
                        data_packages_to_prune.append(
                            ((tree[data_packages][site][data_version]), "Regen")
                        )
                    if delete_target:
                        delete_targets.append(delete_target)
                        data_packages_to_prune.append(
                            ((tree[data_packages][site][data_version]), "Delete")
                        )
    c = console.Console()
    if len(data_packages_to_prune) == 0:
        c.print(f"No data found for {site} {target}.")
        exit()
    t = table.Table(title="Data cleanup summary")
    t.add_column("Site")
    t.add_column("Study")
    t.add_column("Version")
    t.add_column("Removals")
    t.add_column("Regens")
    t.add_column("Deletes")
    t.add_row(
        site,
        target,
        version if version else "All",
        str(len(data_packages_to_prune)),
        str(len(regen_targets)),
        str(len(delete_targets)),
    )
    c.print(t)
    c.print("Proceed with cleanup? Y to proceed, D for details, any other value to quit.")
    response = input()
    if response.lower() == "d":
        t = table.Table()
        t.add_column("File to remove")
        t.add_column("Mititgation strategy")
        for file in data_packages_to_prune:
            t.add_row(file[0], file[1])
        c.print(t)
        c.print("Proceed with cleanup? Y to proceed, any other value to quit.")
        response = input()
    if response.lower() != "y":
        c.print("Skipping cleanup")
        exit()
    for file in progress.track(data_packages_to_prune, description="Deleting objects..."):
        client.delete_object(Bucket=bucket, Key=file[0])
    for key in progress.track(regen_targets, description="Regenerating objects"):
        client.copy(
            CopySource={"Bucket": bucket, "Key": key},
            Bucket=bucket,
            Key=key.replace(enums.BucketPath.LAST_VALID.value, enums.BucketPath.UPLOAD.value, 1),
        )
    for key in delete_targets:
        client.delete_object(Bucket=bucket, Key=key)
    c.print("""Cleanup complete.

You may need to run this again due to bucket backup policy reasons.
Don't forget to rerun the glue crawler.""")


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
