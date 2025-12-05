import argparse
import json

import boto3
import rich
from rich import console, progress, table

from src.shared import enums

meta = enums.BucketPath.META.value
study_meta = enums.BucketPath.AGGREGATE.value


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


def remove_site_metadata(bucket: str, target: str, site: str, version: str | None):
    client = boto3.client("s3")
    meta_versions = []
    file_uploads = []
    try:
        study_periods = json.load(
            client.get_object(Bucket=bucket, Key=f"{meta}/study_periods.json")["Body"]
        )
        site_dict = study_periods.get(site, {})
        study_dict = site_dict.get(target, {})
        if version:
            if version in study_dict:
                meta_versions.append(version)
        else:
            meta_versions = [*study_dict.keys()]
    except client.exceptions.NoSuchKey:
        rich.print(f"{meta}/study_periods.json not found, skipping study period update")
    for data_type in ["meta_date", "meta_version"]:
        found_files = get_subbucket_contents(
            client, bucket, f"{study_meta}/{target}/{target}__{data_type}"
        )
        if version:
            found_files = [u for u in found_files if version in u]
        file_uploads = [*file_uploads, *found_files]

    c = console.Console()
    if len(file_uploads) == 0 and len(meta_versions) == 0:
        c.print(f"No metadata found for {site} {target}.")
        exit()
    t = table.Table(title="Data cleanup summary")
    t.add_column("Site")
    t.add_column("Study")
    t.add_column("Version")
    t.add_column("Metadata")
    t.add_column("Uploads")
    t.add_row(
        site,
        target,
        version if version else "All",
        str(len(meta_versions)),
        str(len(file_uploads)),
    )
    c.print(t)
    c.print("Proceed with cleanup? Y to proceed, any other value to quit.")
    response = input()
    if response.lower() != "y":
        c.print("Skipping cleanup")
        exit()
    for file in progress.track(file_uploads, description="Deleting uploads..."):
        client.delete_object(Bucket=bucket, Key=file[0])
    for version in progress.track(meta_versions, description="Removing metadata"):
        study_periods[site][target].pop(version)
    if study_periods[site][target].keys() == {}:
        study_periods[site].pop(target)
    client.put_object(
        Bucket=bucket, Key=f"{meta}/study_periods.json", Body=json.dump(study_periods)
    )
    c.print("Cleanup complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""Removes metadata from a site for a given study. """
    )
    parser.add_argument("-b", "--bucket", help="bucket name", required=True)
    parser.add_argument("-t", "--target", help="target study", required=True)
    parser.add_argument("-s", "--site", help="site data to remove", required=True)
    parser.add_argument("-v", "--version", help="Specific version of data to remove (optional)")
    args = parser.parse_args()
    remove_site_metadata(args.bucket, args.target, args.site, args.version)
