import argparse
from collections import defaultdict

import boto3
from rich import console, progress, table

from src.shared import enums


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


def reprocess_site_data(
    bucket: str,
    target: str,
    site: str,
    version: str,
    target_bucket: str | None = None,
    subfolder: str | None = None,
):
    if subfolder is None:
        subfolder = enums.BucketPath.LAST_VALID.value
    if target_bucket is None:
        target_bucket = bucket
    client = boto3.client("s3")
    data_packages_to_reprocess = []
    for source in [enums.BucketPath.STUDY_META.value, subfolder]:
        contents = get_subbucket_contents(client, bucket, source)
        tree = defaultdict(lambda: defaultdict(dict))
        for path in contents:
            s3_key = path.split("/")
            if target == s3_key[1]:
                if source == enums.BucketPath.FLAT.value:
                    _, dp, _, path_version = s3_key[3].split("__")
                    tree[dp][s3_key[2]][path_version] = path
                else:
                    tree[s3_key[2]][s3_key[3]][s3_key[4]] = path
        for data_packages in tree.keys():
            if site in tree[data_packages].keys():
                for data_version in tree[data_packages][site].keys():
                    if data_version == version:
                        data_packages_to_reprocess.append(tree[data_packages][site][version])
    c = console.Console()
    if len(data_packages_to_reprocess) == 0:
        c.print(f"No data found for {site} {target} {version}.")
        exit()
    t = table.Table(title="Data cleanup summary")
    t.add_column("Site")
    t.add_column("Study")
    t.add_column("Version")
    t.add_column("Files")
    t.add_row(
        site,
        target,
        version,
        str(len(data_packages_to_reprocess)),
    )
    c.print(t)
    c.print("Proceed with reprocess? Y to proceed, any other value to quit.")
    response = input()
    if response.lower() != "y":
        c.print("Skipping reprocess")
        exit()
    for key in progress.track(
        data_packages_to_reprocess, description=f"regenerated {site} {target} {version}"
    ):
        if enums.BucketPath.STUDY_META.value in key:
            new_key = key.replace(
                enums.BucketPath.STUDY_META.value, enums.BucketPath.UPLOAD.value, 1
            )
        else:
            new_key = key.replace(subfolder, enums.BucketPath.UPLOAD.value, 1)
        client.copy(CopySource={"Bucket": bucket, "Key": key}, Bucket=target_bucket, Key=new_key)
    c.print("""Reprocessing complete.
Don't forget to rerun the glue crawler.""")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""Reprocess artifacts from a site for a given study. """
    )
    parser.add_argument("-b", "--bucket", help="bucket name", required=True)
    parser.add_argument("-t", "--target", help="target study", required=True)
    parser.add_argument("-s", "--site", help="site data to copy", required=True)
    parser.add_argument("-v", "--version", help="Study version", required=True)
    parser.add_argument(
        "--target-bucket", help="Specifies an alternate bucket to use as the copy target"
    )
    parser.add_argument(
        "--subfolder",
        help=(
            "Specifies an alternate bucket subfolder to use as the "
            "data source (probably latest or site_upload)"
        ),
    )
    args = parser.parse_args()
    reprocess_site_data(
        args.bucket, args.target, args.site, args.version, args.target_bucket, args.subfolder
    )
