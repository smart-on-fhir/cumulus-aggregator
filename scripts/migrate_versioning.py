""" Utility for adding versioning to an existing aggregator data store

This is a one time thing for us, so the CLI/Boto creds are not robust.
"""
import argparse
import io
import json

import boto3

UPLOAD_ROOT_BUCKETS = [
    "archive",
    "error",
    "last_valid",
    "latest",
    "site_upload",
    "study_metadata",
]


def _get_s3_data(key: str, bucket_name: str, client) -> dict:
    """Convenience class for retrieving a dict from S3"""
    try:
        bytes_buffer = io.BytesIO()
        client.download_fileobj(Bucket=bucket_name, Key=key, Fileobj=bytes_buffer)
        return json.loads(bytes_buffer.getvalue().decode())
    except Exception:  # pylint: disable=broad-except
        return {}


def _put_s3_data(key: str, bucket_name: str, client, data: dict) -> None:
    """Convenience class for writing a dict to S3"""
    b_data = io.BytesIO(json.dumps(data).encode())
    client.upload_fileobj(Bucket=bucket_name, Key=key, Fileobj=b_data)


def _get_depth(d):
    if isinstance(d, dict):
        return 1 + (max(map(_get_depth, d.values())) if d else 0)
    return 0


def migrate_bucket_versioning(bucket: str):
    client = boto3.client("s3")
    res = client.list_objects_v2(Bucket=bucket)
    contents = res["Contents"]
    moved_files = 0
    for s3_file in contents:
        if s3_file["Key"].split("/")[0] in UPLOAD_ROOT_BUCKETS:
            key = s3_file["Key"]
            key_array = key.split("/")
            if len(key_array) == 5:
                key_array.insert(4, "000")
                new_key = "/".join(key_array)
                client.copy({"Bucket": bucket, "Key": key}, bucket, new_key)
                client.delete_object(Bucket=bucket, Key=key)
                moved_files += 1
    print(f"Moved {moved_files} uploads")
    study_periods = _get_s3_data("metadata/study_periods.json", bucket, client)

    if _get_depth(study_periods) == 3:
        new_sp = {}
        for site in study_periods:
            new_sp[site] = {}
            for study in study_periods[site]:
                new_sp[site][study] = {}
                new_sp[site][study]["000"] = study_periods[site][study]
                new_sp[site][study]["000"].pop("version")
                new_sp[site][study]["000"]["study_period_format_version"] = 2
        # print(json.dumps(new_sp, indent=2))
        _put_s3_data("metadata/study_periods.json", bucket, client, new_sp)
        print("study_periods.json updated")
    else:
        print("study_periods.json does not need update")

    transactions = _get_s3_data("metadata/transactions.json", bucket, client)
    if _get_depth(transactions) == 4:
        new_t = {}
        for site in transactions:
            new_t[site] = {}
            for study in transactions[site]:
                new_t[site][study] = {}
                for dp in transactions[site][study]:
                    new_t[site][study][dp] = {}
                    new_t[site][study][dp]["000"] = transactions[site][study][dp]
                    new_t[site][study][dp]["000"].pop("version")
                    new_t[site][study][dp]["000"]["transacton_format_version"] = 2
        # print(json.dumps(new_t, indent=2))
        _put_s3_data("metadata/transactions.json", bucket, client, new_t)
        print("transactions.json updated")
    else:
        print("transactions.json does not need update")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""Util for migrating aggregator data"""
    )
    parser.add_argument("-b", "--bucket", help="bucket name")
    args = parser.parse_args()
    migrate_bucket_versioning(args.bucket)
