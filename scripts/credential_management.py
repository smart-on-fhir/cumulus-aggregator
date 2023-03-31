#!/usr/bin/env python3
"""Utility script for modifying authorization artifacts in S3"""
import argparse
import io
import json
import sys

import boto3

from requests.auth import _basic_auth_str


def _get_s3_data(name: str, bucket_name: str, client, path: str = "admin") -> dict:
    """Convenience class for retrieving a dict from S3"""
    try:
        bytes_buffer = io.BytesIO()
        client.download_fileobj(
            Bucket=bucket_name, Key=f"{path}/{name}", Fileobj=bytes_buffer
        )
        return json.loads(bytes_buffer.getvalue().decode())
    except Exception:  # pylint: disable=broad-except
        return {}


def _put_s3_data(
    name: str, bucket_name: str, client, data: dict, path: str = "admin"
) -> None:
    """Convenience class for writing a dict to S3"""
    b_data = io.BytesIO(json.dumps(data).encode())
    client.upload_fileobj(Bucket=bucket_name, Key=f"{path}/{name}", Fileobj=b_data)


def create_auth(client, bucket_name: str, user: str, auth: str, site: str) -> str:
    """Adds a new entry to the auth dict used to issue pre-signed URLs"""
    file = "auth.json"
    auth_dict = _get_s3_data(file, bucket_name, client)
    site_id = _basic_auth_str(user, auth).split(" ")[1]
    auth_dict[site_id] = {"site": site}
    _put_s3_data(file, bucket_name, client, auth_dict)
    return site_id


def delete_auth(client, bucket_name: str, site_id: str) -> bool:
    """Removes an entry from the auth dict used to issue pre-signed urls"""
    file = "auth.json"
    auth_dict = _get_s3_data(file, bucket_name, client)
    if site_id in auth_dict.keys():
        auth_dict.pop(site_id)
        _put_s3_data(file, bucket_name, client, auth_dict)
        return True
    else:
        return False


def create_meta(client, bucket_name: str, site: str, folder: str) -> None:
    """Adds an entry to the metadata dictionary for routing files"""
    file = "metadata.json"
    meta_dict = _get_s3_data(file, bucket_name, client)
    meta_dict[site] = {"path": folder}
    _put_s3_data(file, bucket_name, client, meta_dict)


def delete_meta(client, bucket_name: str, site: str) -> bool:
    """Removes an entry to the metadata dictionary for routing files"""
    file = "metadata.json"
    meta_dict = _get_s3_data(file, bucket_name, client)
    if site in meta_dict.keys():
        meta_dict.pop(site)
        _put_s3_data(file, bucket_name, client, meta_dict)
        return True
    else:
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""Convenience script for managing cumulus auth credentials.

        Note that create arguements are passed as arrays to the relevant function.
        Please note the order requested in each argument's description"""
    )
    parser.add_argument(
        "-b", "--bucket", default="cumulus-aggregator-site-counts", help="bucket name"
    )
    parser.add_argument("-e", "--env", default="dev", help="Name of deploy environment")
    s3_modification = parser.add_mutually_exclusive_group(required=True)
    s3_modification.add_argument(
        "--create_auth",
        action="extend",
        nargs="+",
        help="Create auth. Expects: User Auth Site",
    )
    s3_modification.add_argument("--delete_auth", help="Delete auth. Expects: SiteId")
    s3_modification.add_argument(
        "--create_meta",
        action="extend",
        nargs="+",
        help="Create metadata. Expects: Site Folder",
    )
    s3_modification.add_argument("--delete_meta", help="Delete metadata. Expects: Site")
    args = parser.parse_args()
    if args.env == "prod":
        response = input("🚨🚨 Modifying production, are you sure? (y/N) 🚨🚨\n")
        if response.lower() != "y":
            sys.exit()
    s3_client = boto3.client("s3")
    bucket = f"{args.bucket}-{args.env}"
    if args.ca:
        id_str = create_auth(s3_client, bucket, args.ca[0], args.ca[1], args.ca[2])
        print(f"{id_str} created")
    elif args.da:
        succeeded = delete_auth(s3_client, bucket, args.da)
        if succeeded:
            print(f"Removed {args.da}")
        else:
            print(f"{args.da} not found")
    elif args.cm:
        create_meta(s3_client, bucket, args.cm[0], args.cm[1])
        print(f"{args.cm[0]} mapped to S3 folder {args.cm[1]}")
    elif args.dm:
        succeeded = delete_meta(s3_client, bucket, args.dm)
        if succeeded:
            print(f"Unmapped {args.dm}")
        else:
            print(f"{args.dm} not found")
