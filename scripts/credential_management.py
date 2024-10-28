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
        client.download_fileobj(Bucket=bucket_name, Key=f"{path}/{name}", Fileobj=bytes_buffer)
        return json.loads(bytes_buffer.getvalue().decode())
    except Exception:  # pylint: disable=broad-except
        return {}


def _put_s3_data(name: str, bucket_name: str, client, data: dict, path: str = "admin") -> None:
    """Convenience class for writing a dict to S3"""
    b_data = io.BytesIO(json.dumps(data).encode())
    client.upload_fileobj(Bucket=bucket_name, Key=f"{path}/{name}", Fileobj=b_data)


def create_auth(client, user: str, auth: str, site: str) -> str:
    """Adds a new entry to the auth dict used to issue pre-signed URLs"""
    site_id = _basic_auth_str(user, auth).split(" ")[1]
    return f'"{site_id}"": {{"site":{site}}}'


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
        "-b",
        "--bucket",
        default="cumulus-aggregator-site-counts",
        help="base bucket name",
    )
    parser.add_argument("-a", "--account", help="aws account number")
    parser.add_argument("-e", "--env", default="dev", help="Name of deploy environment")
    s3_modification = parser.add_mutually_exclusive_group(required=True)
    s3_modification.add_argument(
        "--create-auth",
        action="extend",
        nargs="+",
        help="Create auth. Expects: User Auth Site",
    )
    s3_modification.add_argument("--delete-auth", help="Delete auth. Expects: SiteId")
    s3_modification.add_argument(
        "--create-meta",
        action="extend",
        nargs="+",
        help="Create metadata. Expects: Site Folder",
    )
    s3_modification.add_argument("--delete-meta", help="Delete metadata. Expects: Site")
    args = parser.parse_args()
    if args.env == "prod":
        response = input("🚨🚨 Modifying production, are you sure? (y/N) 🚨🚨\n")
        if response.lower() != "y":
            sys.exit()
    s3_client = boto3.client("s3")
    if args.account is not None:
        bucket = f"{args.bucket}-{args.account}-{args.env}"
    else:
        bucket = f"{args.bucket}-{args.env}"
    if args.create_auth:
        id_str = create_auth(
            args.create_auth[0],
            args.create_auth[1],
            args.create_auth[2],
        )
        print(id_str)
    elif args.create_meta:
        create_meta(s3_client, bucket, args.create_meta[0], args.create_meta[1])
        print(f"{args.create_meta[0]} mapped to S3 folder {args.create_meta[1]}")
    elif args.delete_meta:
        succeeded = delete_meta(s3_client, bucket, args.delete_meta)
        if succeeded:
            print(f"Unmapped {args.delete_meta}")
        else:
            print(f"{args.delete_meta} not found")
