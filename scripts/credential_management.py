#!/usr/bin/env python3
"""Utility script for modifying authorization artifacts in S3"""

import argparse
import io
import json
import sys

import boto3
from requests.auth import _basic_auth_str

SITE_FILE = "sites.json"


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


def create_auth(user: str, auth: str, site: str) -> str:
    """Adds a new entry to the auth dict used to issue pre-signed URLs"""
    if "@" in user:
        sys.exit("Usernames should not contain an @ due to S3 url character re-encoding issues.")
    site_id = _basic_auth_str(user, auth).split(" ")[1]
    return f'"{site_id}": {{"user": "{user}", "site":"{site}"}}'


def create_site(client, bucket_name: str, site: str, folder: str, display: str) -> None:
    """Adds an entry to the site dictionary"""
    site_dict = _get_s3_data(SITE_FILE, bucket_name, client)
    site_dict[site] = {"path": folder, "display": display}
    _put_s3_data(SITE_FILE, bucket_name, client, site_dict)


def delete_site(client, bucket_name: str, site: str) -> bool:
    """Removes an entry to the site dictionary"""
    site_dict = _get_s3_data(SITE_FILE, bucket_name, client)
    if site in site_dict.keys():
        site_dict.pop(site)
        _put_s3_data(SITE_FILE, bucket_name, client, site_dict)
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
    s3_modification = parser.add_mutually_exclusive_group(required=True)
    s3_modification.add_argument(
        "--create-auth",
        action="extend",
        nargs="+",
        help="Create auth. Expects: User Auth Site",
    )
    s3_modification.add_argument("--delete-auth", help="Delete auth. Expects: SiteId")
    s3_modification.add_argument(
        "--create-site",
        action="extend",
        nargs="+",
        help="Create site data. Expects: Site Folder Display",
    )
    s3_modification.add_argument("--delete-site", help="Delete a specific site. Expects: Site")
    args = parser.parse_args()
    if "-prod-" in args.bucket:
        response = input("🚨🚨 Modifying production, are you sure? (y/N) 🚨🚨\n")
        if response.lower() != "y":
            sys.exit()
    s3_client = boto3.client("s3")
    if args.create_auth:
        id_str = create_auth(
            args.create_auth[0],
            args.create_auth[1],
            args.create_auth[2],
        )
        print(f"Add the following key/value to secrets manager: \n {id_str}")
    elif args.create_site:
        create_site(
            s3_client, args.bucket, args.create_site[0], args.create_site[1], args.create_site[2]
        )
        print(f"{args.create_site[0]} mapped to S3 folder {args.create_site[1]}")
    elif args.delete_site:
        succeeded = delete_site(s3_client, args.bucket, args.delete_site)
        if succeeded:
            print(f"Unmapped {args.delete_site}")
        else:
            print(f"{args.delete_site} not found")
