#!/usr/bin/env python3
""" basic utility for debugging merge behavior"""

import argparse
import os
import sys
from pathlib import Path

import boto3
import requests

# workaround - the lambda environment resolves dependencies primarily based
# on absolute path, and rather than modify the lambda code to support this test
# script, we'll tack on the project root to the path for just this case.
sys.path.append(str(Path(__file__).resolve().parents[1]))


def upload_file(cli_args):
    """Handles S3 uploads via a pre-signed post"""
    if cli_args["url"] is None:
        try:
            api_client = boto3.client("apigateway")
            res = api_client.get_rest_apis()
            api_dict = list(
                filter(
                    lambda x: "cumulus-aggregator-dev"
                    in x["tags"]["aws:cloudformation:stack-name"],
                    res["items"],
                )
            )
            for api in api_dict:
                if api["name"] == "CumulusAggregatorSiteAPI-dev":
                    url = (
                        f"https://{api['id']}.execute-api.us-east-1.amazonaws.com/dev/"
                    )
        except Exception:  # pylint: disable=broad-except
            print("No response recieved from AWS API gateway.")
            sys.exit(1)
    else:
        url = cli_args["url"]
    try:
        object_name = cli_args["file"].split("/")[-1]
    except Exception:  # pylint: disable=broad-except
        print("No filename provided for upload.")
        sys.exit(1)
    response = requests.post(
        url,
        json={
            "study": cli_args["study"],
            "data_package": cli_args["data_package"],
            "filename": f"{cli_args['user']}_{object_name}",
        },
        auth=(cli_args["user"], cli_args["auth"]),
        timeout=60,
    )
    if response is None:
        print(f"API at {url} not found")
        sys.exit(1)
    if response.status_code != 200:
        print(response.request.headers)
        print(response.text)
        print("Provided site/auth credentials are invalid.")
        sys.exit(1)
    body = response.json()
    with open(cli_args["file"], "rb") as f:
        files = {"file": (object_name, f)}
        http_response = requests.post(
            body["url"], data=body["fields"], files=files, timeout=60
        )

    # If successful, returns HTTP status code 204
    print(
        f"{cli_args['user']}_{object_name} upload HTTP status code: "
        f"{http_response.status_code}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""Uploads de-ID counts data to a cumulus aggregator instance.

        Each non-test argument can optionally be defined as an enviroment variable.
        They should be of the form CUMULUS_UPLOAD_[param name, capitalized]. Values
        passed via flags will always replace these values. All non-test values are
        required, except for url; if url is not provided, your AWS profile will be
        used to try to connect to your AWS instance"""
    )
    parser.add_argument("-f", "--file", help="The data file to upload")
    parser.add_argument("-u", "--user", help="the name of the site uploading data")
    parser.add_argument("-a", "--auth", help="the secret assigned to a site")
    parser.add_argument("-s", "--study", help="the name of the data's study")
    parser.add_argument(
        "-d", "--data_package", help="the data_package name within the study"
    )
    parser.add_argument("-r", "--url", help="the public URL of the aggregator")
    parser.add_argument(
        "-t",
        "--test",
        default=False,
        action="store_true",
        help="Use test data for all params (but can override w/ other args)",
    )
    args_dict = {}
    args = vars(parser.parse_args())
    for key in args.keys():
        args_dict[key] = os.getenv(f"CUMULUS_UPLOAD_{key.upper()}")
    if args["test"]:
        args_dict["user"] = os.environ.get("CUMULUS_TEST_UPLOAD_USER", "general")
        args_dict["file"] = (
            f"{str(Path(__file__).resolve().parents[1])}"
            f"/tests/test_data/count_synthea_patient.parquet"
        )
        args_dict["auth"] = os.environ.get("CUMULUS_TEST_UPLOAD_AUTH", "secretval")
        args_dict["study"] = "core"
        args_dict["data_package"] = "patient"

    for key in args.keys():
        if args[key] is not None:
            args_dict[key] = args[key]
    upload_file(args_dict)
