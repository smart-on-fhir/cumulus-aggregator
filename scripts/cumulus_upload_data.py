# basic utility for debugging merge behavior

import argparse
import json
import requests
import os
import sys

import boto3

# workaround - the lambda environment resolves dependencies primarily based
# on absolute path, and rather than modify the lambda code to support this test
# script, we'll tack on the project root to the path for just this case.

from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from src.handlers import fetch_upload_url


def upload_file(args):
    """Handles S3 uploads via a pre-signed post"""
    if args["url"] == None:
        try:
            api_client = boto3.client("apigateway")
            res = api_client.get_rest_apis()
            site_api_dict = list(
                filter(
                    lambda x: x["tags"]["aws:cloudformation:logical-id"]
                    == "SiteApiGateway",
                    res["items"],
                )
            )
            api_id = site_api_dict[0]["id"]
            url = f"https://{api_id}.execute-api.us-east-1.amazonaws.com/Prod/"
        except:
            print("No response recieved from AWS API gateway.")
            exit(1)
    else:
        url = args["url"]
    try:
        object_name = args["file"].split("/")[-1]
    except:
        print("No filename provided for upload.")
        exit(1)
    headers = {"user": args["site"], "authorization": args["auth"]}
    response = requests.post(
        url,
        headers=headers,
        json={"study": "covid", "filename": f"{args['site']}_{object_name}"},
    )
    if response is None:
        print(f"API at {url} not found")
        exit(1)
    if response.status_code != 200:
        print("Provided site/auth credentials are invalid.")
        exit(1)
    body = response.json()
    with open(args["file"], "rb") as f:
        files = {"file": (object_name, f)}
        http_response = requests.post(body["url"], data=body["fields"], files=files)

    # If successful, returns HTTP status code 204
    print(
        f"{args['site']}_{object_name} upload HTTP status code: {http_response.status_code}"
    )


if __name__ == "__main__":
    arg_names = ["file", "study", "auth", "studyname", "url", "profile"]
    parser = argparse.ArgumentParser(
        description="""Uploads de-ID counts data to a cumulus aggregator instance.

        Each non-test argument can optionally be defined as an enviroment variable.
        They should be of the form CUMULUS_UPLOAD_[param name, capitalized]. Values
        passed via flags will always replace these values. All non-test values are 
        required, except for url; if url is not provided, your AWS profile will be
        used to try to connect to your AWS instance"""
    )
    parser.add_argument("-f", "--file", help="The data file to upload")
    parser.add_argument("-s", "--site", help="the name of the site uploading data")
    parser.add_argument("-a", "--auth", help="the secret assigned to a site")
    parser.add_argument("-n", "--studyname", help="the name of the data's study")
    parser.add_argument("-u", "--url", help="the public URL of the aggregator")
    parser.add_argument("-p", "--profile", help="the public URL of the aggregator")
    parser.add_argument(
        "-t",
        "--test",
        default=False,
        action="store_true",
        help="Use test data for all params (but can override w/ other args)",
    )
    args_dict = {}
    args = vars(parser.parse_args())
    for name in arg_names:
        args_dict[name] = os.getenv(f"CUMULUS_UPLOAD_{name.upper()}")
    if args["test"]:
        args_dict["site"] = "general"
        args_dict[
            "file"
        ] = f"{os.path.realpath(os.path.dirname(__file__))}/cube_simple_example.csv"
        args_dict["auth"] = "secretval"
        args_dict["studyname"] = "covid"
    for key in args.keys():
        if args[key] is not None:
            args_dict[key] = args[key]
    upload_file(args_dict)