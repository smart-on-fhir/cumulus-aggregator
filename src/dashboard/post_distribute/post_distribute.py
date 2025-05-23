"""Handler for validating the payload contents and submitting to queue"""

import json
import os
import urllib

import boto3
import requests

from shared import decorators, functions

sns_client = boto3.client("sns", os.environ.get("AWS_REGION"))
valid_chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~:/?#[]@!$&'()*+,"


def validate_github_url(config):
    parsed_url = urllib.parse.urlparse(config["url"])
    if (
        not parsed_url.netloc == "github.com"
        or not parsed_url.path.startswith("/smart-on-fhir/")
        or any(c not in valid_chars for c in config["url"])
    ):
        raise ValueError(f"{config['url']} is not an official Cumulus study.")
    res = requests.get(config["url"], timeout=10)
    if res.status_code != 200:
        raise ValueError(f"{config['url']} is not a valid git repository")
    if "tag" in config and config["tag"] is not None:
        res = requests.get(config["url"] + f"/tree/{config['tag']}", timeout=10)
        if res.status_code != 200:
            raise ValueError(f"{config['tag']} is not a valid tag")


def validate_body(body: dict):
    """Selects the appropriate handler for processing study requests"""
    for key in body.keys():
        match key:
            case "study_name":
                pass
            case "github":
                validate_github_url(body[key])
            case _:
                raise ValueError(f"Invalid key {body[key]} received.")


@decorators.generic_error_handler(msg="Error generating distributed request")
def distribute_handler(event: dict, context):
    """Creates a distribution packages and queues for delivery"""
    del context
    body = json.loads(event["body"])
    validate_body(body)
    topic_sns_arn = os.environ.get("TOPIC_QUEUE_API_ARN")
    sns_client.publish(TopicArn=topic_sns_arn, Message=event["body"], Subject=body["study_name"])
    # TODO: should we create an ID/API for the dashboard to track request status?
    res = functions.http_response(200, f"Preparing to queue {body['study_name']}.")
    return res
