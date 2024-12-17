import json
import os
import pathlib

import boto3
import requests
from cumulus_library import cli
from shared import decorators, functions

# lambda performance tuning - moving these outside mean that they will not
# contribute to the lambda init window
sns_client = boto3.client("sns", os.environ.get("AWS_REGION"))
# in dockerized lambdas, `/tmp` is the only valid write location
BASE_DIR = "/tmp"  # noqa: S108


def get_study_from_github(url):
    if "smart-on-fhir" not in url:
        raise ValueError(f"{url} is not an official Cumulus study.")
    if not url.endswith("/"):
        url = url + "/"
    api_url = (
        url.replace("https://github.com/", "https://api.github.com/repos/")
        + "git/trees/main?recursive=1"
    )
    raw_url_base = (
        url.replace("https://github.com/", "https://raw.githubusercontent.com/") + "main/"
    )
    study_name = url.split("/")[-2]
    res = requests.get(api_url, timeout=10)
    if res.status_code != 200:
        raise ValueError(f"{url} is not a valid git repository")
    files = res.json()["tree"]
    for file in files:
        if file["type"] != "blob":
            continue
        write_path = pathlib.Path(f"{BASE_DIR}/studies") / study_name / file["path"]
        write_path.parent.mkdir(parents=True, exist_ok=True)
        with requests.get(raw_url_base + file["path"], timeout=10) as res:
            with open(write_path, "w", encoding="UTF-8") as f:
                f.write(res.text)


def prepare_study(body: dict):
    write_path = pathlib.Path(f"{BASE_DIR}/prepared")
    write_path.mkdir(parents=True, exist_ok=True)
    cli.main(
        cli_args=[
            "build",
            "-t",
            body["study_name"],
            "-s",
            f"{BASE_DIR}/studies",
            "--prepare",
            f"{BASE_DIR}/prepared",
        ],
    )
    return pathlib.Path(f"/{BASE_DIR}/prepared") / f"{body['study_name']}.zip"


def process_body(body: dict):
    """Selects the appropriate handler for processing study requests"""
    for key in body.keys():
        match key:
            case "study_name":
                pass
            case "github":
                get_study_from_github(body[key])
            case _:
                raise ValueError(f"Invalid key {key} received.")


@decorators.generic_error_handler(msg="Error generating distributed request")
def distribute_handler(event: dict, context):
    """Creates a distribution packages and queues for delivery"""
    del context
    body = json.loads(event["body"])
    process_body(body)
    payload = prepare_study(body)
    topic_sns_arn = os.environ.get("TOPIC_STUDY_PAYLOAD_ARN")
    with open(payload, "rb") as f:
        sns_client.publish(
            TopicArn=topic_sns_arn,
            Message=str(f.read()),
            MessageGroupId=body["study_name"],
            Subject=body["study_name"],
        )
    res = functions.http_response(200, f'Study {body["study_name"]} queued.')
    return res
