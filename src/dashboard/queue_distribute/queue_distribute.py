"""Handler for submitting studies to an SNS queue for distribution to remote sites.

In the long term, this module (or a submodule imported by this module) will be responsible
for parsing the output of a dashboard guided workflow/query builder generated payload,
and converting it to a cumulus library compatible study.

It also enables distribution of a smart-on-fhir owned study directly from github,
which is the mode it operates in today.

Due to needing cumulus library, this handler is different from all the other lambda
handlers, in that it is packaged in a docker image and loaded from an elastic container
registry. This has a few architectural implications not present in other lambdas - notably,
the home directory is static and we have to write any data to disk inside of /tmp as the
only writable location.

"""

import json
import os
import pathlib
import subprocess

import boto3
from cumulus_library import cli

from shared import decorators, functions

# lambda performance tuning - moving these two variables to be global in the module
# means that their initialization happens during build, rather than invocation.
# This helps reduce the spinup time, especially for the boto client, and since
# there are some hefty bits in here already with the docker spinup, shaving a second
# or two off here is helpful to keep us under the ten second timeout.
#
# https://docs.aws.amazon.com/lambda/latest/dg/best-practices.html#function-code

sns_client = boto3.client("sns", os.environ.get("AWS_REGION"))
BASE_DIR = "/tmp"  # noqa: S108


def get_study_from_github(config):
    try:
        args = ["--depth", "1", config["url"], f"{BASE_DIR}/studies"]
        if config["tag"]:
            args = ["--branch", config["tag"], *args]
        subprocess.run(["/usr/bin/git", "clone", *args], check=True)  # noqa: S603

    except subprocess.CalledProcessError:
        # TODO: retry/backoff logic? or do we just have a user queue again?
        raise ValueError(f"{config['url']} is unavailable")


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
            f"{write_path}",
        ],
    )
    return pathlib.Path(f"{write_path}") / f"{body['study_name']}.zip"


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
def queue_handler(event: dict, context):
    """Creates a distribution packages and queues for delivery"""
    del context
    body = event["Records"][0]["Sns"]["Message"]
    body = json.loads(body)
    process_body(body)
    payload = prepare_study(body)
    topic_sns_arn = os.environ.get("TOPIC_STUDY_PAYLOAD_ARN")
    with open(payload, "rb") as f:
        file = f.read()
        sns_client.publish(
            TopicArn=topic_sns_arn,
            Message=json.dumps({"study": body["study_name"]}),
            MessageGroupId=body["study_name"],
            Subject=body["study_name"],
            MessageAttributes={"study": {"DataType": "Binary", "BinaryValue": file}},
        )
    res = functions.http_response(200, f'Study {body["study_name"]} queued.')
    return res
