import os
import pdb
from unittest import TestCase
import json
from urllib import request
import boto3
import requests

# TODO: this boilerplate needs to be made into a valid test case

"""
Make sure env variable AWS_SAM_STACK_NAME exists with the name of the stack we are going to test. 
"""


class TestApiGateway(TestCase):
    api_endpoint: str

    @classmethod
    def get_stack_name(cls) -> str:
        stack_name = os.environ.get("AWS_SAM_STACK_NAME", "cumulus-aggregator")
        if not stack_name:
            raise Exception(
                "Cannot find env var AWS_SAM_STACK_NAME. \n"
                "Please setup this environment variable with the stack name where we are running integration tests."
            )

        return stack_name

    def setUp(self) -> None:
        """
        Based on the provided env variable AWS_SAM_STACK_NAME,
        here we use cloudformation API to find out what the HelloWorldApi URL is
        """
        stack_name = TestApiGateway.get_stack_name()

        client = boto3.client("cloudformation")

        try:
            response = client.describe_stacks(StackName=stack_name)
        except Exception as e:
            raise Exception(
                f"Cannot find stack {stack_name}. \n"
                f'Please make sure stack with the name "{stack_name}" exists.'
            ) from e

        stacks = response["Stacks"]

        stack_outputs = stacks[0]["Outputs"]
        api_outputs = [
            output for output in stack_outputs if output["OutputKey"] == "WebEndpoint"
        ]
        print(api_outputs)
        self.assertTrue(api_outputs, f"Cannot find web API in stack {stack_name}")

        self.api_endpoint = api_outputs[0]["OutputValue"]

    def test_fetch_upload_url(self):
        """
        Call the API Gateway endpoint and check the response
        """
        response = requests.post(
            self.api_endpoint, data=json.dumps({"name": "St. Elsewhere"})
        )
        print(response)
        self.assertEqual(response.status_code, 200)

        response = requests.post(self.api_endpoint, data=json.dumps({}))
        print(response)
        self.assertEqual(response.status_code, 400)
