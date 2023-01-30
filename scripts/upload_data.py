# basic utility for debugging merge behavior

import requests
import json
import sys

# workaround - the lambda environment resolves dependencies primarily based
# on absolute path, and rather than modify the lambda code to support this test
# script, we'll tack on the project root to the path for just this case.

from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from src.handlers import fetch_upload_url

url_subdomain = "p8eb5jgrr3"
url = f"https://{url_subdomain}.execute-api.us-east-1.amazonaws.com/Prod/"

# Generate two presigned S3 POST URLs for this file for merge testing
object_name = "cube_simple_example.csv"
for site in ["elsewhere", "general"]:
    headers = {"user": site, "authorization": "secretval"}
    response = requests.post(
        url,
        headers=headers,
        json={"study": "covid", "filename": f"{site}_{object_name}"},
    )
    if response is None:
        exit(1)
    body = response.json()
    print(response)
    # Uploading to S3 with requests
    with open(object_name, "rb") as f:
        files = {"file": (object_name, f)}
        http_response = requests.post(body["url"], data=body["fields"], files=files)

    # If successful, returns HTTP status code 204
    print(f"{site}_{object_name} upload HTTP status code: {http_response.status_code}")
