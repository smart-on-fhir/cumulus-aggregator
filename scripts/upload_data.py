# basic utility for debugging merge behavior

import requests
import sys
import json

sys.path.insert(0, "../src/handlers")
import fetch_upload_url

# TODO : look into entering via the API endpoint

# Generate two presigned S3 POST URLs for this file for merge testing
object_name = "cube_simple_example.csv"
for prefix in ["a_", "b_"]:
    response = fetch_upload_url.create_presigned_post(
        "cumulus-aggregator", "site_uploads/" + prefix + object_name
    )
    if response is None:
        exit(1)
    body = json.loads(response["body"])

    # Uploading to S3 with requests
    with open(object_name, "rb") as f:
        files = {"file": (object_name, f)}
        http_response = requests.post(body["url"], data=body["fields"], files=files)

    # If successful, returns HTTP status code 204
    print(f"{prefix+object_name} upload HTTP status code: {http_response.status_code}")
