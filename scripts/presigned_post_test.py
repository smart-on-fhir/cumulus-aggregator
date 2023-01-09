# an example of how we could use the presigned post
# based on https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html

import requests
import sys
import json

sys.path.insert(0, "/Users/mgarber/code/cumulus-aggregator/src/handlers")
import fetch_upload_url

# Generate a presigned S3 POST URL for this file
object_name = "presigned_post_test.py"
response = fetch_upload_url.create_presigned_post(
    "cumulus-aggregator", "dir/" + object_name
)
if response is None:
    exit(1)
body = json.loads(response["body"])

# Uploading to S3 with requests
with open(object_name, "rb") as f:
    files = {"file": (object_name, f)}
    http_response = requests.post(body["url"], data=body["fields"], files=files)

# If successful, returns HTTP status code 204
print(f"File upload HTTP status code: {http_response.status_code}")
