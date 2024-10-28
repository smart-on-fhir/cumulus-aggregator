"""Lambda for retrieving list of available data packages"""

import os

from shared import decorators, enums, functions


@decorators.generic_error_handler(msg="Error retrieving data packages")
def data_packages_handler(event, context):
    """Retrieves list of data packages from S3."""
    del context
    status = 200
    data_packages = functions.get_s3_json_as_dict(
        os.environ.get("BUCKET_NAME"),
        f"{enums.BucketPath.CACHE.value}/{enums.JsonFilename.DATA_PACKAGES.value}.json",
    )
    payload = data_packages
    if event.get("queryStringParameters"):
        filtered_packages = []
        for package in data_packages:
            if package["name"] == event["queryStringParameters"]["name"]:
                filtered_packages.append(package)
        payload = filtered_packages
    elif event.get("pathParameters"):
        found = None
        for package in data_packages:
            if event["pathParameters"]["data_package_id"] == package["id"]:
                found = package
        if found:
            payload = found
        else:
            status = 404
            payload = None
    res = functions.http_response(status, payload, allow_cors=True)
    return res
