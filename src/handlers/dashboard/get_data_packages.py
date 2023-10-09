""" Lambda for retrieving list of available data packages
"""

import os

from ..shared.decorators import generic_error_handler
from ..shared.enums import BucketPath, JsonFilename
from ..shared.functions import get_s3_json_as_dict, http_response


@generic_error_handler(msg="Error retrieving data packages")
def data_packages_handler(event, context):
    """Retrieves list of data packages from S3."""
    del event
    del context
    data_packages = get_s3_json_as_dict(
        os.environ.get("BUCKET_NAME"),
        f"{BucketPath.CACHE.value}/{JsonFilename.DATA_PACKAGES.value}.json",
    )
    res = http_response(200, data_packages, allow_cors=True)
    return res
