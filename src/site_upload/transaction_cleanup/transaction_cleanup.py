"""Lambda for cleaning up transactions in case of an error"""

import json

from shared import functions, s3_manager


def transaction_cleanup_handler(event, context):
    """Cleans up a transaction if it the job has stalled for some reason

    TODO: this should become more sophisticated as the upload manifest functionality is built out"""
    del context
    payload = json.loads(event["Records"][0]["body"])
    manager = s3_manager.S3Manager(event, site=payload["site"], study=payload["study"])
    manager.delete_transaction()
    res = functions.http_response(200, "cleared lock")
    return res
