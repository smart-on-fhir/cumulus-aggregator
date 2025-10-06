"""Lambda for updating metadata

Note: since this lambda is used with an EventSource of a FIFO queue,
this lambda will not be allowed to be concurrently executed. Instead,
it will be executed once, and fed up to ten events from that queue
to be run in batch. Once it terminates, it will be re-invoked if there
are remaining events in the queue.
"""

import json
import os

import boto3
import botocore

from shared import decorators, functions

s3_client = boto3.client("s3")


def update_source(metadata, update):
    for update_key, update_val in update.items():
        if isinstance(update_val, dict):
            metadata[update_key] = update_source(metadata.get(update_key, {}), update[update_key])
        elif update_val is not None or metadata.get(update_key) is None:
            metadata[update_key] = update_val
    return metadata


def process_event_queue(records):
    sources = {}
    metadata = {}
    for record in records:
        # Frustratingly, AWS and moto generate different cases for this key
        message = json.loads(record["body"] if "body" in record.keys() else record["Body"])
        if message["key"] not in sources:
            sources[message["key"]] = []
        updates = json.loads(message["updates"])
        sources[message["key"]].append(updates)

    for key, updates in sources.items():
        if key not in metadata.keys():
            try:
                metadata[key] = functions.get_s3_json_as_dict(os.environ.get("BUCKET_NAME"), key)
            except botocore.exceptions.ClientError:
                metadata[key] = {}
        for update in updates:
            metadata[key] = update_source(metadata[key], update)
    for key, metadata in metadata.items():
        functions.put_s3_file(s3_client, os.environ.get("BUCKET_NAME"), key, metadata)


@decorators.generic_error_handler(msg="Error processing metadata events")
def update_metadata_handler(event, context):
    del context
    process_event_queue(event["Records"])
    return functions.http_response(200, "Metadata event processing successful")
