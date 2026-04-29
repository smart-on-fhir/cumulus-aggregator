import json
import logging
import os

from shared import functions, s3_manager

log_level = os.environ.get("LAMBDA_LOG_LEVEL", "INFO")
logger = logging.getLogger()
logger.setLevel(log_level)


def process_manifest(manager: s3_manager.S3Manager):
    key = manager.s3_key
    manifest = manager.get_manifest()
    print(manifest)
    if manifest != {}:
        # Is this upload from someone other than the owning institution?
        if manifest["study_owner"] != manager.site:
            manager.delete_file(key)
            return
    new_manifest = functions.get_s3_toml_as_dict(bucket=manager.s3_bucket_name, key=key)
    # Did we get a non-manifest file somehow?
    if new_manifest.get("study_prefix") is None:
        manager.delete_file(key)
        return

    new_manifest["study_owner"] = manager.site
    manager.write_data_to_file(json.dumps(new_manifest, indent=2), manager.manifest)
    manager.delete_file(key)


# @decorators.generic_error_handler(msg="Error processing manifest")
def process_manifest_handler(event, context):
    """manages event from S3, triggers file processing"""
    del context
    manager = s3_manager.S3Manager(event)
    process_manifest(manager)
    res = functions.http_response(200, "Manifest processed")
    return res
