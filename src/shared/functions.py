"""Functions used across different lambdas"""

import copy
import dataclasses
import io
import json
import logging
import os
from datetime import UTC, datetime

import boto3

from . import enums, errors

logger = logging.getLogger()
logger.setLevel("INFO")

TRANSACTION_METADATA_TEMPLATE = {
    enums.TransactionKeys.TRANSACTION_FORMAT_VERSION: "2",
    enums.TransactionKeys.LAST_UPLOAD: None,
    enums.TransactionKeys.LAST_DATA_UPDATE: None,
    enums.TransactionKeys.LAST_AGGREGATION: None,
    enums.TransactionKeys.LAST_ERROR: None,
    enums.TransactionKeys.DELETED: None,
}

STUDY_PERIOD_METADATA_TEMPLATE = {
    enums.StudyPeriodMetadataKeys.STUDY_PERIOD_FORMAT_VERSION: "2",
    enums.StudyPeriodMetadataKeys.EARLIEST_DATE: None,
    enums.StudyPeriodMetadataKeys.LATEST_DATE: None,
    enums.StudyPeriodMetadataKeys.LAST_DATA_UPDATE: None,
}

COLUMN_TYPES_METADATA_TEMPLATE = {
    enums.ColumnTypesKeys.COLUMN_TYPES_FORMAT_VERSION: "3",
    enums.ColumnTypesKeys.COLUMNS: None,
    enums.ColumnTypesKeys.LAST_DATA_UPDATE: None,
}


def http_response(
    status: int,
    body: str,
    allow_cors: bool = False,
    extra_headers: dict | None = None,
    skip_convert: bool = False,
    alt_log: str | None = None,
) -> dict:
    """Generates the payload AWS lambda expects as a return value

    :param status: the HTTP status code
    :param body: the message to return in the HTTP body
    :allow_cors: if True, appends CORS allow headers
    :extra_headers: A dictionary of additional headers to append to the response
    :skip_convert: if False, attempts to dump the body from a json object to a string,
        otherwise leaves the body as is
    :alt_log: if true, writes the contents of alt_log to cloudwatch logs, otherwise
        writes the contents of the body.

    """
    headers = {"Content-Type": "application/json"}
    if allow_cors:
        headers.update(
            {
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET",
            }
        )
    if extra_headers:
        headers.update(extra_headers)
    if status >= 200 and status < 300:
        logging.info(alt_log or body)
    else:
        logging.error(body)
    return {
        "isBase64Encoded": False,
        "statusCode": status,
        "body": body if skip_convert else json.dumps(body, default=str),
        "headers": headers,
    }


# S3 json processing


def check_meta_type(meta_type: str) -> None:
    """helper for ensuring specified metadata types"""
    types = [item for item in enums.JsonFilename]
    if meta_type not in types:
        raise ValueError("invalid metadata type specified")


def read_metadata(
    s3_client,
    s3_bucket_name: str,
    *,
    meta_type: str = enums.JsonFilename.TRANSACTIONS,
) -> dict:
    """Reads transaction information from an s3 bucket as a dictionary"""
    check_meta_type(meta_type)
    s3_path = f"{enums.BucketPath.META}/{meta_type}.json"
    res = s3_client.list_objects_v2(Bucket=s3_bucket_name, Prefix=s3_path)
    if "Contents" in res:
        res = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_path)
        doc = res["Body"].read()
        return json.loads(doc)
    else:
        return {}


def update_metadata(
    *,
    metadata: dict,
    study: str,
    data_package: str,
    version: str,
    target: str,
    site: str | None = None,
    dt: datetime | None = None,
    value: str | list | None = None,
    meta_type: str | None = enums.JsonFilename.TRANSACTIONS,
    extra_items: dict | None = None,
):
    """Safely updates items in metadata dictionary


    It's assumed that, other than the version/column/type fields, every item in one
    of these metadata dicts is a ISO date string corresponding to an S3 event timestamp.

    TODO: if we have other cases of non-datetime metadata, consider breaking this
    function into two, one for updating datetimes and one for updating values
    """
    if extra_items is None:
        extra_items = {}

    check_meta_type(meta_type)
    match meta_type:
        case enums.JsonFilename.TRANSACTIONS:
            site_metadata = metadata.setdefault(site, {})
            study_metadata = site_metadata.setdefault(study, {})
            data_package_metadata = study_metadata.setdefault(data_package, {})
            data_version_metadata = _update_or_clone_template(
                data_package_metadata, version, TRANSACTION_METADATA_TEMPLATE
            )

            dt = dt or datetime.now(UTC)
            data_version_metadata[target] = dt.isoformat()
        case enums.JsonFilename.STUDY_PERIODS:
            site_metadata = metadata.setdefault(site, {})
            study_period_metadata = site_metadata.setdefault(study, {})
            data_version_metadata = _update_or_clone_template(
                study_period_metadata, version, STUDY_PERIOD_METADATA_TEMPLATE
            )
            dt = dt or datetime.now(UTC)
            data_version_metadata[target] = dt.isoformat()
        case enums.JsonFilename.COLUMN_TYPES:
            study_metadata = metadata.setdefault(study, {})
            if extra_items.get("type") == "flat":
                data_package_metadata = study_metadata.setdefault(f"{data_package}__{site}", {})
            else:
                data_package_metadata = study_metadata.setdefault(data_package, {})
            data_version_metadata = _update_or_clone_template(
                data_package_metadata, version, COLUMN_TYPES_METADATA_TEMPLATE
            )
            if target == enums.ColumnTypesKeys.COLUMNS:
                data_version_metadata[target] = value
            else:
                dt = dt or datetime.now(UTC)
                data_version_metadata[target] = dt.isoformat()
        # Should only be hit if you add a new JSON dict and forget to add it
        # to this function
        case _:  # pragma: no cover
            raise ValueError(f"{meta_type} does not have a handler for updates.")
    data_version_metadata.update(extra_items)
    return metadata


def _update_or_clone_template(meta_dict: dict, version, template: str):
    return meta_dict.setdefault(version, copy.deepcopy(template))


def write_metadata(
    *,
    sqs_client,
    s3_bucket_name: str,
    metadata: dict,
    meta_type: str = enums.JsonFilename.TRANSACTIONS,
) -> None:
    """Queues transaction deltas to be written to an S3 bucket"""
    check_meta_type(meta_type)
    sqs_client.send_message(
        QueueUrl=os.environ.get("QUEUE_METADATA_UPDATE"),
        MessageBody=json.dumps(
            {
                "s3_bucket_name": s3_bucket_name,
                "key": f"{enums.BucketPath.META}/{meta_type}.json",
                "updates": json.dumps(metadata, default=str, indent=2),
            }
        ),
        MessageGroupId="cumulus",
    )


# S3 data management


def put_s3_file(s3_client, s3_bucket_name: str, key: str, payload: str | dict) -> None:
    """Puts the object in payload into S3 at the specified key"""
    if isinstance(payload, dict):
        payload = json.dumps(payload, default=str, indent=2)
    payload = payload.encode("UTF-8")
    s3_client.put_object(Bucket=s3_bucket_name, Key=key, Body=payload)


def delete_s3_file(s3_client, s3_bucket_name: str, key: str) -> None:
    """Move file to different S3 location"""
    delete_response = s3_client.delete_object(Bucket=s3_bucket_name, Key=key)
    if delete_response["ResponseMetadata"]["HTTPStatusCode"] != 204:
        logger.error("error deleting file %s", key)
        raise errors.S3UploadError


def move_s3_file(s3_client, s3_bucket_name: str, old_key: str, new_key) -> None:
    """Move file to different S3 location"""
    source = {"Bucket": s3_bucket_name, "Key": old_key}
    copy_response = s3_client.copy_object(CopySource=source, Bucket=s3_bucket_name, Key=new_key)
    if copy_response["ResponseMetadata"]["HTTPStatusCode"] != 200:
        logger.error("error copying file %s to %s", old_key, new_key)
        raise errors.S3UploadError
    delete_s3_file(s3_client, s3_bucket_name, old_key)


def get_s3_keys(
    s3_client,
    s3_bucket_name: str,
    prefix: str,
    token: str | None = None,
    max_keys: int | None = None,
) -> list[str]:
    """Gets the list of all keys in S3 starting with the prefix"""
    if max_keys is None:
        max_keys = 1000
    if token:
        res = s3_client.list_objects_v2(
            Bucket=s3_bucket_name, Prefix=prefix, ContinuationToken=token, MaxKeys=max_keys
        )
    else:
        res = s3_client.list_objects_v2(Bucket=s3_bucket_name, Prefix=prefix, MaxKeys=max_keys)
    if "Contents" not in res:
        return []
    contents = [record["Key"] for record in res["Contents"]]
    if res["IsTruncated"]:
        contents += get_s3_keys(s3_client, s3_bucket_name, prefix, res["NextContinuationToken"])
    return contents


def get_filename_from_s3_path(s3_path: str):
    """Given an s3 path/key, returns the filename"""
    return s3_path.split("/")[-1]


def get_folder_from_s3_path(s3_path: str):
    """Given an s3 path/key,returns the folder path"""
    s3_path = get_s3_key_from_path(s3_path)
    return s3_path.rsplit("/", 1)[0]


def get_s3_key_from_path(s3_path: str):
    """returns a valid S3 key given an S3 path (or given a key, returns the key)"""
    if s3_path.startswith("s3"):
        return "/".join(s3_path.split("/")[3:])
    return s3_path


def get_s3_json_as_dict(bucket, key: str, s3_client=None):
    """reads a json object as dict (typically metadata in this case)"""
    s3_client = s3_client or boto3.client("s3")
    bytes_buffer = io.BytesIO()
    s3_client.download_fileobj(
        Bucket=bucket,
        Key=key,
        Fileobj=bytes_buffer,
    )
    return json.loads(bytes_buffer.getvalue().decode())


def get_latest_data_package_version(bucket, prefix):
    """Returns the newest version in a data package folder"""
    s3_client = boto3.client("s3")
    if not prefix.endswith("/"):
        prefix = prefix + "/"
    s3_res = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    highest_ver = None
    if "Contents" in s3_res:
        for item in s3_res["Contents"]:
            ver_str = parse_s3_key(item["Key"]).version
            if ver_str.isdigit():
                if highest_ver is None:
                    highest_ver = ver_str
                else:
                    if int(highest_ver) < int(ver_str):
                        highest_ver = ver_str
    if "Contents" not in s3_res or highest_ver is None:
        logger.error("No data package versions found for %s", prefix)
    return highest_ver


@dataclasses.dataclass(kw_only=True)
class PackageMetadata:
    study: str
    site: str | None = None
    version: str
    data_package: str | None = None
    filename: str | None = None

    def __eq__(self, other):
        if not isinstance(other, PackageMetadata):
            return NotImplemented
        return (
            self.study == other.study
            and self.site == other.site
            and self.data_package == other.data_package
            and self.version == other.version
            and self.filename == other.filename
        )


def parse_s3_key(key: str) -> PackageMetadata:
    """Handles extraction of package metadata from an s3 key"""
    try:
        # did we get a full path instead?
        key_parts = get_s3_key_from_path(key)
        key_parts = key_parts.split("/")
        match key_parts[0]:
            case enums.BucketPath.AGGREGATE:
                package = PackageMetadata(
                    study=key_parts[1],
                    site=None,
                    data_package=key_parts[2].split("__")[1],
                    version=key_parts[3],
                    filename=key_parts[4],
                )
            case (
                enums.BucketPath.ARCHIVE
                | enums.BucketPath.ERROR
                | enums.BucketPath.LAST_VALID
                | enums.BucketPath.LATEST
                | enums.BucketPath.STUDY_META
            ):
                package = PackageMetadata(
                    study=key_parts[1],
                    site=key_parts[3],
                    data_package=key_parts[2].split("__")[1],
                    version=key_parts[4],
                    filename=key_parts[5],
                )
            case enums.BucketPath.FLAT | enums.BucketPath.LATEST_FLAT:
                package = PackageMetadata(
                    study=key_parts[1],
                    site=key_parts[2],
                    data_package=key_parts[3].split("__")[1],
                    version=key_parts[3].split("__")[3],
                    filename=key_parts[4],
                )
            case enums.BucketPath.UPLOAD:
                package = PackageMetadata(
                    study=key_parts[1],
                    site=key_parts[3],
                    data_package=key_parts[2],
                    version=key_parts[4],
                    filename=key_parts[5],
                )
            case enums.BucketPath.UPLOAD_STAGING | enums.BucketPath.ARCHIVE:
                package = PackageMetadata(
                    study=key_parts[1],
                    site=key_parts[2],
                    data_package=None,
                    version=key_parts[3],
                    filename=key_parts[4],
                )
            case _:
                raise errors.AggregatorS3Error(f" {key} does not correspond to a data package")
        if "__" in package.version:
            package.version = package.version.split("__")[-1]

        return package
    except IndexError:
        raise errors.AggregatorS3Error(f"{key} is not an expected S3 key")


def construct_s3_key(
    subbucket: str,
    dp_meta: PackageMetadata | None = None,
    study: str | None = None,
    site: str | None = None,
    data_package: str | None = None,
    version: str | None = None,
    filename: str | None = None,
    subkey: bool = False,
) -> str:
    """Generates the appropriate key for a particular location in S3

    :param subbucket: A root folder used in data processing.
        Should be a member of enum.BucketPath
    :param dp_meta: A PackageMetadata object.
    :param study: The name of a study
    :param site: The site uploading a specified file
    :param data_package: the name of the specific table being uploaded
    :param version: The version of the data package
    :param filename: The filename of the data package
    :returns: If file name is present, a full S3 key, otherwise, a path
      to a given location

    An important hidden property of these paths is that, for
    paths that are meant to be targeted by a glue crawler, these file paths
    should have a specific number of subfolders in them (4). Otherwise, the
    crawler will not be able to correctly identify how to group data into tables.
    """

    # If no dp_meta is present, we'll make one; otherwise, we'll override a
    # provided dp_meta with any additionally provided args
    if dp_meta is None:
        dp_meta = PackageMetadata(
            site=site,
            study=study,
            data_package=data_package,
            version=version,
            filename=filename,
        )
    else:
        dp_meta = dataclasses.replace(dp_meta)
        dp_meta.site = site or dp_meta.site
        dp_meta.study = study or dp_meta.study
        dp_meta.data_package = data_package or dp_meta.data_package or data_package
        dp_meta.version = version or dp_meta.version
        dp_meta.filename = filename or dp_meta.filename
    match subbucket:
        case enums.BucketPath.AGGREGATE:
            key = (
                f"{subbucket}/{dp_meta.study}/{dp_meta.study}__{dp_meta.data_package}/"
                f"{dp_meta.study}__{dp_meta.data_package}__{dp_meta.version}"
            )
        case (
            enums.BucketPath.ERROR
            | enums.BucketPath.LAST_VALID
            | enums.BucketPath.LATEST
            | enums.BucketPath.STUDY_META
        ):
            key = (
                f"{subbucket}/{dp_meta.study}/{dp_meta.study}__{dp_meta.data_package}/"
                f"{dp_meta.site}/{dp_meta.version}"
            )

        case enums.BucketPath.LATEST_FLAT | enums.BucketPath.FLAT:
            key = (
                f"{subbucket}/{dp_meta.study}/{dp_meta.site}/"
                f"{dp_meta.study}__{dp_meta.data_package}__{dp_meta.site}__{dp_meta.version}"
            )
        case enums.BucketPath.UPLOAD:
            key = (
                f"{subbucket}/{dp_meta.study}/{dp_meta.data_package}/"
                f"{dp_meta.site}/{dp_meta.version}"
            )
        case enums.BucketPath.UPLOAD_STAGING:
            key = f"{subbucket}/{dp_meta.study}/{dp_meta.site}/{dp_meta.version}"
        case enums.BucketPath.ARCHIVE:
            key = (
                f"{subbucket}/{dp_meta.study}/{dp_meta.site}/{dp_meta.version}/"
                f"{datetime.now(UTC).isoformat()}"
            )
        case _:
            raise errors.AggregatorS3Error(
                f"{subbucket} does not correspond to a subbucket used for processing data packages"
            )
    if subkey:
        key = key.replace(f"{subbucket}/", "")
    if dp_meta.filename:
        return f"{key}/{dp_meta.filename}"
    return key
