"""Lambda for running and caching query results"""

import json
import os

import awswrangler
import boto3

from shared import decorators, enums, functions


def cache_api_data(s3_client, s3_bucket_name: str, db: str, target: str) -> None:
    """Performs caching of API data"""
    if target == enums.JsonFilename.DATA_PACKAGES.value:
        df = awswrangler.athena.read_sql_query(
            (
                f"SELECT table_name FROM information_schema.tables "  # noqa: S608
                f"WHERE table_schema = '{db}'"  # nosec
            ),
            database=db,
            s3_output=f"s3://{s3_bucket_name}/awswrangler",
            workgroup=os.environ.get("WORKGROUP_NAME"),
        )
    else:
        raise KeyError("Invalid API caching target")
    # this filters out system tables
    data_packages = df[df["table_name"].str.contains("__")].iloc[:, 0]
    column_types = functions.get_s3_json_as_dict(
        os.environ.get("BUCKET_NAME"),
        f"{enums.BucketPath.META.value}/{enums.JsonFilename.COLUMN_TYPES.value}.json",
    )
    dp_details = []
    files = functions.get_s3_keys(s3_client, s3_bucket_name, enums.BucketPath.AGGREGATE.value)
    flat_files = functions.get_s3_keys(s3_client, s3_bucket_name, enums.BucketPath.FLAT.value)
    files += flat_files
    for dp in list(data_packages):
        if not any([f"/{dp}" in x for x in files]):
            continue
        dp_detail = {}
        dp_parts = dp.split("__")
        dp_detail["study"] = dp_parts[0]
        # if the data package has four elements, it's a flat table
        if len(dp_parts) == 4:
            dp_detail["name"] = "__".join([dp_parts[1], dp_parts[2]])
        else:
            dp_detail["name"] = dp_parts[1]
        studies = column_types.get(dp_detail["study"], {"name": None})
        dp_ids = studies.get(dp_detail["name"], None)
        if dp_ids is None:  # pragma: no cover
            continue
        for dp_id in dp_ids:
            if dp_id not in dp:
                continue

            metadata = functions.parse_s3_key(
                functions.get_s3_key_from_path(dp_ids[dp_id]["s3_path"])
            )
            dp_dict = {
                **dp_detail,
                **dp_ids[dp_id],
                "version": metadata.version,
                "id": f"{metadata.study}__{metadata.data_package}__{metadata.version}",
            }
            if "__flat" in dp_dict["s3_path"]:
                dp_dict["site"] = metadata.site
                dp_dict["type"] = "flat"
                dp_dict["id"] = (
                    f"{metadata.study}__{metadata.data_package}__"
                    f"{metadata.site}__{metadata.version}"
                )
            dp_details.append(dp_dict)
    s3_client.put_object(
        Bucket=s3_bucket_name,
        Key=f"{enums.BucketPath.CACHE.value}/{enums.JsonFilename.DATA_PACKAGES.value}.json",
        Body=json.dumps(dp_details, indent=2),
    )


@decorators.generic_error_handler(msg="Error caching API responses")
def cache_api_handler(event, context):
    """manages event from SNS, executes queries and stashes cache in S#"""
    del context
    print("Event recieved")
    print(event)
    s3_bucket_name = os.environ.get("BUCKET_NAME")
    s3_client = boto3.client("s3")
    db = os.environ.get("GLUE_DB_NAME")
    target = event["Records"][0]["Sns"]["Subject"]
    cache_api_data(s3_client, s3_bucket_name, db, target)
    res = functions.http_response(200, "Study period update successful")
    return res
