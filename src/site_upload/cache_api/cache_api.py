"""Lambda for running and caching query results"""

import json
import os

import awswrangler
import boto3

from shared import decorators, enums, functions


def cache_data_packages(s3_client, s3_bucket_name: str, db: str):
    """Creates a cache of data package metadata information"""
    df = awswrangler.athena.read_sql_query(
        (
            f"SELECT table_name FROM information_schema.tables "  # noqa: S608
            f"WHERE table_schema = '{db}'"  # nosec
        ),
        database=db,
        s3_output=f"s3://{s3_bucket_name}/awswrangler",
        workgroup=os.environ.get("WORKGROUP_NAME"),
    )
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

    # TODO: move the above into the following study-level endpoint
    manifest_keys = functions.get_s3_keys(
        s3_bucket_name=s3_bucket_name, prefix=enums.BucketPath.MANIFEST.value, s3_client=s3_client
    )
    manifest_keys = [x for x in manifest_keys if x.endswith(".json")]
    studies = {}
    for key in manifest_keys:
        dp = functions.parse_s3_key(key)
        if dp.study not in studies.keys():
            studies[dp.study] = {}
        studies[dp.study][dp.version] = functions.get_s3_json_as_dict(
            bucket=s3_bucket_name, key=key, s3_client=s3_client
        )
        # For now, we'll exclude the build stages, but we may pull it back in the future
        # if we have a use case for it.
        studies[dp.study][dp.version].pop("stages", None)
    s3_client.put_object(
        Bucket=s3_bucket_name,
        Key=f"{enums.BucketPath.CACHE.value}/{enums.JsonFilename.STUDIES.value}.json",
        Body=json.dumps(studies, indent=2),
    )


def _get_metadata_from_table(table: dict, study_cols: dict, dp: dict, data_dict: dict) -> dict:
    output = {"description": table["description"], "columns": {}}
    # Note: we can't use dp.data_package here - we have the manifest, not
    # a path to a file

    table_cols = study_cols[table["name"].split("__")[1]][f"{table['name']}__{dp.version}"][
        "columns"
    ]
    for col, details in table_cols.items():
        output["columns"][col] = table_cols[col]
        col_type = next((x for x in data_dict if x["name"] == col), {})
        for key in col_type:
            output["columns"][col][key] = col_type[key]
    return output


def cache_study_data(s3_client, s3_bucket_name: str, db: str) -> None:
    """Creates a cache of study metadata information"""
    manifest_keys = functions.get_s3_keys(
        s3_bucket_name=s3_bucket_name, prefix=enums.BucketPath.MANIFEST.value, s3_client=s3_client
    )
    column_types = functions.get_s3_json_as_dict(
        os.environ.get("BUCKET_NAME"),
        f"{enums.BucketPath.META.value}/{enums.JsonFilename.COLUMN_TYPES.value}.json",
    )
    site_info = functions.get_s3_json_as_dict(
        os.environ.get("BUCKET_NAME"),
        f"{enums.BucketPath.ADMIN.value}/metadata.json",
    )
    manifest_keys = [x for x in manifest_keys if x.endswith(".json")]
    studies = {}
    for key in manifest_keys:
        dp = functions.parse_s3_key(key)
        if dp.study not in studies.keys():
            studies[dp.study] = {}
        study_cols = column_types[dp.study]
        manifest = functions.get_s3_json_as_dict(
            bucket=s3_bucket_name, key=key, s3_client=s3_client
        )
        owning_site_info = next(
            (
                site_info[x]
                for x in site_info.keys()
                if site_info[x]["path"] == manifest.get("study_owner")
            ),
            {"foo": "bar"},
        )
        print("site info", site_info)
        print("manifest", manifest)
        print("owner", owning_site_info)
        if display := owning_site_info.get("display"):
            manifest["study_owner_display"] = display
        else:  # pragma: no cover
            manifest["study_owner_display"] = manifest["study_owner"]
        # we'll flatten the manifest table metadata for the api
        manifest["tables"] = {}
        for stage in manifest.get("stages", []):
            for action in manifest["stages"][stage]:
                if "tables" in action.keys():
                    for table in action["tables"]:
                        if isinstance(table, dict):
                            manifest["tables"][table["name"]] = _get_metadata_from_table(
                                table, study_cols, dp, manifest.get("data_dictionary", {})
                            )

        manifest.pop("stages", None)
        studies[dp.study][dp.version] = manifest
    s3_client.put_object(
        Bucket=s3_bucket_name,
        Key=f"{enums.BucketPath.CACHE.value}/{enums.JsonFilename.STUDIES.value}.json",
        Body=json.dumps(studies, indent=2),
    )


def cache_api_data(s3_client, s3_bucket_name: str, db: str, target: str) -> None:
    """Performs caching of API data"""
    if target == enums.JsonFilename.DATA_PACKAGES.value:
        cache_data_packages(s3_client, s3_bucket_name, db)
        cache_study_data(s3_client, s3_bucket_name, db)
    else:
        raise KeyError("Invalid API caching target")


@decorators.generic_error_handler(msg="Error caching API responses")
def cache_api_handler(event, context):
    """manages event from SNS, executes queries and stashes cache in S#"""
    del context
    s3_bucket_name = os.environ.get("BUCKET_NAME")
    s3_client = boto3.client("s3")
    db = os.environ.get("GLUE_DB_NAME")
    if "Records" in event:
        target = event["Records"][0]["Sns"]["Subject"]
    elif event.get("detail-type") == "Glue Crawler State Change":
        target = enums.JsonFilename.DATA_PACKAGES.value
    else:  # pragma: no cover
        return functions.http_response(500, "Unexpected event source")
    cache_api_data(s3_client, s3_bucket_name, db, target)
    return functions.http_response(200, "Study period update successful")
