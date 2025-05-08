import logging
import os

import awswrangler

from shared import awswrangler_functions, decorators, enums, functions, pandas_functions, s3_manager

log_level = os.environ.get("LAMBDA_LOG_LEVEL", "INFO")
logger = logging.getLogger()
logger.setLevel(log_level)


def process_flat(manager: s3_manager.S3Manager):
    if awswrangler.s3.does_object_exist(
        f"s3://{manager.s3_bucket_name}/{manager.parquet_flat_key}"
    ):
        manager.move_file(
            manager.parquet_flat_key,
            manager.parquet_flat_key.replace(
                enums.BucketPath.FLAT.value, enums.BucketPath.ARCHIVE.value
            ),
        )

    manager.move_file(
        manager.s3_key,
        manager.parquet_flat_key,
    )
    awswrangler_functions.generate_csv_from_parquet(
        manager.s3_bucket_name,
        manager.parquet_flat_key.split("/", 1)[0],
        manager.parquet_flat_key.split("/", 1)[1],
        f"s3://{manager.s3_bucket_name}/{manager.csv_flat_key}",
    )
    df = awswrangler.s3.read_parquet(f"s3://{manager.s3_bucket_name}/{manager.parquet_flat_key}")
    column_dict = pandas_functions.get_column_datatypes(df)
    extras = {
        "s3_path": f"s3://{manager.s3_bucket_name}/{manager.parquet_flat_key}",
        "type": "flat",
        "total": len(df),
    }
    manager.update_local_metadata(
        enums.ColumnTypesKeys.COLUMNS.value,
        value=column_dict,
        site=manager.site,
        metadata=manager.types_metadata,
        meta_type=enums.JsonFilename.COLUMN_TYPES.value,
        extra_items=extras,
    )
    manager.update_local_metadata(enums.TransactionKeys.LAST_DATA_UPDATE.value, site=manager.site)
    manager.update_local_metadata(
        enums.ColumnTypesKeys.LAST_DATA_UPDATE.value,
        value=column_dict,
        metadata=manager.types_metadata,
        meta_type=enums.JsonFilename.COLUMN_TYPES.value,
        extra_items=extras,
    )
    manager.write_local_metadata(
        metadata=manager.types_metadata, meta_type=enums.JsonFilename.COLUMN_TYPES.value
    )
    manager.write_local_metadata()
    manager.cache_api()


@decorators.generic_error_handler(msg="Error processing flat upload")
def process_flat_handler(event, context):
    """manages event from S3, triggers file processing"""
    del context
    manager = s3_manager.S3Manager(event)
    process_flat(manager)
    res = functions.http_response(200, "Merge successful")
    return res
