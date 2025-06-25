from datetime import datetime

import awswrangler
import pyarrow

from shared import decorators, enums, functions, s3_manager


@decorators.generic_error_handler(msg="Error retrieving data from parquet")
def from_parquet_handler(event, context):
    """manages event and returns parquet file as json dict"""
    del context
    try:
        s3_path = event["queryStringParameters"]["s3_path"]
        df = awswrangler.s3.read_parquet(s3_path)
    except (KeyError, FileNotFoundError, pyarrow.lib.ArrowInvalid):
        res = functions.http_response(404, "S3_path not found")
        return res
    if output_type := event["queryStringParameters"].get("type"):
        match output_type:
            case "csv":
                data = df.to_csv(index=False)
            case "tsv":
                data = df.to_csv(index=False, sep="|")
            case "json":
                data = df.to_json(orient="table", index=False)

    else:
        data = df.to_json(orient="table", index=False)
    ts = datetime.utcnow().timestamp()
    temp_path = f"{enums.BucketPath.TEMP.value}/{ts}/{functions.get_s3_key_from_path(s3_path)}"
    manager = s3_manager.S3Manager(event)
    manager.write_data_to_file(data=data, path=temp_path)
    url = manager.get_presigned_download_url(temp_path)
    res = functions.http_response(302, None, extra_headers={"location": url})
    return res
