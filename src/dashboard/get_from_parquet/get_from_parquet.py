import awswrangler
import pyarrow

from shared import decorators, functions


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
                payload = df.to_csv(index=False)
            case "tsv":
                payload = df.to_csv(index=False, sep="|")
            case "json":
                return functions.http_response(
                    200, df.to_json(orient="table", index=False), skip_convert=True
                )
        # TODO: this should be converted to a streamingresponse at some point
        # https://github.com/awslabs/aws-lambda-web-adapter/tree/main/examples/fastapi-response-streaming
        return functions.http_response(
            200,
            payload,
            extra_headers={
                "Content-Type": "text/csv",
                "Content-disposition": (
                    f"attachment; filename={s3_path.split('/')[-1].replace('.parquet', '.csv')}"
                ),
                "Content-Length": len(payload.encode("UTF-8")),
            },
            skip_convert=True,
        )
    else:
        return functions.http_response(
            200, df.to_json(orient="table", index=False), skip_convert=True
        )
