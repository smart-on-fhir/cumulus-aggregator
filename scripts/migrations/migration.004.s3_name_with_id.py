"""Removes unexpected root nodes/templates/misspelled keys from transaction log."""

import argparse
import io
import json

import boto3


def _get_s3_data(key: str, bucket_name: str, client) -> dict:
    """Convenience class for retrieving a dict from S3"""
    try:
        bytes_buffer = io.BytesIO()
        client.download_fileobj(Bucket=bucket_name, Key=key, Fileobj=bytes_buffer)
        return json.loads(bytes_buffer.getvalue().decode())
    except Exception:  # pylint: disable=broad-except
        return {}


def _put_s3_data(key: str, bucket_name: str, client, data: dict) -> None:
    """Convenience class for writing a dict to S3"""
    b_data = io.BytesIO(json.dumps(data).encode())
    client.upload_fileobj(Bucket=bucket_name, Key=key, Fileobj=b_data)


def s3_name_with_id(bucket: str):
    client = boto3.client("s3")
    res = client.list_objects_v2(Bucket=bucket)
    contents = res["Contents"]
    moved_files = 0
    for s3_file in contents:
        key = s3_file["Key"]
        key_array = key.split("/")
        if key_array[0] == "aggregates" and len(key_array[3]) == 3:
            key_array[3] = f"{key_array[2]}__{key_array[3]}"
            new_key = "/".join(key_array)
            client.copy({"Bucket": bucket, "Key": key}, bucket, new_key)
            client.delete_object(Bucket=bucket, Key=key)
            moved_files += 1
    print(f"Updated {moved_files} aggregates")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="""Changes lowest directory in S3 to file id""")
    parser.add_argument("-b", "--bucket", help="bucket name")
    args = parser.parse_args()
    s3_name_with_id(args.bucket)
