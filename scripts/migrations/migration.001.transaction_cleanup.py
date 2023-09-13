""" Removes unexpected root nodes/templates/misspelled keys from transaction log. """

import argparse
import io
import json

import arrow
import boto3

EXPECTED_UPLOADERS = [
    "boston_childrens",
    "regenstrief_institute",
    "rush_university",
    "washington_university_st_louis",
    "uc_davis",
    "boston_childrens_rdw",
]


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


def transaction_cleanup(bucket: str):
    client = boto3.client("s3")
    transactions = _get_s3_data("metadata/transactions.json", bucket, client)

    new_t = {}

    # looping twice here to optimize for readability vs speed

    # trimming erroneous keys
    for site in transactions:
        if site in EXPECTED_UPLOADERS:
            site_dict = transactions[site]
            site_dict.pop("template", None)
            new_t[site] = site_dict

    # updating incorrectly spelled keys
    # for future migrations, start from scratch with items rather than ignoring
    for site in new_t:  # pylint: disable=consider-using-dict-items
        for study in new_t[site]:
            for dp in new_t[site][study]:
                for version in new_t[site][study][dp]:
                    if "last_uploaded_date" in new_t[site][study][dp][version]:
                        if new_t[site][study][dp][version]["last_upload"] is None:
                            new_t[site][study][dp][version]["last_upload"] = new_t[
                                site
                            ][study][dp][version]["last_uploaded_date"]
                        else:
                            lud = arrow.get(
                                new_t[site][study][dp][version]["last_uploaded_date"]
                            )
                            lu = arrow.get(
                                new_t[site][study][dp][version]["last_upload"]
                            )
                            if lud > lu:
                                new_t[site][study][dp][version]["last_upload"] = new_t[
                                    site
                                ][study][dp][version]["last_uploaded_date"]
                        new_t[site][study][dp][version].pop("last_uploaded_date")
                    if "transacton_format_version" in new_t[site][study][dp][version]:
                        new_t[site][study][dp][version][
                            "transaction_format_version"
                        ] = new_t[site][study][dp][version]["transacton_format_version"]
                        new_t[site][study][dp][version].pop("transacton_format_version")
        print(json.dumps(new_t, indent=2))
        # _put_s3_data("metadata/transactions.json", bucket, client, new_t)
        print("transactions.json updated")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""Removes erroneous data from transaction log"""
    )
    parser.add_argument("-b", "--bucket", help="bucket name")
    args = parser.parse_args()
    transaction_cleanup(args.bucket)
