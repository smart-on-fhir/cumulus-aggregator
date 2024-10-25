import argparse
import io
import os
import pathlib
import sys
import zipfile

import requests
from rich.progress import track


def bulk_csv_download(args):
    if args.api_key is None:
        args.api_key = os.getenv("CUMULUS_AGGREGATOR_API_KEY")
    args.type = args.type.replace("_", "-")
    if args.type not in ["last-valid", "aggregates"]:
        sys.exit('Invalid type. Expected "last-valid" or "aggregates"')
    dp_url = f"https://{args.domain}/{args.type}"
    try:
        res = requests.get(dp_url, headers={"x-api-key": args.api_key}, timeout=300)
    except requests.exceptions.ConnectionError:
        sys.exit("Invalid domain name")
    if res.status_code == 403:
        sys.exit("Invalid API key")
    file_urls = res.json()
    urls = []
    version = 0
    for file_url in file_urls:
        file_array = file_url.split("/")
        dp_version = int(file_array[4 if args.type == "last-valid" else 3])
        if file_array[1] == args.study:
            if dp_version > version:
                version = int(dp_version)
                urls = []
            elif int(dp_version) == version:
                if (
                    args.type == "last-valid" and args.site == file_array[3]
                ) or args.type == "aggregates":
                    urls.append(file_url)
    if len(urls) == 0:
        sys.exit(f"No aggregates matching {args.study} found")
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w") as zip_archive:
        for file in track(urls, description=f"Downloading {args.study} aggregates"):
            csv_url = f"https://{args.domain}/{file}"
            res = requests.get(
                csv_url, headers={"x-api-key": args.api_key}, allow_redirects=True, timeout=300
            )
            with zip_archive.open(file.split("/")[-1], "w") as f:
                f.write(bytes(res.text, "UTF-8"))
    with open(pathlib.Path.cwd() / f"{args.study}.zip", "wb") as output:
        output.write(archive.getbuffer())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="""Fetches all data for a given study""")
    parser.add_argument("-s", "--study", help="Name of study to download", required=True)
    parser.add_argument("-i", "--site", help="Name of site to download (last-valid only)")
    parser.add_argument(
        "-d", "--domain", help="Domain of aggregator", default="api.smartcumulus.org"
    )
    parser.add_argument("-t", "--type", help="type of aggregate", default="last-valid")
    parser.add_argument("-a", "--apikey", dest="api_key", help="API key of aggregator")
    args = parser.parse_args()
    bulk_csv_download(args)
