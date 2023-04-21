# Cumulus upload integration testing

This document will walk you through completing two kinds of integration testing for transmitting data to the Cumulus aggregator.

## Prerequesites

You’ll need:

- A local copy of the [Cumulus library project](https://github.com/smart-on-fhir/library)
For the quick test: a copy of an example export with synthea data - The library has a set of [test data](https://github.com/smart-on-fhir/library/tree/main/tests/test_data) you can use for this, or BCH can provide a larger dataset to use for this.
- For the long test: a local copy of the [sample bulk fhir datasets](https://github.com/smart-on-fhir/sample-bulk-fhir-datasets), and an instance of the [Cumulus ETL](https://github.com/smart-on-fhir/cumulus-etl)
- You’ll need to either set up your own aggregator instance or reach out to BCH to get credentials configured to generate a site ID using the [credential management](https://github.com/smart-on-fhir/cumulus-aggregator/blob/main/scripts/credential_management.py) script.

## Configuring uploads

The Cumulus library has a script for [Uploading data in bulk](https://github.com/smart-on-fhir/library/blob/main/data_export/bulk_upload.py). You can pass values to it via the command line, but we recommend setting up envrionment variables instead. Specifically:

`CUMULUS_AGGREGATOR_USER` \ `CUMULUS_AGGREGATOR_ID` - these should match the credentials configured in aggregator via the credential management script.

`CUMULUS_AGGREGATOR_URL` - this should, for this testing, be set to a non production environment. We're using `https://staging.aggregator.smartcumulus.org/upload/`, but you can use an endpoint of your choice if you are self-hosting an aggregator.

## Quick test: uploading test data

With these environment variables set, the bulk uploader is all set to load data. Put your data in a folder inside `library/data_export/`, and the bulk uploader will use the folder name to generate a study and push it out to the aggregator. You can use the `-p` CLI switch to print out the prefetch request/response and what the data upload would look like without acutally sending any files over the wires. A user with direct access to the aggregator S3 bucket can verify if the data was successfully processed.

## Integration test: Processing synthetic data through ETL

With the above passing, you can test your processing pipeline entirely with synthetic data by running through the following steps:

- In the synthetic data repository, check out one of the branches containing a synthetic dataset.
- Create an environment variable, `CUMULUS_SAMPLE_DATA`, which points to the synthetic dataset directory
- If you haven't already, set up the ETL, following the [AWS setup guide](https://github.com/smart-on-fhir/cumulus-etl/blob/main/docs/howtos/set-up-aws.md) and the [ETL setup guide](https://github.com/smart-on-fhir/cumulus-etl/blob/main/docs/howtos/run-cumulus-etl.md)
- Run the ETL with a command like the following:
```
docker compose -f $CUMULUS_REPO_PATH/compose.yaml \
 run --volume $CUMULUS_SAMPLE_DATA:/cumulus-etl/data --rm \
 cumulus-etl \
  --input-format=ndjson \
  --output-format=deltalake \
  --batch-size=100000 \
  --s3-region=us-east-1 \
  --task-filter=cpu \
  /cumulus-etl/data  \
  s3://your-etl-output-bucket/output/  \
  s3://your-etl-phi-output-bucket/output
```
- When it's complete, run the crawler you created in the AWS setup guide to make sure the appropriate athena tables are generated correctly.
- In the cumulus library repo, build the athena tables and export results, with `./library/make.py -b -t` (make sure you set the cumulus library [setup instructions][https://github.com/smart-on-fhir/library#setup] and set the appropriate environment variables/aws credentials)
- when the export completes, you should have folders in `./library/data_export` corresponding to the currently configured exportable studies (at the time of this writing, core and covid). Running the bulkd uploader script should push this data to the aggregator instance configured above.

If this works, then you've proved out the whole data export flow and should be able to run a production export flow, just changing the `CUMUMULUS_AGGREGATOR_*` environment variables to point to the production instance.
