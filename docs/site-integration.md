<!-- Target audience: engineer familiar with the project, helpful direct tone -->
# Cumulus upload integration testing

This document will walk you through completing two kinds of integration testing for transmitting data to the Cumulus aggregator.

## Prerequesites

You’ll need:

- A local copy of the [Cumulus library project](https://github.com/smart-on-fhir/library)
- For the quick test: a copy of an example export with synthea data - The library has a set of [test data](https://github.com/smart-on-fhir/library/tree/main/tests/test_data) you can use for this.
- For the long test: a local copy of the [sample bulk fhir datasets](https://github.com/smart-on-fhir/sample-bulk-fhir-datasets#downloads), and an instance of the [Cumulus ETL](https://github.com/smart-on-fhir/cumulus-etl)
- You’ll need to either set up your own aggregator instance or reach out to BCH to get credentials configured to generate a site ID using the [credential management](https://github.com/smart-on-fhir/cumulus-aggregator/blob/main/scripts/credential_management.py) script.

## Configuring uploads

The Cumulus library has a script for [Uploading data in bulk](https://github.com/smart-on-fhir/library/blob/main/data_export/bulk_upload.py). You can pass values to it via the command line, but we recommend setting up environment variables instead. Specifically:

`CUMULUS_AGGREGATOR_USER` \ `CUMULUS_AGGREGATOR_ID` - these should match the credentials configured in aggregator via the credential management script.

`CUMULUS_AGGREGATOR_URL` - this should, for this testing, be set to a non production environment. The BCH aggregator is using `https://staging.aggregator.smartcumulus.org/upload/` for this, but you can use an endpoint of your choice if you are self-hosting an aggregator.

## Quick test: uploading test data

With these environment variables set, the bulk uploader is all set to load data. Perform the following steps, inside of the `library` project:

- Copy the test data from `./tests/test_data` into `./data_export/test_data`
- If desired, perform an upload dry run with `./data_export/bulk_upload.py --preview` - this will show you what the bulk uploader will do without actually sending data
- Run the bulk uploader with `./data_export/bulk_upload.py`
- A user with access to the agggregator's S3 bucket can verify if the upload was successful

## Integration test: Processing synthetic data through ETL

If the quick test was successful, you can test your processing pipeline entirely with synthetic data. by running through the following steps:

- If you haven't already, you'll want to set up the ETL with synthetic data. The [first time setup guide](https://github.com/smart-on-fhir/cumulus-etl/blob/main/docs/howtos/first-time-setup.md) and the documents it links to will help you get the pipeline configured - make sure to note the instructions in the latter for deploying with synthetic datasets.
- When it's complete, you should be able to view data in athena to verify.
- In the cumulus library repo, build the athena tables and export results, with `./library/make.py --build --export` (make sure you set the cumulus library [setup instructions](https://github.com/smart-on-fhir/library#setup) and set the appropriate environment variables/aws credentials)
- When the export completes, you should have folders in `./library/data_export` corresponding to the currently configured exportable studies (at the time of this writing, core and covid). 
- Run the bulk uploader with `./data_export/bulk_upload.py`

If this works, then you've proved out the whole data export flow and should be able to run a production export flow, just changing the `CUMUMULUS_AGGREGATOR_*` environment variables to point to the production instance.
