---
title: Library Integration
parent: Aggregator
nav_order: 3
# audience: engineer familiar with the project
# type: howto
---

# Cumulus upload integration testing

This document will walk you through completing two kinds of integration testing for transmitting
data to the Cumulus Aggregator.

## Prerequisites

You’ll need:

- A local copy of the [Cumulus Library project](https://github.com/smart-on-fhir/cumulus-library-core)
- For the quick test: a copy of an example export with synthea data - the Library has a set of [test data](https://github.com/smart-on-fhir/cumulus-library-core/tree/main/tests/test_data) you can use for this.
- For the long test: a local copy of the [sample bulk fhir datasets](https://github.com/smart-on-fhir/sample-bulk-fhir-datasets#downloads), and an instance of the [Cumulus ETL](https://github.com/smart-on-fhir/cumulus-etl)
- You’ll need to either set up your own Aggregator instance or reach out to BCH to get credentials configured to generate a site ID using the [credential management](https://github.com/smart-on-fhir/cumulus-aggregator/blob/main/scripts/credential_management.py) script.

## Configuring uploads

The Cumulus Library has a script for [Uploading data in bulk](https://github.com/smart-on-fhir/cumulus-library-core/blob/main/data_export/bulk_upload.py).
You can pass values to it via the command line, but we recommend setting up environment variables instead.
Specifically:

`CUMULUS_AGGREGATOR_USER` \ `CUMULUS_AGGREGATOR_ID` - these should match the credentials configured in the Aggregator via the credential management script.

`CUMULUS_AGGREGATOR_URL` - this should, for this testing, be set to a non production environment. The BCH Aggregator is using `https://staging.aggregator.smartcumulus.org/upload/` for this, but you can use an endpoint of your choice if you are self-hosting an Aggregator.

## Quick test: uploading test data

With these environment variables set, the bulk uploader is all set to load data.
Perform the following steps, inside the `cumulus-library-core` project:

- Copy the test data file `./tests/test_data/count_synthea_patient.parquet` into `./data_export/test_data`
- If desired, perform an upload dry run with `./data_export/bulk_upload.py --preview` - this will show you what the bulk uploader will do without actually sending data
- Run the bulk uploader with `./data_export/bulk_upload.py`
- A user with access to the Aggregator's S3 bucket can verify if the upload was successful

## Integration test: Processing synthetic data through ETL

If the quick test was successful, you can test your processing pipeline entirely with synthetic data. by running through the following steps:

- If you haven't already, you'll want to set up the ETL with synthetic data.
  The setup guide in the [Cumulus ETL documentation](https://docs.smarthealthit.org/cumulus/etl/)
  includes instructions to deploy with a synthetic dataset.
- When it's complete, you should be able to view data in athena to verify.
- In the cumulus library repo, build the Athena tables and export results, with\
  `./library/make.py --build --export` (make sure you set the setup guide in the
  [Cumulus Library documentation](https://docs.smarthealthit.org/cumulus/etl/library/) 
  and set the appropriate environment variables/AWS credentials)
- When the export completes, you should have folders in `./library/data_export` corresponding to the currently configured exportable studies (at the time of this writing, `core` and `covid`). 
- Run the bulk uploader with `./data_export/bulk_upload.py`

If this works, then you've proved out the whole data export flow and should be able to run a production export flow, 
just changing the `CUMUMULUS_AGGREGATOR_*` environment variables to point to the production instance. 
If you're using the BCH aggregator, you do not need to specify `CUMULUS_AGGREGATOR_URL`, as that URL is the default
value in the bulk upload tool.
