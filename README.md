# cumulus-aggregator

AWS tooling for reading and combining data from the Cumulus ETL for use in the dashboard.

The aggregator aims to provide a serverless implementation that accomplish the following goals:
- Allow external users to upload fully de-ID and binned study data from the [Cumulus ETL](https://github.com/smart-on-fhir/cumulus-etl) to an S3 bucket outside their organization
- Combine binned count data from multiple locations into a single data set
- Provide this data for injestion by the [Cumulus Dashboard](https://github.com/smart-on-fhir/cumulus-app)

## Requirements

* AWS SAM CLI - [Install the AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html)
* Docker - [Install Docker community edition](https://hub.docker.com/search/?type=edition&offering=community) - for local development

## Working with the SAM CLI

### Deployment
If you are just trying to deploy the aggregator into your AWS environment, run the following commands:

```bash
sam build
sam deploy --guided
```

The guided deploy will ask you several questions about your AWS environment, and then create a `samconfig.toml`. After that file has been created, you can optionally remove the `--guided` flag on future deploys - the SAM CLI will reference that file as part of the deploy process.

### Local Development

If you are working locally, first make sure that Docker is running.

Generally, you'll want to use SAM to build an image, and then use a specific lambda function with an event file to mimic the real traffic. The following one liner accomplishes this:

```bash
sam build && sam local invoke FetchUploadUrlFunction --event events/event-fetch-upload-url.json
```

The AWS SAM CLI can also emulate your application's API. Use the `sam local start-api` command to run the API locally on port 3000.

```bash
sam local start-api
curl http://localhost:3000/
```

### Cloud Development

Assuming you have already generated a `samconfig.toml`, you can have the SAM CLI use this to hot deploy changes to AWS for live debugging/generating realistic event files. Make sure you're not pointed at prod! When in doubt, rerun `sam deploy --guided` to make sure, or you can edit your samconfig.toml directly.

To run the appplication in this mode, use the following command:

```bash
sam sync --stack-name cumulus-aggregator --watch
```

After the build completes, you should be able to test with real events, either using the contents of the `scripts/` directory, or via another means of your choosing.

It might be more useful to live tail the logs, rather than waiting for the CloudFormation dashboard to update. To do this for a specific Lambda, for example, use the `sam logs` command:

```bash
sam logs -n PowersetMergeFunction --stack-name cumulus-aggregator --tail
```

There is more advanced log filtering available - see the [SAM Logs documentation](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/sam-cli-command-reference-sam-logs.html)

When you're finished, you can clean up your deployment with the following command:

```bash
aws cloudformation delete-stack --stack-name cumulus-aggregator
```

If your bucket has data in it, you need to delete the bucket contents before removing your deployment:

```bash
aws s3 rm s3://cumulus-aggregator --recursive && aws cloudformation delete-stack --stack-name cumulus-aggregator
```

### Unit Tests
