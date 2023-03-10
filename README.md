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

### Deployment Prequisites

#### Accessing via external URL

If you need your instance of the aggregator to be accessible outside your organization, follow these steps prior to trying to deploy the aggregator:

- Register an appropriate domain via AWS Route 53
- Update the domain in the template.certificate.yaml to match that domain
- Deploy the certificate & hosted zone with 
```bash
sam deploy --guided --stack-name=cumulus-dns -t template.hostedzone.yaml
```
- In the Route 53 dashboard, under hosted zones, find your domain, and get the DNS servers from the NS key associated with the zone
- In the Route 53 dashboard, under domains, find the domain, and update its DNS server info to match the hosted zone
- After the DNS info is published, the Certificate should finish deploying - this may take 5-10 minutes
- Get the Aggregator Certificate ARN and the Hosted Zone ID from the deployment output

Once you've done this, you can do one of two things:
- Update the default parameters for `AggregatorCertArn` and `AggregatorHostedZoneID`
- Create environment variables (suggested: CUMULUS_AGG_CERN_ARN and CUMULUS_AGG_ZONE_ID) and assign those parameters. Then, whenever you run `sam`, provide a parameter override, with `--parameter-overrides AggregatorCertArn=$CUMULUS_AGG_CERT_ARN AggregatorHostedZoneID=$CUMULUS_AGG_ZONE_ID`.

#### Accessing via Amazon account

If your aggregator does not need public external access, remove/comment the `Domain` resource under `SiteApiGateway` inside of template.yaml

### Deployment

If you are just trying to deploy the aggregator into your AWS environment, run the following commands:

```bash
sam build
sam deploy --guided --stack-name=cumulus-aggregator-[zone]
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
sam sync --stack-name cumulus-aggregator-dev --watch
```

If you'd rather sync on demand, just omit the --watch parameter.

```bash
sam sync --stack-name cumulus-aggregator-dev
```

After the build completes, you should be able to test with real events, either using the contents of the `scripts/` directory, or via another means of your choosing.

It might be more useful to live tail the logs, rather than waiting for the CloudFormation dashboard to update. To do this for a specific Lambda, for example, use the `sam logs` command:

```bash
sam logs -n PowersetMergeFunction --stack-name cumulus-aggregator-dev --tail
```

There is more advanced log filtering available - see the [SAM Logs documentation](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/sam-cli-command-reference-sam-logs.html)

When you're finished, you can clean up your deployment with the following command:

```bash
aws cloudformation delete-stack --stack-name cumulus-aggregator-dev
```

If your bucket has data in it, you need to delete the bucket contents before removing your deployment:

```bash
aws s3 rm s3://cumulus-aggregator-site-counts-dev --recursive && aws cloudformation delete-stack --stack-name cumulus-aggregator-dev
```

### Unit Tests

Testing is done via `pytest` at the project root. You will need to `pip install -r tests/requirements.txt` to get the various local dependencies. These tests are also run via post-commit hooks.
