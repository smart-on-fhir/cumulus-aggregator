# Maintainer notes

## Python versioning gotchas

It's :strongly: recommended you use a venv based on python 3.9 for this project, and the SAM tooling, as opposed to relying on system python, due to the various risky interactions between the different tools the aggregator uses.

Mac brew specific: If for some reason you don't want to use a venv, note that updates for the SAM CLI have been observed in at least one case to install new python versions without confirmation - so please tread lightly on recommended upgrades.

## Testing

Testing relies heavily on `pytest`. It's assumed that you're going to be running unit tests from the root directory.

If you're writing unit tests - note that moto support for Athena mocking is currently still somewhat limited, so you may have to more aggressively mock AWS wrangler calls.

## Managing upload credentials

For security reasons, we've migrated the credentials for users to AWS secrets manager. Passwords should be managed via
the secrets manager console. We provide a [utility](../scripts/credential_management.py) to simplify managing S3-based
dictionaries for authorizing pre-signed URLs, or generating the user strings for secrets manager. The S3 artifacts should
persist between cloudformation deployments, but if for some reason you need to delete the contents of the bucket and recreate,
you can back up credentials by copying the contents of the admin/ virtual directory to another location.

To create a user credential for secrets manager, you would need to run the following command:

Creating a user:
`./scripts/cumulus_upload_data.py --ca user_name auth_secret site_short_name`

To set up, or add to, a dictionary of short names to display names, you can run the following:

Associating a site with an s3 directory:
`./scripts/cumulus_upload_data.py --cm site_short_name s3_folder_name`

These commands allow you to create a many to one relation of users to a given site, if so desired.

Without running these commands, no credentials are created by default, and so no access to pre-signed URLs is allowed.

## Cloudwatch logging

Configuring API gateways to log to Cloudwatch requires creating a role based on the Amazon 
Managed role for API gatways. See [this documentation ](https://aws.amazon.com/premiumsupport/knowledge-center/api-gateway-cloudwatch-logs/) for more info.

Since this is usually unneeded noise, it's recommended to only turn it on when you're 
actually using it to debug very early stages of message handling, primarily authorization.

## Updating dashboard OpenAPI definition

Run the following AWS CLI command to regenerate the openAPI definition (assuming you're at the project root):

`aws apigateway get-export --rest-api-id effmuaxft2 --stage-name dev --export-type OAS30 --accepts application/yaml ./docs/dashboard_api.yaml`

Note that this is listed in .gitignore due to containing AWS IDs in the output. Switch this to the appropriate public URL and put it in dashboard_api.[stage].yaml.

## SAM Framework

### SAM vs Cloudformation

The SAM framework extends native cloudformation, usually with a lighter syntax, which is nice! But it does make googing things a little more tricky. Anything related to lambdas, gateways, or the policies that apply to them should be checked against SAM syntax specifically

### Sync --watch mode gotchas

- If you modify S3 bucket permissions while in watch mode, changes to the bucket may generate a permission denied message. You'll need to delete the bucket and bring down the deployment before restarting to apply your changes, or manually update the S3 bucket permissions.

- Similarly, if you end up in a ROLLBACK_FAILED state, usually the only recourse is to bring the deployment down and resync, do a regular deployment deployment, or manually initiate a rollback from the AWS console.

Using deploy is a little safer than sync in this regard, though it does take longer for each deployment. Use your best judgement.

## Testing ECR containers locally

If you want to test a lambda ECR container, do the following from a console at the root of this repo:
- `docker buildx build -t <your image name> -f src/<dockerfile> src/`
- `docker run  \
-e AWS_REGION='us-east-1' \
-e AWS_ACCESS_KEY_ID='<your key id' \
-e AWS_SECRET_ACCESS_KEY='<your access key>' \
-e AWS_SESSION_TOKEN='<your session token>' \
-e <any other env vars you need> \
-p 9000:8080 \
<your image name>`

You can then post messages to your endpoints on port 9000 on localhost, and it should interact with the rest of your
AWS account (if required).