# Maintainer notes

## Python versioning gotchas

It's strongly recommended you use a venv for this project, rather than the global python, especially if you're using a package manager like brew - As of this writing, the SAM CLI is occasionally just force upgrading dependencies to install python 3.11, while moto is pinned to no newer than 3.10, and lambda execution is still on 3.9. You should use the python version that corresponds to however you decide to configure your lambda execution.

## Testing

Testing relies heavily on `pytest`. It's assumed that you're going to be running unit tests from the root directory.

If you're writing unit tests - note that moto support for Athena mocking is currently still somewhat limited, so you may have to more aggressively mock AWS wrangler calls.

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

- If you modify S3 bucket permissions while in watch mode, changes to the bucket may generate a permission denied message. You'll need to delete the bucket and bring down the deployment before restarting to apply your changes.

- Similarly, if you end up in a ROLLBACK_FAILED state, usually the only recourse is to bring the deployment down and resync, or do a regular deployment deployment.

Using deploy is a little safer than sync in this regard, though it does take longer for each deployment. Use your best judgement.