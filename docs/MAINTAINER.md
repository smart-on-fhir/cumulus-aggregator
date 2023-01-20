# Maintainer notes

## Cloudwatch logging

Configuring API gateways to log to Cloudwatch requires creating a role based on the Amazon 
Managed role for API gatways. See [this documentation ](https://aws.amazon.com/premiumsupport/knowledge-center/api-gateway-cloudwatch-logs/) for more info.

Since this is usually unneeded noise, it's recommended to only turn it on via the console,
and only when you're actually using it.

### Sync --watch mode gotchas

- If you modify the S3 bucket permissions while in watch mode, changes to the bucket may generate a permission denied message. You'll need to delete the bucket and bring down the deployment before restarting to apply your changes.

- Similarly, if you end up in a ROLLBACK_FAILED state, usually the only recourse is to bring the deployment down and redeploy.