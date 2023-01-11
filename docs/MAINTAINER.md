# Maintainer notes

## Cloudwatch logging

Configuring API gateways to log to Cloudwatch requires creating a role based on the Amazon 
Managed role for API gatways. See [this documentation ](https://aws.amazon.com/premiumsupport/knowledge-center/api-gateway-cloudwatch-logs/) for more info.

Since this is usually unneeded noise, it's recommended to only turn it on via the console,
and only when you're actually using it.