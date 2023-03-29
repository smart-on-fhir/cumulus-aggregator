# cumulus-aggregator

AWS tooling for reading and combining data from the Cumulus ETL for use in the dashboard.

The aggregator aims to provide a serverless implementation that accomplish the following goals:
- Allow external users to upload fully de-ID and binned study data from the [Cumulus ETL](https://github.com/smart-on-fhir/cumulus-etl) to an S3 bucket outside their organization
- Combine binned count data from multiple locations into a single data set
- Provide this data for injestion by the [Cumulus Dashboard](https://github.com/smart-on-fhir/cumulus-app)

Detailed instructions on developing/running the application are available in the [docs folder](./docs/)
