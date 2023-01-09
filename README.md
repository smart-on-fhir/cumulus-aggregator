<<<<<<< HEAD
# cumulus-aggregator-node

### endpoint
`/:id/api`

### URL segments
- `:id` - The ID of the data subscription we are querying

### Query parameters
- `column` - `string`, `required`; The name of the column we are requesting from the data table.
    - Must be other than `"cnt"`!
    - Must exist as a column in the table
- `stratifier` - `string`, `optional`; The name of the column to stratify by.
    - If provided, must be other than `"cnt"` and then the `column` parameter!
    - If provided, must exist as a column in the table
- `filter` - `string`, `optional`; Can be used multiple times. Each filter parameter represents a filter condition that is parsed and added to the WHERE clause of the SQL query. Each `filter` consist of 3 parts joined with `:` - `column:filterType:value`. The `column` part is a name of a column in the table. Supported `filterType` values are:
    - strEq
    - strContains
    - strStartsWith
    - strEndsWith
    - matches
    - strEqCI
    - strContainsCI
    - strStartsWithCI
    - strEndsWithCI
    - matchesCI
    - strNotEq
    - strNotContains
    - strNotStartsWith
    - strNotEndsWith
    - notMatches
    - strNotEqCI
    - strNotContainsCI
    - strNotStartsWithCI
    - strNotEndsWithCI
    - notMatchesCI
    - sameDay
    - sameMonth
    - sameYear
    - sameDayOrBefore
    - sameMonthOrBefore
    - sameYearOrBefore
    - sameDayOrAfter
    - sameMonthOrAfter
    - sameYearOrAfter
    - beforeDay
    - beforeMonth
    - beforeYear
    - afterDay
    - afterMonth
    - afterYear
    - isTrue
    - isNotTrue
    - isFalse
    - isNotFalse
    - isNull
    - isNotNull
    - eq
    - ne
    - gt
    - gte
    - lt
    - lte

Multiple filter parameters can be joined with `AND` or with `OR`, depending on how they have been requested:

- `filter=gender:eq:male,age:gt:3` -> `gender = 'male' OR age > 3`
- `filter=gender:eq:male&filter=age:gt:3` -> `gender = 'male' AND age > 3`
- `filter=gender:eq:male,age:gt:3&filter=year:gt:2022` -> `(gender = 'male' OR age > 3) AND year > 2022`


### Response format
The response is a JSON object having the following properties

- `column`     - `string`, `required`; the column we have selected (same as the `column` query parameter) 
- `stratifier` - `string`, `optional`; the column we stratify by (same as the `stratifier` query parameter) 
- `filters`    - `string[]`, `required`; All filter params in an array (can be empty array)
- `totalCount` - `number`, `required`; The total count of the table. (same as `SELECT cnt FROM {the table} WHERE {everything other than cnt} IS NULL`)
- `rowCount`   - `number`, `required`; count of result rows (regardless of stratifying)
- `data`       - `array`, `required`; See examples below

### Caching
This endpoint can be slow! It is important to have a reasonable caching to improve UX. Currently, the dashboard uses the following cache implementation, in which every variable that might change the end result is included in a hash that controls the cache behavior:
```js
const cacheKey = crypto.createHash("sha1").update([
    table,
    column,
    stratifier,
    filtersParams.join("+"),
    subscription.completed + ""
].join("-")).digest("hex");

res.setHeader('Cache-Control', 'max-age=31536000, no-cache')
res.setHeader('Vary', 'Origin, ETag')
res.setHeader('ETag', cacheKey)

// exit early with 304 if the client has already cached this
let ifNoneMatchValue = req.headers['if-none-match']
if (ifNoneMatchValue && ifNoneMatchValue === cacheKey) {
    res.statusCode = 304
    return res.end()
}

// otherwise, continue as usual for first-time requests...
```

In this example `table` is the name of the database table we are querying, `column`, `stratifier` and `filtersParams` are coming from the query parameters, and `subscription.completed` is the timestamp when
this table was last updated.

### How does it work?
The primary job of this endpoint is to build and execute an SQL SELECT query against the aggregate (CUBE-ed) table.
The basic query looks like `SELECT {column}, sum(cnt) FROM {table} WHERE {every column other than column and cnt} IS NULL`. A real query might look like:
```sql
SELECT symptom, sum(cnt) FROM covid WHERE age IS NULL AND gender IS NULL GROUP BY symptom ORDER BY symptom
```

If a `stratifier` is requested, it is added to the list of selected columns (thus, removed from the list of non-null columns). The stratifier is also added to the group by list. Example:
```sql
SELECT symptom, gender, sum(cnt) FROM covid WHERE age IS NULL NULL GROUP BY gender, symptom
```

As a post-processing step, after the results are fetched from DB, if a stratifier is provided the data is "grouped" by it. An example response with `stratifier` could look like:
```js
{ 
    column: "enct_month",
    stratifier: "symptom_text",
    filters: ["enct_month:sameMonthOrBefore:2022-01-15", "enct_month:afterYear:2020-01-01"],
    totalCount: 165527,
    rowCount: 156,
    data: [
        { 
            stratifier: "Anosmia",
            rows: [
                ["2021-01-01", 31],
                ["2021-02-01", 8],
                ...
            ]
        },
        { 
            stratifier: "Congestion or runny nose",
            rows: [
                ["2021-01-01", 237],
                ["2021-02-01", 246],
                ...
            ]
        },
        ...
    ]
}
```

The same response without a stratifier would kook like:
```js
{ 
    column: "enct_month",
    filters: ["enct_month:sameMonthOrBefore:2022-01-15", "enct_month:afterYear:2020-01-01"],
    totalCount: 165527,
    rowCount: 13,
    data: [{ 
        rows: [
            ["2021-01-01", 3631],
            ["2021-02-01", 2956],
            ["2021-03-01", 3763],
            ["2021-04-01", 4012],
            ["2021-05-01", 4879],
            ["2021-06-01", 4859],
            ["2021-07-01", 4956],
            ["2021-08-01", 4928],
            ["2021-09-01", 5404],
            ["2021-10-01", 5638],
            ["2021-11-01", 5022],
            ["2021-12-01", 5352],
            ["2022-01-01", 4419]
        ]
    }]
}
```
=======
# cumulus-aggregator


## BCH specific installation notes

- Install the [BCH AWS Login](https://ccts3.aws.chboston.org/bchcloud/ccts-devops/bch-aws-login) utility
- Auth with your username/pass with the command `bch-aws-login`
- get the profile name create by the above script from the console, or ~/.aws/credentials
- `sam-deploy --guided` will walk you through deploying to BCH AWS with the above profile



# AWS `sam init` boilerplate subset

## Deploy the sample application

To use the AWS SAM CLI, you need the following tools:

* AWS SAM CLI - [Install the AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html).
* Node.js - [Install Node.js 16](https://nodejs.org/en/), including the npm package management tool.
* Docker - [Install Docker community edition](https://hub.docker.com/search/?type=edition&offering=community).

To build and deploy your application for the first time, run the following in your shell:

```bash
sam build
sam deploy --guided
```

The first command will build the source of your application. The second command will package and deploy your application to AWS, with a series of prompts:

* **Stack Name**: The name of the stack to deploy to CloudFormation. This should be unique to your account and region, and a good starting point would be something matching your project name.
* **AWS Region**: The AWS region you want to deploy your app to.
* **Confirm changes before deploy**: If set to yes, any change sets will be shown to you before execution for manual review. If set to no, the AWS SAM CLI will automatically deploy application changes.
* **Allow SAM CLI IAM role creation**: Many AWS SAM templates, including this example, create AWS IAM roles required for the AWS Lambda function(s) included to access AWS services. By default, these are scoped down to minimum required permissions. To deploy an AWS CloudFormation stack which creates or modifies IAM roles, the `CAPABILITY_IAM` value for `capabilities` must be provided. If permission isn't provided through this prompt, to deploy this example you must explicitly pass `--capabilities CAPABILITY_IAM` to the `sam deploy` command.
* **Save arguments to samconfig.toml**: If set to yes, your choices will be saved to a configuration file inside the project, so that in the future you can just re-run `sam deploy` without parameters to deploy changes to your application.

The API Gateway endpoint API will be displayed in the outputs when the deployment is complete.

## Use the AWS SAM CLI to build and test locally

Build your application by using the `sam build` command.

```bash
my-application$ sam build
```

The AWS SAM CLI installs dependencies that are defined in `package.json`, creates a deployment package, and saves it in the `.aws-sam/build` folder.

Test a single function by invoking it directly with a test event. An event is a JSON document that represents the input that the function receives from the event source. Test events are included in the `events` folder in this project.

Run functions locally and invoke them with the `sam local invoke` command.

```bash
my-application$ sam local invoke FetchUploadUrlFunction --event events/event-fetch-upload-url.json
```

The AWS SAM CLI can also emulate your application's API. Use the `sam local start-api` command to run the API locally on port 3000.

```bash
my-application$ sam local start-api
my-application$ curl http://localhost:3000/
```

The AWS SAM CLI reads the application template to determine the API's routes and the functions that they invoke. The `Events` property on each function's definition includes the route and method for each path.

```yaml
      Events:
        Api:
          Type: Api
          Properties:
            Path: /
            Method: GET
```


## Fetch, tail, and filter Lambda function logs

To simplify troubleshooting, the AWS SAM CLI has a command called `sam logs`. `sam logs` lets you fetch logs that are generated by your Lambda function from the command line. In addition to printing the logs on the terminal, this command has several nifty features to help you quickly find the bug.

**NOTE:** This command works for all Lambda functions, not just the ones you deploy using AWS SAM.

```bash
my-application$ sam logs -n FetchUploadUrlFunction --stack-name cumulus-aggregator --tail
```

**NOTE:** This uses the logical name of the function within the stack. This is the correct name to use when searching logs inside an AWS Lambda function within a CloudFormation stack, even if the deployed function name varies due to CloudFormation's unique resource name generation.

You can find more information and examples about filtering Lambda function logs in the [AWS SAM CLI documentation](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-logging.html).



## Cleanup

To delete the sample application that you created, use the AWS CLI. Assuming you used your project name for the stack name, you can run the following:

```bash
aws cloudformation delete-stack --stack-name cumulus-aggregator
```

>>>>>>> 6a55ea0 (Initial Gateway/Lambda POC)
