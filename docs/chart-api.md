# Dashboard chart API definition spec

This document describes the customer implementation of the Cumulus dashboard's API for recieving chart data for display in the chart builder, which the aggregator re-implements as a lambda building Athena queries.

### endpoint
`/chart_data/{subscription_name}`

### URL segments
- `subscription_name` - The name of the data subscription we are querying

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
