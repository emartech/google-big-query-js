# @emartech/google-big-query

Helper library to easily use Google BigQuery in NodeJS

# Configuration

`@emartech/google-big-query` uses the `config` package for configuration. You should make the following settings available under `"GoogleCloud" namespace:

```json
{
  "projectId": "",
  "bucket": "",
  "dataset": "",
  "credentials": {},
  "maximumBillingTier": 10
}
```

### projectId

The project you'll use for billing.

### dataset

The default dataset to connect to if it was not specified.

### credentials

The credentials the connection will use.

Example:
```json
{
  "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
  "client_email": "example@my-service.iam.gserviceaccount.com"
}
```

### maximumBillingTier

More info about pricing: [https://cloud.google.com/bigquery/pricing](https://cloud.google.com/bigquery/pricing)

# API

```javascript
const BigQuery = require('@emartech/google-big-query');
```

## BigQuery

### Methods

#### create(datasetName):BigQuery `static`

Creates a BigQuery instance.

##### Parameters

| Name          | Type   | Description                                                  |
| ------------- | ------ | ------------------------------------------------------------ |
| `datasetName` | string | The name of the default dataset.<br />Default: `config.GoogleCloud.dataset`<br />`optional` |

##### Returns

| Type     | Description                             |
| -------- | --------------------------------------- |
| BigQuery | Instance of the BigQuery wrapper class. |

#### constructor(datasetName, client):BigQuery

Constructor of BigQuery wrapper.

##### Parameters

| Name          | Type                   | Description                      |
| ------------- | ---------------------- | -------------------------------- |
| `datasetName` | string                 | The name of the default dataset. |
| `client`      | @google-cloud/bigquery | The BigQuery client.             |

#### table(tableName):Table

Returns a [Table](https://cloud.google.com/nodejs/docs/reference/bigquery/2.0.x/Table) instance for the given table name.

##### Parameters

| Name        | Type   | Description                          |
| ----------- | ------ | ------------------------------------ |
| `tableName` | string | The name of the table in the dataset |

##### Returns

| Type                                                         | Description                                 |
| ------------------------------------------------------------ | ------------------------------------------- |
| [Table](https://cloud.google.com/nodejs/docs/reference/bigquery/2.0.x/Table) | Google Cloud BigQuery Table class instance. |

#### createQueryStream(query):ReadableStream

This is a wrapper of the original [createQueryStream()](https://cloud.google.com/nodejs/docs/reference/bigquery/2.0.x/BigQuery#createQueryStream) method.

##### Parameters

| Name    | Type             | Description                                                  |
| ------- | ---------------- | ------------------------------------------------------------ |
| `query` | string or object | The query to run.<br />[More info](https://cloud.google.com/nodejs/docs/reference/bigquery/2.0.x/BigQuery#query) about the parameter. |

##### Returns

| Type           | Description |
| -------------- | ----------- |
| ReadableStream |             |

#### query(query):Promise

This is a wrapper of the original [query()](https://cloud.google.com/nodejs/docs/reference/bigquery/2.0.x/BigQuery#query) method.

##### Parameters

| Name    | Type             | Description                                                  |
| ------- | ---------------- | ------------------------------------------------------------ |
| `query` | string or object | The query to run.<br />[More info](https://cloud.google.com/nodejs/docs/reference/bigquery/2.0.x/BigQuery#query) about the parameter. |

##### Returns

| Type    | Description |
| ------- | ----------- |
| Promise |             |

#### createQueryJob(query):Promise `generator`

This is a wrapper of the original [createQueryJob()](https://cloud.google.com/nodejs/docs/reference/bigquery/2.0.x/BigQuery#createQueryJob) method.

##### Parameters

| Name      | Type             | Description |
| --------- | ---------------- | ----------- |
| `options` | object or string |             |

##### Returns

| Type    | Description |
| ------- | ----------- |
| Promise |             |

##### Example

```javascript
let [job] = yield BigQuery.create().createQueryJob(query);
job.getQueryResultsStream({ maxResults: 100000, startIndex: 0 }).pipe(processRow);
```

#### createTableIfNotExists(table, schema):Table `generator`

Creates the table with the specified [schema](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#schema).

##### Parameters

| Name     | Type                                                         | Description                    |
| -------- | ------------------------------------------------------------ | ------------------------------ |
| `table`  | [Table](https://cloud.google.com/nodejs/docs/reference/bigquery/2.0.x/Table) | The table instance             |
| `schema` | object                                                       | Schema definition of the table |

##### Returns

| Type                                                         | Description    |
| ------------------------------------------------------------ | -------------- |
| [Table](https://cloud.google.com/nodejs/docs/reference/bigquery/2.0.x/Table) | The new Table. |

#### dropTableIfExists(table):void `generator`

Drops the given table if exists.

##### Parameters

| Name    | Type                                                         | Description                                |
| ------- | ------------------------------------------------------------ | ------------------------------------------ |
| `table` | [Table](https://cloud.google.com/nodejs/docs/reference/bigquery/2.0.x/Table) | The table instance that should be dropped. |

#### createTable(tableName, options):Table `generator`

This is a wrapper of the original [createTable()](https://cloud.google.com/nodejs/docs/reference/bigquery/2.0.x/Dataset#createTable) method.

##### Parameters

| Name        | Type   | Description                                                  |
| ----------- | ------ | ------------------------------------------------------------ |
| `tableName` | string | The name of the table.                                       |
| `options`   | object | More info in the [original documentation](https://cloud.google.com/nodejs/docs/reference/bigquery/2.0.x/Dataset#createTable). `optional` |

##### Returns

| Type                                                         | Description    |
| ------------------------------------------------------------ | -------------- |
| [Table](https://cloud.google.com/nodejs/docs/reference/bigquery/2.0.x/Table) | The new Table. |

## BigQuery.FileToTable

#### create(file, tableName, schema, options):FileToTable `static`

#### createWith(file, table, schema, options):FileToTable `static`

#### constructor(file, table, schema options):FileToTable

#### run():Job

## BigQuery.QueryToFile

#### create(baseName):QueryToFile `static`

#### constructor(tableName):QueryToFile

#### run(query, options):File `generator`

## BigQuery.QueryToFileToStream

#### create(baseName):QueryToFileToStream `static`

#### constructor(queryToFile):QueryToFileToStream

#### createQueryStream(query, options):JSONStream `generator`

## BigQuery.QueryToStream

#### create():QueryToStream `static`

#### constructor(client):QueryToStream

#### run(query, parameters):ReadableStream

## BigQuery.QueryToTable

#### create(tableName, dataset):QueryToTable `static`

#### constructor(client, tableName, dataset):QueryToTable

#### run(query, params):Job `generator`

## BigQuery.QueryToView

#### create(viewName):QueryToView `static`

#### constructor(client, viewName):QueryToView

#### run():Table `generator`

## BigQuery.StreamToTable

#### create(tableName, tableSchema):StreamToTable `static`

#### constructor(table, tableSchema):StreamToTable

#### saveStream():Promise

## BigQuery.TableToFile

#### create(tableName, file):TableToFile `static`

#### constructor(table, file):TableToFile

#### run():Job `generator`

