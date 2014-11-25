## loopback-connector-dynamodb

DynamoDB connector for loopback-datasource-juggler. Because Dynamo doesn't get enough love.

## DynamoDB connector

 * Installation
 * Creating a DynamoDB data source
   * Properties
 * Using the DynamoDB connector
   * Local development with DynamoDB Local

### Installation

In your application root directory, enter:

    $ npm install loopback-connector-dynamodb --save

This will install the module from npm and add it as a depenency to the application's [package.json](http://docs.strongloop.com/display/LB/package.json) file.

### Creating a DynamoDB data source

Use the [Datasource generator](http://docs.strongloop.com/display/LB/Datasource+generator) to add a DynamoDB data source to your application. The entry in the application's `/server/datasources.json` will look like this:

```json
"mydb": {
  "name": "mydb",
  "connector": "dynamodb"
}
```

Edit `datasources.json` to add other properties to enable you to connect the data source to a DynamoDB database.

### Properties

Property | Type | Description
-------- | ---- | -----------
connector | String | Connector name, either `loopback-connector-dynamodb` or `dynamodb`
region | String | AWS Region to connect to. May also be set to `local` to use an instance of DynamoDB Local.
debug | Boolean | If `true`, turn on verbose mode to debug requests and lifecycle.
credentials | String | Method to locate credentials for the AWS SDK for Javascript. Valid values are: `env`, `shared`, `iamrole`, `file`. Default value: `shared`.
credfile | String | If credentials method is `file`, specify the location of the JSON file to load.
profile | String | Name the profile to use if using the `shared` credentials method.
endpoint | Number | URL to use connecting to DynamoDB Local. Default is `8000`. Example: `http://localhost:8000/` Note: this property is ignored if `region` is not `local`.

For example:

*Example datasources.json file*
```json
{
  "dynamo_dev": {
    "name": "dynamo_dev",
    "connector": "dynamodb",
    "region": "local",
    "credentials": "shared",
    "profile": "localdev",
    "port": 4567
  },
  "dynamo_qa": {
    "name": "dynamo_qa",
    "connector": "dynamodb",
    "region": "us-west-2",
    "credentials": "iamrole"
  }
}
```

:warning: You can't specify `aws_access_key_id` and `aws_secret_access_key` directly in your `datasources.json` file.
This is intentional. Putting credentials like that into a file are A Very Bad Thing. Quit trying to do that.


## TODO
* Handle queries that include an "order" property better.  Should either throw an error if there is not a rangeKey for the ordered key, or just throw an error entirely. Additionally, if there is a rangeKey for the order, it should make sure a query is possible.
* Handle declaring local and global secondary indexes better.  The current support is rudimentary.
* Add better documentation for rudimentary local and global secondary indexes.

