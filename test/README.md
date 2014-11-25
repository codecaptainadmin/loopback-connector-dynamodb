## Initialization

To get these tests to work, modify init.js and give it configuration to connect to your test DyanmoDB instance.

Here is a sample configuration block, as well as a sample credentials block.

### test/Init.js

    var config = require('rc')('loopback', {test: {dynamodb: {
        region: 'local',
        credentials: 'file',
        credfile: './credentials.json',
        endpoint: 'http://localhost:8000'
    }}}).test.dynamodb;

### credentials.json

    {
        "credentials": {
            "secretAccessKey": "fake",
            "accessKeyId": "fake"
        }
    }

### Debugging tests
If you use nodeinspector, this is a handy command to run from the test directory.

node-debug ./../node_modules/.bin/_mocha -G --timeout 10000 *.test.js