var uuid = require('node-uuid'),
    should = require('should'),
    path = require('path');


describe('dynamodb imported features', function () {
  var Post, db;


  function initializePostTableForTests() {
    require('./init.js');
    db = getDataSource();

    Post = db.define('Post', {
      title: { type: String, length: 255, index: true },
      content: { type: String },
      comments: [String]
    }, {
        /**
         * Extra information supplied for this model.
         * The DynamoDb connector needs to know this information to determine
         * what properties to pass to update and delete operations.  It also
         * can use it to reject queries that are operating on an un-indexed property.
         */
        primaryIndex: {
            hashKey: {
                key: 'id',
                type: 'S'
            },
            rangeKey: {
                key: 'title',
                type: 'S'
            }
        },
        indices: [{
            hashKey: {
                key: 'id',
                type: 'S'
            },
            rangeKey: {
                key: 'comments',
                type: 'S'
            }
        }]
    });

    function createTable(params) {
        db.connector.client.createTable(params, function(err, data) {
          // if (err) console.log(err, err.stack); // an error occurred
          // else     console.log(data);           // successful response
        });
    }

    /**
     * The create parameter syntax for DynamoDB
     */
    var postTableParams = {
        AttributeDefinitions: [{
            AttributeName: 'id',
            AttributeType: 'S'
        }, {
            AttributeName: 'title',
            AttributeType: 'S'
        }],
        KeySchema: [{
            AttributeName: 'id', /* required */
            KeyType: 'HASH' /* required */
        }, {
            AttributeName: 'title', /* required */
            KeyType: 'RANGE' /* required */
        }],
        ProvisionedThroughput: { /* required */
            ReadCapacityUnits: 1,
            WriteCapacityUnits: 1
        },
        TableName: 'Post'
    };

    //The common data type tests expect a 'Model' table.
    var modelTableParams = {
        AttributeDefinitions: [{
            AttributeName: 'id',
            AttributeType: 'S'
        }],
        KeySchema: [{
            AttributeName: 'id', /* required */
            KeyType: 'HASH' /* required */
        }],
        ProvisionedThroughput: { /* required */
            ReadCapacityUnits: 1,
            WriteCapacityUnits: 1
        },
        TableName: 'Model'
    };

    var userTableParams = {
        AttributeDefinitions: [{
            AttributeName: 'id',
            AttributeType: 'S'
        }],
        KeySchema: [{
            AttributeName: 'id', /* required */
            KeyType: 'HASH' /* required */
        }],
        ProvisionedThroughput: { /* required */
            ReadCapacityUnits: 1,
            WriteCapacityUnits: 1
        },
        TableName: 'User'
    };

    /**
     * We're creating this table in the dynamo test instance so our tests have something to work with.
     * If we don't, dynamodb will error out.
     */
    createTable(postTableParams);
    createTable(modelTableParams);
    createTable(userTableParams);
  }


  before(initializePostTableForTests);

  beforeEach(function (done) {
    done();
  });



  it('should create and then exist', function (done) {
    var testPostId = uuid.v1();
    Post.create({
        title: 'Post',
        content: 'Post content',
        comments: ["nice", "ooo"],
        id: testPostId
    }, function (err, post) {
        err && console.log("Error:", err);

        Post.exists(testPostId, function (err, exists) {
            err && console.log("Error:", err);
            done(err);
        });
    });
  });

  it('should create and then find', function (done) {
    var testPostId = uuid.v1();
    Post.create({
        title: 'Post',
        content: 'Post content',
        comments: ["nice", "ooo"],
        id: testPostId
    }, function (err, post) {
        err && console.log("Error:", err);
        Post.find({
            where: {
                id: testPostId
            }
        }, function (err, item) {
            err && console.log("Error:", err);
            done(err);
        });
    });
  });

  it ('should create and then delete', function (done) {
    var testPostId = uuid.v1();
    Post.create({
        title: 'Post',
        content: 'Post content',
        comments: ["nice", "ooo"],
        id: testPostId
    }, function (err, post) {
        err && console.log("Error:", err);

        Post.removeById(testPostId, function (err, exists) {
            err && console.log("Error:", err);
            done(err);
        });
    });
  });

  it ('should return one', function (done) {
    var testPostId = uuid.v1();
    Post.findOne(function (err, post) {
        err && console.log("Error:", err);
        done(err, post);
    });
  });

  it ('should create and then persist update', function (done) {
    var testPostId = uuid.v1();
    Post.create({
        title: 'Post',
        content: 'Post content',
        comments: ["nice", "ooo"],
        id: testPostId
    }, function (err, post) {
        err && console.log("Error:", err);

        Post.updateAll({
            id: testPostId
        }, {
            comments: undefined,
            content: 'bar'
        }, function (err, data) {
            should.not.exist(err);
            Post.find({
                where: {
                    id: testPostId
                }
            }, function (err, item) {
                should.not.exist(err);
                item[0].content.should.be.equal('bar');
                done(err);
            });
        });
    });
  });

  after(function (done) {
    done();
  });



  require('loopback-datasource-juggler/test/hooks.test.js');

  //Copied because dynamo does not support some of the ordering operations the tests want to enforce.
  require(path.join(__dirname, 'copied-basic-querying-test.js'));

  //Copied because the existing one is using an older version of should which leads to tons of syntax errors.
  require(path.join(__dirname, 'copied-datatypes-test.js'));

  require(path.join(__dirname, 'copied-relations-test.js'));
  // require('loopback-datasource-juggler/test/relations.test.js');
});