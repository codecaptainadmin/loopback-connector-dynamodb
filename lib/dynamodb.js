/*eslint no-underscore-dangle:0*/
'use strict';

/*!
 * Module dependencies
 */
var AWS, DOC; // loaded JIT with settings
var util = require('util');
var Connector = require('loopback-connector').Connector;
var async = require('async');
var uuid = require('node-uuid');
var debug = false;
try {
    debug = require('debug')('dynamo');
} catch(err) {
    //we have installed this package somewhere else and devDependencies are not installed.
}
/**
 * The constructor for the DynamoDB connector
 * @param {Object} client The DOC.DynamoDB client object
 * @param {Object} dataSource The data source instance
 * @constructor
 */
function DynamoDB(client, dataSource) {
  Connector.call(this, 'dynamodb', dataSource.settings);
  this.client = client;

  this.dataSource = dataSource;
}

util.inherits(DynamoDB, Connector);


/**
 * Test connection to DynamoDB
 * @param {Function} [cb] The callback function
 *
 * @callback cb
 * @param {Error} err The error object
 * @param {Client} client The DynamoDB DOC object
 */
DynamoDB.prototype.connect = function(cb) {
  var self = this;
  if (self.client) {
    process.nextTick(function () {
      if (cb) {
        cb(null, self.client);
      }
    });
  } else {
    if (cb) {
      cb(null, self.client);
    }
  }
};

/**
 * Get types associated with the connector
 * @returns {String[]} The types for the connector
 */
DynamoDB.prototype.getTypes = function() {
  return ['db', 'nosql'];
};

/**
 * Get the default data type for ID
 * @returns {Function} The default type for ID
 */
DynamoDB.prototype.getDefaultIdType = function() {
  return String;
};

/**
 * Create a new model instance for the given data
 * @param {String} model The model name
 * @param {Object} data The model data
 * @param {Function} [cb] The callback function
 */
DynamoDB.prototype.create = function (model, data, cb) {
  /* Note: We need to create the table name beforehand since
     the table generation will take a few seconds; Or we need to
     use the wait API call until the table is created to
     proceed to insert records into the table
  */

  debug && debug("Create " + model + ".");
  debug && debug(data);

  var primaryKeys = this.getPrimaryKeyProperties(model),
      hashKeyProperty = this.idKey(model);

  primaryKeys.forEach(function (primaryKey) {
    if (data[primaryKey.key] === undefined) {
        if (primaryKey.type === 'S') {
            data[primaryKey.key] = uuid.v1();
        } else if (primaryKey.type === 'N') {
            //Try time since epoch to set it automatically.
            data[primaryKey.key] = new Date() / 1;
        }
    }
  });

  //The dynamo doc library does not like undefined values.
  var sanitizedData = this.sanitizeData(data);

  data = sanitizedData;

  var params = {
    TableName: model,
    Item: data
  },
  itemToPut = data;



  this.client.putItem(params, function(err, data) {
    if (err) {
      cb(err);
    } else {
      cb(null, itemToPut[hashKeyProperty]);
    }
  });
};

DynamoDB.prototype.idKey = function (model) {
    var primaryKeys = this.getPrimaryKeyProperties(model),
        hashKey;

    primaryKeys.forEach(function (key) {
        if (key.isHash) {
            hashKey = key.key;
        }
    });

    return hashKey;
};

DynamoDB.prototype.sanitizeData = function (data) {
  var self = this;
  if (data) {
      Object.keys(data).forEach(function (key) {
        if (data[key] === undefined) {
            data[key] = null;
        } else if (data[key] instanceof Date) {
            data[key] = data[key] / 1;
        } else if (util.isArray(data[key])) {
            data[key] = data[key].map(function (value) {
                var subKey = self.sanitizeData({data:value});
                return subKey.data;
            });
        } else if (typeof data[key] === "object") {
            data[key] = self.sanitizeData(data[key]);
        }
      });
  }
  return data;
};

/**
 * Save the model instance for the given data
 * @param {String} model The model name
 * @param {Object} data The model data
 * @param {Function} [cb] The callback function
 */
DynamoDB.prototype.save = function(model, data, cb) {
    debug && debug("Save " + model + ".");
    debug && debug(data);
    this.updateAttributes(model, data[this.idKey(model)], data, cb);
};

/**
 * Check if a model instance exists by id
 * @param {String} model The model name
 * @param {*} id The id value
 * @param {Function} [cb] The callback function
 *
 */
DynamoDB.prototype.exists = function (model, id, cb) {
    debug && debug("Exists " + model + " id: " + id);
    var idWhere = this.getIdWhere(model, id);
    this.client.getItem({
        TableName: model,
        Key: idWhere
    }, function (err, data) {
        if (err) {
            cb(err);
        } else {
            cb(null, true);
        }
    });
};

/**
 * Find a model instance by id
 * @param {String} model The model name
 * @param {*} id The id value
 * @param {Function} [cb] The callback function
 */
DynamoDB.prototype.find = function find(model, id, cb) {
    debug && debug("Find single " + model + " id: " + id);
    var idWhere = this.getIdWhere(model, id),
        self = this;

    this.client.getItem({
        TableName: model,
        Key: idWhere
    },function (err, data) {
        if (err) {
            cb(err);
        } else if (!data.Item) {
            self.create(model, data, function (err, id) {
                if (err) {
                    cb(err);
                } else {
                    cb(null, data);
                }
            });
        }   else {
            cb(null, data.Item);
        }
    });
};

/**
 * Delete a model instance by id
 * @param {String} model The model name
 * @param {*} id The id value
 * @param [cb] The callback function
 */
DynamoDB.prototype.destroy = function destroy(model, id, cb) {
    var idWhere = this.getIdWhere(model, id);
    this.destroyAll(model, idWhere, cb);
};

DynamoDB.prototype.getIdWhere = function (model, id) {
    var idWhere = {};
    idWhere[this.idKey(model)] = id;
    return idWhere;
};

/**
 * Find matching model instances by the filter
 *
 * @param {String} model The model name
 * @param {Object} filter The filter
 * @param {Function} [cb] The callback function
 */
DynamoDB.prototype.all = function all(model, filter, cb) {
    debug && debug("Getting all " + model);
    debug && debug(filter);
    /**
      * Ideally we would support the full filter syntax for loopback.
      * Initially, just going to support the fields.

        *** Note -- Object syntax seems to be the only thing supported at this layers
        * fields  Object, Array, or String
            Specify fields to include in or exclude from the response.
            See Fields filter.

        (*** Note -- NOT SUPPORTED RIGHT NOW
        * include String, Object, or Array
           Include results from related models, for relations such as belongsTo and hasMany.
            See Include filter.)

        * limit   Number
           Limit the number of instances to return.
            See Limit filter.

        *** Note, order will only work if a range key has been specified properly.
            So if this is part of the filter, it's assuming the table was setup to allow
            it to be used.  Keys: RangeKeyCondition, ScanIndexForward
            This will only work if the order property is the range key in the same index
            this is being used to query in the "where" object.
        * order   String
           Specify sort order: ascending or descending.
            See Order filter.


        *** DynamoDB keys LastEvaluatedKey and ExclusiveStartKey as progressive queries are made until limit and skip are met.
        * skip (offset)   Number
           Skip the specified number of instances.
            See Skip filter.

        *** Note, where will only work in a performant fashion if the primary key, a LocalSecondaryIndex or a GlobalSecondaryIndex
            contains all the keys in the where clause.  Otherwise, the whole table is scanned.  Additionally, the more complex query
            notation defined in /loopback-datasource-juggler/lib/dao.js ~line 620 in the documentation for the find
            method is NOT fully supported.

            Each property in the where clause might have a special "inc" key, which is an array of acceptable values.

        * where   Object
           Specify search criteria; similar to a WHERE clause in SQL.
            See Where filter.
      **/

    //Because dynamodb only returns 1 MB at a time, we might need to query multiple times to populate this array.
    var items = [],
        conditions = [],
        queryParams = {},
        properties = filter.where ? Object.keys(filter.where) : null,
        findOperation = properties ? "query" : "scan",
        whereIsIndexed = true,
        me = this;


    if (properties) {
        properties.forEach(function (property) {
            if (!me.isInPrimaryIndex(property)) {
                whereIsIndexed = false;
            }
        });
    }

    //We have to use scan if the where clause is not indexed.
    if (!whereIsIndexed) {
        findOperation = "scan";
    }


    if (!cb) {
        cb = filter;
    }

    queryParams.TableName = model;

    if (properties) {
        if (findOperation === 'query' && !me.whereCanBeQueried(filter.where)) {
            findOperation = "scan";
        }

        me.addWhereObjectToConditions(queryParams, findOperation, filter.where);
    }

    if (filter.order && findOperation === "query") {
        var order = filter.order.split(' ');
        if (order[1] === 'ASC') {
            queryParams.ScanIndexForward = true;
        } else {
            queryParams.ScanIndexForward = false;
        }
    }

    if (filter.fields) {
        var fieldsToInclude = [];

        if (util.isArray(filter.fields)) {
            fieldsToInclude = filter.fields;
        } else {
            Object.keys(filter.fields).forEach(function (key) {
                if (filter.fields[key]) {
                    fieldsToInclude.push(key);
                }
            });
        }

        queryParams.AttributesToGet = fieldsToInclude;

    }

    if (findOperation === "scan" && !queryParams.AttributesToGet) {
        queryParams.Select = 'ALL_ATTRIBUTES';
    }

    function runQuery(params, cb) {
        if (filter.limit) {
            var max = filter.skip ? filter.limit + filter.skip : filter.limit;
            if (items.length >= max) {
                cb(null);
            }
        }
        me.client[findOperation](params, function (err, data) {
            if (err) {
                cb(err);
                return;
            }

            if (data.Items) {
                items = items.concat(data.Items);
            }

            //If there is more data to read then read it.
            if (data.LastEvaluatedKey) {
                params.ExclusiveStartKey = data.LastEvaluatedKey;
                runQuery(params, cb);
                return;
            }

            cb();
        });
    }

    runQuery(queryParams, function (err) {
        if (err) {
            cb(err);
            return;
        }
        //At many items as we can find that are near the limit.  It may be more items than the limit
        //Trim to match the query.
        if (filter.limit) {
            var maxNumberOfItems = filter.limit,
                start = filter.skip ? filter.skip : 0;

            if (items.length >= maxNumberOfItems) {
                items = items.slice(start, Math.min(maxNumberOfItems + start, items.length));
            }
        }

        process.nextTick(function () {
          if (filter && filter.include) {
            me._models[model].model.include(items, filter.include, cb);
          } else {
            cb(err, items);
          }
        });


    });
};

DynamoDB.prototype.runAll = function (model, where, cb) {
    //If no criteria is provided, refuse to perform the costly scan/delete operation.
    if (!where) {
        this.all(model, cb);
    } else {
        this.all(
            model,
            {
                "where": where
            },
            cb
        );
    }
};

/**
 * Delete all instances for the given model
 * @param {String} model The model name
 * @param {Object} [where] The filter for where
 * @param {Function} [cb] The callback function
 */
DynamoDB.prototype.destroyAll = function destroyAll(model, where, cb) {
    debug && debug("Destroying all " + model);
    if (!cb) {
        cb = where;
        where = null;
    }

    var me = this,
        handleDestroy = function (err, items) {
            if (err) {
                if (cb) {
                    cb(err);
                }
            } else {
                //There is something to destroy
                if (items && items.length > 0) {
                    var batchParams = {
                            RequestItems: {},
                            ReturnConsumedCapacity: 'NONE',
                            ReturnItemCollectionMetrics: 'NONE'
                        },
                        primaryKeyProperties = me.getPrimaryKeyProperties(model);

                    batchParams.RequestItems[model] = [];

                    me.dataSource.definitions


                    items.forEach(function (item) {
                        var deleteRequest = {
                                DeleteRequest: {
                                    "Key": {}
                                }
                            };

                        primaryKeyProperties.forEach(function (key) {
                            deleteRequest.DeleteRequest.Key[key.key] = item[key.key];
                        });

                        batchParams.RequestItems[model].push(deleteRequest);
                    });

                    me.client.batchWriteItem(
                        batchParams,
                        cb
                    );
                } else {
                    cb();
                }
            }
        };


    this.runAll(model, where, handleDestroy);
};

/**
 * Count the number of instances for the given model
 *
 * @param {String} model The model name
 * @param {Function} [cb] The callback function
 * @param {Object} filter The filter for where
 *
 */
DynamoDB.prototype.count = function count(model, cb, where) {
    debug && debug("Performing count on: ");
    debug && debug(model);
    var properties = where ? Object.keys(where) : [],
        conditions = [],
        me = this;


    this.all(model, {where: where}, function (err, items) {
        if (items) {
            cb(err, items.length);
        } else {
            cb(err, null);
        }
    });
};

/**
 * Update properties for the model instance data
 *
 * Updates in loopback are implicit merges, so first get the object, then merge it.
 *
 * @param {String} model The model name
 * @param {Object} data The model data
 * @param {Function} [cb] The callback function
 *
 *
 * Must have an id -- seems to get called by update and update All in other connectors
 */
DynamoDB.prototype.updateAttributes = function updateAttrs(model, id, data, cb, baseData) {
    var me = this;
    if (!id) {
      var err = new Error('You must provide an id when updating attributes!');
      if (cb) {
        return cb(err);
      } else {
        throw err;
      }
    }

    function doUpdate(model, id, data, cb, baseData) {
        var attributeUpdates = {},
            primaryKeyValues = {},
            primaryKeyProperties = me.getPrimaryKeyProperties(model);

        data = me.sanitizeData(data);

        Object.keys(data).forEach(function (modified_key) {
            if (!me.isInPrimaryIndex(modified_key, model)) {
                if (data[modified_key] === undefined && baseData[modified_key]) {
                    attributeUpdates[modified_key] = {
                        Action: 'PUT',
                        Value: null
                    };
                } else if (baseData[modified_key] === undefined && data[modified_key] !== undefined) {
                    attributeUpdates[modified_key] = {
                        Action: 'PUT',
                        Value: data[modified_key]
                    };
                } else if (data[modified_key] !== undefined) {
                    attributeUpdates[modified_key] = {
                        Action: 'PUT',
                        Value: data[modified_key]
                    };
                }debug
            }
        });

        primaryKeyProperties.forEach(function (property) {
            primaryKeyValues[property.key] = baseData[property.key];
        });

        var updateParams = {
            TableName: model,
            Key: primaryKeyValues,
            AttributeUpdates: attributeUpdates
        };

        me.client.updateItem(updateParams, cb);

    }
    if (baseData) {
        baseData = me.sanitizeData(baseData);
        doUpdate(model, id, data, cb, baseData);
    } else {
        this.find(model, id, function (err, baseData) {
            if (err) {
                cb(err);
                return;
            } else if (!baseData) {
                cb("Unable to find " + model + " with id " + id);
            }

            doUpdate(model, id, data, cb, baseData);
        });
    }
};

/**
 * Update all matching instances
 * @param {String} model The model name
 * @param {Object} where The search criteria
 * @param {Object} data The property/value pairs to be updated
 * @callback {Function} cb Callback function
 */
DynamoDB.prototype.update =
  DynamoDB.prototype.updateAll = function (model, where, data, cb) {
    var me = this;

    debug && debug("Update/UpdateAll on:");
    debug && debug(model);
    debug && debug(where);
    debug && debug(data);
    this.runAll(model, where, function (err, items) {

        async.each(items, function (item, done) {
            me.updateAttributes(model, item[me.idKey(model)], data, done, item);
        }, cb);
    });
};

/**
 * Clean a Table Name
 * @param {String} model Model to select table name from
 * @returns String
 * @type String
 */
DynamoDB.prototype.tableSanitized = function(model) {
  model = model.replace(/[^a-zA-Z0-9_\-\.]/, '');

  if (model.length < 3) {
    var i = model.length;
    while (i < 3) {
      model += '_';
      i++;
    }
  } else if (model.length > 255) {
    model = model.substring(0, 255);
  }

  return model;
};

/**
 * Perform automigrate for the given models.
 *
 * @param {String[]} [models] A model name or an array of model names. If not present, apply to all models
 * @param {Function} [cb] The callback function
 */
DynamoDB.prototype.automigrate = function (models, cb) {
  debug && debug('Performing automigrate on:');
  debug && debug(models);
  var self = this;
  if (self.client && self.dataSource) {
    if (self.debug) {
      debug('automigrate');
    }
    if ((!cb) && ('function' === typeof models)) {
      cb = models;
      models = undefined;
    }
    // First arg is a model name
    if ('string' === typeof models) {
      models = [models];
    }

    models = models || Object.keys(self._models);

    async.each(models, function (model, modelCallback) {
      if (self.debug) {
        debug('drop ')
      }

      if (self.dataSource.definitions[model]) {
        self.client.deleteTable({
            TableName: model
        }, function (err, data) {
            var attributeKeys = {},
               //Initialize indexes based on special model properties. they don't exist, just do id.
                primaryIndex = self.dataSource.definitions[model].settings.primaryIndex || {hashKey: {key: self.idKey(model), type: 'S'}},
                globalSecondaryIndices = self.dataSource.definitions[model].settings.indices,
                tableParams = {
                    KeySchema: [{
                        AttributeName: primaryIndex.hashKey.key,
                        KeyType: 'HASH'
                    }],
                    ProvisionedThroughput: {
                        ReadCapacityUnits: 1,
                        WriteCapacityUnits: 1
                    }
                };

            if (primaryIndex.rangeKey) {
                tableParams.KeySchema.push({
                    AttributeName: primaryIndex.rangeKey.key,
                    KeyType: 'RANGE'
                });
            }

            attributeKeys[primaryIndex.hashKey.key] = primaryIndex.hashKey;

            if (primaryIndex.rangeKey) {
                attributeKeys[primaryIndex.rangeKey.key] = primaryIndex.rangeKey;
            }

            if (globalSecondaryIndices) {
                tableParams.GlobalSecondaryIndexes = [];
                globalSecondaryIndexes.forEach(function (index) {
                    tableParams.GlobalSecondaryIndexes.push({
                        IndexName: 'UserEmail',
                        KeySchema: [{
                            AttributeName: index.hashKey.key,
                            KeyType: 'HASH'
                        }, {
                            AttributeName: index.rangeKey.key,
                            KeyType: 'RANGE'
                        }],
                        Projection: {
                            ProjectionType: 'ALL'
                        },
                        ProvisionedThroughput: {
                            ReadCapacityUnits: 1,
                            WriteCapacityUnits: 1
                        }
                    });

                    attributeKeys[index.hashKey.key] = index.hashKey;
                    if (attributeKeys.rangeKey) {
                        attributeKeys[index.rangeKey.key] = index.rangeKey;
                    }
                });
            }

            tableParams.AttributeDefinitions = [];

            Object.keys(attributeKeys).forEach(function (key) {
                tableParams.AttributeDefinitions.push({
                    AttributeType: attributeKeys[key].type,
                    AttributeName: attributeKeys[key].key
                });
            });

            tableParams.TableName = model;

            self.client.createTable(tableParams, function (err) {
                if (err) {
                    modelCallback(err);
                } else {
                    modelCallback(null);
                }
            });
        });
      } else {
        modelCallback();
      }
    }, function (err) {
        cb(err);
    });
  } else {
    cb();
  }
};

DynamoDB.prototype.disconnect = function () {

};

DynamoDB.prototype.ping = function (cb) {
    this.client.listTables({}, function(err, data) {
      if (err) {
        cb(err); // an error occurred
      } else{
          cb(err, data);           // successful response
      }
    });

};

/**
 * @private
 * @param {String}    model The name of the model
 *
 * @returns {String[]} Any model properties that are in the primary key of the dynamo db table.  If the model did
 *                     not define the primary key keys, then we assume 'id'.
 */
DynamoDB.prototype.getPrimaryKeyProperties = function (model) {
    var primaryKeyProperties = [];
    if (this.dataSource.definitions[model] &&
            this.dataSource.definitions[model].settings.primaryIndex) {

        var primaryKeyDefinition = this.dataSource.definitions[model].settings.primaryIndex;

        primaryKeyDefinition.hashKey.isHash = true;

        primaryKeyProperties.push(primaryKeyDefinition.hashKey);

        if (primaryKeyDefinition.rangeKey) {
            primaryKeyDefinition.rangeKey.isRange = true;
            primaryKeyProperties.push(primaryKeyDefinition.rangeKey);
        }
    } else {
        primaryKeyProperties.push({
            key: this.dataSource.definitions[model].idColumnName() || 'id',
            type: 'S',
            isHash: true
        });
    }

    return primaryKeyProperties;
};

/**
 *
 * @private
 */
DynamoDB.prototype.isInPrimaryIndex = function (key, model) {
    if (this.dataSource.definitions[model] &&
            this.dataSource.definitions[model].settings.primaryIndex) {
        var primaryKeyDefinition = this.dataSource.definitions[model].settings.primaryIndex;

        return key === primaryKeyDefinition.rangeKey.key || key === primaryKeyDefinition.hashKey.key;
    } else {
        return key === 'id';
    }
};

/**
 *
 * @private
 *
 * More information on where operations:
 * http://docs.strongloop.com/display/public/LB/Where+filter#Wherefilter-Operators
 *
 * Here we are checking to see if this where clause requests an operator that does not work for DynamoDB
 * querying.  This lets us know we have to scan instead.
 *
 */
DynamoDB.prototype.whereCanBeQueried = function (whereObject) {
    var properties = Object.keys(whereObject),
        whereIsQueriable = true;

    properties.forEach(function (conditionKey) {
        var whereValue = whereObject[conditionKey],
            hasNonQueryableAttributes = false;
        //The where definition is a complex and not just an equivalancy operation.
        if (Object.prototype.toString.call(whereValue) == "[object Object]") {
            hasNonQueryableAttributes = !!whereValue.inq ||
                                                !!whereValue.and ||
                                                    !!whereValue.or ||
                                                        !!whereValue.gt ||
                                                            !!whereValue.gte ||
                                                                !!whereValue.lt ||
                                                                    !!whereValue.lte ||
                                                                        !!whereValue.between ||
                                                                            !!whereValue.nin ||
                                                                                !!whereValue.near ||
                                                                                    !!whereValue.neq ||
                                                                                        !!whereValue.like ||
                                                                                            !!whereValue.nlike;
            if (hasNonQueryableAttributes) {
                whereIsQueriable = false;
            }
        }
    });

    return whereIsQueriable;
};

/**
 * @private
 */
DynamoDB.prototype.addWhereObjectToConditions = function (params, findOperation, whereObject, operator) {
    var properties = Object.keys(whereObject),
        me = this;
    //For each property in the where condition, decide what to add to the query or scan operation.
    properties.forEach(function (conditionKey) {
        var whereValue = whereObject[conditionKey];
        if (conditionKey === "and") {
            whereValue.forEach(function (whereClause) {
                me.addWhereObjectToConditions(params, findOperation, whereClause, 'AND');
            });
        } else if (conditionKey === "or") {
            whereValue.forEach(function (whereClause) {
                me.addWhereObjectToConditions(params, findOperation, whereClause, 'OR');
            });
        } else {
            me.addConditionToParam(params, findOperation, conditionKey, whereValue, operator || "AND");
        }

    });
};

/**
 * @private
 *
 * @param {Object}        params                   The params to the query or scan DynamoDB operation.
 * @param {String}        operation                "scan" or "query"
 * @param {String}        key                      The key in the where clause, either a property of the model, or a logical operator.
 * @param {Object|String|Number|String[]|Number[]} Whatever the value is in the logical operation.
 *
 */
DynamoDB.prototype.addConditionToParam = function (params, operation, key, conditionValue, operator) {
    if (key === "and") {
        throw new Error("And operator not currently supported");
    }

    if (key === "or") {
        throw new Error("Or operator not currently supported");
    }


    var attributeShorthand = "#" + key;
    //If it is a scan, we have to add expression attributes.
    if (operation === "scan") {

        params.ExpressionAttributeNames = params.ExpressionAttributeNames || {};

        params.ExpressionAttributeValues = params.ExpressionAttributeValues || {};

        var expressionAttributeName = {};

        params.ExpressionAttributeNames[attributeShorthand] = key;
    }

    function getExpressionValueToken(value) {
        if (!util.isArray(value)) {
            var token = value ? ":" + value.toString().replace(/[^a-zA-Z0-9]/g, "") : ":" + value;
            if (!params.ExpressionAttributeValues[token]) {
                if (value instanceof Date) {
                    params.ExpressionAttributeValues[token] = value / 1;
                } else {
                    params.ExpressionAttributeValues[token] = value;
                }
            }
            return token;
        } else {
            var tokens = [];
            value.forEach(function (subValue) {
                tokens.push(getExpressionValueToken(subValue));
            });
            return tokens.join(',');
        }
    }

    //The where definition is a complex and not just an equivalancy operation.
    if (Object.prototype.toString.call(conditionValue) == "[object Object]") {
        var operators = Object.keys(conditionValue);

        if (!params.FilterExpression) {
            params.FilterExpression = "";
        }

        operators.forEach(function (keyValue) {
            if (params.FilterExpression !== "") {
                params.FilterExpression += " " + operator + " ";
            }
            //We are already into a scan situation because queries can't support any of this.
            switch (keyValue) {
            case 'inq':
            params.FilterExpression += "(#" + key + " IN (" + getExpressionValueToken(conditionValue.inq) + ")" + ")";
            break;
            case 'gt':
            params.FilterExpression += "(#" + key + " > " + getExpressionValueToken(conditionValue.gt) + ")";
            break;
            case 'gte':
            params.FilterExpression += "(#" + key + " >= " + getExpressionValueToken(conditionValue.gte) + ")";
            break;
            case 'lt':
            params.FilterExpression += "(#" + key + " < " + getExpressionValueToken(conditionValue.lt) + ")";
            break;
            case 'lte':
            params.FilterExpression += "(#" + key + " <= " + getExpressionValueToken(conditionValue.lte) + ")";
            break;
            case 'between':
            params.FilterExpression += "(#" + key + " BETWEEN " + getExpressionValueToken(conditionValue.between[0]) + " AND " + getExpressionValueToken(conditionValue.between[1]) + ")";
            break;
            case 'nin':
            params.FilterExpression += "NOT " + "(#" + key + " IN (" + getExpressionValueToken(conditionValue.nin) + "))"
            break;
            case 'near':
            throw new Error('near is not supported');
            break;
            case 'neq':
            params.FilterExpression += "(#" + key + " <> " + getExpressionValueToken(conditionValue.neq) + ")";
            break;
            case 'like':
            params.FilterExpression += "(contains(#" + key + "," + getExpressionValueToken(conditionValue.like) + "))";
            break;
            case 'nlike':
            params.FilterExpression += "(NOT contains(#" + key + "," + getExpressionValueToken(conditionValue.nlike) + "))";
            break;
            case 'or':
            case 'and':
            throw new Error('and and or conditions are not supported');
            }
        });
    } else {
        //Simple case -- do equivalency in query/scan notation.
        if (operation === 'query') {
            params.KeyConditions = params.KeyConditions || [];
            params.KeyConditions.push(this.client.Condition(key, "EQ", conditionValue));
        } else {
            if (!params.FilterExpression) {
                params.FilterExpression = "";
            } else {
                params.FilterExpression += " " + operator + " ";
            }
            params.FilterExpression += "#" + key + " = " + getExpressionValueToken(conditionValue);
        }
    }
};


function merge(base, update) {
  if (!base) {
    return update;
  }
  // We cannot use Object.keys(update) if the update is an instance of the model
  // class as the properties are defined at the ModelClass.prototype level
  for(var key in update) {
    var val = update[key];
    if(typeof val === 'function') {
      continue; // Skip methods
    }
    base[key] = val;
  }
  return base;
}

/**
 * Initialize the DynamoDB connector for the given data source
 * @param {DataSource} dataSource The data source instance
 * @param {Function} [cb] The callback function
 */
exports.initialize = function initializeDataSource(dataSource, cb) {

  var settings = dataSource.settings || {};

  debug && debug('Initializing dynamo');

  // prepare for loading AWS SDK
  if (settings.region) {
    process.env.AWS_REGION = settings.region;
  }
  if (settings.credentials === 'shared') {
    if (settings.profile) {
      process.env.AWS_PROFILE = settings.profile;
    }
  }

  AWS = require('aws-sdk');
  DOC = require('dynamodb-doc');

  if (!AWS) {
    return;
  }

  if (settings.credentials === 'file') {
    AWS.config.loadFromPath(settings.credfile);
  //allowing in memory credentials against original repository creator's wishes
  } else if (settings.credentials === 'memory') {
      AWS.config.credentials = settings.inMemoryCredentials;
  }

  AWS.config.apiVersions = {
    dynamodb: 'latest'
  };


  var client = new DOC.DynamoDB();
  if (settings.region === 'local') {
    client.endpoint = new AWS.Endpoint(settings.endpoint);
  }

  dataSource.connector = new DynamoDB(client, dataSource);
  dataSource.connector.dataSource = dataSource;

  if (cb) {
    dataSource.connector.connect(cb);
  }
};
