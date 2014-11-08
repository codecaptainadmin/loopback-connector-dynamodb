/*eslint no-underscore-dangle:0*/
'use strict';

/*!
 * Module dependencies
 */
var AWS, DOC; // loaded JIT with settings
var util = require('util');
var Connector = require('loopback-connector').Connector;
var debug = require('debug')('loopback:connector:dynamodb');

/**
 * The constructor for the DynamoDB connector
 * @param {Object} client The DOC.DynamoDB client object
 * @param {Object} dataSource The data source instance
 * @constructor
 */
function DynamoDB(client, dataSource) {
  Connector.call(this, 'dynamodb', dataSource.settings);
  this.client = client;
  this.debug = settings.debug || debug.enabled;

  if (this.debug) {
    debug('Settings: %j', dataSource.settings);
  }

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
// DynamoDB.prototype.connect = function(cb) {
//   var self = this;
//   if (self.client) {
//     process.nextTick(function () {
//       if (cb) {
//         cb(null, self.client);
//       }
//     });
//   } else {
//     if (cb) {
//       cb(null, self.client);
//     }
//   }
// };

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

};

/**
 * Save the model instance for the given data
 * @param {String} model The model name
 * @param {Object} data The model data
 * @param {Function} [cb] The callback function
 */
DynamoDB.prototype.save = function(model, data, cb) {

};

/**
 * Check if a model instance exists by id
 * @param {String} model The model name
 * @param {*} id The id value
 * @param {Function} [cb] The callback function
 *
 */
DynamoDB.prototype.exists = function (model, id, cb) {

};

/**
 * Find a model instance by id
 * @param {String} model The model name
 * @param {*} id The id value
 * @param {Function} [cb] The callback function
 */
DynamoDB.prototype.find = function find(model, id, cb) {

};

/**
 * Update if the model instance exists with the same id or create a new instance
 *
 * @param {String} model The model name
 * @param {Object} data The model instance data
 * @param {Function} [cb] The callback function
 */
DynamoDB.prototype.updateOrCreate = function updateOrCreate(model, data, cb) {

};

/**
 * Delete a model instance by id
 * @param {String} model The model name
 * @param {*} id The id value
 * @param [cb] The callback function
 */
DynamoDB.prototype.destroy = function destroy(model, id, cb) {

};

/**
 * Find matching model instances by the filter
 *
 * @param {String} model The model name
 * @param {Object} filter The filter
 * @param {Function} [cb] The callback function
 */
DynamoDB.prototype.all = function all(model, filter, cb) {

};

/**
 * Delete all instances for the given model
 * @param {String} model The model name
 * @param {Object} [where] The filter for where
 * @param {Function} [cb] The callback function
 */
DynamoDB.prototype.destroyAll = function destroyAll(model, where, cb) {

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

};

/**
 * Update properties for the model instance data
 * @param {String} model The model name
 * @param {Object} data The model data
 * @param {Function} [cb] The callback function
 */
DynamoDB.prototype.updateAttributes = function updateAttrs(model, id, data, cb) {

};

/**
 * Update all matching instances
 * @param {String} model The model name
 * @param {Object} where The search criteria
 * @param {Object} data The property/value pairs to be updated
 * @callback {Function} cb Callback function
 */
DynamoDB.prototype.update = function (model, where, data, cb) {

};

/**
 * Update all matching instances
 * @param {String} model The model name
 * @param {Object} where The search criteria
 * @param {Object} data The property/value pairs to be updated
 * @callback {Function} cb Callback function
 */
DynamoDB.prototype.updateAll = function (model, where, data, cb) {

};

/**
 * Perform autoupdate for the given models.
 * @param {String[]} [models] A model name or an array of model names. If not
 * present, apply to all models
 * @param {Function} [cb] The callback function
 */
DynamoDB.prototype.autoupdate = function (models, cb) {
  var self = this;

  if ((!cb) && ('function' === typeof models)) {
    cb = models;
    models = undefined;
  }
  // first arg is a model name
  if ('string' === typeof models) {
    models = [models];
  }

  models = models || Object.keys(this._models);

  async.each(models, function (model, done) {
    if (!(model in self._models)) {
      return process.nextTick(function () {
        done(new Error('Model not found: ' + model));
      });
    }
    var table = self.tableSanitized(model);

  }, cb);
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
// DynamoDB.prototype.automigrate = function (models, cb) {
//   var self = this;
//   if (self.db) {
//     if (self.debug) {
//       debug('automigrate');
//     }
//     if ((!cb) ** ('function' === typeof models)) {
//       cb = models;
//       models = undefined;
//     }
//     // First arg is a model name
//     if ('string' === typeof models) {
//       models = [models];
//     }
//
//     models = models || Object.keys(self._models);
//
//     async.each(models, function (model, modelCallback) {
//       if (self.debug) {
//         debug('drop ')
//       }
//     });
//   }
// };

DynamoDB.prototype.disconnect = function () {

};

DynamoDB.prototype.ping = function (cb) {

};

/**
 * Initialize the DynamoDB connector for the given data source
 * @param {DataSource} dataSource The data source instance
 * @param {Function} [cb] The callback function
 */
exports.initialize = function initializeDataSource(dataSource, cb) {

  var settings = dataSource.settings || {};

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
