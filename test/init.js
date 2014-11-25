module.exports = require('should');

var path = require('path'),
    DataSource = require('loopback-datasource-juggler').DataSource,
    ModelBuilder = require('loopback-datasource-juggler').ModelBuilder,
    Schema = require('loopback-datasource-juggler').Schema;


var config = require('rc')('loopback', {test: {dynamodb: {
    region: 'local',
    credentials: 'file',
    credfile: path.join(__dirname, 'credentials.json'),
    endpoint: 'http://localhost:8000'
}}}).test.dynamodb;

global.getDataSource = global.getSchema = function (customConfig) {
  var db = new DataSource(require('../'), customConfig || config);
  db.log = function (a) {
    console.log(a);
  };

  return db;
};

if (!('getSchema' in global)) {
  global.getSchema = function (connector, settings) {
    return new Schema(connector || 'memory', settings);
  };
}

if (!('getModelBuilder' in global)) {
  global.getModelBuilder = function () {
    return new ModelBuilder();
  };
}