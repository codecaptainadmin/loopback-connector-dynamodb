describe('dynamodb imported features', function () {
  before(function () {
    require('./init.js');
  });
  
  require('loopback-datasource-juggler/test/common.batch.js');
});