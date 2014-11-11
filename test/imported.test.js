describe('dynamodb imported features', function () {
  var Post, db;

  before(function () {
    require('./init.js');
    db = getDataSource();

    Post = db.define('Post', {
      title: { type: String, length: 255, index: true },
      content: { type: String },
      comments: [String]
    });
  });

  beforeEach(function (done) {
    done();
  });

  it('should create', function (done) {
    console.log("Create Test");
    Post.create({title: 'Post', content: 'Post content', comments: ["nice", "ooo"]}, function (err, post) {
      console.log("Error:", err);
      done(err);
    });
  });

  after(function (done) {
    done();
  });

  require('loopback-datasource-juggler/test/common.batch.js');
});

