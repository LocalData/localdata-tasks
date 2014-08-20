/*jslint node: true */
'use strict';

var mongoose = require('mongoose');


// Database options
var mongooseOptions = {
  db: {
    w: 1,
    safe: true,
    native_parser: true
  },
  server: {
    socketOptions: {
      // If we attempt to connect for 45 seconds, stop.
      connectTimeoutMS: 45000
    }
  }
};

exports.connect = function connect(config, done) {
  // Connect to the database
  mongoose.connect(config.mongo, mongooseOptions);
  var db = mongoose.connection;

  db.on('error', done);

  db.once('open', function () {
    done(null, db);
  });

  return db;
};
