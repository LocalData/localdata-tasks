'use strict';

var stream = require('stream');
var util = require('util');

var moment = require('moment-timezone');
var Promise = require('bluebird');
var sqlite = require('sqlite3');
var wellknown = require('wellknown');

Promise.promisifyAll(sqlite);

function columnifier(prefix) {
  return function columnify(name) {
    var tmp = name.replace(/[^a-zA-Z0-9]/g, '_');
    if (prefix) {
      return prefix + tmp;
    }
    return tmp;
  };
}

function SQLiteStream(options) {
  stream.Writable.call(this, {
    objectMode: true
  });

  var coreFields = ['object_id', 'WKT_GEOMETRY', 'address', 'collector', 'timestamp', 'source', 'lat', 'long', 'photos'];
  var infoFields = options.infoFields;
  var responseFields = options.responseFields;
  var fileCount = options.fileCount;

  var i;
  for (i = 1; i < fileCount; i += 1) {
    coreFields.push('photos' + i);
  }

  this.fields = coreFields.map(columnifier()).concat(infoFields.map(columnifier('i_'))).concat(responseFields.map(columnifier('r_')));

  var itemFieldMod = columnifier('$i_');
  var responseFieldMod = columnifier('$r_');
  this.getData = function getData(item) {
    var created;
    if (this.timezone) {
      created = moment(item.properties.created).tz(this.timezone).format();
    } else {
      created = item.properties.created.toISOString();
    }

    var files = item.properties.files;

    if (!files) {
      files = [];
    }

    var ret = {
      $object_id: item.properties.object_id,
      $WKT_GEOMETRY: wellknown.stringify(item.geometry),
      $address: item.properties.humanReadableName || '',
      $collector: item.properties.source.collector,
      $timestamp: created,
      $source: item.properties.source.type,
      $lat: item.properties.centroid[1],
      $long: item.properties.centroid[0],
      $photos: files[0] || ''
    };

    var i;
    if (fileCount > 1) {
      for (i = 1; i < fileCount; i += 1) {
        ret['$photos' + i] = files[i];
      }
    }

    var info = item.properties.info;
    var responses = item.properties.responses;

    var len;
    var field;

    if (!info) {
      info = {};
    }

    for (i = 0, len = infoFields.length; i < len; i += 1) {
      field = infoFields[i];
      ret[itemFieldMod(field)] = info[field];
    }

    if (!responses) {
      responses = {};
    }

    for (i = 0, len = responseFields.length; i < len; i += 1) {
      field = responseFields[i];
      ret[responseFieldMod(field)] = responses[field];
    }

    return ret;
  };

  var self = this;

  this.init = (new Promise(function (resolve, reject) {
    var db = new sqlite.Database(options.filename, function (error) {
      if (error) {
        reject(error);
      } else {
        resolve(db);
      }
    });
  })).then(function (db) {
    self.db = db;

    // Create the table.
    var createString = 'CREATE TABLE tbl(';
    createString += self.fields.join(',');
    createString += ');';
    return self.db.runAsync(createString);
  }).then(function () {
    // Don't wait for things to sync after each INSERT, so we get a
    // performance boost without having to put every INSERT into a single
    // transaction.
    return self.db.runAsync('PRAGMA synchronous=OFF');
  }).then(function () {
    // Prepare the INSERT statement.
    var insertString = 'INSERT INTO tbl VALUES(';
    insertString += self.fields.map(function (field) {
      return '$' + field;
    }).join(',');
    insertString += ')';

    // The sqlite3 driver breaks convention by returning the statement
    // instead of passing it to the callback, so we can't rely on Bluebird to
    // promisify it.
    return new Promise(function (resolve, reject) {
      var statement = self.db.prepare(insertString, function (error) {
        if (error) {
          reject(error);
        } else {
          resolve(statement);
        }
      });
    });
  }).then(function (statement) {
    self.insert = statement;
  }).catch(function (error) {
    self.emit('error', error);
  });

  this.on('finish', function () {
    self.db.close(function (error) {
      if (error) {
        self.emit('error', error);
      }
    });
  });
}

util.inherits(SQLiteStream, stream.Writable);

SQLiteStream.prototype._write = function _write(item, encoding, done) {
  var data = this.getData(item);

  var promise;

  if (this.insert) {
    promise = this.insert.runAsync(data);
  } else {
    var self = this;
    promise = this.init.then(function () {
      return self.insert.runAsync(data);
    });
  }

  promise.then(function () {
    done();
  }).catch(done);
};

module.exports = SQLiteStream;
