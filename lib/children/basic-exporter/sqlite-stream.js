'use strict';

var stream = require('stream');
var util = require('util');

var moment = require('moment-timezone');
var Promise = require('bluebird');
var sqlite = require('sqlite3');
var wellknown = require('wellknown');

Promise.promisifyAll(sqlite);

function columnSafe(name) {
  return name.replace(/[^a-zA-Z0-9]/g, '_');
}

function prefixer(prefix) {
  return function (name) {
    return prefix + name;
  };
}

function compose(f, g) {
  return function (name) {
    return f(g(name));
  };
}

function SQLiteStream(options) {
  stream.Writable.call(this, {
    objectMode: true
  });

  var coreFields = ['object_id', 'WKT_GEOMETRY', 'address', 'collector', 'timestamp', 'source', 'entry_id', 'lat', 'long', 'photos'];
  var infoFields = options.infoFields;
  var responseFields = options.responseFields;
  var fileCount = options.fileCount;
  
  var i;
  for (i = 1; i < fileCount; i += 1) {
    coreFields.push('photos' + i);
  }

  var uniqueNames = {};
  var uniqueNamesLower = {};
  // Called once on each response field name. Builds a mapping of response field
  // names to SQLite-safe unique column names.
  // SQLite requires column names to be unique according to case-insensitive
  // comarison, so we can't always reuse unique field names as unique column
  // names.
  // We make column names unique by appending underscores to the name, which is
  // a little gross. But we're only doing this to columns that were previously
  // just distinuished by case, and we will most likely convert the data into a
  // Shapefile, which performs its own mangling of the names (likely unimpacted
  // by the extra underscores).
  function buildUniqueColumns(name) {
    var modified = name;
    var modifiedLower = modified.toLowerCase();
    while (uniqueNamesLower[modifiedLower]) {
      modified = modified + '_';
      modifiedLower = modifiedLower + '_';
    }
    uniqueNamesLower[modifiedLower] = true;
    uniqueNames[modified] = name;
    return modified;
  }

  var responseColumnMap;
  // Returns the unique, SQLite-safe column name for a response field name.
  function getColumn(name) {
    if (!responseColumnMap) {
      responseColumnMap = {};
      uniqueNamesLower = undefined;
      Object.keys(uniqueNames).forEach(function (modified) {
        responseColumnMap[uniqueNames[modified]] = modified;
      });
      uniqueNames = undefined;
    }
    return responseColumnMap[name];
  }
  
  this.fields = coreFields.map(columnSafe)
  .concat(infoFields.map(compose(prefixer('i_'), columnSafe)))
  .concat(responseFields.map(compose(prefixer('r_'), compose(buildUniqueColumns, columnSafe))));

  var itemFieldMod = compose(prefixer('$i_'), columnSafe);
  var responseFieldMod = compose(prefixer('$r_'), compose(getColumn, columnSafe));
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
      $entry_id: item.id.toString(),
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
