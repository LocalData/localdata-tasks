'use strict';

var fs = require('fs');
var stream = require('stream');
var util = require('util');

var _ = require('lodash');
var async = require('async');
var concat = require('concat-stream');
var duplexify = require('duplexify');
var knox = require('knox');
var minimist = require('minimist');
var Promise = require('bluebird');
var through2 = require('through2');
var uuid = require('uuid');
var xml2js = require('xml2js');

var CSVStream = require('./csv');
var KMLStream = require('./kml');
var mongo = require('./mongo');
// TODO: we reference this in a couple of projects. Should we split the
// Mongoose model definitions out to a separate module?
var Response = require('./Response');
var shapefile = require('./shapefile');
var SQLiteStream = require('./sqlite-stream');

Promise.promisifyAll(fs);

var argv = minimist(process.argv.slice(2));
var data = JSON.parse(argv._[0]);

var mode = argv.mode;

if (mode === undefined) {
  mode = data.mode;
}

// data should contain the survey ID and any export options
var surveyId = data.survey;
var s3Object = data.s3Object;
var bucket = data.bucket;
// Used for Shapefile export
var name = data.name;
var timezone = data.timezone;
var open = data.open; // is the export public?

function makeTempPath() {
  return '/tmp/' + uuid.v1();
}

var s3Client = knox.createClient({
  key: process.env.S3_KEY,
  secret: process.env.S3_SECRET,
  bucket: bucket
});

var db;

// Only call func approximately once per period.
function throttle(func, period) {
  var timer;

  return function throttled() {
    if (timer) {
      return;
    }
    timer = setTimeout(function () {
      timer = false;
    }, period);
    func.apply(this, arguments);
  };
}

function promisifyStreamEnd(stream) {
  return new Promise(function (resolve, reject) {
    stream.on('error', reject);
    stream.on('end', resolve);
    stream.on('finish', resolve);
  });
}

// For this dataset, get a list of all info fields, a mapping of all response
// fields to their maximum arity, and the maximum number of files linked by an
// entry.
function getMetadata(surveyId, done) {
  Response.collection.group(
    {},
    { 'properties.survey': surveyId },
    {
      infoFields: {},
      responseArity: {},
      fileCount: 0
    },
    function reduce (doc, memo) {
      var k;
      var entry;
      for (k = 0; k < doc.entries.length; k += 1) {
        entry = doc.entries[k];

        // File count
        var files = entry.files;
        if (files) {
          var fileCount = files.length;
          if (fileCount > memo.fileCount) {
            memo.fileCount = fileCount;
          }
        }

        // Response field arity
        var fields;
        if (entry.responses) {
          fields = Object.keys(entry.responses);
        } else {
          fields = [];
        }
        var i;
        var len = fields.length;
        var field;
        var value;
        for (i = 0; i < len; i += 1) {
          field = fields[i];
          value = entry.responses[field];

          var max = memo.responseArity[field];

          if (max === undefined) {
            max = 0;
          }

          var arity = 1;

          if (Object.prototype.toString.call(value) === '[object Array]') {
            arity = value.length;
          }

          if (arity > max) {
            memo.responseArity[field] = arity;
          }
        }
      }

      // Info fields
      if (doc.properties.info) {
        var keys = Object.keys(doc.properties.info);
        for (k = 0; k < keys.length; k += 1) {
          memo.infoFields[keys[k]] = true;
        }
      }
    }, function (error, docs) {
      if (error) {
        console.log(error);
        done(error);
        return;
      }

      var doc;

      if (docs && docs.length > 0) {
        doc = docs[0];
        done(null, [
          Object.keys(doc.infoFields),
          Object.keys(doc.responseArity),
          doc.responseArity,
          doc.fileCount
        ]);
      } else {
        done(null, [[], [], {}, 0]);
      }
    }
  );
}

function EntryStream(options) {
  stream.Transform.call(this, {
    highWaterMark: 5,
    objectMode: true
  });
  this.filter = options.filter;
  this.count = 0;
}

util.inherits(EntryStream, stream.Transform);

var progress = throttle(function (count) {
  console.log('event=worker_info at=basic_exporter processed_entries=' + count);
}, 5000, { leading: false });


EntryStream.prototype._transform = function transform(chunk, encoding, done) {
  var items = this.filter(chunk);
  var i;
  var len;
  for (i = 0, len = items.length; i < len; i += 1) {
    this.push(items[i]);
    this.count += 1;
  }
  progress(this.count);
  done();
};

/**
* Get raw data for the survey and return a stream of entry objects.
*
* @method getEntryStream
* @param surveyID {String}
* @param filter {Function} Prepares the results for exporting Transforms or
* eliminates responses. For example, a filter might retain only the most recent
* entry for each response item.
*/
function getEntryStream(surveyId, filter) {
  var entryStream = new EntryStream({
    filter: filter
  });

  // Sort in ascending order of creation, so CSV exports vary only at the
  // bottom (i.e., they grow from the bottom as we get more responses).
  var query = Response.find({ 'properties.survey': surveyId });

  // If this is a public survey, don't include collector info
  if (open) {
    query.select('-entries.source.collector');
  }

  query.sort({ 'entries.created': 'asc' });

  var docStream = query.stream();

  docStream
  .on('error', function (error) {
    entryStream.emit('error', error);
  })
  .on('close', function () {
    entryStream.end();
  })
  .on('data', function (doc) {
    if (!entryStream.write(doc)) {
      docStream.pause();
    }
  });

  entryStream.on('drain', function () {
    docStream.resume();
  });


  return entryStream;
}

/*
 * Return only the most recent result for each base feature
 */
exports.getLatestEntries = function getLatestEntries(item) {
  return [item.getLatestEntry()];
};

exports.getAllEntries = function getAllEntries(item) {
  return item.entries.map(function (entry) {
    return item.getSingleEntry(entry.id);
  });
};


function storeRemoteData(filename, s3Object, done) {
  s3Client.putFile(filename, s3Object, function (error, res) {
    if (error) {
      return done(error);
    }

    var write = concat(function (s3Response) {
      if (res.statusCode === 200) {
        console.log('event=worker_info at=basic_exporter status=s3_storage_success message="' + s3Response + '"');
        done();
        return;
      }

      xml2js.parseString(s3Response, function (error, data) {
        if (error) {
          console.log('event=worker_error s3_status_code=' + res.statusCode + ' code=unknown message=unknown');
        } else {
          console.log('event=worker_error s3_status_code=' + res.statusCode + ' code=' + data.Error.Code + ' message="' + data.Error.Message + '"');
        }
        done();
      });
    });

    res.pipe(write).on('error', done);
  });
}

function fakeSQLiteConverter(options, postprocessor) {
  var converter = duplexify();
  var write;
  var read;

  var tmpSQLite = makeTempPath();
  write = new SQLiteStream(_.defaults({ filename: tmpSQLite }, options));
  converter.setWritable(write);
  converter._writableState.objectMode = true;

  write.on('finish', function () {
    if (postprocessor) {
      read = postprocessor(tmpSQLite);
    } else {
      read = fs.createReadStream(tmpSQLite);
    }
    read.on('end', function () {
      fs.unlink(tmpSQLite, function (error) {
        if (error) {
          converter.emit('error', error);
        }
      });
    });
    converter.setReadable(read);
  });

  return converter;
}

function filterGeometries(types) {
  return through2.obj(function (chunk, enc, done) {
    if (_.contains(types, chunk.geometry.type)) {
      this.push(chunk);
    }
    done();
  });
}

function fakeShapefileConverter(options) {
  var converter = duplexify();

  // Create a a temporary sqlite file for each geometry type.
  var pointTmp = makeTempPath();
  var lineStringTmp = makeTempPath();
  var polygonTmp = makeTempPath();
  var pointStream = new SQLiteStream(_.defaults({ filename: pointTmp }, options));
  var lineStringStream = new SQLiteStream(_.defaults({ filename: lineStringTmp }, options));
  var polygonStream = new SQLiteStream(_.defaults({ filename: polygonTmp }, options));

  var distributor = through2.obj();
  converter.setWritable(distributor);
  converter._writableState.objectMode = true;

  distributor.pipe(filterGeometries(['Point', 'MultiPoint'])).pipe(pointStream);
  distributor.pipe(filterGeometries(['LineString', 'MultiLineString'])).pipe(lineStringStream);
  distributor.pipe(filterGeometries(['Polygon', 'MultiPolygon'])).pipe(polygonStream);

  // Wait for the SQLite conversion to finish.
  Promise.join(
    promisifyStreamEnd(pointStream),
    promisifyStreamEnd(lineStringStream),
    promisifyStreamEnd(polygonStream)
  ).then(function () {
    // Pass the array of files to the shapefile converter, which will add all
    // resulting files to a ZIP archive.
    var files = [ pointTmp, lineStringTmp, polygonTmp ];
    var read = shapefile.convert(files, makeTempPath(), [
      options.name + '_point',
      options.name + '_linestring',
      options.name + '_polygon'
    ]);

    // Use the resulting readable stream as duplexify's readable half.
    converter.setReadable(read);

    // Remove the temporary files when the read stream has ended.
    read.on('end', function () {
      // TODO: use a through stream, so we can wait for the unlinks to finish
      // before emitting 'end' on the readable part of duplexify? The chance of
      // this causing a problem is low right now.
      Promise.map(files, fs.unlink.bind(fs))
      .catch(function (error) {
        converter.emit('error', error);
      });
    });
  }).catch(function (error) {
    converter.emit('error', error);
  });
  return converter;
}

var output;

// Set up filters
var filter;
if (data.latest) {
  filter = exports.getLatestEntries;
} else {
  filter = exports.getAllEntries;
}


var infoFields;
var responseFields;
var responseArity;
var fileCount;
// Temporary output file that we'll upload to S3
var filename = makeTempPath();

async.series([
  function (next) {
    db = mongo.connect({
      mongo: process.env.MONGO
    }, next);
  },
  function (next) {
    console.log('event=worker_info at=basic_exporter mongo_status=connected');

    // Create a file stream for the temporary file
    output = fs.createWriteStream(filename)
    .on('error', function (error) {
      console.log(error);
      process.exit(1);
    })
    .on('open', function () {
      next();
    });
  },
  async.waterfall.bind(async, [
    getMetadata.bind(null, surveyId),
    function (metadata, next) {
      infoFields = metadata[0];
      responseFields = metadata[1];
      responseArity = metadata[2];
      fileCount = metadata[3];
      next();
    }
  ]),
  function (next) {
    // Get a stream of raw entries.
    var entryStream = getEntryStream(surveyId, filter);

    var converter;

    if (mode === 'csv') {
      converter = new CSVStream({
        timezone: timezone,
        infoFields: infoFields,
        responseFields: responseFields,
        responseArity: responseArity
      });
    } else if (mode === 'kml') {
      converter = new KMLStream({
        timezone: timezone,
        infoFields: infoFields,
        responseFields: responseFields
      });
    } else if (mode === 'sqlite') {
      converter = fakeSQLiteConverter({
        timezone: timezone,
        infoFields: infoFields,
        responseFields: responseFields,
        fileCount: fileCount
      });
    } else if (mode === 'shapefile') {
      converter = fakeShapefileConverter({
        name: name,
        timezone: timezone,
        infoFields: infoFields,
        responseFields: responseFields,
        fileCount: fileCount
      });
    } else {
      return next(new Error('Unsupported format: ' + mode));
    }

    entryStream.pipe(converter).pipe(output)
    .on('finish', next)
    .on('error', next);

    converter.on('error', function (error) {
      output.emit('error', error);
    });
  },
  // Store the export on S3
  function (next) {
    var stats = fs.statSync(filename);
    console.log('event=worker_info at=basic_exporter export_file_size=' + stats.size);
    storeRemoteData(filename, s3Object, next);
  },
  // Remove the temporary file
  fs.unlink.bind(fs, filename),
  function (next) {
    db.close(next);
  }
], function (error) {
  // TODO: report failure vs. success to the worker management infrastructure.
  if (error) {
    console.log(error);
    console.log(error.stack);
  }

  process.send({
    type: 'result',
    data: null
  });

  process.exit(0);
});
