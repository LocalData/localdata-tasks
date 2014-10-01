'use strict';

var EventEmitter = require('events').EventEmitter;
var fs = require('fs');
var stream = require('stream');
var util = require('util');

var _ = require('lodash');
var async = require('async');
var knox = require('knox');
var minimist = require('minimist');
var uuid = require('uuid');
var xml2js = require('xml2js');

var csv = require('./csv');
var GeoJSONStream = require('./geojson-stream');
var kml = require('./kml');
var mongo = require('./mongo');
// TODO: we reference this in a couple of projects. Should we split the
// Mongoose model definitions out to a separate module?
var Response = require('./Response');
var RowStream = require('./row-stream');
var shapefile = require('./shapefile');

var argv = minimist(process.argv.slice(2));
var data = JSON.parse(argv._[0]);

// TODO: choose export format based on mode
var mode = argv.mode;

// data should contain the survey ID and any export options
var surveyId = data.survey;
var s3Object = data.s3Object;
var bucket = data.bucket;
var uploadPrefix = 'http://' + bucket + '.s3.amazonaws.com/';
// Used for Shapefile export
var name = data.name;
var timezone = data.timezone;

// Temporary output file that we'll upload to S3
var TMPDIR = '/tmp';
var filename = TMPDIR + '/' + uuid.v1();

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
  items.forEach(function (item) {
    this.push(item);
    this.count += 1;
  }.bind(this));
  progress(this.count);
  done();
};

function pushBlanks(row, count) {
  var i;
  for (i = 0; i < count; i += 1) {
    row.push('');
  }
}

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
  var tmpFile = TMPDIR + '/' + uuid.v1();


  var entryStream = new EntryStream({
    filter: filter
  });

  // Sort in ascending order of creation, so CSV exports vary only at the
  // bottom (i.e., they grow from the bottom as we get more responses).
  var docStream = Response.find({ 'properties.survey': surveyId })
  .sort({ 'entries.created': 'asc' })
  .stream();

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

// Convert temporary row data to the output format
function ProcessIntermediate(options) {
  stream.Transform.call(this, {
    encoding: 'utf8'
  });
  this.headers = options.headers;
  this.maxEltsInCell = options.maxEltsInCell;
  this.toPush = options.toPush;
  this.headerToString = options.headerToString;
  this.rowToString = options.rowToString;
  this.codaString = options.codaString;
  this.buf = '';
  this.index = 0;

  this.headersWritten = false;

}

util.inherits(ProcessIntermediate, stream.Transform);

ProcessIntermediate.prototype.convert = function convert(raw) {
  var row;
  var converted;
  try {
    row = JSON.parse(raw);
  } catch (e) {
    this.emit(e);
    return;
  }

  pushBlanks(row, this.toPush[this.index]);
  this.index += 1;
  converted = this.rowToString(row, this.headers, this.maxEltsInCell);

  return converted;
};

ProcessIntermediate.prototype._transform = function transform(chunk, encoding, done) {
  var pieces;
  var convert = this.convert.bind(this);
  var push = this.push.bind(this);
  var tail;

  if (!this.headersWritten) {
    this.headersWritten = true;
    this.push(this.headerToString(this.headers, this.maxEltsInCell) + '\n');
  }

  chunk = chunk.toString();

  if (chunk !== null) {
    pieces = chunk.split('\n');

    if (chunk[chunk.length - 1] !== '\n') {
      // We didn't read a full last line.
      tail = pieces.pop();
    }

    while (pieces.length > 0 && pieces[pieces.length - 1] === '') {
      pieces.pop();
    }

    if (pieces.length > 0) {
      pieces[0] = this.buf + pieces[0];
      this.buf = '';
      // Each element of pieces is JSON data for a row.
      pieces.forEach(function (raw) {
        push(convert(raw) + '\n');
      });
    }

    if (tail) {
      this.buf += tail;
    }
  }
  done();
};

ProcessIntermediate.prototype._flush = function flush(done) {
  if (this.buf.length > 0) {
    this.push(this.convert(this.buf) + '\n');
  }
  this.push(this.codaString() + '\n');
  done();
};

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

    var s3Response = '';
    res
    .on('readable', function () {
      var chunk = res.read();
      if (chunk) {
        s3Response += chunk;
      }
    })
    .on('end', function () {
      if (res.statusCode === 200) {
        res.resume();
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
  });
}

var output;

// Set up filters
var filter;
if (data.latest) {
  filter = exports.getLatestEntries;
} else {
  filter = exports.getAllEntries;
}


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
  function (next) {
    // Get a stream of raw entries.
    var entryStream = getEntryStream(surveyId, filter);

    // Create a write stream to a temporary file.
    var tmpFile = TMPDIR + '/' + uuid.v1();
    var tmp = fs.createWriteStream(tmpFile);

    if (mode === 'csv' || mode === 'kml') {
      // Handle CSV or KML export.

      var rowStream = new RowStream({
        timezone: data.timezone
      });

      // Convert the entries to semi-processed rows and pipe to a temporary
      // file.
      entryStream.pipe(rowStream).pipe(tmp)
      .on('finish', function () {
        var options = {
          headers: rowStream.headers,
          maxEltsInCell: rowStream.maxEltsInCell,
          toPush: rowStream.toPush
        };

        if (mode === 'kml') {
          // KML
          options.headerToString = kml.headerToString;
          options.rowToString = kml.rowToString;
          options.codaString = kml.codaString;
        } else {
          // CSV
          options.headerToString = csv.headerToString;
          options.rowToString = csv.rowToString;
          options.codaString = csv.codaString;
        }

        // Finish processing the rows, and stream to a finished file.
        var readStream = fs.createReadStream(tmpFile);
        readStream.on('error', next);
        readStream.pipe(new ProcessIntermediate(options)).pipe(output)
        .on('error', next)
        .on('finish', function () {
          // Remove the temporary file.
          fs.unlink(tmpFile, next);
        });
      })
      .on('error', function (error) {
        next(error);
      });
    } else if (mode === 'shapefile') {
      // Handle Shapefile export.
      // We'll convert to GeoJSON and then call ogr2ogr.

      // Convert the entries to GeoJSON features and pipe to a temporary file.
      var geoJSONStream = new GeoJSONStream();
      entryStream.pipe(geoJSONStream).pipe(tmp)
      .on('error', next)
      .on('finish', function () {
        var tmpShapeDir = TMPDIR + '/' + uuid.v1();
        // Convert the temporary GeoJSON file to a ZIP'd shapefile
        var zip = shapefile.convert(tmpFile, tmpShapeDir, name);
        zip.pipe(output)
        .on('error', next)
        .on('finish', function () {
          fs.unlink(tmpFile, next);
        });

        zip.on('error', next);
      });
    }
  },
  // Store the export on S3
  function (next) {
    var stats = fs.statSync(filename);
    console.log('event=worker_info at=basic_exporter temp_file_size=' + stats.size);
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
  }

  process.send({
    type: 'result',
    data: null
  });

  process.exit(0);
});

