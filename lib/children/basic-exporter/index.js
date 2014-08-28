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
var mongo = require('./mongo');
// TODO: we reference this in a couple of projects. Should we split the
// Mongoose model definitions out to a separate module?
var Response = require('./Response');

var argv = minimist(process.argv.slice(2));
var data = JSON.parse(argv._[0]);

// TODO: choose export format based on mode
var mode = argv.mode;

// data should contain the survey ID and any export options
var surveyId = data.survey;
var s3Object = data.s3Object;
var bucket = data.bucket;
var uploadPrefix = 'http://' + bucket + '.s3.amazonaws.com/';

// Temporary output file that we'll upload to S3
var TMPDIR = '/tmp';
var filename = TMPDIR + '/' + uuid.v1();

var s3Client = knox.createClient({
  key: process.env.S3_KEY,
  secret: process.env.S3_SECRET,
  bucket: bucket
});

var db;

function EntryStream(options) {
  stream.Transform.call(this, {
    highWaterMark: 5,
    objectMode: true
  });
  this.filter = options.filter;
  this.count = 0;
}

util.inherits(EntryStream, stream.Transform);

var progress = _.throttle(function (count) {
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
* Get raw data for the survey and stream it to a temp file as semi-processed
* rows.
*
* @method pullData
* @param surveyID {String}
* @param filter {Function} Prepares the results for exporting Transforms or
* eliminates responses. For example, a filter might retain only the most recent
* entry for each response item.
* @param done {Function} Callback that gets the temporary filename, headers,
* the max number of elements to put in one cell, and how many empty cells to
* add to the various rows.
*/
function pullData(surveyId, filter, done) {
  var tmpFile = TMPDIR + '/' + uuid.v1();

  // Start with some basic headers
  var headers = ['object_id', 'address', 'collector', 'timestamp', 'source', 'lat', 'long', 'photos'];

  // Record which header is at which index
  var headerIndices = {};
  var maxEltsInCell = {};
  var i;
  var j;
  for (i = 0; i < headers.length; i += 1) {
    headerIndices[headers[i]] = i;
    maxEltsInCell[headers[i]] = 1;
  }

  var rows = [];
  var toPush = [];

  function addCell(row, dataset, key) {
    // Have we encountered this header/key before?
    if (!headerIndices.hasOwnProperty(key)) {
      headerIndices[key] = headers.length;
      maxEltsInCell[key] = 1;
      headers.push(key);
      // Add an empty entry to each existing row, since they didn't
      // have this column.
      for (j = 0; j < toPush.length - 1; j += 1) {
        toPush[j] += 1;
      }
    }

    var data = dataset[key];

    // Check if we have multiple answers.
    if (util.isArray(data)) {
      if (data.length > maxEltsInCell[key]) {
        maxEltsInCell[key] = data.length;
      }
    }

    // Add the response to the CSV row.
    row[headerIndices[key]] = data;
  }

  var entryStream = new EntryStream({
    filter: filter
  });

  // Sort in ascending order of creation, so CSV exports vary only at the
  // bottom (i.e., they grow from the bottom as we get more responses).
  var docStream = Response.find({ 'properties.survey': surveyId })
  .sort({ 'entries.created': 'asc' })
  .stream();

  docStream
  .on('error', done)
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

  function RowStream(options) {
    stream.Transform.call(this, {});
    this._writableState.objectMode = true;
  }

  util.inherits(RowStream, stream.Transform);

  RowStream.prototype._transform = function transform(item, encoding, done) {
    toPush.push(0);
    // Add context entries (object ID, source type)
    var row = [
      item.properties.object_id,
      item.properties.humanReadableName || '',
      item.properties.source.collector,
      item.properties.created.toISOString(), // Convert the date to ISO8601 format
      item.properties.source.type,
      item.properties.centroid[1],
      item.properties.centroid[0],
      item.properties.files
    ];

    // Add info subfields
    var info = item.properties.info;
    var key;
    for (key in info) {
      if (info.hasOwnProperty(key)) {
        addCell(row, info, key);
      }
    }

    // Then, add the survey results
    var resp;
    var responses = item.properties.responses;
    for (resp in responses) {
      if (responses.hasOwnProperty(resp)) {
        addCell(row, responses, resp);
      }
    }

    // Write the row of data.
    this.push(JSON.stringify(row) + '\n');
    done();
  };

  var output = fs.createWriteStream(tmpFile);

  entryStream.pipe(new RowStream()).pipe(output)
  .on('finish', function () {
    done(null, tmpFile, headers, maxEltsInCell, toPush);
  })
  .on('error', function (error) {
    done(error);
  });
}

function ProcessIntermediate(options) {
  stream.Transform.call(this, {
    encoding: 'utf8'
  });
  this.headers = options.headers;
  this.maxEltsInCell = options.maxEltsInCell;
  this.toPush = options.toPush;
  this.rowToString = options.rowToString;
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
    this.push(this.rowToString(this.headers, this.headers, this.maxEltsInCell) + '\n');
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
  async.waterfall.bind(async, [
    // Get raw data and stream it to a temp file as semi-processed rows
    pullData.bind(null, surveyId, filter),
    // Stream the list of rows to a finished file, converting to the output format
    function (tmpFile, headers, maxEltsInCell, toPush, next) {
      var readStream = fs.createReadStream(tmpFile);
      readStream.on('error', next);
      readStream.pipe(new ProcessIntermediate({
        headers: headers,
        maxEltsInCell: maxEltsInCell,
        toPush: toPush,
        // TODO: respect the specified format
        rowToString: csv.rowToString
      })).pipe(output)
      .on('error', next)
      .on('finish', function () {
        fs.unlink(tmpFile, next);
      });
    }
  ]),
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

