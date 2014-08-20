'use strict';

var EventEmitter = require('events').EventEmitter;
var fs = require('fs');
var util = require('util');

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


function getChunks(surveyId) {
  var emitter = new EventEmitter();

  function getChunk(start) {
    var count = 1000;
    var query = Response.find({
      'properties.survey': surveyId
    });

    // Sort in ascending order of creation, so CSV exports vary only at the
    // bottom (i.e., they grow from the bottom as we get more responses).
    query.sort({ 'entries.created': 'asc' });

    query.skip(start);
    query.limit(count);

    query.exec(function (error, items) {
      if (error) {
        emitter.emit('error', error);
      }

      console.log('event=worker-info at=basic-exporter chunk-start=' + start);

      // If we didn't get as many items as we asked for, then this is the last chunk.
      var last = (items.length < count);

      if (!last) {
        getChunk(start + count);
      }

      emitter.emit('chunk', items);

      if (last) {
        emitter.emit('end');
      }
    });
  }

  getChunk(0);
  return emitter;
}

/**
* Export a given survey. Includes options to filter and format the results.
*
* @method exportResults
* @param surveyID {String}
* @param filter {Function} Prepares the results for exporting
*           Transforms or eliminates responses. For example, a filter might
*           retain only the most recent entry for each response item.
* @param done {Function} Callback that gets the rows, headers, and the max
* number of elements to put in one cell
*/
function exportResults(surveyId, filter, done) {
  // Start with some basic headers
  var headers = ['object_id', 'address', 'collector', 'timestamp', 'source', 'centroid'];

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

  getChunks(surveyId)
  .on('error', function (error) {
    if (error) {
      done(error);
      return;
    }
  })
  .on('chunk', function (chunk) {
    var items = [];
    var i;
    var len;
    var item;
    
    len = chunk.length;
    for (i = 0; i < len; i += 1) {
      items = items.concat(filter(chunk[i]));
    }

    len = items.length;
    for (i = 0; i < len; i += 1) {
      item = items[i];
      // Add context entries (object ID, source type)
      var row = [
        item.properties.object_id,
        item.properties.humanReadableName || '',
        item.properties.source.collector,
        item.properties.created.toISOString(), // Convert the date to ISO8601 format
        item.properties.source.type,
        item.properties.centroid[0] + ',' + item.properties.centroid[1]
      ];

      // Add info subfields
      var info = item.properties.info;
      var key;
      for (key in info) {
        if (info.hasOwnProperty(key)) {
          if (!headerIndices.hasOwnProperty(key)) {
            headerIndices[key] = headers.length;
            maxEltsInCell[key] = 1;
            headers.push(key);
            // Add an empty entry to each existing row, since they didn't
            // have this column.
            for (j = 0; j < rows.length; j += 1) {
              rows[j].push('');
            }
          }

          var data = info[key];
          // Check if we have multiple answers.
          if (util.isArray(data)) {
            if (data.length > maxEltsInCell[key]) {
              maxEltsInCell[key] = data.length;
            }
          }

          // Add the response to the CSV row.
          row[headerIndices[key]] = data;
        }
      }

      // Then, add the survey results
      var resp;
      var responses = item.properties.responses;
      for (resp in responses) {

        if (responses.hasOwnProperty(resp)) {
          // If we haven't encountered this column, track it.
          if (!headerIndices.hasOwnProperty(resp)) {
            headerIndices[resp] = headers.length;
            maxEltsInCell[resp] = 1;
            headers.push(resp);
            // Add an empty entry to each existing row, since they didn't
            // have this column.
            for (j = 0; j < rows.length; j += 1) {
              rows[j].push('');
            }
          }
          var entry = responses[resp];

          // Check if we have multiple answers.
          if (util.isArray(entry)) {
            if (entry.length > maxEltsInCell[resp]) {
              maxEltsInCell[resp] = entry.length;
            }
          }

          // Add the response to the CSV row.
          row[headerIndices[resp]] = responses[resp];
        }
      }

      // Add the CSV row.
      rows.push(row);
    }
  })
  .on('end', function () {
    // Write the response
    done(null, rows, headers, maxEltsInCell);
  });
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
        console.log('event=worker-info at=basic-exporter status=s3-storage-success message="' + s3Response + '"');
        done();
        return;
      }

      xml2js.parseString(s3Response, function (error, data) {
        if (error) {
          console.log('event=worker-error s3-status-code=' + res.statusCode + ' code=unknown message=unknown');
        } else {
          console.log('event=worker-error s3-status-code=' + res.statusCode + ' code=' + data.Error.Code + ' message="' + data.Error.Message + '"');
        }
        done();
      });
    });
  });
}

var stream;

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
    console.log('event=worker-info at=basic-exporter mongo-status=connected');

    // Create a file stream for the temporary file
    stream = fs.createWriteStream(filename)
    .on('error', function (error) {
      console.log(error);
      process.exit(1);
    })
    .on('open', function () {
      next();
    });
  },
  async.waterfall.bind(async, [
    // Get data and turn it into a list of rows
    exportResults.bind(null, surveyId, filter),
    // Format the list of rows as CSV
    function (rows, headers, maxEltsInCell, next) {
      csv.writer(stream, rows, headers, maxEltsInCell, next);
    }
  ]),
  // Store the export on S3
  function (next) {
    var stats = fs.statSync(filename);
    console.log('event=worker-info at=basic-exporter temp-file-size=' + stats.size);
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

