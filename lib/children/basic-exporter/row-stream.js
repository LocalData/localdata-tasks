'use strict';

var moment = require('moment-timezone');
var stream = require('stream');
var util = require('util');

function RowStream(options) {
  stream.Transform.call(this, {});
  this._writableState.objectMode = true;

  if(options.timezone) {
    this.timezone = options.timezone;
  }

  // Start with some basic headers
  this.headers = ['object_id', 'address', 'collector', 'timestamp', 'source', 'lat', 'long', 'photos'];

  // Record which header is at which index
  this.headerIndices = {};
  this.maxEltsInCell = {};
  var i;
  var j;
  for (i = 0; i < this.headers.length; i += 1) {
    this.headerIndices[this.headers[i]] = i;
    this.maxEltsInCell[this.headers[i]] = 1;
  }

  var rows = [];
  this.toPush = [];

  this.addCell = function addCell(row, dataset, key) {
    // Have we encountered this header/key before?
    if (!this.headerIndices.hasOwnProperty(key)) {
      this.headerIndices[key] = this.headers.length;
      this.maxEltsInCell[key] = 1;
      this.headers.push(key);
      // Add an empty entry to each existing row, since they didn't
      // have this column.
      for (j = 0; j < this.toPush.length - 1; j += 1) {
        this.toPush[j] += 1;
      }
    }

    var data = dataset[key];

    // Check if we have multiple answers.
    if (util.isArray(data)) {
      if (data.length > this.maxEltsInCell[key]) {
        this.maxEltsInCell[key] = data.length;
      }
    }

    // Add the response to the CSV row.
    row[this.headerIndices[key]] = data;
  };
}

util.inherits(RowStream, stream.Transform);

RowStream.prototype._transform = function transform(item, encoding, done) {
  this.toPush.push(0);
  // Add context entries (object ID, source type)
  var created = item.properties.created.toISOString(); // Convert the date to ISO8601 format
  if (this.timezone) {
    created = moment.tz(created, this.timezone).format();
  }

  var row = [
    item.properties.object_id,
    item.properties.humanReadableName || '',
    item.properties.source.collector,
    created,
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
      this.addCell(row, info, key);
    }
  }

  // Then, add the survey results
  var resp;
  var responses = item.properties.responses;
  for (resp in responses) {
    if (responses.hasOwnProperty(resp)) {
      this.addCell(row, responses, resp);
    }
  }

  // Write the row of data.
  this.push(JSON.stringify(row) + '\n');
  done();
};


module.exports = RowStream;
