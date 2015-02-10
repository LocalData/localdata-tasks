'use strict';

var stream = require('stream');
var util = require('util');

var moment = require('moment-timezone');

var coreFields = ['object_id', 'address', 'collector', 'timestamp', 'source', 'entry_id', 'lat', 'long', 'photos'];

function CSVStream(options) {
  stream.Transform.call(this, {
    encoding: 'utf8'
  });

  this._writableState.objectMode = true;

  this.infoFields = options.infoFields;
  this.responseFields = options.responseFields;
  this.responseArity = options.responseArity;

  this.headerWritten = false;
}

util.inherits(CSVStream, stream.Transform);


function escapeCommas(val) {
  // Check if we need to escape the value
  if (val.indexOf(',') !== -1) {
    return '"' + val + '"';
  }

  return val;
}

CSVStream.prototype._headerString = function () {
  var arr = coreFields;
  var index = arr.length;
  var i;

  // Add the metadata headers (eg created, surveyor name)
  for (i = 0; i < this.infoFields.length; i += 1) {
    arr[index] = this.infoFields[i];
    index += 1;
  }

  // Add the response answer headers
  for (i = 0; i < this.responseFields.length; i += 1) {
    arr[index] = escapeCommas(this.responseFields[i]);
    index += 1;
  }

  return arr.join(',') + '\n';
};

function clean(val) {
  // Check for null/undefined cells
  if (val === undefined || val === null) {
    return '';
  }

  // Check if we need to escape the value
  var str = String(val);
  val = escapeCommas(str);

  return val;
}

CSVStream.prototype._transform = function transform(item, encoding, done) {
  if (!this.headerWritten) {
    this.push(this._headerString());
    this.headerWritten = true;
  }

  var created;
  if (this.timezone) {
    created = moment(item.properties.created).tz(this.timezone).format();
  } else {
    created = item.properties.created.toISOString(); // Convert the date to ISO8601 format
  }
  var arr = [
    item.properties.object_id,
    clean(item.properties.humanReadableName) || '',
    clean(item.properties.source.collector),
    created,
    item.properties.source.type,
    item.id.toString(),
    item.properties.centroid[1],
    item.properties.centroid[0],
    item.properties.files
  ];

  var index = arr.length;

  var i;
  var val;

  var info = item.properties.info;
  if (!info) {
    info = {};
  }

  for (i = 0; i < this.infoFields.length; i += 1) {
    val = info[this.infoFields[i]];
    arr[index] = clean(val);
    index += 1;
  }

  var responses = item.properties.responses;
  if (!responses) {
    responses = {};
  }

  var field;
  var arity;
  for (i = 0; i < this.responseFields.length; i += 1) {
    field = this.responseFields[i];
    val = responses[field];
    arity = this.responseArity[field];

    if (arity === 1) {
      arr[index] = clean(val);
    } else if (!util.isArray(val)) {
      arr[index] = clean(val);
    } else {
      // If it's an array of responses, join them with a semicolon
      arr[index] = val.join(';');
    }
    index += 1;
  }

  this.push(arr.join(',') + '\n');
  done();
};

module.exports = CSVStream;
