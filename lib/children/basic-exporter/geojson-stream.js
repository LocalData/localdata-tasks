'use strict';

var stream = require('stream');
var util = require('util');

var _ = require('lodash');

// Convert docs to GeoJSON with flat properties
function GeoJSONStream(options) {
  stream.Transform.call(this, {
    encoding: 'utf8'
  });

  // We write objects to this stream but read String data
  this._writableState.objectMode = true;
  this._readableState.objectMode = false;

  this.first = true;
}

util.inherits(GeoJSONStream, stream.Transform);

function flatten(data, field, prefix) {
  var nested = data[field];
  data[field] = undefined;
  _.each(_.keys(nested), function (key) {
    var newKey = prefix + key;
    data[newKey] = nested[key];
  });
}

GeoJSONStream.prototype._transform = function transform(item, encoding, done) {
  if (this.first) {
    this.push('{"type":"FeatureCollection","features":[');
    this.first = false;
  } else {
    this.push(',');
  }

  // Flatten the response fields
  flatten(item.properties, 'responses', 'r.');

  // Flatten the info fields
  flatten(item.properties, 'info', 'i.');

  this.push(JSON.stringify(item));
  done();
};

GeoJSONStream.prototype._flush = function flush(done) {
  this.push(']}\n');
  done();
};

module.exports = GeoJSONStream;
