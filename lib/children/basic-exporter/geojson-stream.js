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
  flatten(item.properties, 'responses', 'r_');

  // Flatten the info fields
  flatten(item.properties, 'info', 'i_');

  if (item.properties.files && item.properties.files.length > 0) {
    item.properties.photo = item.properties.files[0];
    var i;
    for (i = 1; i < item.properties.files.length; i += 1) {
      item.properties['photo' + i] = item.properties.files[i];
    }
  }
  item.properties.files = undefined;

  item.properties.lng = item.properties.centroid[0];
  item.properties.lat = item.properties.centroid[1];
  item.properties.centroid = undefined;

  this.push(JSON.stringify(item));
  done();
};

GeoJSONStream.prototype._flush = function flush(done) {
  this.push(']}\n');
  done();
};

module.exports = GeoJSONStream;
