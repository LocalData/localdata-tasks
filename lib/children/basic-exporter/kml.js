'use strict';

var stream = require('stream');
var util = require('util');

var _ = require('lodash');
var moment = require('moment-timezone');

function emptyOrEscape(str) {
  if (str === undefined) {
    return '';
  }
  return _.escape(str);
}

/*
 * Turn a stream of entries into KML data
 */
function KMLStream(options) {
  stream.Transform.call(this, {
    encoding: 'utf8'
  });

  this._writableState.objectMode = true;

  this.infoFields = options.infoFields;
  this.responseFields = options.responseFields;

  this.headerWritten = false;
}

util.inherits(KMLStream, stream.Transform);

var edString = '<Data name="${name}"><displayName>${name}</displayName><value>${value}</value></Data>\n';

function generatePartialDataString(fields, data) {
  var str = '';
  var field;
  var value;
  var i;

  for (i = 0; i < fields.length; i += 1) {
    field = fields[i];
    value = data[field];

    str += _.template(edString, {
      name: field,
      value: emptyOrEscape(value)
    });
  }

  return str;
}

KMLStream.prototype._headerString = function () {
  return '<?xml version="1.0" encoding="UTF-8"?>\n' +
    '<kml xmlns="http://www.opengis.net/kml/2.2">\n' +
    '<Document><name>KML Export</name><open>1</open><description></description>\n' +
    '<Folder>\n<name>Placemarks</name>\n<description></description>\n';
};

var placemarkString = '<Placemark><name></name><description></description><Point><coordinates>${x},${y}</coordinates></Point><ExtendedData>${extended}</ExtendedData></Placemark>\n';

KMLStream.prototype._transform = function transform(item, encoding, done) {
  if (!this.headersWritten) {
    this.push(this._headerString());
    this.headersWritten = true;
  }

  var ed = '';
  var value;
  var field;
  var i;

  var created;
  if (this.timezone) {
    created = moment(item.properties.created).tz(this.timezone).format();
  } else {
    created = item.properties.created.toISOString(); // Convert the date to ISO8601 format
  }

  var coreFields = ['object_id', 'address', 'collector', 'timestamp', 'source', 'lat', 'long', 'photos'];

  ed += generatePartialDataString(coreFields, {
    object_id: item.properties.object_id,
    address: emptyOrEscape(item.properties.humanReadableName),
    collector: emptyOrEscape(item.properties.source.collector),
    timestamp: created,
    source: item.properties.source.type,
    lat: item.properties.centroid[1],
    long: item.properties.centroid[0],
    photos: item.properties.files
  });

  if (item.properties.info) {
    ed += generatePartialDataString(this.infoFields, item.properties.info);
  }

  if (item.properties.responses) {
    ed += generatePartialDataString(this.responseFields, item.properties.responses);
  }

  this.push(_.template(placemarkString, {
    x: item.properties.centroid[0],
    y: item.properties.centroid[1],
    extended: ed
  }));
  done();
};

KMLStream.prototype._flush = function flush(done) {
  this.push('\n</Folder></Document></kml>');
  done();
};

module.exports = KMLStream;
