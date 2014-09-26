'use strict';

var util = require('util');

var _ = require('lodash');

var edString = '<Data name="${name}"><displayName>${name}</displayName><value>${value}</value></Data>';
var placemarkString = '<Placemark><name></name><description></description><Point><coordinates>${x},${y}</coordinates></Point><ExtendedData>${extended}</ExtendedData></Placemark>';
/*
 * Turn a list of parcel attributes into a KML entry.
 */
exports.rowToString = function rowtoKMLString(row, headers, maxEltsInCell) {
  var ed = '';
  var value;
  var i;
  for (i = 0; i < row.length; i += 1) {
    value = '';
    if (row[i] !== undefined) {
      value = row[i];
    }
    ed += _.template(edString, {
      name: headers[i],
      value: value
    });
  }

  return _.template(placemarkString, {
    x: row[6],
    y: row[5],
    extended: ed
  });
};

var header = '<?xml version="1.0" encoding="UTF-8"?>\n' +
  '<kml xmlns="http://www.opengis.net/kml/2.2">\n' +
  '<Document><name>KML Export</name><open>1</open><description></description>\n' +
  '<Folder>\n<name>Placemarks</name>\n<description></description>\n';

exports.headerToString = function headerToKMLString(headers, maxEltsInCell) {
  return header;
};

exports.codaString = function codaString() {
  return '\n</Folder></Document></kml>';
};


exports.listToKMLString = function listToKMLString(row, headers, maxEltsInCell) {
  var i;
  var elt = '\n<Placemark>';
  elt += '<name></name>';
  elt += '<description></description>';

  // The coordinates come escaped, so we need to unescape them:
  elt += '<Point><coordinates>' + row[4] + '</coordinates></Point>';

  elt += '<ExtendedData>';
  for (i = 0; i < row.length; i += 1) {
      elt += '<Data name="' + headers[i] + '">';
      elt += '<displayName>' + headers[i] + '</displayName>';

      if (row[i] !== undefined) {
        elt += '<value>' + row[i] + '</value>';
      } else {
        elt += '<value></value>';
      }
      elt += '</Data>';
  }
  elt += '</ExtendedData></Placemark>\n';

  return elt;
};
