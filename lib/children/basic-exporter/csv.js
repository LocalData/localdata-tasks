'use strict';

var util = require('util');

/*
 * Turn a list of parcel attributes into a comma-separated string.
 * NOTE: Will break if used with strings with commas (doesn't escape!)
 */
exports.rowToString = function rowtoCSVString(row, headers, maxEltsInCell) {
  var arr = [];
  var i;
  for (i = 0; i < row.length; i += 1) {
    if (maxEltsInCell[headers[i]] === 1) {

      // Check for null/undefined cells
      if (row[i] === undefined || row[i] === null) {
        row[i] = '';
      }

      // Check if we need to escape the value
      row[i] = String(row[i]);
      if(row[i].indexOf(',') !== -1){
        row[i] = '"' + row[i] + '"';
      }

      // No multiple-choice for this column
      arr.push(row[i]);

    } else {
      // There might be multiple items in this cell.
      // FIXME: It doesn't look like we use len
      var len;
      if (!util.isArray(row[i])) {

        // This row only has one answer in this column, so just push that.
        // Check first to see if it's an empty value
        if(row[i] !== undefined) {

          // Check if we need to escape the value
          row[i] = String(row[i]);
          if(row[i].indexOf(',') !== -1){
            row[i] = '"' + row[i] + '"';
          }

          arr.push(row[i]);
        } else {
          arr.push('');
        }

        len = 1;
      } else {
        // If it's an array of responses, join them with a semicolon
        arr.push(row[i].join(';'));
      }
    }
  }
  return arr.join(',');
}
