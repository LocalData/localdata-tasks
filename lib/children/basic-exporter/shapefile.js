/*jslint node: true */
'use strict';

var childProcess = require('child_process');
var fs = require('fs');
var stream = require('stream');
var util = require('util');

var archiver = require('archiver');
var Promise = require('bluebird');
var qfs = require('q-io/fs');

Promise.promisifyAll(fs);
Promise.promisifyAll(childProcess);

var TMPDIR;

var OGRCMD = 'ogr2ogr -f "ESRI Shapefile" ';

exports.convert = function convert(geoJSONFile, outdir, outname) {
  var zip = archiver('zip');

  // Make the temporary directory
  fs.mkdirAsync(outdir)
  .then(function () {
    // Run the ogr2ogr command
    var cmd = OGRCMD + outdir + '/' + outname + '.shp ' + geoJSONFile;
    console.log('running command: ' + cmd);
    return childProcess.execAsync(cmd, {
      env: process.env
    });
  })
  .spread(function (stdout, stderr) {
    // Gather the names of the files created by ogr2ogr.

    // XXX improve logging
    console.log(stdout);
    console.log(stderr);

    // Return a promise for the file names
    return fs.readdirAsync(outdir);
  })
  .then(function (files) {
    // Add files to the zip stream.
    files.forEach(function (name) {
      zip.append(fs.createReadStream(outdir + '/' + name), { name : name });
    });

    // Finalize the zip stream.
    return Promise.promisify(zip.finalize, zip)()
    .then(function (count) {
      // XXX improve logging
      console.log('Wrote ' + count + ' bytes to the ZIP archive.');
      // Remove temporary directory
      return Promise.resolve(qfs.removeTree(outdir));
    });
  })
  .catch(function (error) {
    zip.emit('error', error);
  });

  return zip;
};
