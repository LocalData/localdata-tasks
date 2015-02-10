/*jslint node: true */
'use strict';

var childProcess = require('child_process');
var fs = require('fs');

var archiver = require('archiver');
var Promise = require('bluebird');
var qfs = require('q-io/fs');

Promise.promisifyAll(fs);
Promise.promisifyAll(childProcess);

var OGRCMD = 'ogr2ogr -s_srs "EPSG:4326" -t_srs "EPSG:4326" -f "ESRI Shapefile" ';

function promisifyStreamEnd(stream) {
  return new Promise(function (resolve, reject) {
    stream.on('error', reject);
    stream.on('end', resolve);
    stream.on('finish', resolve);
  });
}

exports.convert = function convert(sourceFiles, outdir, layerNames) {
  var zip = archiver('zip');

  // Make the temporary directory
  fs.mkdirAsync(outdir)
  .then(function () {
    return sourceFiles;
  }).map(function (input, i) {
    // Run the ogr2ogr command
    // TODO: upgrade the geometries to Multi-geometries, so we don't run into
    // conflicts between Polygon nand MultiPolygon, for example.
    var cmd = OGRCMD + outdir + '/' + layerNames[i] + '.shp ' + input;
    console.log('running command: ' + cmd);
    return childProcess.execAsync(cmd, {
      env: process.env
    });
  }, {
    concurrency: 2
  }).map(function (pipes) {
    // TODO: improve logging
    console.log(pipes[0]);
    console.log(pipes[1]);
  }).then(function () {
    // Add files to the zip stream.
    zip.bulk([{
      expand: true,
      cwd: outdir,
      src: '*'
    }]);
  }).then(function () {
    var promise = promisifyStreamEnd(zip);
    zip.finalize();
    return promise;
  }).then(function () {
    // Remove temporary directory
    return Promise.resolve(qfs.removeTree(outdir));
  }).catch(function (error) {
    zip.emit('error', error);
  });

  return zip;
};
