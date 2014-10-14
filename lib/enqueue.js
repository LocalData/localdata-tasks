'use strict';

/*
 * node enqueue.js QUEUE_NAME JOB_TYPE JSON_DATA
 * node enqueue.js math add '[1, 2]'
 */

var util = require('util');

var nr = require('node-resque');
var Promise = require('bluebird');

var redisClient = require('./redis-client');

Promise.promisifyAll(nr);

var connected = new Promise(function (resolve, reject) {
  redisClient.on('connect', resolve);
  redisClient.on('error', reject);
});

var queue = process.argv[2];
var jobType = process.argv[3];
var data = JSON.parse(process.argv[4]);

new Promise(function (resolve, reject) {
  var resque = new nr.queue({
    connection: {
      redis: redisClient
    }
  }, function (error) {
    if (error) { return reject(error); }
    resolve(resque);
  });
}).bind({}).then(function (resque) {
  this.resque = resque;
  return connected;
}).then(function () {
  return this.resque.enqueueAsync(queue, jobType, [data]);
}).then(function () {
  return this.resque.lengthAsync(queue);
}).then(function (len) {
  console.log(util.format('event=job_added queue=%s length=%s type=%s data=%s', queue, len, jobType, JSON.stringify(data)));
  return this.resque.endAsync();
}).catch(function (error) {
  console.log(error);
  process.exit();
});


