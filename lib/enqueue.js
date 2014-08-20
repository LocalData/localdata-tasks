'use strict';

/*
 * node enqueue.js QUEUE_NAME JOB_TYPE JSON_DATA
 * node enqueue.js math add '[1, 2]'
 */

var util = require('util');

var nr = require('node-resque');

var redisClient = require('./redis-client');

var resque = new nr.queue({
  connection: {
    redis: redisClient
  }
}, function (error) {
  if (error) {
    console.log(error);
    process.exit();
  }
  var queue = process.argv[2];
  var jobType = process.argv[3];
  var data = JSON.parse(process.argv[4]);


  redisClient.on('connect', function () {
    resque.enqueue(queue, jobType, [data], function (error) {
      if (error) {
        console.log(error);
        process.exit();
      }
      resque.length(queue, function (error, len) {
        if (error) {
          console.log(error);
          process.exit();
        }
        console.log(util.format('event=job_added queue=%s length=%s type=%s data=%s', queue, len, jobType, JSON.stringify(data)));
        resque.end(function () {});
      });
    });
  });
});


