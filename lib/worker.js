'use strict';

var util = require('util');

var nr = require('node-resque');

var redisClient = require('./redis-client');
var runJob = require('./run-job');

var options = JSON.parse(process.argv[2]);
var queues = options.queues;
var jobs = options.jobs;

var jobTypes = {};
jobs.forEach(function (job) {
  jobTypes[job.name] = {
    perform: function (options, done) {
      console.log(util.format('event=run_job name=%s args=%s options=%s', job.name, JSON.stringify(job.args), JSON.stringify(options)));
      runJob(job.command, job.args, options, job.env, done);
    }
  };
});

var worker = new nr.worker({
  connection: {
    redis: redisClient,
    timeout: 1000
  },
  queues: queues,
  timeout: 500
}, jobTypes, function () {
  worker.start();
});

function shutdown() {
  console.log('event=worker_stopping');
  worker.end(function () {
    process.exit();
  });
}

worker.on('start', function () {
  console.log(util.format('event=worker_started queues=%s jobs=%s', queues, jobs));
});

worker.on('job', function (queue, job) {
  process.stdout.write('\n');
  console.log('event=job_starting worker=' + worker.name +' queue=' + queue + ' job_class=' + job.class);
});

worker.on('error', function (queue, job, error) {
  console.log(error);
  console.log(error.stack);
  console.log('event=job_failed worker=' + worker.name +' queue=' + queue + ' job_class=' + job.class);
});

worker.on('success', function (queue, job, result) {
  console.log('event=job_complete worker=' + worker.name +' queue=' + queue + ' job_class=' + job.class + ' result=' + result);
});

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
