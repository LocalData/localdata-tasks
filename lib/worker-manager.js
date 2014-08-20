'use strict';

var events = require('events');
var fork = require('child_process').fork;

var manager = module.exports;

var jobs = [];
var workers = [];

var watcher = new events.EventEmitter();

manager.registerJob = function registerJob(name, command, args, env) {
  jobs.push({
    name: name,
    command: command,
    args: args,
    env: env
  });
};

manager.startWorker = function startWorker(queues) {
  var child = fork('./lib/worker.js', [JSON.stringify({
    jobs: jobs,
    queues: queues
  })]);

  var id = workers.length;
  workers[id] = child;

  // TODO: differentiate between expected and unexpected shutdown of workers
  function handleError(error) {
    child.removeAllListeners('exit');
    workers[id] = undefined;
  }

  function handleExit(code, signal) {
    child.removeAllListeners('error');
    workers[id] = undefined;
    watcher.emit('worker_exit');
  }

  child.on('error', handleError);
  child.on('exit', handleExit);
};

manager.processQueueLists = function processQueueLists(queueLists) {
  queueLists.forEach(function (queues) {
    manager.startWorker(queues);
  });
};

manager.shutdown = function shutdown() {
  // Make sure workers have shut down before we quit.
  watcher.on('exit', function () {
    var empty = true;
    var i;
    for (i = 0; i < workers.length; i += 1) {
      if (workers[i]) {
        empty = false;
      }
    }

    if (empty) {
      process.exit();
    }
  });

  var i;
  var worker;
  for (i = 0; i < workers.length; i += 1) {
    worker = workers[i];
    if (worker) {
      console.log('event=kill_worker worker=' + i);
      worker.kill('SIGTERM');
    }
  }
};

