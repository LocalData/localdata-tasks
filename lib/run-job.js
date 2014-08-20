'use strict';

var fork = require('child_process').fork;

module.exports = function runJob(command, args, options, env, done) {
  var result;

  // TODO: create a temporary directory and use that as the child process's CWD
  // TODO: create a unique job ID
  args = args.concat([JSON.stringify(options)]);
  var child = fork(command, args, {
    env: env
  });

  function handleError(error) {
    child.removeAllListeners('exit');
    done(error);
  }

  function handleExit(code, signal) {
    child.removeAllListeners('error');
    done(null, result);
  }

  child.on('error', handleError);
  child.on('exit', handleExit);
  child.on('message', function (message) {
    if (message.type === 'result') {
      result = message.data;
    }
  });

  // TODO: read a maximum job run time from the config and kill jobs that run
  // for too long.
};
