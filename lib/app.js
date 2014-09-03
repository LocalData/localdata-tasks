'use strict';

var config = require('./config');
var manager = require('./worker-manager');

manager.registerJob({
  name: 'csv-exporter',
  command: './lib/children/basic-exporter/index.js',
  args: ['--mode', 'csv'],
  env: config.csvExportEnv
});

manager.registerJob({
  name: 'shapefile-exporter',
  command: './lib/children/basic-exporter/index.js',
  args: ['--mode', 'shapefile'],
  env: config.csvExportEnv,
  inheritEnv: true
});

manager.processQueueLists(config.queues);

process.on('SIGINT', manager.shutdown);
process.on('SIGTERM', manager.shutdown);
