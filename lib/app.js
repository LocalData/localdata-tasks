'use strict';

var config = require('./config');
var manager = require('./worker-manager');

manager.registerJob('csv-exporter', './lib/children/basic-exporter/index.js', ['--mode', 'csv'], config.csvExportEnv);
manager.registerJob('shapefile-exporter', './lib/children/basic-exporter/index.js', ['--mode', 'shapefile'], config.csvExportEnv);

manager.processQueueLists(config.queues);

process.on('SIGINT', manager.shutdown);
process.on('SIGTERM', manager.shutdown);
