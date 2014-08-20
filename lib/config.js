'use strict';

var url = require('url');

var config = module.exports;

var redisURL = url.parse(process.env.REDISCLOUD_URL);
config.redisHost = redisURL.hostname;
config.redisPort = redisURL.port;
config.redisPassword = undefined;
if (redisURL.auth) {
  config.redisPassword = redisURL.auth.split(':')[1];
}

config.queues = JSON.parse(process.env.QUEUES);

config.csvExportEnv = JSON.parse(process.env.CSV_EXPORT_ENV);
