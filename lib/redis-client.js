'use strict';

var redis = require('redis');

var config = require('./config');

var client = module.exports = redis.createClient(config.redisPort, config.redisHost, {
  auth_pass: config.redisPassword
});

client.on('connect', function () {
  console.log('Connected to redis: ' + config.redisHost + ':' + config.redisPort);
});

client.on('error', function (error) {
  console.log('Redis error', error);
});
