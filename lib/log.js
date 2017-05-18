let log = require('taskcluster-lib-log');

let env = process.env.NODE_ENV || 'development';

module.exports = log('ec2-manager-' + env.trim());
