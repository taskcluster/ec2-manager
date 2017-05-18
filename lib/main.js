const log = require('./log');
const loader = require('taskcluster-lib-loader');
const _ = require('lodash');
const {initCloudWatchEventListener} = require('./cloud-watch-event-listener');
const aws = require('aws-sdk');
const monitor = require('taskcluster-lib-monitor');
const config =require('typed-env-config');
const setup = require('./cloud-watch-ingest');

let load = loader({
  cfg: {
    requires: ['profile'],
    setup: async ({profile}) => config({profile}),
  },

  monitor: {
    requires: ['process', 'profile', 'cfg'],
    setup: async ({process, profile, cfg}) => monitor({
      project: cfg.monitor.project,
      credentials: cfg.taskcluster.credentials,
      mock: cfg.monitor.mock,
      process,
    }),
  },

  ec2: {
    requires: ['cfg', 'monitor'],
    setup: async ({cfg, monitor}) => {
      let ec2 = {};
      for (let region of cfg.app.regions) {
        ec2[region] = new aws.EC2(_.defaults({}, {region}, cfg.ec2, cfg.aws));
        monitor.patchAWS(ec2[region]);
      }
      return ec2;
    }
  },

  sqs: {
    requires: ['cfg', 'monitor'],
    setup: async ({cfg, monitor}) => {
      let sqs = {};
      for (let region of cfg.app.regions) {
        sqs[region] = new aws.SQS(_.defaults({}, {region}, cfg.sqs, cfg.aws));
        monitor.patchAWS(sqs[region]);
      }
      return sqs;
    }
  },

  cwe: {
    requires: ['cfg', 'monitor'],
    setup: async ({cfg, monitor}) => {
      let cwe = {};
      for (let region of cfg.app.regions) {
        cwe[region] = new aws.CloudWatchEvents(_.defaults({}, {region}, cfg.cwe, cfg.aws));
        monitor.patchAWS(cwe[region]);
      }
      return cwe;
    }
  },

  eventlistener: {
    requires: ['cfg', 'sqs', 'profile', 'monitor'],
    setup: async ({cfg, sqs, profile, monitor}) => {
      let listeners = [];

      for (let region of cfg.app.regions) {
        let listener = await initCloudWatchEventListener({
          sqs: sqs[region],
          region,
          queueName: `${cfg.app.queueName}-${profile}`,
          monitor: monitor.prefix(`listener.${region}`)
        });

        listener.start();
        listeners.push(listener);
      }

      return listeners;
    }
  },

  setup: {
    requires: ['cfg', 'sqs', 'cwe', 'profile'],
    setup: async ({cfg, sqs, cwe, profile}) => {
      await setup({
        sqs, cwe,
        regions: cfg.app.regions,
        queueName: `${cfg.app.queueName}-${profile}`,
        ruleName: `${cfg.app.ruleName}-${profile}`,
      });
    }
  },

}, ['process', 'profile']);

if (!module.parent) {
  require('source-map-support').install;
  load(process.argv[2], {
    profile: process.env.NODE_ENV || 'development',
    process: process.argv[2],
  }).catch(err => {
    console.log(err.stack || err);
    process.exit(1);
  });
}

module.exports = load;

