'use strict';
const log = require('./log');
const loader = require('taskcluster-lib-loader');
const _ = require('lodash');
const {initCloudWatchEventListener} = require('./cloud-watch-event-listener');
const aws = require('aws-sdk');
const monitor = require('taskcluster-lib-monitor');
const config =require('typed-env-config');
const setup = require('./cloud-watch-ingest');
const PG = require('pg');
const Pool = require('pg-pool');
const {State} = require('./state');
const url = require('url');
const assert = require('assert');

const types = PG.types;

// Since Postgres returns 64-bit ints from certain operations,
// we want to parse them down to a JS number.  Yes, we're loosing
// precision here, but the chances of us having any integer that's
// greater than ~53-bits here tiny
types.setTypeParser(20, x => parseInt(x))

//PG.defaults.ssl = true;

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

  postgres: {
    requires: ['cfg'],
    setup: async ({cfg}) => {
      const dburl = cfg.postgres.databaseUrl;
      assert(dburl);
      let parsedUrl = url.parse(dburl);
      let [user, password] = parsedUrl.auth.split([':']);

      let client = PG.native ? PG.native.Client : PG.Client;

      let pool = new Pool({
        user,
        password,
        database: parsedUrl.pathname.replace(/^\//, ''),
        port: Number.parseInt(parsedUrl.port, 10),
        host: parsedUrl.hostname,
        //ssl: true,
        application_name: 'ec2_manager',
        max: cfg.postgres.maxClients || 20,
        min: cfg.postgres.minClients || 4,
        idleTimeoutMillis: cfg.postgres.idleTimeoutMillis || 1000,
        Client: client,
      });

      // Per readme, this is a rare occurence but should be at least logged.
      // Basically, this error event is emitted when a client emits an error
      // while not claimed by any process.
      pool.on('error', (err, client) => {
        console.log(err.stack || err);
      });

      return pool;
    },
  },

  state: {
    requires: ['postgres', 'monitor'],
    setup: async ({postgres, monitor}) => {
      return new State({pgpool: postgres, monitor});
    }
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

