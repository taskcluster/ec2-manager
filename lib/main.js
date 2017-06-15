'use strict';
// Standard modules
const url = require('url');
const assert = require('assert');

// Yarn modules
const _ = require('lodash');
const aws = require('aws-sdk');
const PG = require('pg');
const Pool = require('pg-pool');

// TC modules
const loader = require('taskcluster-lib-loader');
const monitor = require('taskcluster-lib-monitor');
const config = require('typed-env-config');
const App = require('taskcluster-lib-app');
const Validator = require('taskcluster-lib-validate');
const Iterate = require('taskcluster-lib-iterate');

// Local modules
const log = require('./log');
const {initCloudWatchEventListener} = require('./cloud-watch-event-listener');
const {initDeadCloudWatchEventListener} = require('./cloud-watch-event-listener');
const {setupCloudWatchEvents} = require('./setup-cloud-watch');
const {State} = require('./state');
const {api} = require('./api');
const {pollSpotRequests} = require('./spot-request-poll');
const {runAWSRequest} = require('./aws-request');
const {LaunchSpecificationChecker} = require('./launch-specification-checker');

// Since Postgres returns 64-bit ints from certain operations,
// we want to parse them down to a JS number.  Yes, we're loosing
// precision here, but the chances of us having any integer that's
// greater than ~53-bits here tiny
PG.types.setTypeParser(20, x => parseInt(x))

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

  validator: {
    requires: ['cfg'],
    setup: async ({cfg}) => {
      return await Validator({
        prefix: 'ec2-manager/v1/',
        aws: cfg.aws,
      });
    },
  },

  runaws: {
    requires: [],
    setup: async () => {
      return runAWSRequest;
    }
  },

  lsChecker: {
    requires: ['cfg', 'ec2', 'runaws'],
    setup: async ({cfg, ec2, runaws}) => {
      return new LaunchSpecificationChecker({ec2, runaws, regions: cfg.app.regions});
    },
  },

  api: {
    requires: ['cfg', 'validator', 'monitor', 'state', 'sqs', 'ec2', 'profile', 'lsChecker', 'runaws'],
    setup: async ({cfg, validator, monitor, state, sqs, ec2, profile, lsChecker, runaws}) => {
      let router = await api.setup({
        context: {
          keyPrefix: cfg.app.keyPrefix,
          instancePubKey: cfg.app.instancePubKey,
          state: state,
          regions: cfg.app.regions,
          apiBaseUrl: cfg.server.baseUrl,
          queueName: `${cfg.app.queueName}-${profile}`,
          sqs: sqs,
          ec2: ec2,
          runaws: runaws,
          lsChecker: lsChecker,
        },
        validator: validator,
        authBaseUrl: cfg.taskcluster.authBaseUrl,
        publish: cfg.app.publishMetaData,
        baseUrl: cfg.server.publicUrl + '/v1',
        referencePrefix: 'aws-provisioner/v1/api.json',
        aws: cfg.aws,
        monitor: monitor.prefix('api'),
      });

      return router;
    },
  },

  server: {
    requires: ['cfg', 'api'],
    setup: ({cfg, api}) => {
      let app = App(cfg.server);
      app.use('/v1', api);
      return app.createServer();
    },
  },

  postgres: {
    requires: ['cfg'],
    setup: async ({cfg}) => {
      const dburl = cfg.postgres.databaseUrl;
      assert(dburl, 'Must have a DATABASE_URL value');
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
        idleTimeoutMillis: cfg.postgres.idleTimeoutMillis,
        maxWaitingClients: 50,
        Client: client,
      });

      // Per readme, this is a rare occurence but should be at least logged.
      // Basically, this error event is emitted when a client emits an error
      // while not claimed by any process.
      pool.on('error', (err, client) => {
        log.error({err}, 'Postgres Pool error');
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

  eventlisteners: {
    requires: ['cfg', 'sqs', 'ec2', 'profile', 'monitor', 'state'],
    setup: async ({cfg, sqs, ec2, profile, monitor, state}) => {
      let listeners = await Promise.all(cfg.app.regions.map(async region => {
        let listener = await initCloudWatchEventListener({
          state,
          sqs: sqs[region],
          ec2: ec2[region],
          region,
          keyPrefix: cfg.app.keyPrefix,
          queueName: `${cfg.app.queueName}-${profile}`,
          monitor: monitor.prefix(`listener.${region}`),
        });
        return listener;
      }));
      return listeners;
    }
  },

  deadeventlisteners: {
    requires: ['cfg', 'sqs', 'profile', 'monitor', 'state'],
    setup: async ({cfg, sqs, ec2, profile, monitor, state}) => {
      let listeners = await Promise.all(cfg.app.regions.map(async region => {
        let listener = await initDeadCloudWatchEventListener({
          sqs: sqs[region],
          region,
          queueName: `${cfg.app.queueName}-${profile}_dead`,
          monitor: monitor.prefix(`listener.${region}.dead`)
        });
        return listener;
      }));
      return listeners;
    },
  },

  spotpollers: {
    requires: ['cfg', 'state', 'ec2', 'monitor'],
    setup: async ({cfg, state, ec2, monitor}) => {
      return cfg.app.regions.map(region => {
        return new Iterate({
          maxIterationTime: 1000 * 60 * 15,
          watchDog: 1000 * 60 * 15,
          maxFailures: 1,
          waitTime: cfg.app.spotPollDelay,
          handler: async (watchdog) => {
            try {
              await pollSpotRequests({ec2: ec2[region], region, state});
            } catch (err) {
              monitor.reportError(err, 'Error polling spot request');
            }
          },
        });
      });
    }
  },

  setup: {
    requires: ['cfg', 'sqs', 'cwe', 'profile', 'state'],
    setup: async ({cfg, sqs, cwe, profile, state}) => {
      await state._runScript('drop-db.sql');
      await state._runScript('create-db.sql');
      log.info('Database setup is complete');

      await setupCloudWatchEvents({
        sqs, cwe,
        regions: cfg.app.regions,
        queueName: `${cfg.app.queueName}-${profile}`,
        ruleName: `${cfg.app.ruleName}-${profile}`,
      });
      log.info('CloudWatch Events and SQS setup is complete');
    }
  },

  start: {
    requires: ['eventlisteners', 'deadeventlisteners', 'server', 'spotpollers'],
    setup: async ({eventlisteners, deadeventlisteners, server, spotpollers}) => {
      eventlisteners.map(x => x.start());
      deadeventlisteners.map(x => x.start());
      spotpollers.map(x => x.start());
      log.info('Started');
    },
  },

}, ['process', 'profile']);

if (!module.parent) {
  process.on('unhandledRejection', err => {
    log.fatal({err}, 'Unhanled Promise Rejection!');
    throw err;
  });
  require('source-map-support').install();
  load(process.argv[2], {
    profile: process.env.NODE_ENV || 'development',
    process: process.argv[2],
  }).catch(err => {
    log.error({err, module: process.argv[2]}, 'Error loading module');
    process.exit(1);
  });
}

module.exports = load;

