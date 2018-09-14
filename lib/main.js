// This lets us have useful aws-sdk stack traces
Error.stackTraceLimit = 30;

// Standard modules
const url = require('url');
const querystring = require('querystring');
const assert = require('assert');

// Yarn modules
const aws = require('aws-sdk');
const PG = require('pg');
const Pool = require('pg-pool');

// TC modules
const loader = require('taskcluster-lib-loader');
const monitor = require('taskcluster-lib-monitor');
const config = require('typed-env-config');
const App = require('taskcluster-lib-app');
const SchemaSet = require('taskcluster-lib-validate');
const Iterate = require('taskcluster-lib-iterate');

// Local modules
const log = require('./log');
const {initCloudWatchEventListener} = require('./cloud-watch-event-listener');
const {initDeadCloudWatchEventListener} = require('./cloud-watch-event-listener');
const {setupCloudWatchEvents} = require('./setup-cloud-watch');
const {State} = require('./state');
const {builder} = require('./api');
const {HouseKeeper} = require('./housekeeping');
const {runAWSRequest} = require('./aws-request');
const {LaunchSpecificationChecker} = require('./launch-specification-checker');
const {PricingPoller} = require('./pricing-poller');
const {ResourceTagger} = require('./resource-tagger');
const {TerminationPoller} = require('./termination-poller');

// Since Postgres returns 64-bit ints from certain operations,
// we want to parse them down to a JS number.  Yes, we're loosing
// precision here, but the chances of us having any integer that's
// greater than ~53-bits here tiny
PG.types.setTypeParser(20, x => parseInt(x, 10));

// We want to parse dates back to date objects
PG.types.setTypeParser(1184, x => {
  return new Date(x);
});

let load = loader({
  cfg: {
    requires: ['profile'],
    setup: async ({profile}) => config({profile}),
  },

  monitor: {
    requires: ['process', 'profile', 'cfg'],
    setup: async ({process, profile, cfg}) => monitor({
      projectName: cfg.monitoring.project || 'ec2-manager',
      enable: cfg.monitoring.enable,
      credentials: cfg.taskcluster.credentials,
      rootUrl: cfg.taskcluster.rootUrl,
      process,
    }),
  },

  schemaset: {
    requires: ['cfg'],
    setup: ({cfg}) => new SchemaSet({
      serviceName: 'ec2-manager',
      publish: cfg.app.publishMetaData,
      aws: cfg.aws,
    }),
  },

  runaws: {
    requires: ['state'],
    setup: async ({state}) => {
      return function(service, method, body, maxDuration) {
        return runAWSRequest(service, method, body, state, maxDuration);
      };
    },
  },

  lsChecker: {
    requires: ['cfg', 'ec2', 'runaws'],
    setup: async ({cfg, ec2, runaws}) => {
      return new LaunchSpecificationChecker({ec2, runaws, regions: cfg.app.regions});
    },
  },

  api: {
    requires: [
      'cfg',
      'schemaset',
      'monitor',
      'state',
      'sqs',
      'ec2',
      'profile',
      'lsChecker',
      'runaws',
      'pricing',
      'tagger',
    ],
    setup: async ({cfg, schemaset, monitor, state, sqs, ec2, profile, lsChecker, runaws, pricing, tagger}) => {
      let router = await builder.build({
        rootUrl: cfg.taskcluster.rootUrl,
        context: {
          state: state,
          regions: cfg.app.regions,
          queueName: `${cfg.app.queueName}-${profile}`,
          sqs: sqs,
          ec2: ec2,
          runaws: runaws,
          lsChecker: lsChecker,
          pricing,
          tagger,
          monitor,
        },
        schemaset: schemaset,
        publish: cfg.app.publishMetadata,
        aws: cfg.aws,
        monitor: monitor.prefix('api'),
      });

      return router;
    },
  },

  server: {
    requires: ['cfg', 'api'],
    setup: ({cfg, api}) => App({
      port: cfg.server.port,
      env: cfg.server.env,
      forceSSL: cfg.server.forceSSL,
      trustProxy: cfg.server.trustProxy,
      apis: [api],
    }),
  },

  postgres: {
    requires: ['cfg'],
    setup: async ({cfg}) => {
      const dburl = cfg.postgres.databaseUrl;
      assert(dburl, 'Must have a DATABASE_URL value');
      let parsedUrl = url.parse(dburl);
      let [user, password] = parsedUrl.auth.split([':']);

      let client = PG.native ? PG.native.Client : PG.Client;

      if (process.env.PGSSLMODE) {
        switch (process.env.PGSSLMODE) {
          case 'prefer':
          case 'require':
          case 'verify-ca':
          case 'verify-full':
            client = PG.Client;
            log.info({sslmode: process.env.PGSSLMODE}, 'disabling pg-native because of ssl mode');
            break;
          default:
            break;
        }
      }

      if (process.env.FORCE_JS_PG_CLIENT) {
        client = PG.Client;
      }

      let poolcfg = {
        user,
        password,
        database: parsedUrl.pathname.replace(/^\//, ''),
        port: Number.parseInt(parsedUrl.port, 10),
        host: parsedUrl.hostname,
        application_name: 'ec2_manager',
        max: cfg.postgres.maxClients || 5,
        min: cfg.postgres.minClients || 1,
        idleTimeoutMillis: cfg.postgres.idleTimeoutMillis,
        maxWaitingClients: 50,
        Client: client,
      };

      let pool = new Pool(poolcfg);

      // Per readme, this is a rare occurence but should be at least logged.
      // Basically, this error event is emitted when a client emits an error
      // while not claimed by any process.
      pool.on('error', (err, client) => {
        log.error({err}, 'Postgres Pool error');
      });

      // As a best effort, if we're full, we'll start printing the last query
      // executed by each active client
      setInterval(function() {
        try {
          if (pool.waitingCount > 1) {
            currentQueries = pool._clients.map(client => {
              return (client._activeQuery || {}).text;
            });
            log.info({currentQueries, waiting: pool.waitingCount}, 'waiting on queries');
          }
        } catch (err) {

        }
      }, 10000);

      return pool;
    },
  },

  state: {
    requires: ['postgres', 'monitor'],
    setup: async ({postgres, monitor}) => {
      return new State({pgpool: postgres, monitor});
    },
  },

  ec2: {
    requires: ['cfg', 'monitor'],
    setup: async ({cfg, monitor}) => {
      let ec2 = {};
      for (let region of cfg.app.regions) {
        let ec2Opt = Object.assign({}, {region}, cfg.ec2, cfg.aws);
        ec2[region] = new aws.EC2(Object.assign({}, cfg.aws, cfg.ec2, {region}));
        monitor.patchAWS(ec2[region]);
      }
      return ec2;
    },
  },

  sqs: {
    requires: ['cfg', 'monitor'],
    setup: async ({cfg, monitor}) => {
      let sqs = {};
      for (let region of cfg.app.regions) {
        sqs[region] = new aws.SQS(Object.assign({}, cfg.aws, cfg.sqs, {region}));
        monitor.patchAWS(sqs[region]);
      }
      return sqs;
    },
  },

  cwe: {
    requires: ['cfg', 'monitor'],
    setup: async ({cfg, monitor}) => {
      let cwe = {};
      for (let region of cfg.app.regions) {
        cwe[region] = new aws.CloudWatchEvents(Object.assign({}, cfg.aws, cfg.sqs, {region}));
        monitor.patchAWS(cwe[region]);
      }
      return cwe;
    },
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
          queueName: `${cfg.app.queueName}-${profile}`,
          monitor: monitor.prefix('listener'),
        });
        return listener;
      }));
      return listeners;
    },
  },

  deadeventlisteners: {
    requires: ['cfg', 'sqs', 'profile', 'monitor', 'state'],
    setup: async ({cfg, sqs, ec2, profile, monitor, state}) => {
      let listeners = await Promise.all(cfg.app.regions.map(async region => {
        let listener = await initDeadCloudWatchEventListener({
          sqs: sqs[region],
          region,
          queueName: `${cfg.app.queueName}-${profile}_dead`,
          monitor: monitor.prefix('listener-dead'),
        });
        return listener;
      }));
      return listeners;
    },
  },

  pricing: {
    requires: ['cfg', 'ec2', 'runaws', 'monitor'],
    setup: async ({cfg, ec2, runaws, monitor}) => {
      return new PricingPoller({
        ec2,
        regions: cfg.app.regions,
        runaws,
        timePeriod: cfg.app.pricingTimePeriod,
        pollDelay: cfg.app.pricingPollDelay,
        monitor,
      });
    },
  },

  tagger: {
    requires: ['cfg', 'runaws', 'ec2'],
    setup: async ({cfg, runaws, ec2}) => {
      return new ResourceTagger({
        runaws,
        ec2,
        managerId: cfg.app.id,
      });
    },
  },    

  housekeeper: {
    requires: ['cfg', 'state', 'ec2', 'monitor', 'runaws'],
    setup: async ({cfg, state, ec2, monitor, runaws}) => {
      let houseKeeper = new HouseKeeper({
        ec2,
        state,
        regions: cfg.app.regions,
        maxHoursLife: cfg.app.maxInstanceLifeHours,
        monitor,
        managerId: cfg.app.id,
        runaws,
      });
      return new Iterate({
        maxIterationTime: 1000 * 60 * 15,
        watchDog: 1000 * 60 * 15,
        maxFailures: 10,
        waitTime: cfg.app.houseKeepingPollDelay,
        handler: async (watchdog) => {
          try {
            await houseKeeper.sweep();
          } catch (err) {
            console.dir(err);
            monitor.reportError(err, 'Error running house keeping');
          }
        },
      });
    },
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
    },
  },

  apiClient: {
    requires: ['cfg'], 
    setup: async ({cfg}) => {
      let http = require('http');
      let taskcluster = require('taskcluster-client');
      let baseUrl = cfg.server.baseUrl;
      let reference = builder.reference({baseUrl});
      let ClientClass = taskcluster.createClient(reference);
      return new ClientClass({
        agent: http.globalAgent,
        baseUrl,
        credentials: cfg.taskcluster.credentials,
      });

    },
  },

  purgequeues: {
    requires: ['server', 'apiClient'],
    setup: async ({server, apiClient}) => {
      await apiClient.purgeQueues();
      console.log('Done purging queues');
      process.exit(0);
    },
  },

  terminationpoller: {
    requires: ['ec2', 'cfg', 'runaws', 'state', 'monitor'],
    setup: async ({ec2, cfg, runaws, state, monitor}) => {
      return new TerminationPoller({
        ec2,
        regions: cfg.app.regions,
        runaws,
        state,
        monitor,
      });
    },
  },

  workers: {
    requires: ['eventlisteners', 'deadeventlisteners', 'housekeeper', 'pricing', 'terminationpoller'],
    setup: async ({eventlisteners, deadeventlisteners, housekeeper, pricing, terminationpoller}) => {
      eventlisteners.map(x => x.start());
      deadeventlisteners.map(x => x.start());
      housekeeper.start();
      terminationpoller.start();
      pricing.start();
      log.info('Started');
    },
  },

  start: {
    requires: ['eventlisteners', 'deadeventlisteners', 'server', 'housekeeper', 'pricing', 'terminationpoller'],
    setup: async ({eventlisteners, deadeventlisteners, server, housekeeper, pricing, terminationpoller}) => {
      eventlisteners.map(x => x.start());
      deadeventlisteners.map(x => x.start());
      housekeeper.start();
      terminationpoller.start();
      pricing.start();
      log.info('Started');
    },
  },

}, ['process', 'profile']);

if (!module.parent) {
  process.on('unhandledRejection', err => {
    log.fatal({err}, 'Unhandled Promise Rejection!');
    throw err;
  });
  load(process.argv[2], {
    profile: process.env.NODE_ENV || 'development',
    process: process.argv[2],
  }).catch(err => {
    console.dir(err);
    log.error({err, module: process.argv[2]}, 'Error loading module');
    process.exit(1);
  });
}

module.exports = load;

