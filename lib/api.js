
const APIBuilder = require('taskcluster-lib-api');
const assert = require('assert');
const crypto = require('crypto');

const {getQueueStats, getQueueUrl, purgeQueue} = require('sqs-simple');

const log = require('./log');

let builder = new APIBuilder({
  title: 'EC2 Instance Manager',
  description: [
    'A taskcluster service which manages EC2 instances.  This service does not understand',
    'any taskcluster concepts intrinsicaly other than using the name `workerType` to',
    'refer to a group of associated instances.  Unless you are working',
    'on building a provisioner for AWS, you almost certainly do not want to use this service',
  ].join(' '),
  serviceName: 'ec2-manager',
  version: 'v1',
  context: [
    'state',
    'regions',
    'queueName',
    'sqs',
    'ec2',
    'lsChecker',
    'runaws',
    'pricing',
    'tagger',
    'monitor',
  ],
});

/**
 * List the workertypes which are known to this ec2-manager to have pending or
 * running capacity
 */
builder.declare({
  method: 'get',
  route: '/worker-types',
  name: 'listWorkerTypes',
  title: 'See the list of worker types which are known to be managed',
  stability: APIBuilder.stability.experimental,
  output: 'list-worker-types.yml',
  description: 'This method is only for debugging the ec2-manager',
}, async function(req, res) {
  let result = await this.state.listWorkerTypes();
  return res.reply(result);
});

/**
 * Run an EC2 instance
 */
builder.declare({
  method: 'put',
  route: '/worker-types/:workerType/instance',
  name: 'runInstance',
  title: 'Run an instance',
  stability: APIBuilder.stability.experimental,
  input: 'run-instance-request.yml',
  scopes: 'ec2-manager:manage-resources:<workerType>',
  description: [
    'Request an instance of a worker type',
  ].join(' '),
}, async function(req, res) {
  try {
    let workerType = req.params.workerType;
    let {
      ClientToken,
      Region,
      SpotPrice,
      RequestType,
      LaunchInfo,
    } = req.body;

    let rLog = log.child({
      region: Region,
      requestType: RequestType,
      workerType,
      instanceType: LaunchInfo.InstanceType,
    });

    if (RequestType === 'spot') {
      LaunchInfo.InstanceMarketOptions = {
        MarketType: 'spot',
        SpotOptions: {
          SpotInstanceType: 'one-time',
        },
      };

      // using typeof to account for a 0.0 value... I guess no one would use a
      // price of 0, but I'd rather handle that in a safe way (insta-rejected)
      // than an expensive way (spending on-demand)
      if (typeof SpotPrice === 'number') {
        LaunchInfo.InstanceMarketOptions.SpotOptions.MaxPrice = SpotPrice.toString(10);
      }
    }

    // TODO: LS-Checker should have a once-over to double check it's doing the right thing here
    let valid = await this.lsChecker.check({
      launchSpecification: LaunchInfo,
      region: Region,
    });

    if (!valid) {
      return res.reportError('InputError', 'LaunchInfo is invalid!');
    }

    // lsChecker has already ensured that these keys aren't set so no need to
    // duplicate the effort
    LaunchInfo.ClientToken = ClientToken;
    // Once we have a different solution for security tokens, we can make this
    // configurable
    LaunchInfo.MaxCount = 1;
    LaunchInfo.MinCount = 1;

    // We want to tag the instance and the volumes
    let tags = this.tagger.generateTags({workerType});
    LaunchInfo.TagSpecifications = [
      {ResourceType: 'instance', Tags: tags},
      {ResourceType: 'volume', Tags: tags},
    ];

    let telemetrify = (codeType, code) =>{
      // We want to track how long an instance lived for
      let groupings = [
        'overall',
        `overall-${codeType}`,
        `instance-type.${LaunchInfo.InstanceType}`,
        `worker-type.${workerType}`,
        `region.${Region}`,
      ];

      if (code) {
        groupings.push(`code-${codeType}.${Region}.${LaunchInfo.InstanceType}.${code}`);
      }
      
      for (grouping of groupings.map(x => `create-instance.${x}`)) {
        this.monitor.count(grouping);
      }
    };

    let result;
    try {
      result = await this.runaws(this.ec2[Region], 'runInstances', LaunchInfo);
      // TODO: Put a couple more useful fields here, maybe instance type and price.
      // This should be simple, just reaching into the launch spec.
      rLog.info('Requested instance');
      telemetrify('clean');
    } catch (err) {
      // https://docs.aws.amazon.com/AWSEC2/latest/APIBuilderReference/errors-overview.html
      rLog.warn({err, LaunchInfo}, 'Error requesting an instance');
      let errMsg = [
        `THIS IS A WORKER TYPE CONFIGURATION ISSUE OF ${workerType} OR `,
        `AN EC2 SERVICE DISRUPTION IN ${Region}/${LaunchInfo.InstanceType}!!!  `,
        'This report is part of the normal operation of EC2-Manager ',
        `and is not a bug: ${err.code}: ${err.message}`,
      ];
      this.monitor.reportError(new Error(errMsg.join('')), 'info', {
        workerType,
        instanceType: LaunchInfo.InstanceType,
        region: Region,
      });
      switch (err.code) {
        case 'InvalidParameter':
        case 'InvalidParameterCombination':
        case 'InvalidParameterValue':
        case 'UnknowParameter':
          telemetrify('invalid-input', err.code);
          return res.reportError('InputError', 'EC2 APIBuilder says this is bad input data');
        default:
          // default behaviour
          telemetrify('exceptional', err.code);
          throw err;
      }
    }

    assert(Array.isArray(result.Instances));
    assert(result.Instances.length === 1);

    let [instance] = result.Instances;
    
    let opts = {
      workerType,
      region: Region,
      az: instance.Placement.AvailabilityZone,
      id: instance.InstanceId,
      instanceType: instance.InstanceType,
      state: instance.State.Name,
      imageId: instance.ImageId,
      launched: new Date(instance.LaunchTime),
      lastEvent: new Date(),
    };

    try {
      await this.state.reportAmiUsage({
        region: Region,
        id: instance.InstanceId,
      });
    } catch (err) {
      rLog.warn({err}, 'Error while reporting AMI usage, ignoring');
    }

    await this.state.upsertInstance(opts); 

    rLog.debug(opts, 'finished inserting instance into database'); 
    res.status(204).end();
  } catch (err) {
    log.error({err}, 'runInstance');
    throw err;
  }
});

/**
 * Destroy all EC2 resources of a given worker type
 */
builder.declare({
  method: 'delete',
  route: '/worker-types/:workerType/resources',
  name: 'terminateWorkerType',
  title: 'Terminate all resources from a worker type',
  scopes: 'ec2-manager:manage-resources:<workerType>',
  stability: APIBuilder.stability.experimental,
  description: [
    'Terminate all instances for this worker type',
  ].join(' '),
}, async function(req, res) {
  let workerType = req.params.workerType;

  let ids = await this.state.listIdsOfWorkerType({workerType});

  await Promise.all(this.regions.map(async region => {
    let instanceIds = ids.instanceIds.filter(x => x.region === region).map(x => x.id);
    if (instanceIds.length > 0) {
      await this.runaws(this.ec2[region], 'terminateInstances', {
        InstanceIds: instanceIds,
      });
    }
    log.info({instanceIds, region}, 'Terminated resources in region');
  }));

  return res.status(204).end();
});

builder.declare({
  method: 'get',
  route: '/worker-types/:workerType/stats',
  name: 'workerTypeStats',
  title: 'Look up the resource stats for a workerType',
  stability: APIBuilder.stability.experimental,
  output: 'worker-type-resources.yml',
  description: [
    'Return an object which has a generic state description.', 
    'This only contains counts of instances',
  ].join(' '),
}, async function(req, res) {
  let workerType = req.params.workerType;
  let counts = await this.state.instanceCounts({workerType});
  return res.reply(counts);
});

builder.declare({
  method: 'get',
  route: '/worker-types/:workerType/health',
  name: 'workerTypeHealth',
  title: 'Look up the resource health for a workerType',
  stability: APIBuilder.stability.experimental,
  output: 'health.yml',
  description: [
    'Return a view of the health of a given worker type',
  ].join(' '),
}, async function(req, res) {
  let workerType = req.params.workerType;
  let health = await this.state.getHealth({workerType});
  return res.reply(health);
});

builder.declare({
  method: 'get',
  route: '/worker-types/:workerType/errors',
  name: 'workerTypeErrors',
  title: 'Look up the most recent errors of a workerType',
  stability: APIBuilder.stability.experimental,
  output: 'errors.yml',
  description: [
    'Return a list of the most recent errors encountered by a worker type',
  ].join(' '),
}, async function(req, res) {
  let workerType = req.params.workerType;
  let errors = await this.state.getRecentErrors({workerType});
  return res.reply({errors: errors.map(x => {
    x.time = x.time.toISOString();
    return x;
  })});
});

builder.declare({
  method: 'get',
  route: '/worker-types/:workerType/state',
  name: 'workerTypeState',
  title: 'Look up the resource state for a workerType',
  stability: APIBuilder.stability.experimental,
  output: 'worker-type-state.yml',
  description: [
    'Return state information for a given worker type',
  ].join(' '),
}, async function(req, res) {
  let workerType = req.params.workerType;
  let result = {
    instances: await this.state.listInstances({workerType}),
  };
  return res.reply(result);
});

builder.declare({
  method: 'get',
  route: '/key-pairs/:name',
  name: 'ensureKeyPair', 
  title: 'Ensure a KeyPair for a given worker type exists',
  stability: APIBuilder.stability.experimental,
  input: 'create-key-pair.yml',
  scopes: 'ec2-manager:manage-key-pairs:<name>',
  description: 'Idempotently ensure that a keypair of a given name exists',
}, async function(req, res) {
  let name = req.params.name;

  await Promise.all(this.regions.map(async region => {
    // TODO: We should just try to create the key and check for 
    // InvalidKeyPair.Duplicate
    let keyPairs = await this.runaws(this.ec2[region], 'describeKeyPairs', {
      Filters: [{
        Name: 'key-name',
        Values: [name],
      }],
    });
    if (keyPairs.KeyPairs.length === 0) {
      await this.runaws(this.ec2[region], 'importKeyPair', {
        KeyName: name,
        PublicKeyMaterial: req.body.pubkey,
      });
      log.info({name, region, pubkey: req.body.pubkey}, 'creating key');
    }
  }));

  return res.status(204).end();
});

/**
 * Delete a KeyPair
 */
builder.declare({
  method: 'delete',
  route: '/key-pairs/:name',
  name: 'removeKeyPair', 
  title: 'Ensure a KeyPair for a given worker type does not exist',
  stability: APIBuilder.stability.experimental,
  scopes: 'ec2-manager:manage-key-pairs:<name>',
  description: [
    'Ensure that a keypair of a given name does not exist.',
  ].join(' '),
}, async function(req, res) {
  let name = req.param.name;

  await Promise.all(this.regions.map(async region => {
    let keyPairs = await this.runaws(this.ec2[region], 'describeKeyPairs', {
      Filters: [{
        Name: 'key-name',
        Values: [name],
      }],
    });
    if (keyPairs.KeyPairs[0]) {
      await this.runaws(this.ec2[region], 'deleteKeyPair', {
        KeyName: name,
      });
    }
  }));

  return res.status(204).end();
});

// NOTE Idempotency is being enforced by the database for the import operation.
// I guess this is a problem because we could have a situation where the first
// call to this APIBuilder succeeds but the client doesn't get the response and so
// retries.  Then the second attempt would fail because it would get an error
// thrown by the postgres client.  This is a risk that is acceptable because a)
// this method is a transtional method only b) this type of failure shouldn't
// happen often and c) because this method doesn't actually spend money or
// errors from it cause management to incorrectly operat.

/**
 * Terminate a single instance
 */
builder.declare({
  method: 'delete',
  route: '/region/:region/instance/:instanceId',
  name: 'terminateInstance',
  title: 'Terminate an instance',
  stability: APIBuilder.stability.experimental,
  scopes: {AnyOf: [
    'ec2-manager:manage-instances:<region>:<instanceId>',
    {
      if: 'hasWorkerType',
      then: 'ec2-manager:manage-resources:<workerType>',
    },
  ]},
  description: [
    'Terminate an instance in a specified region',
  ].join(' '),
}, async function(req, res) {
  let region = req.params.region;
  let instanceId = req.params.instanceId;

  // We need to look up the worker type in the database, but since that's more
  // work that just using route parameters, we should first try the relatively
  // lower cost check for just the region/instanceId scope (which in practise
  // would be an ec2-manager[:manage-instances]:* scope) first.  If this fails,
  // we'll look up the instanceId and region in the database to see if we can
  // determine its worker type and use that for auth.  If we find no instances
  // in the database, we're just going to return undefined because this
  // instance is not managed, and should not be authorized.  If there's more
  // than one, the database is not providing the relational guarantees it
  // should
  try {
    await req.authorize({region, instanceId, hasWorkerType: false});
  } catch (err) {
    if (err.code === 'AuthorizationError') {
      throw err;
    }
    let workerTypes = await this.state.listInstances({id: instanceId, region});
    switch (workerTypes.length != 1) {
      case 0:
        return undefined;
      case 1:
        await req.authorize({
          region,
          instanceId,
          workerType: workerTypes[0],
          hasWorkerType: true,
        });
        break;
      default:
        throw new Error('Database schema guarantees aren\'t being enforced');
    }
  }

  if (!this.regions.includes(region)) {
    res.reportError('ResourceNotFound', 'Region is not configured', {});
  }
  assert(this.regions.includes(region));

  let result = await this.runaws(this.ec2[region], 'terminateInstances', {
    InstanceIds: [instanceId],
  });

  // I'm not sure if this response will always happen from the APIBuilder and it doesn't
  // really describe anything about it.  Since this is only being given for informational
  // purposes, I'm not too concered
  if (result.TerminatingInstances) {
    assert(Array.isArray(result.TerminatingInstances));
    assert(result.TerminatingInstances.length === 1);
    let x = result.TerminatingInstances[0];
    return res.reply({
      current: x.CurrentState.Name,
      previous: x.PreviousState.Name,
    });
  } else {
    return res.status(204).end();
  }

});

/**
 * Request prices
 */
builder.declare({
  method: 'get',
  route: '/prices',
  name: 'getPrices',
  title: 'Request prices for EC2',
  stability: APIBuilder.stability.experimental,
  output: 'prices.yml',
  description: [
    'Return a list of possible prices for EC2',
  ].join(' '),
}, async function(req, res) {
  let prices = await this.pricing.getPrices();
  res.reply(prices);
});

/**
 * Request prices
 */
builder.declare({
  // Too bad GET can't have a body or there wasn't a QUERY method or something
  method: 'post',
  route: '/prices',
  name: 'getSpecificPrices',
  title: 'Request prices for EC2',
  stability: APIBuilder.stability.experimental,
  input: 'prices-request.yml',
  output: 'prices.yml',
  description: [
    'Return a list of possible prices for EC2',
  ].join(' '),
}, async function(req, res) {
  let prices = await this.pricing.getPrices(req.body);
  res.reply(prices);
});

/**
 * Determine health of our EC2 Account
 */
builder.declare({
  method: 'get',
  route: '/health',
  name: 'getHealth',
  title: 'Get EC2 account health metrics',
  stability: APIBuilder.stability.experimental,
  output: 'health.yml',
  description: [
    'Give some basic stats on the health of our EC2 account',
  ].join(' '),
}, async function(req, res) {
  let health = await this.state.getHealth();
  return res.reply(health);
});

builder.declare({
  method: 'get',
  route: '/errors',
  name: 'getRecentErrors',
  title: 'Look up the most recent errors in the provisioner across all worker types',
  stability: APIBuilder.stability.experimental,
  output: 'errors.yml',
  description: [
    'Return a list of recent errors encountered',
  ].join(' '),
}, async function(req, res) {
  let errors = await this.state.getRecentErrors();
  return res.reply({errors: errors.map(x => {
    x.time = x.time.toISOString();
    return x;
  })});
});
/*****************************************************************************/
/*****************************************************************************/
/*    NOTE:  ALL FOLLOWING METHODS ARE INTERNAL ONLY AND ARE NOT             */
/*           INTENDED FOR GENERAL USAGE.  AS SUCH THEY ARE ALL               */
/*           CONSIDERED TO BE EXPERIMENTAL, DO NOT HAVE ANY                  */
/*           SCHEMA DESCRIPTIONS AND ARE INTENDED TO BE CHANGED              */
/*           WITHOUT ANY NOTICE.                                             */
/*****************************************************************************/
/*****************************************************************************/

/**
 * List managed regions
 */
builder.declare({
  method: 'get',
  route: '/internal/regions',
  name: 'regions',
  title: 'See the list of regions managed by this ec2-manager',
  stability: APIBuilder.stability.experimental,
  scopes: 'ec2-manager:internals',
  description: 'This method is only for debugging the ec2-manager',
}, async function(req, res) {
  return res.reply({regions: this.regions});
});

/**
 * List AMIs and their usage
 */
builder.declare({
  method: 'get',
  route: '/internal/ami-usage',
  name: 'amiUsage',
  title: 'See the list of AMIs and their usage',
  stability: APIBuilder.stability.experimental,
  scopes: 'ec2-manager:internals',
  description:
    [
      'List AMIs and their usage by returning a list of objects in the form:',
      '{',
      [
        'region: string',
        'volumetype: string',
        'lastused: timestamp',
      ].join('\n  '),
      '}',
    ].join('\n'),
}, async function(req, res) {
  let amiUsage = await this.state.listAmiUsage();
  return res.reply(amiUsage);
});

/**
 * Lists current EBS volume usage by returning a list of objects
 * that are unique defined by {region, volumetype, state} in the form:
 * {
 *  region: string,
 *  volumetype: string,
 *  state: string,
 *  totalcount: integer,
 *  totalgb: integer,
 *  touched: timestamp,
 * }
 */
builder.declare({
  method: 'get',
  route: '/internal/ebs-usage',
  name: 'ebsUsage',
  title: 'See the current EBS volume usage list',
  stability: APIBuilder.stability.experimental,
  scopes: 'ec2-manager:internals',
  description:
    [
      [
        'Lists current EBS volume usage by returning a list of objects',
        'that are uniquely defined by {region, volumetype, state} in the form:',
      ].join('\n'),
      '{',
      [
        'region: string,',
        'volumetype: string,',
        'state: string,',
        'totalcount: integer,',
        'totalgb: integer,',
        'touched: timestamp (last time that information was updated),',
      ].join('\n  '),
      '}',
    ].join('\n'),
}, async function(req, res) {
  let ebsTotals = await this.state.listEbsUsage();
  return res.reply(ebsTotals);
});

/**
 * Show stats on the Database Pool
 */
builder.declare({
  method: 'get',
  route: '/internal/db-pool-stats',
  name: 'dbpoolStats',
  title: 'Statistics on the Database client pool',
  stability: APIBuilder.stability.experimental,
  scopes: 'ec2-manager:internals',
  description: 'This method is only for debugging the ec2-manager',
}, async function(req, res) {
  let pool = this.state._pgpool;
  let result = {
    max: pool.options.max,
    idle: pool.idleCount,
    total: pool.totalCount,
    waiting: pool.waitingCount,
  };
  return res.reply(result);
});

/**
 * Show all the state tracked in the database
 */
builder.declare({
  method: 'get',
  route: '/internal/all-state',
  name: 'allState',
  title: 'List out the entire internal state',
  stability: APIBuilder.stability.experimental,
  scopes: 'ec2-manager:internals',
  description: 'This method is only for debugging the ec2-manager',
}, async function(req, res) {
  let result = {
    instances: await this.state.listInstances(),
  };
  return res.reply(result);
});

/**
 * Show stats on the SQS Queues
 */
builder.declare({
  method: 'get',
  route: '/internal/sqs-stats',
  name: 'sqsStats',
  title: 'Statistics on the sqs queues',
  stability: APIBuilder.stability.experimental,
  scopes: 'ec2-manager:internals',
  description: 'This method is only for debugging the ec2-manager',
}, async function(req, res) {
  let result = {};
  await Promise.all(this.regions.map(async region => {
    result[region] = await getQueueStats({queueName: this.queueName, sqs: this.sqs[region]}); 
  }));
  return res.reply(result);
});

/**
 * Purge SQS Queues
 */
builder.declare({
  method: 'get',
  route: '/internal/purge-queues',
  name: 'purgeQueues',
  title: 'Purge the SQS queues',
  stability: APIBuilder.stability.experimental,
  scopes: 'ec2-manager:internals',
  description: 'This method is only for debugging the ec2-manager',
}, async function(req, res) {
  // todo make sqs context for api, and also queueName
  let result = await Promise.all(this.regions.map(async region => {
    let queueUrl = await getQueueUrl({sqs: this.sqs[region], queueName: this.queueName});
    return await purgeQueue({sqs: this.sqs[region], queueUrl: queueUrl});
  }));
  return res.status(204).end();
});

module.exports = {builder};
