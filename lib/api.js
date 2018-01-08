
const API = require('taskcluster-lib-api');
const assert = require('assert');
const crypto = require('crypto');

const {getQueueStats, getQueueUrl, purgeQueue} = require('sqs-simple');

const log = require('./log');

// Keep a cache of keypairs which are known to exist
let knownKeyPairs = [];

let api = new API({
  title: 'EC2 Instance Manager',
  description: [
    'A taskcluster service which manages EC2 instances.  This service does not understand',
    'any taskcluster concepts intrinsicaly other than using the name `workerType` to',
    'refer to a group of associated instances.  Unless you are working',
    'on building a provisioner for AWS, you almost certainly do not want to use this service',
  ].join(' '),
  schemaPrefix: 'http://schemas.taskcluster.net/ec2-manager/v1/',
  context: [
    'state',
    'keyPrefix',
    'instancePubKey',
    'regions',
    'apiBaseUrl',
    'queueName',
    'sqs',
    'ec2',
    'lsChecker',
    'runaws',
    'pricing',
    'tagger',
  ],
});

/**
 * List the workertypes which are known to this ec2-manager to have pending or
 * running capacity
 */
api.declare({
  method: 'get',
  route: '/worker-types',
  name: 'listWorkerTypes',
  title: 'See the list of worker types which are known to be managed',
  stability: API.stability.experimental,
  output: 'list-worker-types.json#',
  description: 'This method is only for debugging the ec2-manager',
}, async function(req, res) {
  let result = await this.state.listWorkerTypes();
  return res.reply(result);
});

/**
 * Run an EC2 instance
 */
api.declare({
  method: 'put',
  route: '/worker-types/:workerType/instance',
  name: 'runInstance',
  title: 'Run an instance',
  stability: API.stability.experimental,
  input: 'run-instance-request.json#',
  scopes: [['ec2-manager:manage-resources:<workerType>']],
  description: [
    'Request an instance of a worker type',
  ].join(' '),
}, async function(req, res) {
  try {
    let workerType = req.params.workerType;
    if (!req.satisfies({workerType: workerType})) { return undefined; }
    let {
      ClientToken,
      Region,
      SpotPrice,
      RequestType,
      LaunchInfo,
    } = req.body;

    // This is a crtical check to ensure that we only ever try to create
    // instances which are using the correct key pair name
    let keyName = createKeyPairName(this.keyPrefix, this.instancePubKey, workerType);
    if (keyName !== LaunchInfo.KeyName) {
      log.error({
        requiredKeyName: keyName,
        providedKeyName: LaunchInfo.KeyName,
      }, 'KeyName requirement not met');
      return res.reportError('InputError', 'LaunchInfo is invalid!');
    }

    assert(keyName === LaunchInfo.KeyName);

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

    let result;
    try {
      result = await this.runaws(this.ec2[Region], 'runInstances', LaunchInfo);
      // TODO: Put a couple more useful fields here, maybe instance type and price.
      // This should be simple, just reaching into the launch spec.
      log.info({region: Region}, 'Requested instance');
    } catch (err) {
      // https://docs.aws.amazon.com/AWSEC2/latest/APIReference/errors-overview.html
      log.error({err, LaunchInfo}, 'Error requesting an instance');
      switch (err.code) {
        case 'InvalidParameter':
        case 'InvalidParameterCombination':
        case 'InvalidParameterValue':
        case 'UnknowParameter':
          return res.reportError('InputError', 'EC2 API says this is bad input data');
        default:
          // default behaviour
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
      state: instance.InstanceState.Name,
      imageId: instance.ImageId,
      launched: new Date(instance.LaunchTime),
      lastEvent: new Date(),
    };

    log.debug(opts, 'inserting instance into database'); 

    try {
      await this.state.reportAmiUsage({
        region: Region,
        id: instance.InstanceId,
      });
    } catch (err) {
      log.warn({err}, 'Error while reporting AMI usage, ignoring');
    }

    try {
      await this.state.insertInstance(opts); 

      log.debug(opts, 'finished inserting instance into database'); 
      res.status(204).end();
    } catch (err) {
      // https://www.postgresql.org/docs/9.6/static/errcodes-appendix.html
      if (err.code === '23505') {
        // So when there's a conflict we're going to do nothing.  You might be
        // concerned, but alas, fear not intrepid hacker!  We know that the
        // primary key (region, id) already exists so that means we've already
        // inserted *something* into the database.  Now, you'd be totally right
        // when you question what happens if a second request was made with
        // different values.  Fear not, we wouldn't get here if the same
        // request was made but with values which would alter the values
        // inserted into the database because EC2 enforces idempotency through
        // our use of the ClientToken option.  I guess we're trusting the
        // idempotency of EC2.  When we're trusting it with so many other
        // things, I don't see this being a problem.
        return res.status(204).end();
      } else {
        console.dir(err.stack || err);
        throw err;
      }
    }
  } catch (err) {
    log.error({err}, 'runInstance');
    throw err;
  }
});

/**
 * Destroy all EC2 resources of a given worker type
 */
api.declare({
  method: 'delete',
  route: '/worker-types/:workerType/resources',
  name: 'terminateWorkerType',
  title: 'Terminate all resources from a worker type',
  scopes: [['ec2-manager:manage-resources:<workerType>']],
  stability: API.stability.experimental,
  description: [
    'Terminate all instances for this worker type',
  ].join(' '),
}, async function(req, res) {
  let workerType = req.params.workerType;

  if (!req.satisfies({workerType: workerType})) { return undefined; }

  let ids = await this.state.listIdsOfWorkerType({workerType});

  await Promise.all(this.regions.map(async region => {
    let instanceIds = ids.instanceIds.filter(x => x.region === region).map(x => x.id);
    if (instanceIds.length > 0) {
      await this.runaws(this.ec2[region], 'terminateInstances', {
        InstanceIds: instanceIds,
      });
    }
    for (let id of instanceIds) {
      await this.state.removeInstance({region, id}); 
    }
    log.info({instanceIds, region}, 'Terminated resources in region');
  }));

  return res.status(204).end();
});

api.declare({
  method: 'get',
  route: '/worker-types/:workerType/stats',
  name: 'workerTypeStats',
  title: 'Look up the resource stats for a workerType',
  stability: API.stability.experimental,
  output: 'worker-type-resources.json#',
  description: [
    'Return an object which has a generic state description.', 
    'This only contains counts of instances',
  ].join(' '),
}, async function(req, res) {
  let workerType = req.params.workerType;
  let counts = await this.state.instanceCounts({workerType});
  return res.reply(counts);
});

api.declare({
  method: 'get',
  route: '/worker-types/:workerType/state',
  name: 'workerTypeState',
  title: 'Look up the resource state for a workerType',
  stability: API.stability.experimental,
  output: 'worker-type-state.json#',
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

function createPubKeyHash(pubKey) {
  assert(typeof pubKey === 'string');
  let keyData = pubKey.split(' ');
  assert(keyData.length >= 2, 'pub key must be in a valid format');
  keyData = keyData[0] + ' ' + keyData[1];
  keyData = crypto.createHash('sha256').update(keyData).digest('hex');
  return keyData.slice(0, 7);
};

function createKeyPairName(prefix, pubKey, workerName) {
  assert(typeof prefix === 'string');
  // We want to support the case where we're still using a config setting
  // that ends in : as it used to
  if (prefix.charAt(prefix.length - 1) === ':') {
    prefix = prefix.slice(0, prefix.length - 1);
  }
  assert(prefix.indexOf(':') === -1, 'only up to one trailing colon allowed');
  assert(typeof pubKey === 'string');
  assert(typeof workerName === 'string');
  return prefix + ':' + workerName + ':' + createPubKeyHash(pubKey);
};

function parseKeyPairName(name) {
  assert(typeof name === 'string');
  let parts = name.split(':');
  assert(parts.length === 3, 'Unparsable keypair name: ' + name);
  return {
    prefix: parts[0],
    workerType: parts[1],
    keyHash: parts[2],
  };
};

api.declare({
  method: 'get',
  route: '/worker-types/:workerType/key-pair',
  name: 'ensureKeyPair', 
  title: 'Ensure a KeyPair for a given worker type exists',
  stability: API.stability.experimental,
  scopes: [['ec2-manager:manage-resources:<workerType>']],
  description: [
    'Ensure that a keypair of a given name exists.  This call caches',
    'internally the list of keypair names it has ensured at least one',
    'time, and as such is safe to call repeatedly.  It is idempotent.',
  ].join(' '),
}, async function(req, res) {
  let workerType = req.params.workerType;
  let keyName = createKeyPairName(this.keyPrefix, this.instancePubKey, workerType);

  if (!req.satisfies({workerType: workerType})) { return undefined; }

  if (knownKeyPairs.includes(keyName)) {
    return res.status(204).end();
  }

  await Promise.all(this.regions.map(async region => {
    let keyPairs = await this.runaws(this.ec2[region], 'describeKeyPairs', {
      Filters: [{
        Name: 'key-name',
        Values: [keyName],
      }],
    });
    if (!keyPairs.KeyPairs[0]) {
      await this.runaws(this.ec2[region], 'importKeyPair', {
        KeyName: keyName,
        PublicKeyMaterial: this.instancePubKey,
      });
    }
  }));

  knownKeyPairs.push(keyName);
  res.status(204).end();
});

/**
 * Delete a KeyPair
 */
api.declare({
  method: 'delete',
  route: '/worker-types/:workerType/key-pair',
  name: 'removeKeyPair', 
  title: 'Ensure a KeyPair for a given worker type does not exist',
  stability: API.stability.experimental,
  scopes: [['ec2-manager:manage-resources:<workerType>']],
  description: [
    'Ensure that a keypair of a given name does not exist.',
  ].join(' '),
}, async function(req, res) {
  let workerType = req.params.workerType;
  let keyName = createKeyPairName(this.keyPrefix, this.instancePubKey, workerType);

  if (!req.satisfies({workerType: workerType})) { return undefined; }

  await Promise.all(this.regions.map(async region => {
    let keyPairs = await this.runaws(this.ec2[region], 'describeKeyPairs', {
      Filters: [{
        Name: 'key-name',
        Values: [keyName],
      }],
    });
    if (keyPairs.KeyPairs[0]) {
      await this.runaws(this.ec2[region], 'deleteKeyPair', {
        KeyName: keyName,
      });
    }
  }));

  knownKeyPairs = knownKeyPairs.filter(x => x !== keyName);
  res.status(204).end();
});

// NOTE Idempotency is being enforced by the database for the import operation.
// I guess this is a problem because we could have a situation where the first
// call to this API succeeds but the client doesn't get the response and so
// retries.  Then the second attempt would fail because it would get an error
// thrown by the postgres client.  This is a risk that is acceptable because a)
// this method is a transtional method only b) this type of failure shouldn't
// happen often and c) because this method doesn't actually spend money or
// errors from it cause management to incorrectly operat.

/**
 * Terminate a single instance
 */
api.declare({
  method: 'delete',
  route: '/region/:region/instance/:instanceId',
  name: 'terminateInstance',
  title: 'Terminate an instance',
  stability: API.stability.experimental,
  scopes: [
    ['ec2-manager:manage-instances:<region>:<instanceId>'],
    ['ec2-manager:manage-resources:<workerType>'],
  ],
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
  if (!req.satisfies({region, instanceId})) { 
    let workerTypes = await this.state.listInstances({id: instanceId, region});
    switch (workerTypes.len != 1) {
      case 0:
        return undefined;
      case 1:
        if (!req.satisfies({region, instanceId, workerType: workerTypes[0]})) {
          return undefined;
        }
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

  await this.state.removeInstance({region, id: instanceId});

  // I'm not sure if this response will always happen from the API and it doesn't
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
api.declare({
  method: 'get',
  route: '/prices',
  name: 'getPrices',
  title: 'Request prices for EC2',
  stability: API.stability.experimental,
  output: 'prices.json#',
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
api.declare({
  // Too bad GET can't have a body or there wasn't a QUERY method or something
  method: 'post',
  route: '/prices',
  name: 'getSpecificPrices',
  title: 'Request prices for EC2',
  stability: API.stability.experimental,
  input: 'prices-request.json#',
  output: 'prices.json#',
  description: [
    'Return a list of possible prices for EC2',
  ].join(' '),
}, async function(req, res) {
  let prices = await this.pricing.getPrices(req.body);
  console.dir(req.body);
  res.reply(prices);
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
api.declare({
  method: 'get',
  route: '/internal/regions',
  name: 'regions',
  title: 'See the list of regions managed by this ec2-manager',
  stability: API.stability.experimental,
  scopes: [['ec2-manager:internals']],
  description: 'This method is only for debugging the ec2-manager',
}, async function(req, res) {
  return res.reply({regions: this.regions});
});

/**
 * List AMIs and their usage
 */
api.declare({
  method: 'get',
  route: '/internal/ami-usage',
  name: 'amiUsage',
  title: 'See the list of AMIs and their usage',
  stability: API.stability.experimental,
  scopes: [['ec2-manager:internals']],
  description:
    [
      'List AMIs and their usage by returning a list of objects in the form:',
      '{',
      [
        'region: string',
        'volumetype: string',
        'lastused: timestamp',
      ].join('\n\t'),
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
api.declare({
  method: 'get',
  route: '/internal/ebs-usage',
  name: 'ebsUsage',
  title: 'See the current EBS volume usage list',
  stability: API.stability.experimental,
  scopes: [['ec2-manager:internals']],
  description:
    [
      [
        'Lists current EBS volume usage by returning a list of objects',
        'that are uniquely defined by {region, volumetype, state} in the form:',
      ].join(' '),
      '{',
      [
        'region: string,',
        'volumetype: string,',
        'state: string,',
        'totalcount: integer,',
        'totalgb: integer,',
        'touched: timestamp (last time that information was updated),',
      ].join('\n\t'),
      '}',
    ].join('\n'),
}, async function(req, res) {
  let ebsTotals = await this.state.listEbsUsage();
  return res.reply(ebsTotals);
});

/**
 * Show stats on the Database Pool
 */
api.declare({
  method: 'get',
  route: '/internal/db-pool-stats',
  name: 'dbpoolStats',
  title: 'Statistics on the Database client pool',
  stability: API.stability.experimental,
  scopes: [['ec2-manager:internals']],
  description: 'This method is only for debugging the ec2-manager',
}, async function(req, res) {
  let pool = this.state._pgpool.pool;
  let result = {
    inuse: pool._inUseObjects.length || 0,
    avail: pool._availableObjects.length || 0,
    waiting: pool._waitingClients.length || 0,
    count: pool._count || 0,
  };
  return res.reply(result);
});

/**
 * Show all the state tracked in the database
 */
api.declare({
  method: 'get',
  route: '/internal/all-state',
  name: 'allState',
  title: 'List out the entire internal state',
  stability: API.stability.experimental,
  scopes: [['ec2-manager:internals']],
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
api.declare({
  method: 'get',
  route: '/internal/sqs-stats',
  name: 'sqsStats',
  title: 'Statistics on the sqs queues',
  stability: API.stability.experimental,
  scopes: [['ec2-manager:internals']],
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
api.declare({
  method: 'get',
  route: '/internal/purge-queues',
  name: 'purgeQueues',
  title: 'Purge the SQS queues',
  stability: API.stability.experimental,
  scopes: [['ec2-manager:internals']],
  description: 'This method is only for debugging the ec2-manager',
}, async function(req, res) {
  // todo make sqs context for api, and also queueName
  let result = await Promise.all(this.regions.map(async region => {
    let queueUrl = await getQueueUrl({sqs: this.sqs[region], queueName: this.queueName});
    return await purgeQueue({sqs: this.sqs[region], queueUrl: queueUrl});
  }));
  return res.status(204).end();
});

/**
 * Until this API is more solid, we don't want to publish the reference.  In the meantime,
 * the provisioner will need to be able to build an ec2-manager client.  What I'll do is make this
 * endpoint contain the JSON data structure that the taskcluster-client library needs to build a
 * client dynamically.
 */
api.declare({
  method: 'get',
  route: '/internal/api-reference',
  name: 'apiReference',
  title: 'API Reference',
  stability: API.stability.experimental,
  description: 'Generate an API reference for this service',
}, async function(req, res) {
  res.reply(api.reference({baseUrl: this.apiBaseUrl}));
});

module.exports = {api};
