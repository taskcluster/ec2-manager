'use strict';
const API = require('taskcluster-lib-api');
const assert = require('assert');
const crypto = require('crypto');

const {getQueueStats, getQueueUrl, purgeQueue} = require('sqs-simple');

const {tagResources} = require('./tag-resources');

const log = require('./log');

// Keep a cache of keypairs which are known to exist
let knownKeyPairs = [];

let api = new API({
  title: 'EC2 Instance Manager',
  description: [
    'A taskcluster service which manages EC2 instances.  This service does not understand',
    'any taskcluster concepts intrinsicaly other than using the name `workerType` to',
    'refer to a group of associated instances and spot requests.  Unless you are working',
    'on building a provisioner for AWS, you almost certainly do not want to use this service'
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
  ],
})

/**
 * List the workertypes which are known to this ec2-manager to have pending or
 * running capacity
 */
api.declare({
  method: 'get',
  route: '/worker-types',
  name: 'listWorkerTypes',
  title: 'See the list of spot requests which are to be polled',
  stability: API.stability.experimental,
  output: 'list-worker-types.json#',
  description: 'This method is only for debugging the ec2-manager',
}, async function (req, res) {
  let result = await this.state.listWorkerTypes();
  return res.reply(result);
});

/**
 * Make a request for a spot instance
 */
api.declare({
  method: 'put',
  route: '/worker-types/:workerType/spot-request',
  name: 'requestSpotInstance',
  title: 'Request a spot instance',
  stability: API.stability.experimental,
  input: 'make-spot-request.json#',
  scopes: [['ec2-manager:manage-resources:<workerType>']],
  description: [
    'Request a spot instance of a worker type'
  ].join(' '),
}, async function (req, res) {
  try {
    let workerType = req.params.workerType;
    if (!req.satisfies({workerType: workerType})) { return undefined; }
    let {
      ClientToken,
      Region,
      SpotPrice,
      LaunchSpecification,
    } = req.body;

    // This is a crtical check to ensure that we only ever try to create spot requests
    // which are using the correct key pair name
    let keyName = createKeyPairName(this.keyPrefix, this.instancePubKey, workerType);
    if (keyName !== LaunchSpecification.KeyName) {
      log.error({
        requiredKeyName: keyName,
        providedKeyName: LaunchSpecification.KeyName,
      }, 'KeyName requirement not met');
      return res.reportError('InputError', 'LaunchSpecification is invalid!');
    }
     
    assert(keyName === LaunchSpecification.KeyName);
    
    let valid = await this.lsChecker.check({
      launchSpecification: LaunchSpecification,
      region: Region,
    });
    
    if (!valid) {
      return res.reportError('InputError', 'LaunchSpecification is invalid!');
    }

    let params = {
      Type: 'one-time',
      SpotPrice: SpotPrice.toString(),
      ClientToken,
      LaunchSpecification,
      InstanceCount: 1,
    };
    let result;
    try {
      result = await this.runaws(this.ec2[Region], 'requestSpotInstances', params);
      // TODO: Put a couple more useful fields here, maybe instance type and price.
      // This should be simple, just reaching into the launch spec.
      log.info({region: Region}, 'Requested spot instance');
    } catch (err) {
      // https://docs.aws.amazon.com/AWSEC2/latest/APIReference/errors-overview.html
      log.error({err, params}, 'Error requesting a spot instance');
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

    try {
      await tagResources({
        runaws: this.runaws,
        ec2: this.ec2[region],
        keyPrefix: this.keyPrefix,
        workerType,
      });
    } catch (err) {
      log.warn({err}, 'Error while tagging, ignoring');
    }

    try {
      await handleRequestSpotInstanceResponse({state: this.state, region: Region, response: result});
      res.status(204).end();
    } catch (err) {
      //https://www.postgresql.org/docs/9.6/static/errcodes-appendix.html
      if (err.sqlState === '23505') {
        // So when there's a conflict we're going to do nothing.  You might be
        // concerned, but alas, fear not intrepid hacker!  We know that the
        // primary key (region, id) already exists so that means we've already
        // inserted *something* into the database.  Now, you'd be totally right
        // when you question what happens if a second request was made with
        // different values.  Fear not, we wouldn't get here if the same request
        // was made but with values which would alter the values inserted into
        // the database.  I guess we're trusting the idempotency of EC2.  When
        // we're trusting it with so many other things, I don't see this being a
        // problem.  Further, spot requests should only live in the pre-fulfilled
        // state for a relatively short amount of time anyway!
        return res.status(204).end();
      } else {
        console.dir(err.stack || err);
        throw err;
      }
    }
  } catch (err) {
    log.error({err}, 'requestSpotInstance');
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
    'Terminate all instances and cancel all spot requests for this worker type'
  ].join(' '),
}, async function (req, res) {
  let workerType = req.params.workerType;

  if (!req.satisfies({workerType: workerType})) { return undefined; }

  let ids = await this.state.listIdsOfWorkerType({workerType});

  await Promise.all(this.regions.map(async region => {
    let instanceIds = ids.instanceIds.filter(x => x.region === region).map(x => x.id);
    let requestIds = ids.requestIds.filter(x => x.region === region).map(x => x.id);
    if (instanceIds.length > 0) {
      await this.runaws(this.ec2[region], 'terminateInstances', {
        InstanceIds: instanceIds
      });
    }
    if (requestIds.length > 0) {
      await this.runaws(this.ec2[region], 'cancelSpotInstanceRequests', {
        SpotInstanceRequestIds: requestIds
      });
    }
    // POSSIBLE OPTIMIZATION: add a remove{Instance,SpotRequest}s method to
    // bulk remove them
    for (let id of requestIds) {
      await this.state.removeSpotRequest({region, id}); 
    }
    for (let id of instanceIds) {
      await this.state.removeInstance({region, id}); 
    }
    log.info({instanceIds, requestIds, region}, 'Terminated resources in region');
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
    'This only contains counts of instances and spot requests',
  ].join(' '),
}, async function (req, res) {
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
}, async function (req, res) {
  let workerType = req.params.workerType;
  let result = {
    instances: await this.state.listInstances(),
    requests: await this.state.listSpotRequests(),
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
}, async function (req, res) {
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
    'Ensure that a keypair of a given name does not exist.'
  ].join(' '),
}, async function (req, res) {
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

/**
 * Handle the output of the requestSpotInstance method.  The response property
 * should be just the raw object that the method returned.  This is split out here
 * so that we can use the same logic for imports and calls to the EC2 api made
 * from the api
 */
async function handleRequestSpotInstanceResponse({state, region, response}) {
  assert(typeof region === 'string');
  assert(typeof response === 'object');
  assert(Array.isArray(response.SpotInstanceRequests));
  assert(response.SpotInstanceRequests.length === 1);
  let [spotRequest] = response.SpotInstanceRequests;

  let id = spotRequest.SpotInstanceRequestId;
  let workerType = spotRequest.LaunchSpecification.KeyName.split(':')[1];
  let instanceType = spotRequest.LaunchSpecification.InstanceType;
  let requestState = spotRequest.State;
  let status = spotRequest.Status.Code;

  let opts = {
    workerType,
    region,
    instanceType,
    id,
    state: requestState,
    status,
  };

  log.info(opts, 'inserting spot request into database'); 

  await state.insertSpotRequest(opts); 

  log.info(opts, 'finished inserting spot request into database'); 
}

// NOTE Idempotency is being enforced by the database for the import operation.
// I guess this is a problem because we could have a situation where the first
// call to this API succeeds but the client doesn't get the response and so
// retries.  Then the second attempt would fail because it would get an error
// thrown by the postgres client.  This is a risk that is acceptable because a)
// this method is a transtional method only b) this type of failure shouldn't
// happen often and c) because this method doesn't actually spend money or
// errors from it cause management to incorrectly operat.

/**
 * Import a spot request
 */
api.declare({
  method: 'put',
  // NOTE: THIS ENDPOINT IS TEMPORARY AT BEST, LONG TERM IT WILL BE REPLACED WITH A DIFFERENT ONE
  route: '/spot-requests/region/:region/import',
  name: 'importSpotRequest', 
  title: 'DO NOT USE',
  input: 'import-spot-request.json#',
  // scopes: [['ec2-manager:import-spot-request']],
  stability: API.stability.experimental,
  description: 'DO NOT USE',
}, async function (req, res) {
  try {
    await handleRequestSpotInstanceResponse({state: this.state, region: req.params.region, response: req.body});
    res.status(204).end();
  } catch (err) {
    //https://www.postgresql.org/docs/9.6/static/errcodes-appendix.html
    if (err.sqlState === '23505') {
      res.reportError('RequestConflict', 'spot request already tracked', {});
    } else {
      console.dir(err.stack || err);
      throw err;
    }
  }
});

/**
 * Terminate a single instance
 */
api.declare({
  method: 'delete',
  route: '/region/:region/instance/:instanceId',
  name: 'terminateInstance',
  title: 'Terminate an instance',
  stability: API.stability.experimental,
  scopes: [['ec2-manager:manage-instances:<region>:<instanceId>']],
  description: [
    'Terminate an instance in a specified region',
  ].join(' '),
}, async function (req, res) {
  let region = req.params.region;
  let instanceId = req.params.instanceId;

  if (!req.satisfies({region, instanceId})) { return undefined; }

  if (!this.regions.includes(region)) {
    res.reportError('ResourceNotFound', 'Region is not configured', {});
  }
  assert(this.regions.includes(region));

  let result = await this.runaws(this.ec2[region], 'terminateInstances', {
    InstanceIds: [instanceId]
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
 * Cancel a spot request
 */
api.declare({
  method: 'delete',
  route: '/region/:region/spot-instance-request/:requestId',
  name: 'cancelSpotInstanceRequest',
  title: 'Cancel a request for a spot instance',
  stability: API.stability.experimental,
  scopes: [['ec2-manager:manage-spot-requests:<region>:<requestId>']],
  description: [
    'Cancel a spot instance request in a region',
  ].join(' '),
}, async function (req, res) {
  let region = req.params.region;
  let requestId = req.params.requestId;

  if (!req.satisfies({region, requestId})) { return undefined; }

  if (!this.regions.includes(region)) {
    res.reportError('ResourceNotFound', 'Region is not configured', {});
  }
  assert(this.regions.includes(region));

  let result = await this.runaws(this.ec2[region], 'cancelSpotInstanceRequests', {
    SpotInstanceRequestIds: [requestId]
  });

  await this.state.removeSpotRequest({region, id: requestId});

  // I'm not sure if this response will always happen from the API and it doesn't
  // really describe anything about it.  Since this is only being given for informational
  // purposes, I'm not too concered
  if (result.CancelledSpotInstanceRequests) {
    assert(Array.isArray(result.CancelledSpotInstanceRequests));
    assert(result.CancelledSpotInstanceRequests.length === 1);
    let x = result.CancelledSpotInstanceRequests[0];
    return res.reply({
      current: x.State,
    });
  } else {
    return res.status(204).end();
  }

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
}, async function (req, res) {
  return res.reply({regions: this.regions});
});

/**
 * List the spot requests which are being polled
 */
api.declare({
  method: 'get',
  route: '/internal/spot-requests-to-poll',
  name: 'spotRequestsToPoll',
  title: 'See the list of spot requests which are to be polled',
  stability: API.stability.experimental,
  scopes: [['ec2-manager:internals']],
  description: 'This method is only for debugging the ec2-manager',
}, async function (req, res) {
  let result = await Promise.all(this.regions.map(async region => {
    let values = await this.state.spotRequestsToPoll({region});
    return {region, values};
  }));
  return res.reply(result);
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
}, async function (req, res) {
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
}, async function (req, res) {
  let result = {
    instances: await this.state.listInstances(),
    requests: await this.state.listSpotRequests(),
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
}, async function (req, res) {
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
}, async function (req, res) {
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
}, async function (req, res) {
  res.reply(api.reference({baseUrl: this.apiBaseUrl}));
});

module.exports = {api};
