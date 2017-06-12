'use strict';
const API = require('taskcluster-lib-api');
const assert = require('assert');

const {getQueueStats, getQueueUrl, purgeQueue} = require('sqs-simple');

const log = require('./log');

let api = new API({
  title: 'EC2 Instance Manager',
  description: [
    'A taskcluster service which manages EC2 instances.  This service does not understand ',
    'any taskcluster concepts intrinsicaly other than using the name `workerType` to ',
    'refer to a group of associated instances and spot requests.  Unless you are working',
    'on building a provisioner for AWS, you almost certainly do not want to use this service'
  ].join(''),
  schemaPrefix: 'http://schemas.taskcluster.net/ec2-manager/v1/',
  context: [
    'state',
    'keyPrefix',
    'instancePubKey',
    'regions',
    'apiBaseUrl',
    'queueName',
    'sqs'
  ],
})

api.declare({
  method: 'get',
  route: '/worker-type/:workerType/state',
  name: 'stateForWorkerType',
  title: 'Look up the state for a workerType',
  stability: API.stability.experimental,
  description: 'Return an object which has a generic state description'
}, async function (req, res) {
  let workerType = req.params.workerType;
  let counts = await this.state.instanceCounts({workerType});
  return res.reply(counts);
});

api.declare({
  method: 'get',
  route: '/internal/regions',
  name: 'regions',
  title: 'See the list of regions managed by this ec2-manager',
  stability: API.stability.experimental,
  description: 'This method is only for debugging the ec2-manager',
}, async function (req, res) {
  return res.reply({regions: this.regions});
});

api.declare({
  method: 'get',
  route: '/internal/spot-requests-to-poll',
  name: 'spotRequestsToPoll',
  title: 'See the list of spot requests which are to be polled',
  stability: API.stability.experimental,
  description: 'This method is only for debugging the ec2-manager',
}, async function (req, res) {
  let result = await Promise.all(this.regions.map(async region => {
    let values = await this.state.spotRequestsToPoll({region});
    return {region, values};
  }));
  return res.reply(result);
});

api.declare({
  method: 'get',
  route: '/internal/db-pool-stats',
  name: 'dbpoolStats',
  title: 'Statistics on the Database client pool',
  stability: API.stability.experimental,
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

api.declare({
  method: 'get',
  route: '/internal/sqs-stats',
  name: 'sqsStats',
  title: 'Statistics on the sqs queues',
  stability: API.stability.experimental,
  description: 'This method is only for debugging the ec2-manager',
}, async function (req, res) {
  let result = {};
  await Promise.all(this.regions.map(async region => {
    result[region] = await getQueueStats({queueName: this.queueName, sqs: this.sqs[region]}); 
  }));
  return res.reply(result);
});

api.declare({
  method: 'get',
  route: '/internal/purge-queues',
  name: 'purgeQueues',
  title: 'Purge the SQS queues',
  stability: API.stability.experimental,
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
  let workerType = spotRequest.LaunchSpecification.KeyName.split(':').slice(1, 2)[0];
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

// NOTE Idempotency is being enforced by the database.  I guess this is a
// problem because we could have a situation where the first call to this API
// succeeds but the client doesn't get the response and so retries.  Then the
// second attempt would fail because it would get an error thrown by the
// postgres client.  This is a risk that is acceptable because a) this method
// is a transtional method only b) this type of failure shouldn't happen often
// and c) because this method doesn't actually spend money or errors from it
// cause management to incorrectly operat.
api.declare({
  method: 'put',
  route: '/spot-requests/region/:region/import',
  name: 'importSpotRequest', 
  title: 'Import the result of running EC2.requestSpotInstances',
  input: 'import-spot-request.json#',
  // WAIT FOR TOP LEVEL SCOPE TO BE CREATED
  // scopes: [['ec2-manager:import-spot-request']],
  stability: API.stability.experimental,
  description: 'This method is for getting data without owning the actual requests',
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
 * List the workertypes which are known to this ec2-manager to have pending or
 * running capacity
 */
api.declare({
  method: 'get',
  route: '/worker-types',
  name: 'listWorkerTypes',
  title: 'See the list of spot requests which are to be polled',
  stability: API.stability.experimental,
  description: 'This method is only for debugging the ec2-manager',
}, async function (req, res) {
  let result = await this.state.listWorkerTypes();
  return res.reply(result);
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

/*

TO IMPLEMENT:
 - PUT /spot-requests/region/:region <-- actually run a requestSpotInstance call
 - DELETE /spot-requests/region/:region/:id <-- cancel and kill
 - GET /spot-requests/region/:region/:id <-- describe the spot request
 - DELETE /instances/region/:region/:id <-- cancel and kill
 - GET /state <-- run queries against the database
 - api to purge sqs queues

*/

module.exports = {api};
