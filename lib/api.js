'use strict';
const API = require('taskcluster-lib-api');
const assert = require('assert');

let api = new API({
  title: 'EC2 Instance Manager',
  description: [
    'A taskcluster service which manages EC2 instances.  This service does not understand ',
    'any taskcluster concepts intrinsicaly other than using the name `workerType` to ',
    'refer to a group of associated instances and spot requests.  Unless you are working',
    'on building a provisioner for AWS, you almost certainly do not want to use this service'
  ].join(''),
  context: [
    'state',
    'keyPrefix',
    'instancePubKey',
    'regions',
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

module.exports = {api};
