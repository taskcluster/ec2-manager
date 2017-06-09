'use strict';
const _ = require('lodash');
const main = require('../lib/main');
const assume = require('assume');
const sinon = require('sinon');
const subject = require('../lib/cloud-watch-event-listener');
const {CloudWatchEventListener} = subject;
const runaws = require('../lib/aws-request');


// This is the basis of an example message from cloud watch.  The only thing
// which should change is the state in the detail object.  Intentionally a
// const
const baseExampleMsg = {
  version: '0',
  id: '9129eb4e-07c0-484e-b2a5-204386a2d7fd',
  'detail-type': 'EC2 Instance State-change Notification',
  source: 'aws.ec2',
  account: '692406183521',
  time: '2017-06-04T13:14:15Z',
  region: 'us-west-2',
  resources: [
    'arn:aws:ec2:us-west-2:692406183521:instance/i-0d0cf3d89cbab142c'
  ],
  detail: {
    'instance-id': 'i-0d0cf3d89cbab142c',
    state: 'pending'
  }
};

describe('Cloud Watch Event Listener', () => {
  let sandbox = sinon.sandbox.create();
  let state;
  let ec2;
  let sqs;
  let region = 'us-west-2';
  let instanceType = 'c3.xlarge';
  let listener;

  before(async () => {
    // We want a clean DB state to verify things happen as we intend
    state = await main('state', {profile: 'test', process: 'test'});

    ec2 = await main('ec2', {profile: 'test', process: 'test'});
    ec2 = ec2[region];

    sqs = await main('sqs', {profile: 'test', process: 'test'});
    sqs = sqs[region];

    let monitor = await main('monitor', {profile: 'test', process: 'test'});

    await state._runScript('drop-db.sql');
    await state._runScript('create-db.sql');

    listener = new CloudWatchEventListener({state, sqs, ec2, region, monitor});
  });

  // I could add these helper functions to the actual state.js class but I'd
  // rather not have that be so easy to call by mistake in real code
  beforeEach(async () => {
    await state._runScript('clear-db.sql');
  });

  afterEach(() => {
    sandbox.restore();
  });

  it('should handle pending, deleting spot request', async () => {
    await state.insertSpotRequest({
      workerType: 'workertype',
      region,
      instanceType,
      id: 'r-1234',
      state: 'open',
      status: 'pending-fulfillment',
    });

    let mock = sandbox.stub(listener, 'awsrun');

    mock.onFirstCall().returns(Promise.resolve({
      Reservations: [{
        Instances: [{
          KeyName: 'provisioner:workertype:hash',
          InstanceType: 'c3.xlarge',
          SpotInstanceRequestId: 'r-1234',
        }],
      }]
    }));

    let instances = await state.listInstances();
    let requests = await state.listSpotRequests();
    assume(instances).lengthOf(0);
    assume(requests).lengthOf(1);
    let pendingMsg = _.defaultsDeep({}, baseExampleMsg, {detail: {state: 'pending'}});
    await listener.__handler(JSON.stringify(pendingMsg));
    instances = await state.listInstances();
    requests = await state.listSpotRequests();
    assume(instances).lengthOf(1);
    assume(requests).lengthOf(0);
  });

  it('should handle shutting-down, deleting spot request', async () => {
    await state.insertSpotRequest({
      workerType: 'workertype',
      region,
      instanceType,
      id: 'r-1234',
      state: 'open',
      status: 'pending-fulfillment',
    });

    let mock = sandbox.stub(listener, 'awsrun');

    mock.onFirstCall().returns(Promise.resolve({
      Reservations: [{
        Instances: [{
          KeyName: 'provisioner:workertype:hash',
          InstanceType: 'c3.xlarge',
          SpotInstanceRequestId: 'r-1234',
        }],
      }]
    }));

    let instances = await state.listInstances();
    let requests = await state.listSpotRequests();
    assume(instances).lengthOf(0);
    assume(requests).lengthOf(1);
    let pendingMsg = _.defaultsDeep({}, baseExampleMsg, {detail: {state: 'shutting-down'}});
    await listener.__handler(JSON.stringify(pendingMsg));
    instances = await state.listInstances();
    requests = await state.listSpotRequests();
    assume(instances).lengthOf(1);
    assume(requests).lengthOf(0);
  });

});
