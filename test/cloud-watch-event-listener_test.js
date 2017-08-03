
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
    'arn:aws:ec2:us-west-2:692406183521:instance/i-0d0cf3d89cbab142c',
  ],
  detail: {
    'instance-id': 'i-0d0cf3d89cbab142c',
    state: 'pending',
  },
};

describe('Cloud Watch Event Listener', () => {
  let sandbox = sinon.sandbox.create();
  let state;
  let ec2;
  let sqs;
  let region = 'us-west-2';
  let az = 'us-west-2a';
  let instanceType = 'c3.xlarge';
  let imageId = 'ami-1';
  let listener;

  before(async() => {
    // We want a clean DB state to verify things happen as we intend
    state = await main('state', {profile: 'test', process: 'test'});

    ec2 = await main('ec2', {profile: 'test', process: 'test'});
    ec2 = ec2[region];

    sqs = await main('sqs', {profile: 'test', process: 'test'});
    sqs = sqs[region];

    let monitor = await main('monitor', {profile: 'test', process: 'test'});
    let cfg = await main('cfg', {profile: 'test', process: 'test'});

    await state._runScript('drop-db.sql');
    await state._runScript('create-db.sql');

    listener = new CloudWatchEventListener({state, sqs, ec2, region, monitor, keyPrefix: cfg.app.keyPrefix});
  });

  // I could add these helper functions to the actual state.js class but I'd
  // rather not have that be so easy to call by mistake in real code
  beforeEach(async() => {
    await state._runScript('clear-db.sql');
  });

  afterEach(() => {
    sandbox.restore();
  });

  it('should handle pending, deleting spot request', async() => {
    await state.insertSpotRequest({
      workerType: 'workertype',
      region,
      instanceType,
      id: 'r-1234',
      state: 'open',
      status: 'pending-fulfillment',
      az,
      created: new Date(),
      imageId,
    });

    let mock = sandbox.stub(listener, 'runaws');

    mock.onFirstCall().returns(Promise.resolve({
      Reservations: [{
        Instances: [{
          KeyName: 'ec2-manager-test:workertype:hash',
          InstanceType: 'c3.xlarge',
          SpotInstanceRequestId: 'r-1234',
          LaunchTime: new Date(),
          ImageId: imageId,
          Placement: {
            AvailabilityZone: az,
          },
        }],
      }],
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
  
  it('should handle running transition with the instance already in db in pending state', async() => {
    let pendingTimestamp = new Date();
    let runningTimestamp = new Date(pendingTimestamp);
    runningTimestamp.setMinutes(runningTimestamp.getMinutes() + 1);

    await state.insertInstance({
      workerType: 'workertype',
      region,
      instanceType,
      id: 'i-1',
      state: 'pending',
      az,
      launched: pendingTimestamp,
      imageId,
      lastevent: pendingTimestamp,
    });

    let mock = sandbox.stub(listener, 'runaws');

    mock.onFirstCall().throws(new Error('shouldnt talk to ec2 api'));

    let instances = await state.listInstances();
    assume(instances).lengthOf(1);
    assume(instances[0]).has.property('lastevent');
    assume(instances[0].lastevent.getTime()).equals(pendingTimestamp.getTime());
    let pendingMsg = Object.assign({}, baseExampleMsg, {
      detail: {
        'instance-id': 'i-1',
        state: 'running',
      },
      time: runningTimestamp,
    });

    await listener.__handler(JSON.stringify(pendingMsg));

    instances = await state.listInstances();
    assume(instances).lengthOf(1);
    assume(instances[0]).has.property('lastevent');
    assume(instances[0].lastevent.getTime()).equals(runningTimestamp.getTime());
    assume(instances[0].state).equals('running');

    assume(instances).lengthOf(1);
  });  

  it('should handle out of order delivery', async() => {
    let pendingTimestamp = new Date();
    let runningTimestamp = new Date(pendingTimestamp);
    runningTimestamp.setMinutes(runningTimestamp.getMinutes() + 1);

    await state.insertInstance({
      workerType: 'workertype',
      region,
      instanceType,
      id: 'i-1',
      state: 'running',
      az,
      launched: runningTimestamp,
      imageId,
      lastevent: runningTimestamp,
    });

    let mock = sandbox.stub(listener, 'runaws');

    mock.onFirstCall().throws(new Error('shouldnt talk to ec2 api'));

    let instances = await state.listInstances();
    assume(instances).lengthOf(1);
    assume(instances[0]).has.property('lastevent');
    assume(instances[0].lastevent.getTime()).deeply.equals(runningTimestamp.getTime());
    let pendingMsg = Object.assign({}, baseExampleMsg, {
      detail: {
        'instance-id': 'i-1',
        state: 'pending',
      },
      time: pendingTimestamp,
    });

    await listener.__handler(JSON.stringify(pendingMsg));

    instances = await state.listInstances();
    assume(instances).lengthOf(1);
    assume(instances[0]).has.property('lastevent');
    assume(instances[0].lastevent.getTime()).equals(runningTimestamp.getTime());
    assume(instances[0].state).equals('running');

    assume(instances).lengthOf(1);
  });

  it('should skip a pending message for a different manager', async() => {
    let mock = sandbox.stub(listener, 'runaws');

    mock.onFirstCall().returns(Promise.resolve({
      Reservations: [{
        Instances: [{
          KeyName: 'other-manager:workertype:hash',
          InstanceType: 'c3.xlarge',
          SpotInstanceRequestId: 'r-1234',
          ImageId: imageId,
          LaunchTime: new Date(),
          Placement: {
            AvailabilityZone: az,
          },
        }],
      }],
    }));

    let instances = await state.listInstances();
    let requests = await state.listSpotRequests();
    assume(instances).lengthOf(0);
    assume(requests).lengthOf(0);
  });

  it('should handle shutting-down, deleting spot request', async() => {
    await state.insertSpotRequest({
      workerType: 'workertype',
      region,
      instanceType,
      id: 'r-1234',
      state: 'open',
      status: 'pending-fulfillment',
      az,
      imageId,
      created: new Date(),
    });

    let mock = sandbox.stub(listener, 'runaws');

    mock.onFirstCall().returns(Promise.resolve({
      Reservations: [{
        Instances: [{
          KeyName: 'ec2-manager-test:workertype:hash',
          InstanceType: 'c3.xlarge',
          SpotInstanceRequestId: 'r-1234',
          ImageId: imageId,
          LaunchTime: new Date(),
          Placement: {
            AvailabilityZone: az,
          },
        }],
      }],
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
