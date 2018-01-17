
const testing = require('taskcluster-lib-testing');
const taskcluster = require('taskcluster-client');
const assume = require('assume');
const main = require('../lib/main');
const {api} = require('../lib/api');
const sinon = require('sinon');
const {HouseKeeper} = require('../lib/housekeeping');

describe('House Keeper', () => {
  let state;
  let ec2;
  let region = 'us-west-2';
  let instanceType = 'c3.xlarge';
  let workerType = 'apiTest';
  let az = 'us-west-2a';
  let imageId = 'ami-1';
  let created = new Date();
  let launched = new Date();
  let sandbox = sinon.sandbox.create();
  let describeInstancesStub;
  let terminateInstancesStub;
  let createTagsStub;
  let keyPrefix;
  let regions;
  let houseKeeper;
  let tagger;
  let houseKeeperMock;

  before(async() => {
    // We want a clean DB state to verify things happen as we intend
    state = await main('state', {profile: 'test', process: 'test'});
    ec2 = await main('ec2', {profile: 'test', process: 'test'});
    let cfg = await main('cfg', {profile: 'test', process: 'test'});
    keyPrefix = cfg.app.keyPrefix;
    await state._runScript('drop-db.sql');
    await state._runScript('create-db.sql');
    regions = cfg.app.regions;
  });

  beforeEach(async() => {
    monitor = await main('monitor', {profile: 'test', process: 'test'});
    await state._runScript('clear-db.sql');

    describeInstancesStub = sandbox.stub();
    terminateInstancesStub = sandbox.stub();
    createTagsStub = sandbox.stub();

    // Since many of the tests will not need to specify custom input, we can set the default
    // behaviour of these stubs as returning empty objects. This way, only custom return statements
    // need to be specified in any of the tests.
    describeInstancesStub.returns({
      Reservations: [{
        Instances: [
        ],
      }],
    });

    async function runaws(service, method, params) {
      if (method === 'describeInstances') {
        return describeInstancesStub(service, method, params);
      } else if (method === 'terminateInstances') {
        return terminateInstancesStub(service, method, params);
      } else if (method === 'createTags') {
        return createTagsStub(service, method, params);
      } else {
        throw new Error('Only those two methods should be called, not ' + method);
      }
    }

    tagger = await main('tagger', {profile: 'test', process: 'test', runaws});

    houseKeeper = new HouseKeeper({
      ec2,
      state,
      regions,
      keyPrefix,
      monitor,
      runaws,
      tagger,
    });
    
    houseKeeperMock = sandbox.mock(houseKeeper);
  });

  after(async() => {
    await state._runScript('drop-db.sql');
  });

  afterEach(() => {
    sandbox.restore();
  });

  it('should remove instances not in api state', async() => {
    let status = 'pending-fulfillment';
    await state.insertInstance({
      id: 'i-1',
      workerType,
      region,
      instanceType,
      state: 'running',
      az,
      imageId,
      launched,
      lastEvent: new Date(),
    });
    await state.insertInstance({
      id: 'i-2',
      workerType,
      region,
      instanceType,
      state: 'running',
      az,
      imageId,
      launched,
      lastEvent: new Date(),
    });

    assume(await state.listInstances()).has.lengthOf(2);

    let outcome = await houseKeeper.sweep();
    assume(await state.listInstances()).has.lengthOf(0);
    assume(outcome[region]).deeply.equals({
      state: {
        missingInstances: 0,
        extraneousInstances: 2,
      },
      zombies: [],
    });
  });

  it('should add instances not in local state', async() => {
    assume(await state.listInstances()).has.lengthOf(0);

    describeInstancesStub.returns({
      Reservations: [{
        Instances: [{
          InstanceId: 'i-1',
          LaunchTime: new Date().toString(),
          KeyName: keyPrefix + workerType,
          InstanceType: instanceType,
          ImageId: 'ami-1',
          State: {
            Name: 'running',
          },
          Placement: {
            AvailabilityZone: az,
          },
        }],
      }],
    });

    let outcome = await houseKeeper.sweep();
    // Because we're returning the same thing for all regions, we need to check
    // that we've got one for each region
    assume(await state.listInstances()).has.lengthOf(regions.length);
    assume(outcome[region]).deeply.equals({
      state: {
        missingInstances: 1,
        extraneousInstances: 0,
      },
      zombies: [],
    });
  });

  it('should zombie kill', async() => {
    assume(await state.listInstances()).has.lengthOf(0);

    // We want to have one zombie in internal state and one not in internal state
    // but we want to kill both and delete the one in state
    await state.insertInstance({
      id: 'i-1',
      workerType,
      region,
      instanceType,
      state: 'running',
      az,
      imageId,
      launched,
      lastEvent: new Date(),
    });

    let oldAsMud = new Date();
    oldAsMud.setHours(oldAsMud.getHours() - 97);

    describeInstancesStub.returns({
      Reservations: [{
        Instances: [{
          InstanceId: 'i-1',
          LaunchTime: oldAsMud.toString(),
          KeyName: keyPrefix + workerType,
          InstanceType: instanceType,
          State: {
            Name: 'running',
          },
          ImageId: 'ami-1',
          Placement: {
            AvailabilityZone: az,
          },
        }, {
          InstanceId: 'i-2',
          LaunchTime: oldAsMud.toString(),
          KeyName: keyPrefix + workerType,
          InstanceType: instanceType,
          State: {
            Name: 'running',
          },
          ImageId: 'ami-1',
          Placement: {
            AvailabilityZone: az,
          },
        }],
      }],
    });

    let outcome = await houseKeeper.sweep();
    // Because we're returning the same thing for all regions, we need to check
    // that we've got one for each region
    assume(await state.listInstances()).has.lengthOf(0);
    assume(outcome[region]).deeply.equals({
      state: {
        missingInstances: 0,
        extraneousInstances: 0,
      },
      zombies: ['i-1', 'i-2'],
    });

    assume(terminateInstancesStub.callCount).equals(regions.length);
    for (let argSet of terminateInstancesStub.args) {
      assume(argSet[1] === 'terminateInstances');
      assume(argSet[2]).deeply.equals({
        InstanceIds: ['i-1', 'i-2'],
      });
    }
  });
});
