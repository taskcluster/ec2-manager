
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
  let describeSpotInstanceRequestsStub;
  let describeVolumesStub;
  let terminateInstancesStub;
  let createTagsStub;
  let keyPrefix;
  let regions;
  let houseKeeper;
  let tagger;
  let houseKeeperMock;

  before(async () => {
    // We want a clean DB state to verify things happen as we intend
    state = await main('state', {profile: 'test', process: 'test'});
    ec2 = await main('ec2', {profile: 'test', process: 'test'});
    let cfg = await main('cfg', {profile: 'test', process: 'test'});
    keyPrefix = cfg.app.keyPrefix;
    await state._runScript('drop-db.sql');
    await state._runScript('create-db.sql');
    regions = cfg.app.regions;
  });

  beforeEach(async () => {
    monitor = await main('monitor', {profile: 'test', process: 'test'});
    await state._runScript('clear-db.sql');

    describeInstancesStub = sandbox.stub();
    describeSpotInstanceRequestsStub = sandbox.stub();
    describeVolumesStub = sandbox.stub();
    terminateInstancesStub = sandbox.stub();
    createTagsStub = sandbox.stub();

    async function runaws(service, method, params) {
      if (method === 'describeInstances') {
        return describeInstancesStub(service, method, params);
      } else if (method === 'describeSpotInstanceRequests') {
        return describeSpotInstanceRequestsStub(service, method, params);
      } else if (method === 'describeVolumes') {
        return describeVolumesStub(service, method, params);
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

  afterEach(() => {
    sandbox.restore();
  });

  it('should remove instances and requests not in api state', async () => {
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
      lastevent: new Date(),
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
      lastevent: new Date(),
    });
    await state.insertSpotRequest({id: 'r-1', workerType, region, instanceType, state: 'open', status, az, imageId, created});
    await state.insertSpotRequest({id: 'r-2', workerType, region, instanceType, state: 'open', status, az, imageId, created});

    assume(await state.listInstances()).has.lengthOf(2);
    assume(await state.listSpotRequests()).has.lengthOf(2);

    describeInstancesStub.returns({
      Reservations: [{
        Instances: [
        ],
      }]
    });

    describeSpotInstanceRequestsStub.returns({
      SpotInstanceRequests: [
      ]
    });

    describeVolumesStub.returns({
      Volumes: [
      ]
    });

    let outcome = await houseKeeper.sweep();
    assume(await state.listInstances()).has.lengthOf(0);
    assume(await state.listSpotRequests()).has.lengthOf(0);
    assume(outcome[region]).deeply.equals({
      state: {
        missingRequests: 0,
        missingInstances: 0,
        extraneousRequests: 2,
        extraneousInstances: 2,
      },
      zombies: [],
    });
  });

  it('should add instances and requests not in local state', async () => {
    assume(await state.listInstances()).has.lengthOf(0);
    assume(await state.listSpotRequests()).has.lengthOf(0);

    describeInstancesStub.returns({
      Reservations: [{
        Instances: [{
          InstanceId: 'i-1',
          LaunchTime: new Date().toString(),
          KeyName: keyPrefix + workerType,
          InstanceType: instanceType,
          ImageId: 'ami-1',
          State: {
            Name: 'running'
          },
          SpotInstanceRequestId: 'r-10', // So that we don't delete the spot request
          Placement: {
            AvailabilityZone: az,
          },
        }],
      }]
    });

    describeSpotInstanceRequestsStub.returns({
      SpotInstanceRequests: [{
        SpotInstanceRequestId: 'r-1',
        CreateTime: new Date().toString(),
        LaunchSpecification: {
          KeyName: keyPrefix + workerType,
          InstanceType: instanceType,
          ImageId: 'ami-1',
          Placement: {
            AvailabilityZone: az,
          },
        },
        State: 'open',
        Status: {
          Code: 'pending-evaluation',
        },
      }]
    });

    describeVolumesStub.returns({
      Volumes: [
      ]
    });

    let outcome = await houseKeeper.sweep();
    // Because we're returning the same thing for all regions, we need to check
    // that we've got one for each region
    assume(await state.listInstances()).has.lengthOf(regions.length);
    assume(await state.listSpotRequests()).has.lengthOf(regions.length);
    assume(outcome[region]).deeply.equals({
      state: {
        missingRequests: 1,
        missingInstances: 1,
        extraneousRequests: 0,
        extraneousInstances: 0,
      },
      zombies: [],
    });
  });

  it('should tag instances and requests which arent tagged', async () => {
    assume(await state.listInstances()).has.lengthOf(0);
    assume(await state.listSpotRequests()).has.lengthOf(0);

    describeInstancesStub.returns({
      Reservations: [{
        Instances: [{
          InstanceId: 'i-1',
          LaunchTime: new Date().toString(),
          KeyName: keyPrefix + workerType,
          InstanceType: instanceType,
          State: {
            Name: 'running'
          },
          SpotInstanceRequestId: 'r-10', // So that we don't delete the spot request
          ImageId: 'ami-1',
          Placement: {
            AvailabilityZone: az,
          },
        }],
      }]
    });

    describeSpotInstanceRequestsStub.returns({
      SpotInstanceRequests: [{
        SpotInstanceRequestId: 'r-1',
        CreateTime: new Date().toString(),
        LaunchSpecification: {
          KeyName: keyPrefix + workerType,
          InstanceType: instanceType,
          ImageId: 'ami-1',
          Placement: {
            AvailabilityZone: az,
          },
        },
        State: 'open',
        Status: {
          Code: 'pending-evaluation',
        },
      }]
    });

    describeVolumesStub.returns({
      Volumes: [
      ]
    });

    let outcome = await houseKeeper.sweep();
    assume(createTagsStub.args[0][2].Resources).deeply.equals(['i-1', 'r-1']);
    assume(createTagsStub.args[0][2].Tags).deeply.equals([
      {Key: 'Name', Value: 'apiTest'},
      {Key: 'Owner', Value: 'ec2-manager-test'},
      {Key: 'WorkerType', Value: 'ec2-manager-test/apiTest'},
    ]);
  });

  it('should zombie kill', async () => {
    assume(await state.listInstances()).has.lengthOf(0);
    assume(await state.listSpotRequests()).has.lengthOf(0);

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
      lastevent: new Date(),
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
            Name: 'running'
          },
          SpotInstanceRequestId: 'r-10', // So that we don't delete the spot request
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
            Name: 'running'
          },
          SpotInstanceRequestId: 'r-10', // So that we don't delete the spot request
          ImageId: 'ami-1',
          Placement: {
            AvailabilityZone: az,
          },
        }],
      }]
    });

    describeSpotInstanceRequestsStub.returns({
      SpotInstanceRequests: []
    });

    describeVolumesStub.returns({
      Volumes: [
      ]
    });

    let outcome = await houseKeeper.sweep();
    // Because we're returning the same thing for all regions, we need to check
    // that we've got one for each region
    assume(await state.listInstances()).has.lengthOf(0);
    assume(await state.listSpotRequests()).has.lengthOf(0);
    assume(outcome[region]).deeply.equals({
      state: {
        missingRequests: 0,
        missingInstances: 0,
        extraneousRequests: 0,
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

  it('should call sweepVolumes exactly once', async() => {
    houseKeeperMock.expects("_sweepVolumes").exactly(regions.length);
    
    describeInstancesStub.returns({
      Reservations: [
      ]
    });
    
    describeSpotInstanceRequestsStub.returns({
      SpotInstanceRequests: [
      ]
    });
    
    await houseKeeper.sweep();
    houseKeeperMock.verify();
  });

  it('should not fail if no volume data is returned', async() => {
    houseKeeperMock.expects("_handleVolumeData").never();
    describeInstancesStub.returns({
      Reservations: [
      ]
    });
    
    describeSpotInstanceRequestsStub.returns({
      SpotInstanceRequests: [
      ]
    });
    
    describeVolumesStub.returns({
      Volumes: [
      ]
    });

    await houseKeeper.sweep();
    houseKeeperMock.verify();
  });
  
  it('should call handleVolumeData exactly once per volume', async() => {
    houseKeeperMock.expects("_handleVolumeData").twice();
      
    describeInstancesStub.returns({
      Reservations: [
      ]
    });
    
    describeSpotInstanceRequestsStub.returns({
      SpotInstanceRequests: [
      ]
    });

    describeVolumesStub.withArgs(sinon.match(function(value) {
      return value === ec2['us-west-2'] 
    })).returns({
       Volumes: [{
         Attachments: [],
         AvailabilityZone: 'us-west-2', 
         CreateTime: new Date().toString(), 
         Size: 8, 
         SnapshotId: "snap-1234567890abcdef0", 
         State: "in-use", 
         VolumeId: "vol-049df61146c4d7901", 
         VolumeType: "standard",
       }]
    });

    describeVolumesStub.withArgs(sinon.match(function(value) {
      return value === ec2['us-east-2']
    })).returns({
      Volumes: [{
        Attachments: [], 
        AvailabilityZone: 'us-east-2', 
        CreateTime: new Date().toString(), 
        Size: 16, 
        SnapshotId: "snap-1234567890abcdef09", 
        State: "in-use", 
        VolumeId: "vol-049df61146c4d7902", 
        VolumeType: "standard",
      }]
    });

    describeVolumesStub.returns({
      Volumes: [
      ]
    });
      
    await houseKeeper.sweep();
    houseKeeperMock.verify();
   });
});
