const testing = require('taskcluster-lib-testing');
const taskcluster = require('taskcluster-client');
const assume = require('assume');
const main = require('../lib/main');
const {api} = require('../lib/api');
const sinon = require('sinon');

describe('Api', () => {
  let state;
  let region = 'us-west-2';
  let instanceType = 'c3.xlarge';
  let workerType = 'apiTest';
  let az = 'us-west-2a';
  let launched = new Date();
  let imageId = 'ami-1';
  let client;
  let server;
  let sandbox = sinon.sandbox.create();
  let runaws;
  let regions;

  before(async() => {
    // We want a clean DB state to verify things happen as we intend
    state = await main('state', {profile: 'test', process: 'test'});
    await state._runScript('drop-db.sql');
    await state._runScript('create-db.sql');
    let cfg = await main('cfg', {profile: 'test', process: 'test'});
    regions = cfg.app.regions;

    testing.fakeauth.start({
      hasauth: ['*'],
    });

    let apiRef = api.reference({baseUrl: 'http://localhost:5555/v1'});
    let EC2Manager = taskcluster.createClient(apiRef);

    client = new EC2Manager({
      credentials: {
        clientId: 'hasauth',
        accessToken: 'abcde',
      },
    });
  });

  beforeEach(async() => {
    await state._runScript('clear-db.sql');
    runaws = sandbox.stub();
    server = await main('server', {profile: 'test', process: 'test', runaws});
  });

  after(async() => {
    testing.fakeauth.stop();
    await state._runScript('drop-db.sql');
  });

  afterEach(() => {
    server.terminate();
    sandbox.restore();
  });

  it('api comes up', async() => {
    let result = await client.ping();
    assume(result).has.property('alive', true);
  });

  it('should list worker types', async() => {
    let status = 'pending-evaluation';
    await state.insertInstance({
      id: 'i-1',
      workerType: 'w-1',
      region,
      instanceType,
      state: 'running',
      az,
      launched,
      imageId,
      lastEvent: new Date(),
    });
    await state.insertInstance({
      id: 'i-2',
      workerType: 'w-2',
      region,
      instanceType,
      state: 'pending',
      az,
      launched,
      imageId,
      lastEvent: new Date(),
    });
    let result = await client.listWorkerTypes();
    assume(result).deeply.equals(['w-1', 'w-2']);
  });

  it('should show instance counts', async() => {
    let status = 'pending-evaluation';
    await state.insertInstance({
      id: 'i-1',
      workerType: 'w-1',
      region,
      instanceType,
      state: 'running',
      az,
      launched,
      imageId,
      lastEvent: new Date(),
    });
    await state.insertInstance({
      id: 'i-2',
      workerType: 'w-1',
      region,
      instanceType,
      state: 'pending',
      az,
      launched,
      imageId,
      lastEvent: new Date(),
    });
    let result = await client.workerTypeStats('w-1');
    assume(result).deeply.equals({
      pending: [{
        instanceType,
        count: 1,
        type: 'instance',
      }],
      running: [{
        instanceType,
        count: 1,
        type: 'instance',
      }],
    });
  });

  describe('requesting resources', () => {
    // TODO: Rewrite this set of tests for runInstance
    let ClientToken;
    let Region;
    let SpotPrice;
    let LaunchSpecification;

    beforeEach(() => {
      ClientToken = 'client-token';
      Region = region;

      LaunchInfo = {
        KeyName: `ec2-manager-test:${workerType}:ffe27db`,
        ImageId: 'ami-1',
        InstanceType: instanceType,
        SecurityGroups: [],
        Placement: {
          AvailabilityZone: az,
        },
      };

      runaws.returns({
        Instances: [{
          InstanceId: 'i-1',
          Placement: {
            AvailabilityZone: az,
          },
          InstanceType: instanceType,
          LaunchTime: launched.toString(),
          ImageId: imageId,
          State: {
            Name: 'pending',
          },
        }],
      });
    });

    it('should be able to request an on-demand instance', async() => {
      await client.runInstance(workerType, {
        ClientToken,
        Region,
        RequestType: 'on-demand',
        LaunchInfo,
      });

      assume(runaws.callCount).equals(1);
      assume(runaws.firstCall.args[0].config.region).equals(region);
      assume(runaws.firstCall.args[1]).equals('runInstances');
      assume(runaws.firstCall.args[2]).deeply.equals({
        ClientToken,
        MaxCount: 1,
        MinCount: 1,
        TagSpecifications: [
          {
            ResourceType: 'instance', Tags: [
              {Key: 'Name', Value: 'apiTest'},
              {Key: 'Owner', Value: 'ec2-manager-test'},
              {Key: 'WorkerType', Value: 'ec2-manager-test/apiTest'},
            ],
          },
          {
            ResourceType: 'volume', Tags: [
              {Key: 'Name', Value: 'apiTest'},
              {Key: 'Owner', Value: 'ec2-manager-test'},
              {Key: 'WorkerType', Value: 'ec2-manager-test/apiTest'},
            ],
          },
        ],
        KeyName: `ec2-manager-test:${workerType}:ffe27db`,
        ImageId: 'ami-1',
        InstanceType: instanceType,
        SecurityGroups: [],
        Placement: {
          AvailabilityZone: az,
        },
      });

      let instances = await state.listInstances();
      assume(instances).has.lengthOf(1);
    });
    
    it('should be able to request a spot request at default price', async() => {
      await client.runInstance(workerType, {
        ClientToken,
        Region,
        RequestType: 'spot',
        LaunchInfo,
      });

      assume(runaws.callCount).equals(1);
      assume(runaws.firstCall.args[0].config.region).equals(region);
      assume(runaws.firstCall.args[1]).equals('runInstances');
      assume(runaws.firstCall.args[2]).deeply.equals({
        ClientToken,
        MaxCount: 1,
        MinCount: 1,
        InstanceMarketOptions: {
          MarketType: 'spot',
          SpotOptions: {
            SpotInstanceType: 'one-time',
          },
        },
        TagSpecifications: [
          {
            ResourceType: 'instance', Tags: [
              {Key: 'Name', Value: 'apiTest'},
              {Key: 'Owner', Value: 'ec2-manager-test'},
              {Key: 'WorkerType', Value: 'ec2-manager-test/apiTest'},
            ],
          },
          {
            ResourceType: 'volume', Tags: [
              {Key: 'Name', Value: 'apiTest'},
              {Key: 'Owner', Value: 'ec2-manager-test'},
              {Key: 'WorkerType', Value: 'ec2-manager-test/apiTest'},
            ],
          },
        ],
        KeyName: `ec2-manager-test:${workerType}:ffe27db`,
        ImageId: 'ami-1',
        InstanceType: instanceType,
        SecurityGroups: [],
        Placement: {
          AvailabilityZone: az,
        },
      });

      let instances = await state.listInstances();
      assume(instances).has.lengthOf(1);
    });

    it('should be able to request a spot request at a specific price', async() => {
      await client.runInstance(workerType, {
        ClientToken,
        Region,
        RequestType: 'spot',
        SpotPrice: 0.5,
        LaunchInfo,
      });

      assume(runaws.callCount).equals(1);
      assume(runaws.firstCall.args[0].config.region).equals(region);
      assume(runaws.firstCall.args[1]).equals('runInstances');
      assume(runaws.firstCall.args[2]).deeply.equals({
        ClientToken,
        MaxCount: 1,
        MinCount: 1,
        InstanceMarketOptions: {
          MarketType: 'spot',
          SpotOptions: {
            SpotInstanceType: 'one-time',
            MaxPrice: '0.5',
          },
        },
        TagSpecifications: [
          {
            ResourceType: 'instance', Tags: [
              {Key: 'Name', Value: 'apiTest'},
              {Key: 'Owner', Value: 'ec2-manager-test'},
              {Key: 'WorkerType', Value: 'ec2-manager-test/apiTest'},
            ],
          },
          {
            ResourceType: 'volume', Tags: [
              {Key: 'Name', Value: 'apiTest'},
              {Key: 'Owner', Value: 'ec2-manager-test'},
              {Key: 'WorkerType', Value: 'ec2-manager-test/apiTest'},
            ],
          },
        ],
        KeyName: `ec2-manager-test:${workerType}:ffe27db`,
        ImageId: 'ami-1',
        InstanceType: instanceType,
        SecurityGroups: [],
        Placement: {
          AvailabilityZone: az,
        },
      });

      let instances = await state.listInstances();
      assume(instances).has.lengthOf(1);
    });
  });

  describe('managing resources', () => {
    beforeEach(async() => {
      let status = 'pending-fulfillment';
      await state.insertInstance({
        id: 'i-1',
        workerType,
        region: 'us-east-1',
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
        region: 'us-west-1',
        instanceType,
        state: 'running',
        az,
        imageId,
        launched,
        lastEvent: new Date(),
      });
      await state.insertInstance({
        id: 'i-3',
        workerType,
        region: 'us-west-2',
        instanceType,
        state: 'pending',
        az,
        imageId,
        launched,
        lastEvent: new Date(),
      });
    });

    it('should be able to kill all of a worker type', async() => {
      let result = await client.terminateWorkerType(workerType); 

      // Lengthof doesn't seem to work here.  oh well
      assume(runaws.args).has.property('length', 3);
      for (let call of runaws.args) {
        let region = call[0].config.region;
        let endpoint = call[1];
        let obj = call[2];
        assume(endpoint).equals('terminateInstances');

        if (region === 'us-east-1') {
          assume(obj.InstanceIds).deeply.equals(['i-1']);
        } else if (region === 'us-west-1') {
          assume(obj.InstanceIds).deeply.equals(['i-2']);
        } else if (region === 'us-west-2') {
          assume(obj.InstanceIds).deeply.equals(['i-3']);
        }
      }
      
      let instances = await state.listInstances();
      assume(instances).has.lengthOf(0);
    });

    it('should be able to kill a single instance', async() => {
      runaws.returns({
        TerminatingInstances: [{
          PreviousState: {Name: 'pending'},
          CurrentState: {Name: 'shutting-down'},
        }],
      });
      let result = await client.terminateInstance('us-east-1', 'i-1');
      assume(result).has.property('current', 'shutting-down');
      assume(result).has.property('previous', 'pending');
      assume(runaws.callCount).equals(1);
      let instances = await state.listInstances({id: 'i-1'});
      assume(instances).has.lengthOf(0);
    });
    
  });

  describe('managing key pairs', () => {
    it('should create and delete keypairs idempotently', async() => {
      // We want the following cases covered:
      // 1. nothing exists in internal cache or ec2 --> create
      // 2. it exists in internal cache --> short circuit return
      // 3. it exists in ec2, not internal --> only describe call
      // 4. it deletes properly if key exists in ec2
      // 5. it deletes properly if key does not exist in ec2

      // Case 1
      runaws.returns({
        KeyPairs: [],
      });
      await client.ensureKeyPair(workerType);
      assume(runaws.callCount).equals(regions.length * 2);
      runaws.reset();

      // Case 2
      await client.ensureKeyPair(workerType);
      assume(runaws.callCount).equals(0);
      runaws.reset();

      // Case 4
      runaws.returns({
        KeyPairs: ['placeholder'],
      });
      await client.removeKeyPair(workerType);
      assume(runaws.callCount).equals(regions.length * 2);
      runaws.reset();

      // Case 5
      runaws.returns({
        KeyPairs: [],
      });
      await client.removeKeyPair(workerType);
      assume(runaws.callCount).equals(regions.length);
      runaws.reset();

      // Case 3 (we do this here so it was deleted from internal cache in
      // remove* calls above
      runaws.returns({
        KeyPairs: ['placeholder'],
      });
      await client.ensureKeyPair(workerType);
      assume(runaws.callCount).equals(regions.length);
    });
  });

  // These are functions which are supposed to be used for debugging and
  // troubleshooting primarily.  Maybe some ui stuff?
  describe('internal api', () => {
    it('should list regions', async() => {
      let result = await client.regions();
      result.regions.sort();
      assume(result.regions).deeply.equals(regions.sort());
    });

    it('should list AMI usage', async() => {
      await state.reportAmiUsage({
        region: region,
        id: imageId,
      });
      let result = await client.amiUsage();
      assume(result).has.lengthOf(1);
      assume(result[0]).has.property('region', region);
      assume(result[0]).has.property('id', imageId);
    });
    
    it('should list EBS usage', async() => {
      await state.reportEbsUsage([{
        region: region,
        volumetype: 'standard',
        state: 'active',
        totalcount: 1,
        totalgb: 8,
      }]);
      let result = await client.ebsUsage();
      assume(result).has.lengthOf(1);
      assume(result[0]).has.property('volumetype', 'standard');
      assume(result[0]).has.property('state', 'active');
      assume(result[0]).has.property('totalcount', 1);
      assume(result[0]).has.property('totalgb', 8);
    });
  });
});
