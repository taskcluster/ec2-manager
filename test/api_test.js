const testing = require('taskcluster-lib-testing');
const taskcluster = require('taskcluster-client');
const assume = require('assume');
const main = require('../lib/main');
const {builder} = require('../lib/api');
const sinon = require('sinon');
const uuid = require('uuid');

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

  let dbWorks = false;

  before(async () => {
    // We want a clean DB state to verify things happen as we intend
    state = await main('state', {profile: 'test', process: 'test'});
    await state._runScript('drop-db.sql');
    await state._runScript('create-db.sql');
    let cfg = await main('cfg', {profile: 'test', process: 'test'});
    regions = cfg.app.regions;

    testing.fakeauth.start({
      hasauth: ['*'],
    }, {
      rootUrl: 'http://localhost:5555/',
    });

    let apiRef = builder.reference({baseUrl: 'http://localhost:5555/v1'});
    let EC2Manager = taskcluster.createClient(apiRef);

    client = new EC2Manager({
      rootUrl: 'http://localhost:5555',
      credentials: {
        clientId: 'hasauth',
        accessToken: 'abcde',
      },
    });
  });

  beforeEach(async () => {
    state = await main('state', {profile: 'test', process: 'test'});
    await state._runScript('clear-db.sql');
    runaws = sandbox.stub();
    server = await main('server', {profile: 'test', process: 'test', runaws});
  });

  after(async () => {
    testing.fakeauth.stop();
    await state._runScript('drop-db.sql');
  });

  afterEach(() => {
    // Make sure we're not dropping client references
    for (let client of state._pgpool._clients) {
      try {
        client.release();
        let lastQuery = (client._activeQuery || {}).text;
        let err = new Error('Leaked a client that last executed: ' + lastQuery);
        err.client = client;
        throw err;
      } catch (err) {
        if (!/Release called on client which has already been released to the pool/.exec(err.message)) {
          throw err;
        }
      }
    }
    if (server) {
      server.terminate();
    }
    sandbox.restore();
  });

  it('api comes up', async () => {
    let result = await client.ping();
    assume(result).has.property('alive', true);
  });

  it('should list worker types', async () => {
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

  it('should show instance counts', async () => {
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

  describe('requesting resources (mock)', () => {
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

    it('should be able to request an on-demand instance (mock)', async () => {
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
    
    it('should be able to request a spot request at default price (mock)', async () => {
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

    it('should be able to request a spot request at a specific price (mock)', async () => {
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
    beforeEach(async () => {
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

    it('should be able to kill all of a worker type (mock)', async () => {
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
    });

    it('should be able to kill a single instance (mock)', async () => {
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
    });
    
  });

  describe('health and error reporting', () => {
    it('should give a valid report when theres no state', async () => {
      let result = await client.getHealth();
    });

    async function insertThings(instOW, termOW, reqOW) {
      await state.insertInstance(Object.assign({}, {
        id: 'i-1',
        workerType: 'example-workertype',
        region: 'us-west-1',
        az: 'us-west-1z',
        imageId: 'ami-1',
        instanceType: 'm1.medium',
        state: 'pending',
        launched: new Date(),
        lastEvent: new Date(),
      }, instOW));

      await state.insertTermination(Object.assign({}, {
        id: 'i-2',
        workerType: 'example-workertype',
        region: 'us-west-1',
        az: 'us-west-1z',
        imageId: 'ami-1',
        instanceType: 'm1.medium',
        code: 'Client.InstanceInitiatedShutdown',
        reason: 'Client.InstanceInitiatedShutdown: Instance initiated shutdown',
        terminated: new Date(),
        launched: new Date(),
        lastEvent: new Date(),
      }, termOW));

      await state.logAWSRequest(Object.assign({}, {
        region: 'us-east-1',
        requestId: uuid.v4(),
        duration: 100,
        method: 'runInstances',
        service: 'ec2',
        error: false,
        called: new Date(),
        workerType: 'example-workertype',
        az: 'us-east-1z',
        instanceType: 'm1.medium',
        imageId: 'ami-1',
      }, reqOW));
      
    }

    it('should report global health with empty state', async () => {
      await insertThings({}, {}, {});
      let result = await client.getHealth();
    });

    it('should report recent errors', async () => {
      let termOW = {
        code: 'Server.InternalError',
        reason: 'reason',
      };
      let errorOW = {
        error: true,
        code: 'code',
        message: 'msg',
      };
      await insertThings({}, termOW, errorOW); 
      let result = await client.getRecentErrors();
      assume(result.errors).has.lengthOf(2);
      assume(result.errors[0]).has.property('type');
      assume(result.errors[0]).has.property('code');
      assume(result.errors[0]).has.property('time');
      assume(result.errors[0]).has.property('region');
      assume(result.errors[0]).has.property('az');
      assume(result.errors[0]).has.property('instanceType');
      assume(result.errors[0]).has.property('workerType');
      assume(result.errors[0]).has.property('message');

    });
    
    it('should report recent errors of a specific worker type', async () => {
      let termOW = {
        code: 'Server.InternalError',
        reason: 'reason',
      };
      let errorOW = {
        error: true,
        code: 'code',
        message: 'msg',
      };
      await insertThings({}, termOW, errorOW); 
      let result = await client.workerTypeErrors('example-workertype');
      assume(result.errors).has.lengthOf(2);
      assume(result.errors[0]).has.property('type');
      assume(result.errors[0]).has.property('code');
      assume(result.errors[0]).has.property('time');
      assume(result.errors[0]).has.property('region');
      assume(result.errors[0]).has.property('az');
      assume(result.errors[0]).has.property('instanceType');
      assume(result.errors[0]).has.property('workerType');
      assume(result.errors[0]).has.property('message');

    });
   
    it('should give a valid report with state for a specific worker type', async () => {
      let ow = {workerType: 'has-stuff'};
      await insertThings(ow, ow, ow); 
      let result = await client.workerTypeHealth('has-stuff');
      assume(result).has.property('requestHealth');
      assume(result).has.property('terminationHealth');
      assume(result).has.property('running');
      assume(result.requestHealth).has.lengthOf(1);
      assume(result.terminationHealth).has.lengthOf(1);
      assume(result.running).has.lengthOf(1);
    }); 

    it('should give a valid report without confusing worker types', async () => {
      let ow = {workerType: 'has-stuff'};
      await insertThings(ow, ow, ow); 
      let result = await client.workerTypeHealth('has-no-stuff');
      assume(result).has.property('requestHealth');
      assume(result).has.property('terminationHealth');
      assume(result).has.property('running');
      assume(result.requestHealth).has.lengthOf(0);
      assume(result.terminationHealth).has.lengthOf(0);
      assume(result.running).has.lengthOf(0);
    });

  });

  describe('managing key pairs', () => {
    it('should create and delete keypairs idempotently (mock)', async () => {

      // Let's create a key pair
      runaws.returns(Promise.resolve({
        KeyPairs: [],
      }));
      await client.ensureKeyPair('test', {pubkey: 'ssh-rsa fakekey'});
      assume(runaws.callCount).equals(regions.length * 2);
      runaws.reset();

      // Let's create a key pair when there's already a keypair
      // present with that name
      runaws.returns(Promise.resolve({
        KeyPairs: ['test'],
      }));
      await client.ensureKeyPair('test', {pubkey: 'ssh-rsa fakekey'});
      assume(runaws.callCount).equals(regions.length);
      runaws.reset();

      // Now let's remove it
      runaws.returns({
        KeyPairs: ['test'],
      });
      await client.removeKeyPair('test');
      assume(runaws.callCount).equals(regions.length * 2);
      runaws.reset();

      // Now let's remove it, but find it's already gone
      runaws.returns({
        KeyPairs: [],
      });
      await client.removeKeyPair('test');
      assume(runaws.callCount).equals(regions.length);
      runaws.reset();
    });
  });

  // These are functions which are supposed to be used for debugging and
  // troubleshooting primarily.  Maybe some ui stuff?
  describe('internal api', () => {
    it('should list regions', async () => {
      let result = await client.regions();
      result.regions.sort();
      assume(result.regions).deeply.equals(regions.sort());
    });

    it('should list AMI usage', async () => {
      await state.reportAmiUsage({
        region: region,
        id: imageId,
      });
      let result = await client.amiUsage();
      assume(result).has.lengthOf(1);
      assume(result[0]).has.property('region', region);
      assume(result[0]).has.property('id', imageId);
    });
  });
});
