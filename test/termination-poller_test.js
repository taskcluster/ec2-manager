const testing = require('taskcluster-lib-testing');
const taskcluster = require('taskcluster-client');
const assume = require('assume');
const main = require('../lib/main');
const {api} = require('../lib/api');
const sinon = require('sinon');
const {TerminationPoller} = require('../lib/termination-poller');

function mockTermination(overrides) {
  let launchedTime = new Date();
  launchedTime.setMinutes(launchedTime.getMinutes() - 30);
  let terminatedTime = new Date(launchedTime);
  terminatedTime.setMinutes(terminatedTime.getMinutes() + 30);

  let termination = {
    id: 'i-1',
    workerType: 'example-worker',
    region: 'us-east-1',
    az: 'us-east-1a',
    instanceType: 'm5.xlarge',
    imageId: 'ami-1',
    launched: launchedTime,
    terminated: terminatedTime,
    lastEvent: new Date(),
  };

  Object.assign(termination, overrides);
  return termination;

}

describe('TerminationPoller', () => {
  let state;
  let sandbox = sinon.sandbox.create();
  let ec2;
  let cfg;
  let describeInstancesStub;

  before(async() => {
    // We want a clean DB state to verify things happen as we intend
    state = await main('state', {profile: 'test', process: 'test'});

    ec2 = {'us-east-1': 'us-east-1', 'us-east-2': 'us-east-2'};

    cfg = await main('cfg', {profile: 'test', process: 'test'});
    await state._runScript('drop-db.sql');
    await state._runScript('create-db.sql');
    regions = cfg.app.regions;
  });

  beforeEach(async() => {
    monitor = await main('monitor', {profile: 'test', process: 'test'});
    await state._runScript('clear-db.sql');

    describeInstancesStub = sandbox.stub();

    describeInstancesStub.returns(Promise.resolve({
      Reservations: [],
    }));

    poller = new TerminationPoller({
      ec2,
      state,
      runaws: describeInstancesStub,
      regions: cfg.app.regions,
    });

  });

  after(async() => {
    //await state._runScript('drop-db.sql');
  });

  afterEach(() => {
    sandbox.restore();
  });

  it('should be able to check for instances which have terminated', async() => {
    let terminations = [];

    for (let i = 1; i <= 2; i++) {
      let termination = mockTermination({id: 'i-' + i, region: 'us-east-' + i});
      await state.insertTermination(termination);
      terminations.push(termination);

      describeInstancesStub.withArgs(ec2['us-east-' + i], 'describeInstances', sinon.match.any).returns({
        Reservations: [{
          Instances: terminations.filter(x => x.region === 'us-east-' + i).map(t => {
            return {
              InstanceId: t.id,
              LaunchTime: t.launced,
              KeyName: 'key',
              InstanceType: t.instanceType,
              ImageId: t.imageId,
              State: {
                Name: 'terminated',
              },
              StateReason: {
                Message: 'code: message',
                Code: 'code',
              },
            };
          }),
        }],
      });

    }

    await poller.poll({touch: () => {}});

    let indb = await state.listTerminations();
    assume(indb).has.lengthOf(2);
  
    // Validate that the correct values are stored in the db
    for (let i = 0 ; i < terminations.length ; i++) {
      let t = terminations[i];
      let d = indb[i];

      // NOTE: remember that if we have "code: message" as the message that we
      // chop off the code from the message
      assume(d).has.property('code', 'code');
      assume(d).has.property('reason', 'message');
      assume(t).does.not.have.property('code');
      assume(t).does.not.have.property('reason');
      for (let prop of Object.keys(t).filter(x => x !== 'lastEvent')) {
        if (t[prop].constructor.name === 'Date') {
          assume(t[prop].getTime()).equals(d[prop].getTime());
        } else {
          assume(t[prop]).equals(d[prop]);
        } 
      }
    }
  });
});
