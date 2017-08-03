const subject = require('../lib/spot-request-poll');
const {pollSpotRequests} = subject;
const sinon = require('sinon');
const main = require('../lib/main');
const assume = require('assume');

describe('Spot Request Polling', () => {
  let state;
  let sandbox = sinon.sandbox.create();
  let defaultSR;

  before(async() => {
    // We want a clean DB state to verify things happen as we intend
    state = await main('state', {profile: 'test', process: 'test'});
  });

  beforeEach(async() => {
    await state._runScript('clear-db.sql');
    defaultSR = {
      id: 'r-1',
      workerType: 'example-workertype',
      region: 'us-west-1',
      az: 'us-west-1z',
      imageId: 'ami-1',
      instanceType: 'm1.medium',
      state: 'open',
      status: 'pending-fulfillment',
      created: new Date(),
    };
  });

  afterEach(() => {
    sandbox.restore();
  });

  it('no outstanding spot requests', async() => {
    await pollSpotRequests({ec2: {}, region: defaultSR.region, state, runaws: () => {}});
  });

  it('one open spot request without change', async() => {
    await state.insertSpotRequest(Object.assign({}, defaultSR, {
      state: 'open',
      status: 'pending-fulfillment',
    }));

    let requests = await state.listSpotRequests();
    assume(requests).lengthOf(1);

    let mock = sandbox.stub();
    mock.onCall(0).returns(Promise.resolve({
      SpotInstanceRequests: [{
        SpotInstanceRequestId: defaultSR.id,
        State: 'open',
        Status: {
          Code: 'pending-fulfillment',
        },
      }],
    }));

    await pollSpotRequests({ec2: {}, region: defaultSR.region, state, runaws: mock});
    assume(mock.callCount).equals(1);
    assume(mock.firstCall.args[1]).equals('describeSpotInstanceRequests');

    requests = await state.listSpotRequests();
    assume(requests).lengthOf(1);
  });
  
  it('pending-evaluation -> pending-fulfillment', async() => {
    await state.insertSpotRequest(Object.assign({}, defaultSR, {
      state: 'open',
      status: 'pending-evaluation',
    }));

    let requests = await state.listSpotRequests();
    assume(requests).lengthOf(1);

    let mock = sandbox.stub();
    mock.onCall(0).returns(Promise.resolve({
      SpotInstanceRequests: [{
        SpotInstanceRequestId: defaultSR.id,
        State: 'open',
        Status: {
          Code: 'pending-fulfillment',
        },
      }],
    }));

    await pollSpotRequests({ec2: {}, region: defaultSR.region, state, runaws: mock});
    assume(mock.callCount).equals(1);
    assume(mock.firstCall.args[1]).equals('describeSpotInstanceRequests');

    requests = await state.listSpotRequests();
    assume(requests).lengthOf(1);
    assume(requests[0]).has.property('status', 'pending-fulfillment');
  });

  it('pending-evaluation -> price-too-low', async() => {
    await state.insertSpotRequest(Object.assign({}, defaultSR, {
      state: 'open',
      status: 'pending-evaluation',
    }));

    let requests = await state.listSpotRequests();
    assume(requests).lengthOf(1);

    let mock = sandbox.stub();
    mock.onCall(0).returns(Promise.resolve({
      SpotInstanceRequests: [{
        SpotInstanceRequestId: defaultSR.id,
        State: 'open',
        Status: {
          Code: 'price-too-low',
        },
      }],
    }));

    mock.onCall(1).returns(Promise.resolve({
      CancelledSpotInstanceRequests: [{
        SpotInstanceRequestId: defaultSR.id,
        State: 'open',
      }],
    }));

    await pollSpotRequests({ec2: {}, region: defaultSR.region, state, runaws: mock});
    assume(mock.callCount).equals(2);
    assume(mock.firstCall.args[1]).equals('describeSpotInstanceRequests');
    assume(mock.secondCall.args[1]).equals('cancelSpotInstanceRequests');
    assume(mock.secondCall.args[2]).deeply.equals({
      SpotInstanceRequestIds: [defaultSR.id],
    });

    requests = await state.listSpotRequests();
    assume(requests).lengthOf(0);
  });

  it('pending-evaluation status -> active state', async() => {
    await state.insertSpotRequest(Object.assign({}, defaultSR, {
      state: 'open',
      status: 'pending-evaluation',
    }));

    let requests = await state.listSpotRequests();
    assume(requests).lengthOf(1);

    let mock = sandbox.stub();
    mock.onCall(0).returns(Promise.resolve({
      SpotInstanceRequests: [{
        SpotInstanceRequestId: defaultSR.id,
        State: 'active',
        Status: {
          Code: 'fulfilled',
        },
      }],
    }));

    await pollSpotRequests({ec2: {}, region: defaultSR.region, state, runaws: mock});
    assume(mock.callCount).equals(1);
    assume(mock.firstCall.args[1]).equals('describeSpotInstanceRequests');

    requests = await state.listSpotRequests();
    assume(requests).lengthOf(0);
  });
});
