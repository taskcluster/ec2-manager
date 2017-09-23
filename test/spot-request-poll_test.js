const {SpotRequestPoller} = require('../lib/spot-request-poller');
const sinon = require('sinon');
const main = require('../lib/main');
const assume = require('assume');

describe('Spot Request Polling', () => {
  let state;
  let sandbox = sinon.sandbox.create();
  let defaultSR;

  before(async () => {
    // We want a clean DB state to verify things happen as we intend
    state = await main('state', {profile: 'test', process: 'test'});
  });

  beforeEach(async () => {
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

  it('constructs the SpotRequestPoller', async () => {
    const poller = new SpotRequestPoller({ec2: {}, regions: [defaultSR.region], state, runaws: () => {}});
  });

  it('fails to construct with non-list region', async () => {
    try {
      const poller = new SpotRequestPoller({ec2: {}, regions: defaultSR.region, state, runaws: () => {}});
      return Promise.reject(Error('Line should not be reached'));
    } catch(e) { }
  });

  it('fails to construct with list of non-string region', async () => {
    try {
      const poller = new SpotRequestPoller({ec2: {}, regions: [0, 1, 2], state, runaws: () => {}});
      return Promise.reject(Error('Line should not be reached'));
    } catch(e) { }
  });

  it('successfully polls with a valid region', async () => {
    const poller = new SpotRequestPoller({ec2: {}, regions: [defaultSR.region], state, runaws: () => {}});
    await poller._poll('foobar');
  });

  it('fails to _poll with an invalid region', async () => {
    const poller = new SpotRequestPoller({ec2: {}, regions: [defaultSR.region], state, runaws: () => {}});

    try { 
      await poller._poll(5);
      return Promise.reject(Error('Line should not be reached'));
    } catch (e) { }
  });

  it('no outstanding spot requests', async () => {
    const poller = new SpotRequestPoller({ec2: {}, regions: [defaultSR.region], state, runaws: () => {}});
    await poller.poll();
  });

  it('one open spot request without change', async () => {
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
          Code: 'pending-fulfillment'
        }
      }]
    }));

    const poller = new SpotRequestPoller({ec2: {}, regions: [defaultSR.region], state, runaws: mock});
    await poller.poll();

    assume(mock.callCount).equals(1);
    assume(mock.firstCall.args[1]).equals('describeSpotInstanceRequests');

    requests = await state.listSpotRequests();
    assume(requests).lengthOf(1);
  });

  it('pending-evaluation -> pending-fulfillment', async () => {
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
          Code: 'pending-fulfillment'
        }
      }]
    }));

    const poller = new SpotRequestPoller({ec2: {}, regions: [defaultSR.region], state, runaws: mock});
    await poller.poll();
    assume(mock.callCount).equals(1);
    assume(mock.firstCall.args[1]).equals('describeSpotInstanceRequests');

    requests = await state.listSpotRequests();
    assume(requests).lengthOf(1);
    assume(requests[0]).has.property('status', 'pending-fulfillment');
  });

  it('pending-evaluation -> price-too-low', async () => {
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
          Code: 'price-too-low'
        }
      }]
    }));

    mock.onCall(1).returns(Promise.resolve({
      CancelledSpotInstanceRequests: [{
        SpotInstanceRequestId: defaultSR.id,
        State: 'open',
      }]
    }));

    const poller = new SpotRequestPoller({ec2: {}, regions: [defaultSR.region], state, runaws: mock});
    await poller.poll();

    assume(mock.callCount).equals(2);
    assume(mock.firstCall.args[1]).equals('describeSpotInstanceRequests');
    assume(mock.secondCall.args[1]).equals('cancelSpotInstanceRequests');
    assume(mock.secondCall.args[2]).deeply.equals({
      SpotInstanceRequestIds: [defaultSR.id],
    });

    requests = await state.listSpotRequests();
    assume(requests).lengthOf(0);
  });

  it('pending-evaluation status -> active state', async () => {
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
          Code: 'fulfilled'
        }
      }]
    }));

    const poller = new SpotRequestPoller({ec2: {}, regions: [defaultSR.region], state, runaws: mock});
    await poller.poll();

    assume(mock.callCount).equals(1);
    assume(mock.firstCall.args[1]).equals('describeSpotInstanceRequests');

    requests = await state.listSpotRequests();
    assume(requests).lengthOf(0);
  });
});
