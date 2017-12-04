const {SpotRequestPoller} = require('../lib/spot-request-poller');
const sinon = require('sinon');
const main = require('../lib/main');
const assume = require('assume');

describe('Spot Request Poller', () => {
  let state;
  let ec2;
  let sandbox = sinon.sandbox.create();
  let defaultSR;

  before(async() => {
    // We want a clean DB state to verify things happen as we intend
    state = await main('state', {profile: 'test', process: 'test'});
    ec2 = await main('ec2', {profile: 'test', process: 'test'});
    await state._runScript('drop-db.sql');
    await state._runScript('create-db.sql');
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

  after(async() => {
    await state._runScript('drop-db.sql');
  });

  afterEach(() => {
    sandbox.restore();
  });

  describe('constructor', () => {

    it('succeeds with valid args', () => {
      const poller = new SpotRequestPoller({ec2, regions: [defaultSR.region], state, runaws: () => {}, monitor: {}});
    });

    it('fails with non-list region arg', () => {
      try {
        const poller = new SpotRequestPoller({ec2, regions: defaultSR.region, state, runaws: () => {}, monitor: {}});
        return Promise.reject(Error('Line should not be reached'));
      } catch (e) { }
    });

    it('fails with list containing non-string regions', () => {
      try {
        const poller = new SpotRequestPoller({
          ec2,
          regions: [defaultSR.region, 1],
          state,
          runaws: () => {},
          monitor: {},
        });
        return Promise.reject(Error('Line should not be reached'));
      } catch (e) { }
    });

  });

  describe('_poll helper method', () => {

    it('succeeds with a valid region', async() => {
      const poller = new SpotRequestPoller({ec2, regions: [defaultSR.region], state, runaws: () => {}, monitor: {}});
      await poller._poll('foobar');
    });

    it('fails with an invalid region', async() => {
      const poller = new SpotRequestPoller({ec2, regions: [defaultSR.region], state, runaws: () => {}, monitor: {}});

      try { 
        await poller._poll(5);
        return Promise.reject(Error('Line should not be reached'));
      } catch (e) { }
    });

  });

  describe('polling', () => {

    it('succeeds with no outstanding spot requests', async() => {
      const poller = new SpotRequestPoller({ec2, regions: [defaultSR.region], state, runaws: () => {}, monitor: {}});
      await poller.poll();
    });

    it('succeeds with one open spot request without change', async() => {
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

      const poller = new SpotRequestPoller({ec2, regions: [defaultSR.region], state, runaws: mock, monitor: {}});
      await poller.poll();

      assume(mock.callCount).equals(1);
      assume(mock.firstCall.args[1]).equals('describeSpotInstanceRequests');

      requests = await state.listSpotRequests();
      assume(requests).lengthOf(1);
    });

    it('succeeds with pending-evaluation -> pending-fulfillment', async() => {
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

      const poller = new SpotRequestPoller({ec2, regions: [defaultSR.region], state, runaws: mock, monitor: {}});
      await poller.poll();
      assume(mock.callCount).equals(1);
      assume(mock.firstCall.args[1]).equals('describeSpotInstanceRequests');

      requests = await state.listSpotRequests();
      assume(requests).lengthOf(1);
      assume(requests[0]).has.property('status', 'pending-fulfillment');
    });

    it('succeeds with pending-evaluation -> price-too-low', async() => {
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

      const poller = new SpotRequestPoller({ec2, regions: [defaultSR.region], state, runaws: mock, monitor: {}});
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

    it('succeeds with pending-evaluation status -> active state', async() => {
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

      const poller = new SpotRequestPoller({ec2, regions: [defaultSR.region], state, runaws: mock, monitor: {}});
      await poller.poll();

      assume(mock.callCount).equals(1);
      assume(mock.firstCall.args[1]).equals('describeSpotInstanceRequests');

      requests = await state.listSpotRequests();
      assume(requests).lengthOf(0);
    });

  });
});
