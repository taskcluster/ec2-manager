const {AmiExistencePoller} = require('../lib/ami-existence-poller');
const sinon = require('sinon');
const main = require('../lib/main');
const assume = require('assume');

describe('AMI Existence Poller', () => {
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

  describe('constructor', () => {
    it('succeeds with valid args', () => {
      const poller = new AmiExistencePoller({ec2: {}, state, runaws: () => {}});
    });
  });

  describe('polling', () => {

    it('throws with an AMI not in the database', async () => {
      const mock = sandbox.stub();
      mock.onCall(0).returns(Promise.resolve({
        Images: [{
          ImageId: defaultSR.imageId,
          State: 'available',
        }],
      }));

      const poller = new AmiExistencePoller({ec2: {}, state, runaws: mock});
      try {
        await poller.poll(defaultSR.imageId);
        return Promise.reject(Error('Line should not be reached'));
      } catch(e) { }
    });

    describe('with the ami present in the database', () => {

      beforeEach(async () => {
        await state.reportAmiUsage({
          region: defaultSR.region,
          id: defaultSR.imageId,
        });

        const images = await state.listAmiUsage();
        assume(images).lengthOf(1);
      });

      it('succeeds with an AMI available on EC2', async () => {
        const mock = sandbox.stub();
        mock.onCall(0).returns(Promise.resolve({
          Images: [{
            ImageId: defaultSR.imageId,
            State: 'available',
          }],
        }));

        const poller = new AmiExistencePoller({ec2: {}, state, runaws: mock});

        assume(await poller.poll(defaultSR.imageId)).true();
        assume(mock.callCount).equals(1);
        assume(mock.firstCall.args[1]).equals('describeImages');
      });

      it('throws with an invalid AMI', async () => {
        const mock = sandbox.stub();
        mock.onCall(0).returns(Promise.resolve({
          Images: [{
            ImageId: defaultSR.imageId,
            State: 'available',
          }],
        }));

        const poller = new AmiExistencePoller({ec2: {}, state, runaws: mock});
        try {
          await poller.poll(53);
          return Promise.reject(Error('Line should not be reached'));
        } catch(e) { }
      });

      it('fails with an AMI in EC2 but invalid', async () => {
        const mock = sandbox.stub();
        mock.onCall(0).returns(Promise.resolve({
          Images: [{
            ImageId: defaultSR.imageId,
            State: 'invalid',
          }],
        }));

        const poller = new AmiExistencePoller({ec2: {}, state, runaws: mock});

        assume(await poller.poll(defaultSR.imageId)).false();
        assume(mock.callCount).equals(1);
        assume(mock.firstCall.args[1]).equals('describeImages');
      });

      it('fails with the wrong AMI found in EC2', async () => {
        const mock = sandbox.stub();
        mock.onCall(0).returns(Promise.resolve({
          Images: [{
            ImageId: 'foobar',
            State: 'available',
          }],
        }));

        const poller = new AmiExistencePoller({ec2: {}, state, runaws: mock});

        assume(await poller.poll(defaultSR.imageId)).false();
        assume(mock.callCount).equals(1);
        assume(mock.firstCall.args[1]).equals('describeImages');
      });

      it('fails with no AMI found in EC2', async () => {
        const mock = sandbox.stub();
        mock.onCall(0).returns(Promise.resolve({
          Images: [],
        }));

        const poller = new AmiExistencePoller({ec2: {}, state, runaws: mock});

        assume(await poller.poll(defaultSR.imageId)).false();
        assume(mock.callCount).equals(1);
        assume(mock.firstCall.args[1]).equals('describeImages');
      });
    });
  });
});
