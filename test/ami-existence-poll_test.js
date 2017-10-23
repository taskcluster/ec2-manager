const {AmiExistencePoller} = require('../lib/ami-existence-poller');
const sinon = require('sinon');
const main = require('../lib/main');
const assume = require('assume');

describe('AMI Existence Poller', () => {
  let state;
  let sandbox = sinon.sandbox.create();
  let defaultSR;

  before(async() => {
    state = await main('state', {profile: 'test', process: 'test'});
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

  afterEach(() => {
    sandbox.restore();
  });

  describe('constructor', () => {
    it('succeeds with valid args', () => {
      const poller = new AmiExistencePoller({ec2: {}, state, runaws: () => {}});
    });
  });

  describe('loadAmis', async() => {
    it('succeeds with amis args', async() => {
      const poller = new AmiExistencePoller({ec2: {}, state, runaws: () => {}});
      assume(poller.amis).has.length(0);
      poller.loadAmis(['foobar']);
      assume(poller.amis).has.length(1);
    });

    it('loads nothing with empty amis arg', async() => {
      const poller = new AmiExistencePoller({ec2: {}, state, runaws: () => {}});
      assume(poller.amis).has.length(0);
      poller.loadAmis([]);
      assume(poller.amis).has.length(0);
    });

    it('succeeds with database load', async() => {
      await state.reportAmiUsage({
        region: defaultSR.region,
        id: defaultSR.imageId,
      });

      const poller = new AmiExistencePoller({ec2: {}, state, runaws: () => {}});
      assume(poller.amis).has.length(0);
      await poller.loadAmis();
      assume(poller.amis).has.length(1);
    });
  });

  describe('poll', async() => {
    it('returns [] with all valid amis', async() => {
      const mock = sandbox.stub();
      mock.onCall(0).returns(Promise.resolve({
        Images: [{
          ImageId: defaultSR.imageId,
          State: 'available',
        }],
      }));

      const poller = new AmiExistencePoller({ec2: {}, state, runaws: mock});
      await poller.loadAmis([defaultSR.imageId]);
      assume(await poller.poll()).empty();
    });

    it('returns an invalid ami', async() => {
      const mock = sandbox.stub();
      mock.onCall(0).returns(Promise.resolve({
        Images: [{
          ImageId: defaultSR.imageId,
          State: 'inavailable',
        }],
      }));

      const poller = new AmiExistencePoller({ec2: {}, state, runaws: mock});
      await poller.loadAmis([defaultSR.imageId]);
      assume(await poller.poll()).eql([defaultSR.imageId]);
    });

    describe('batches correctly', async() => {
      let poller, mock;
      beforeEach(() => {
        poller = new AmiExistencePoller({ec2: {}, state, runaws: () => {}});
        mock = sinon.spy(() => []);
        poller.pollBatch = mock;
      });

      it('does not call pollBatch with no amis', async() => {
        await poller.poll();
        assume(mock.notCalled).true();
      });
      
      it('calls pollBatch once with 1 ami', async() => {
        await poller.loadAmis(new Array(1));
        await poller.poll();

        assume(mock.calledOnce).true();
      });

      it('calls pollBatch once with 100 amis', async() => {
        await poller.loadAmis(new Array(100));
        await poller.poll();

        assume(mock.calledOnce).true();
      });

      it('calls pollBatch twice with 101 amis', async() => {
        await poller.loadAmis(new Array(101));
        await poller.poll();

        assume(mock.calledTwice).true();
      });

      it('calls pollBatch twice with 200 amis', async() => {
        await poller.loadAmis(new Array(200));
        await poller.poll();

        assume(mock.calledTwice).true();
      });
    });
  });

  describe('pollBatch', () => {

    it('with non-array amiIds', async() => {
      const mock = sandbox.stub();
      mock.onCall(0).returns(Promise.resolve({
        Images: [{
          ImageId: defaultSR.imageId,
          State: 'available',
        }],
      }));

      const poller = new AmiExistencePoller({ec2: {}, state, runaws: mock});
      try {
        await poller.pollBatch('foobar');
        return Promise.reject(Error('Line should not be reached'));
      } catch (e) {
        assume(e.message).equals('amiIds is not an array');
      }
    });

    it('with amiIds list including an invalid ami', async() => {
      const mock = sandbox.stub();
      mock.onCall(0).returns(Promise.resolve({
        Images: [{
          ImageId: defaultSR.imageId,
          State: 'available',
        }],
      }));

      const poller = new AmiExistencePoller({ec2: {}, state, runaws: mock});
      try {
        await poller.pollBatch([defaultSR.imageId, 53]);
        return Promise.reject(Error('Line should not be reached'));
      } catch (e) {
        assume(e.message).equals('ami 53 is not a string, rather type number');
      }
    });

    describe('with valid AMIs', () => {
      beforeEach(async() => {
        await state.reportAmiUsage({
          region: defaultSR.region,
          id: defaultSR.imageId,
        });

        const images = await state.listAmiUsage();
        assume(images).lengthOf(1);
      });

      it('succeeds with an AMI available on EC2', async() => {
        const mock = sandbox.stub();
        mock.onCall(0).returns(Promise.resolve({
          Images: [{
            ImageId: defaultSR.imageId,
            State: 'available',
          }],
        }));

        const poller = new AmiExistencePoller({ec2: {}, state, runaws: mock});

        assume(await poller.pollBatch([defaultSR.imageId])).empty();
        assume(mock.callCount).equals(1);
        assume(mock.firstCall.args[1]).equals('describeImages');
      });

      it('fails with an invalid AMI in EC2', async() => {
        const mock = sandbox.stub();
        mock.onCall(0).returns(Promise.resolve({
          Images: [{
            ImageId: defaultSR.imageId,
            State: 'invalid',
          }],
        }));

        const poller = new AmiExistencePoller({ec2: {}, state, runaws: mock});

        assume(await poller.pollBatch([defaultSR.imageId])).has.length(1);
        assume(mock.callCount).equals(1);
        assume(mock.firstCall.args[1]).equals('describeImages');
      });

      it('fails with the wrong AMI found in EC2', async() => {
        const mock = sandbox.stub();
        mock.onCall(0).returns(Promise.resolve({
          Images: [{
            ImageId: 'foobar',
            State: 'available',
          }],
        }));

        const poller = new AmiExistencePoller({ec2: {}, state, runaws: mock});

        assume(await poller.pollBatch([defaultSR.imageId])).has.length(1);
        assume(mock.callCount).equals(1);
        assume(mock.firstCall.args[1]).equals('describeImages');
      });

      it('fails with no AMI found in EC2', async() => {
        const mock = sandbox.stub();
        mock.onCall(0).returns(Promise.resolve({
          Images: [],
        }));

        const poller = new AmiExistencePoller({ec2: {}, state, runaws: mock});

        assume(await poller.pollBatch([defaultSR.imageId])).has.length(1);
        assume(mock.callCount).equals(1);
        assume(mock.firstCall.args[1]).equals('describeImages');
      });
    });
  });
});
