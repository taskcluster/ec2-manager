const main = require('../lib/main');
const assume = require('assume');
const uuid = require('uuid');

describe('State', () => {
  let db;
  let defaultInst;
  let defaultTerm;

  before(async() => {
    db = await main('state', {profile: 'test', process: 'test'});
    await db._runScript('drop-db.sql');
    await db._runScript('create-db.sql');
  });

  after(async() => {
    await db._runScript('drop-db.sql');
  });

  // I could add these helper functions to the actual state.js class but I'd
  // rather not have that be so easy to call by mistake in real code
  beforeEach(async() => {
    await db._runScript('clear-db.sql');
    defaultInst = {
      id: 'i-1',
      workerType: 'example-workertype',
      region: 'us-west-1',
      az: 'us-west-1z',
      imageId: 'ami-1',
      instanceType: 'm1.medium',
      state: 'pending',
      launched: new Date(),
      lastEvent: new Date(),
    };
    defaultTerm = {
      id: 'i-1',
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
    };
  });

  afterEach(() => {
    // Make sure we're not dropping client references
    assume(db._pgpool.waitingCount).equals(0);
  });

  describe('type parsers', () => {
    it('should parse ints (20) to js ints', async() => {
      let result = await db._pgpool.query('SELECT count(id) FROM instances;');
      assume(result).has.property('rows');
      assume(result.rows).has.lengthOf(1);
      assume(result.rows[0]).has.property('count', 0);
    });

    it('should parse timestamptz (1184) to js dates (UTC)', async() => {
      let d = new Date(0);
      let result = await db._pgpool.query('SELECT timestamptz \'1970-1-1 UTC\' as a;');
      let {a} = result.rows[0];
      assume(d.getTime()).equals(a.getTime());
    });

    it('should parse timestamptz (1184) to js dates (CEST)', async() => {
      let d = new Date('Tue Jul 04 2017 1:00:00 GMT+0200 (CEST)');
      let result = await db._pgpool.query('SELECT timestamptz \'2017-7-4 1:00:00 CEST\' as a;');
      let {a} = result.rows[0];
      assume(d.getTime()).equals(a.getTime());
    });
  });

  describe('query generation', () => {
    let table = 'junk';

    it('no conditions', () => {
      let expected = {query: 'SELECT * FROM junk;', values: []};
      let actual = db._generateTableListQuery(table);
      assume(expected).deeply.equals(actual);
    });

    it('one flat condition', () => {
      let expected = {
        query: 'SELECT * FROM junk WHERE junk."a" = $1;',
        values: ['aye'],
      };
      let actual = db._generateTableListQuery(table, {a: 'aye'});
      assume(expected).deeply.equals(actual);
    });

    it('two flat conditions', () => {
      let expected = {
        query: 'SELECT * FROM junk WHERE junk."a" = $1 AND junk."b" = $2;',
        values: ['aye', 'bee'],
      };
      let actual = db._generateTableListQuery(table, {a: 'aye', b: 'bee'});
      assume(expected).deeply.equals(actual);
    });

    it('one list condition', () => {
      let expected = {
        query: 'SELECT * FROM junk WHERE junk."a" = $1 OR junk."a" = $2 OR junk."a" = $3;',
        values: ['a', 'b', 'c'],
      };
      let actual = db._generateTableListQuery(table, {a: ['a', 'b', 'c']});
      assume(expected).deeply.equals(actual);
    });

    it('two list conditions', () => {
      let expected = {
        query: 'SELECT * FROM junk WHERE (junk."a" = $1 OR junk."a" = $2) AND (junk."b" = $3 OR junk."b" = $4);',
        values: ['a', 'b', 'c', 'd'],
      };
      let actual = db._generateTableListQuery(table, {a: ['a', 'b'], b: ['c', 'd']});
      assume(expected).deeply.equals(actual);
    });

    it('mixed type flat-list-flat conditions', () => {
      let expected = {
        query: 'SELECT * FROM junk WHERE junk."a" = $1 AND (junk."b" = $2 OR junk."b" = $3) AND junk."c" = $4;',
        values: ['a', 'b', 'c', 'd'],
      };
      let actual = db._generateTableListQuery(table, {a: 'a', b: ['b', 'c'], c: 'd'});
      assume(expected).deeply.equals(actual);
    });

    it('limits', () => {
      let expected = {
        query: 'SELECT * FROM junk WHERE junk."a" = $1 AND (junk."b" = $2 OR junk."b" = $3) AND junk."c" = $4 LIMIT 1;',
        values: ['a', 'b', 'c', 'd'],
      };
      let actual = db._generateTableListQuery(table, {a: 'a', b: ['b', 'c'], c: 'd'}, undefined, 1);
      assume(expected).deeply.equals(actual);
    });

    it('null conditions', () => {
      let expected = {
        query: 'SELECT * FROM junk WHERE junk."a" IS NULL AND (junk."b" IS NULL);',
        values: [],
      };
      let actual = db._generateTableListQuery(table, {a: null, b: [null]});
      assume(expected).deeply.equals(actual);
    });

  });

  it('should be able to filter AMI usages', async() => {
    debugger;
    let result = await db.listAmiUsage();
    assume(result).has.length(0);
    await db.reportAmiUsage({region: defaultInst.region, id: defaultInst.imageId});
    result = await db.listAmiUsage();
    assume(result).has.length(1);
  });

  it('should be able to insert an on-demand instance', async() => {
    await db.insertInstance(defaultInst);
    let instances = await db.listInstances();
    assume(instances).has.length(1);
    assume(instances[0]).has.property('id', defaultInst.id);
  });

  it('should be able to upsert an instance', async() => {
    assume(await db.listInstances()).has.length(0);
    await db.upsertInstance(defaultInst);
    assume(await db.listInstances()).has.length(1);
  });

  it('should upsert an instance and only change the state if the new state is newer', async() => {
    await db.insertInstance(Object.assign({}, defaultInst, {lastEvent: new Date(3600 * 1000), state: 'running'}));
    assume(await db.listInstances()).has.length(1);
    await db.upsertInstance(Object.assign({}, defaultInst, {lastEvent: new Date(0), state: 'pending'}));
    let instances = await db.listInstances();
    assume(instances).has.lengthOf(1);
    assume(instances[0]).has.property('state', 'running');
  });

  it('should be able to update an instance', async() => {
    let firstState = 'pending';
    let secondState = 'running';
    defaultInst.state = firstState;

    await db.insertInstance(defaultInst);
    let instances = await db.listInstances(); 
    assume(instances).has.length(1);
    assume(instances[0]).has.property('state', firstState);

    await db.updateInstanceState({
      region: defaultInst.region,
      id: defaultInst.id,
      state: secondState,
      lastEvent: new Date(),
    });
    instances = await db.listInstances(); 
    assume(instances).has.length(1);
    assume(instances[0]).has.property('state', secondState);
  });

  it('should update instance state and only change the state if the new state is newer', async() => {
    await db.insertInstance(Object.assign({}, defaultInst, {lastEvent: new Date(3600 * 1000), state: 'running'}));
    assume(await db.listInstances()).has.length(1);
    await db.updateInstanceState({
      region: defaultInst.region,
      id: defaultInst.id,
      state: 'pending',
      lastEvent: new Date(0),
    });
    let instances = await db.listInstances();
    assume(instances).has.lengthOf(1);
    assume(instances[0]).has.property('state', 'running');
  });

  it('should be able to insert a complete termination', async() => {
    delete defaultTerm.code;
    delete defaultTerm.reason;
    delete defaultTerm.termination;

    await db.insertTermination(defaultTerm);
    let terminations = await db.listTerminations();
    assume(terminations).has.length(1);
    assume(terminations[0]).has.property('id', defaultTerm.id);
  });

  it('should be able to insert a termination without code, reason or terminated', async() => {
    await db.insertTermination(defaultTerm);
    let terminations = await db.listTerminations();
    assume(terminations).has.length(1);
    assume(terminations[0]).has.property('id', defaultTerm.id);
  });

  it('should be able to upsert an termination', async() => {
    assume(await db.listTerminations()).has.length(0);
    await db.upsertTermination(defaultTerm);
    assume(await db.listTerminations()).has.length(1);
  });

  it('should be able to update an termination', async() => {
    let secondCode = 'code';
    delete defaultTerm.code;
    let secondReason = 'reason';
    delete defaultTerm.reason;
    let secondTermination = new Date();
    delete defaultTerm.termination;

    await db.insertTermination(defaultTerm);
    let terminations = await db.listTerminations(); 
    assume(terminations).has.length(1);

    await db.updateTerminationState({
      region: defaultTerm.region,
      id: defaultTerm.id,
      code: secondCode,
      reason: secondReason,
      terminated: secondTermination,
      lastEvent: new Date(),
    });

    terminations = await db.listTerminations(); 
    defaultTerm.code = secondCode;
    defaultTerm.reason = secondReason;
    defaultTerm.termination = secondTermination;

    assume(terminations).has.length(1);
    assume(terminations[0]).has.property('code', secondCode);
    assume(terminations[0]).has.property('reason', secondReason);
    assume(terminations[0].terminated.getTime()).equals(secondTermination.getTime());
  });

  it('should be able to report an AMI\'s usage', async() => {
    await db.reportAmiUsage({region: defaultInst.region, id: defaultInst.imageId});
    let amiUsage = await db.listAmiUsage(); 
    assume(amiUsage).has.length(1);
    assume(amiUsage[0]).has.property('region', defaultInst.region);
    assume(amiUsage[0]).has.property('id', defaultInst.imageId);
    let lastUse = amiUsage[0].lastUsed;
    
    await db.reportAmiUsage({region: defaultInst.region, id: defaultInst.imageId});
    let updatedAmiUsage = await db.listAmiUsage();
    assume(amiUsage).has.length(1);
    assume(amiUsage[0]).has.property('region', defaultInst.region);
    assume(amiUsage[0]).has.property('id', defaultInst.imageId);
    let updatedUse = updatedAmiUsage[0].lastUsed;
    
    assume(lastUse < updatedUse).true();
  });
   
  it('should have list worker types', async() => {
    // Insert some instances
    await db.insertInstance(Object.assign({}, defaultInst, {
      id: 'i-1',
      workerType: 'a',
      region: 'us-east-1',
    }));
    await db.insertInstance(Object.assign({}, defaultInst, {
      id: 'i-2',
      workerType: 'a',
      region: 'us-east-1',
    }));
    await db.insertInstance(Object.assign({}, defaultInst, {
      id: 'i-3',
      workerType: 'b',
      region: 'us-west-1',
    }));
    await db.insertInstance(Object.assign({}, defaultInst, {
      id: 'i-4',
      workerType: 'c',
      region: 'us-west-1',
    }));

    let actual = await db.listWorkerTypes();
    assume(actual).deeply.equals(['a', 'b', 'c']);

  });

  it('should have valid instance counts', async() => {
    // Insert some instances.  NOTE that we're only counting things in us-east-1
    await db.insertInstance(Object.assign({}, defaultInst, {
      id: 'i-1', region: 'us-east-1', instanceType: 'm3.medium', state: 'running',
    }));
    await db.insertInstance(Object.assign({}, defaultInst, {
      id: 'i-2', region: 'us-east-1', instanceType: 'm3.xlarge', state: 'running',
    }));
    await db.insertInstance(Object.assign({}, defaultInst, {
      id: 'i-3', region: 'us-west-1', instanceType: 'm3.medium', state: 'running',
    }));
    await db.insertInstance(Object.assign({}, defaultInst, {
      id: 'i-4', region: 'us-east-1', instanceType: 'm3.medium', state: 'pending',
    }));
    await db.insertInstance(Object.assign({}, defaultInst, {
      id: 'i-5', region: 'us-east-1', instanceType: 'm3.xlarge', state: 'pending',
    }));
    await db.insertInstance(Object.assign({}, defaultInst, {
      id: 'i-6', region: 'us-west-1', instanceType: 'm3.medium', state: 'pending',
    }));
    // Let's ensure an instance in a state which we don't care about is in there
    await db.insertInstance(Object.assign({}, defaultInst, {
      id: 'i-7', region: 'us-east-1', instanceType: 'm3.2xlarge', state: 'terminated',
    }));

    let result = await db.instanceCounts({workerType: defaultInst.workerType});

    assume(result).has.property('pending');
    assume(result).has.property('running');
    assume(result.pending).has.lengthOf(2);
    assume(result.running).has.lengthOf(2);
  });

  it('should be able to remove an instance', async() => {
    await db.insertInstance(defaultInst);
    assume(await db.listInstances()).has.length(1);
    await db.removeInstance({region: defaultInst.region, id: defaultInst.id});
    assume(await db.listInstances()).has.length(0);
  });

  it('should be able to list all instance ids of a worker type', async() => {
    // Insert some instances
    await db.insertInstance(Object.assign({}, defaultInst, {
      id: 'i-1',
      state: 'running',
      region: 'us-east-1',
    }));
    await db.insertInstance(Object.assign({}, defaultInst, {
      id: 'i-2',
      state: 'running',
      region: 'us-west-1',
    }));
    await db.insertInstance(Object.assign({}, defaultInst, {
      id: 'i-3',
      state: 'pending',
      region: 'us-west-2',
    }));

    let expected = {
      instanceIds: [
        {region: 'us-east-1', id: 'i-1'},
        {region: 'us-west-1', id: 'i-2'},
        {region: 'us-west-2', id: 'i-3'},
      ],
    };
    let actual = await db.listIdsOfWorkerType({workerType: defaultInst.workerType});
    assume(expected).deeply.equals(actual);
  });
  
  it('should log aws requests without optional fields', async() => {
    let called = new Date();
    let rid = uuid.v4();

    await db.logAWSRequest({
      region: 'us-east-1',
      requestId: rid, 
      duration: 1000, // Remember this is us not ms
      method: 'funky',
      service: 'dunky',
      error: false,
      called,
    });

    let result = await db.listAWSRequests();

    assume(result).has.lengthOf(1);

    // serializing to json because it turns out that it's a pain to do date
    // comparisons
    assume(JSON.stringify(result)).equals(JSON.stringify([{
      region: 'us-east-1',
      requestId: rid,
      duration: {milliseconds: 1},
      method: 'funky',
      service: 'dunky',
      error: false,
      called,
      code: null,
      message: null,
      workerType: null,
      az: null,
      instanceType: null,
      imageId: null,
    }]));

  });

  it('should log aws requests with errors', async() => {
    let called = new Date();
    let rid = uuid.v4();

    await db.logAWSRequest({
      region: 'us-east-1',
      requestId: rid, 
      duration: 1000, // Remember this is us not ms
      method: 'funky',
      service: 'dunky',
      error: true,
      code: 'code',
      message: 'message',
      called,
    });

    let result = await db.listAWSRequests();

    assume(result).has.lengthOf(1);

    // serializing to json because it turns out that it's a pain to do date
    // comparisons
    assume(JSON.stringify(result)).equals(JSON.stringify([{
      region: 'us-east-1',
      requestId: rid,
      duration: {milliseconds: 1},
      method: 'funky',
      service: 'dunky',
      error: true,
      called,
      code: 'code',
      message: 'message',
      workerType: null,
      az: null,
      instanceType: null,
      imageId: null,
    }]));

  });

  describe('aws request logging with malformed errors', () => {
    it('should when error is false and code and message are given', async() => {
      try {
        await db.logAWSRequest({
          region: 'us-east-1',
          requestId: uuid.v4(), 
          duration: 1000, // Remember this is us not ms
          method: 'funky',
          service: 'dunky',
          error: false,
          code: 'code',
          message: 'message',
          called: new Date(),
        });
        return Promise.reject(new Error('shouldnt pass'));
      } catch (err) {
        return Promise.resolve();
      }
    });
    
    it('should when error is true and code and message are not given', async() => {
      try {
        await db.logAWSRequest({
          region: 'us-east-1',
          requestId: uuid.v4(), 
          duration: 1000, // Remember this is us not ms
          method: 'funky',
          service: 'dunky',
          error: true,
          //code: 'code',
          //message: 'message',
          called: new Date(),
        });
        return Promise.reject(new Error('shouldnt pass'));
      } catch (err) {
        return Promise.resolve();
      }
    });
  });

  it('should log aws requests with optional fields', async() => {
    let called = new Date();
    let rid = uuid.v4();

    await db.logAWSRequest({
      region: 'us-east-1',
      requestId: rid, 
      duration: 1000, // Remember this is us not ms
      method: 'funky',
      service: 'dunky',
      error: false,
      called,
      workerType: 'wt',
      az: 'az',
      instanceType: 'it',
      imageId: 'ami',
    });

    let result = await db.listAWSRequests();

    assume(result).has.lengthOf(1);

    // serializing to json because it turns out that it's a pain to do date
    // comparisons
    assume(JSON.stringify(result)).equals(JSON.stringify([{
      region: 'us-east-1',
      requestId: rid,
      duration: {milliseconds: 1},
      method: 'funky',
      service: 'dunky',
      error: false,
      called,
      code: null,
      message: null,
      workerType: 'wt',
      az: 'az',
      instanceType: 'it',
      imageId: 'ami',
    }]));

  });

  it('should log cloud watch events (with generated time)', async() => {
    let time = new Date();
    await db.logCloudWatchEvent({
      region: defaultInst.region,
      id: defaultInst.id,
      state: 'pending',
      generated: time,
    });
    let client = await db.getClient();
    let result = await client.query('select * from cloudwatchlog');
    assume(result.rows).has.lengthOf(1);
    let row = result.rows[0];
    assume(row).has.property('region', defaultInst.region);
    assume(row).has.property('id', defaultInst.id);
    assume(row).has.property('state', 'pending');
    assume(row).has.property('generated');
    assume(row.generated).deeply.equals(time);
  });

  it('should find terminations which need polling', async() => {
    let termTime = new Date();
    let launched = new Date(termTime);
    launched.setMinutes(launched.getMinutes() - 15);
    let x = 10;
    for (let i = 0 ; i < x ; i++) {
      await db.insertTermination({
        id: 'i-' + i,
        workerType: 'workertype',
        region: 'us-east-' + i,
        az: 'us-east-' + i + 'a',
        instanceType: 'm3.large',
        imageId: 'ami-' + i,
        terminated: termTime,
        launched: launched,
        lastEvent: termTime,
      });
    }

    let actual = await db.findTerminationsToPoll(1);
    assume(actual).has.lengthOf(1);
    assume(actual[0]).has.property('id', 'i-0');
    assume(actual[0]).has.property('region', 'us-east-0');

    await db.updateTerminationState({
      region: 'us-east-0',
      id: 'i-0',
      code: 'Code',
      reason: 'Reason',
      lastEvent: new Date(),
    });

    actual = await db.listTerminations({
      region: 'us-east-0',
      id: 'i-0',
    });

    assume(actual[0]).has.property('code', 'Code');
    assume(actual[0]).has.property('reason', 'Reason');
  });

  describe('determining health of ec2 account', () => {

    it('should work with empty state', async() => {
      let result = await db.getHealth();
      assume(result).has.property('requestHealth');
      assume(result).has.property('terminationHealth');
      assume(result).has.property('running');
    });

    it('should show running instances stats', async() => {
      await db.insertInstance(defaultInst);
      let result = await db.getHealth();
      assume(result).has.property('running');
      assume(result.running).has.lengthOf(1);
      assume(result.running[0]).has.property('region', defaultInst.region);
      assume(result.running[0]).has.property('az', defaultInst.az);
      assume(result.running[0]).has.property('instanceType', defaultInst.instanceType);
      assume(result.running[0]).has.property('running', 1);
    });

    it('should show terminations stats', async() => {
      let codesToTest = [
        'Client.InstanceInitiatedShutdown',
        'Server.SpotInstanceTermination',
        'Server.InsufficientInstanceCapacity',
        'Client.VolumeLimitExceeded',
        'Client.InvalidSnapshot.NotFound',
        'Server.InternalError',
        'Ooogie.Boogie',
      ];

      let i = 1;
      for (let code of codesToTest) {
        let thisTerm = Object.assign({}, defaultTerm, {id: 'i-' + i});
        await db.insertTermination(thisTerm);
        await db.updateTerminationState({
          region: defaultTerm.region,
          id: 'i-' + i,
          code: code,
          reason: code + ': reason',
          terminated: new Date(),
          lastEvent: new Date(),
        });
        i++;
      }

      let thisTerm = Object.assign({}, defaultTerm, {id: 'i-' + i, terminated: new Date()});
      delete thisTerm.code;
      delete thisTerm.reason;
      await db.insertTermination(thisTerm);

      let result = await db.getHealth();
      assume(result.terminationHealth).has.lengthOf(1);
      assume(result.terminationHealth[0]).deeply.equals({
        region: defaultTerm.region,
        az: defaultTerm.az,
        instanceType: defaultTerm.instanceType,
        clean_shutdown: 1,
        spot_kill: 1,
        insufficient_capacity: 1,
        volume_limit_exceeded: 1,
        missing_ami: 1,
        startup_failed: 1,
        unknown_codes: 1,
        no_code: 1,
      });
    });

    it('should show runInstances failure stats', async() => {
      let codesToTest = [
        'RequestLimitExceeded',
        'InvalidParameter',
        'InsufficientCapacity',
        'HostLimitExceeded',
      ];
      await db.logAWSRequest({
        region: 'us-east-1',
        requestId: uuid.v4(),
        duration: 100,
        method: 'runInstances',
        service: 'ec2',
        error: false,
        called: new Date(),
        workerType: 'example-worker',
        az: 'us-east-1a',
        instanceType: 'm3.xlarge',
        imageId: 'ami-1',
      });

      for (let code of codesToTest) {
        await db.logAWSRequest({
          region: 'us-east-1',
          requestId: uuid.v4(),
          duration: 100,
          method: 'runInstances',
          service: 'ec2',
          error: true,
          code,
          message: code + ': An error!',
          called: new Date(),
          workerType: 'example-worker',
          az: 'us-east-1a',
          instanceType: 'm3.xlarge',
          imageId: 'ami-1',         
        });
      }

      let result = await db.getHealth();
      assume(result.requestHealth).has.lengthOf(1);
      assume(result.requestHealth[0]).deeply.equals({
        region: 'us-east-1',
        az: 'us-east-1a',
        instanceType: 'm3.xlarge',
        successful: 1,
        failed: 4,
        configuration_issue: 1,
        throttled_calls: 1,
        insufficient_capacity: 1,
        limit_exceeded: 1,
      });
    });
  });
});
