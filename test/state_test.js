'use strict';
const main = require('../lib/main');
const assume = require('assume');

describe('State', () => {
  let db;
  let defaultInst;
  let defaultSR;

  before(async () => {
    db = await main('state', {profile: 'test', process: 'test'});
    await db._runScript('drop-db.sql');
    await db._runScript('create-db.sql');
  });

  // I could add these helper functions to the actual state.js class but I'd
  // rather not have that be so easy to call by mistake in real code
  beforeEach(async () => {
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
      lastevent: new Date(),
    };
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

  describe('type parsers', () => {
    it('should parse ints (20) to js ints', async () => {
      let result = await db._pgpool.query('SELECT count(id) FROM instances;');
      assume(result).has.property('rows');
      assume(result.rows).has.lengthOf(1);
      assume(result.rows[0]).has.property('count', 0);
    });

    it('should parse timestamptz (1184) to js dates (UTC)', async () => {
      let d = new Date(0);
      let result = await db._pgpool.query("SELECT timestamptz '1970-1-1 UTC' as a;");
      let {a} = result.rows[0];
      assume(d.getTime()).equals(a.getTime());
    });

    it('should parse timestamptz (1184) to js dates (CEST)', async () => {
      let d = new Date('Tue Jul 04 2017 1:00:00 GMT+0200 (CEST)');
      let result = await db._pgpool.query("SELECT timestamptz '2017-7-4 1:00:00 CEST' as a;");
      let {a} = result.rows[0];
      assume(d.getTime()).equals(a.getTime());
    });
  });

  describe('query generation', () => {
    let table = 'junk';

    it('no conditions', () => {
      let expected = {text: 'SELECT * FROM junk;', values: []};
      let actual = db._generateTableListQuery(table);
      assume(expected).deeply.equals(actual);
    });

    it('one flat condition', () => {
      let expected = {
        text: 'SELECT * FROM junk WHERE junk.a = $1;',
        values: ['aye']
      };
      let actual = db._generateTableListQuery(table, {a: 'aye'});
      assume(expected).deeply.equals(actual);
    });

    it('two flat conditions', () => {
      let expected = {
        text: 'SELECT * FROM junk WHERE junk.a = $1 AND junk.b = $2;',
        values: ['aye', 'bee']
      };
      let actual = db._generateTableListQuery(table, {a: 'aye', b: 'bee'});
      assume(expected).deeply.equals(actual);
    });

    it('one list condition', () => {
      let expected = {
        text: 'SELECT * FROM junk WHERE junk.a = $1 OR junk.a = $2 OR junk.a = $3;',
        values: ['a', 'b', 'c']
      };
      let actual = db._generateTableListQuery(table, {a: ['a','b','c']});
      assume(expected).deeply.equals(actual);
    });

    it('two list conditions', () => {
      let expected = {
        text: 'SELECT * FROM junk WHERE (junk.a = $1 OR junk.a = $2) AND (junk.b = $3 OR junk.b = $4);',
        values: ['a', 'b', 'c', 'd']
      };
      let actual = db._generateTableListQuery(table, {a: ['a', 'b'], b: ['c', 'd']});
      assume(expected).deeply.equals(actual);
    });

    it('mixed type flat-list-flat conditions', () => {
      let expected = {
        text: 'SELECT * FROM junk WHERE junk.a = $1 AND (junk.b = $2 OR junk.b = $3) AND junk.c = $4;',
        values: ['a', 'b', 'c', 'd']
      };
      let actual = db._generateTableListQuery(table, {a: 'a', b: ['b', 'c'], c: 'd'});
      assume(expected).deeply.equals(actual);
    });

  });

  it('should be empty at start of tests', async () => {
    let instances = await db.listInstances();
    let pendingSpotRequests = await db.listSpotRequests();
    let amiUsage = await db.listAmiUsage();
    assume(instances).has.length(0);
    assume(pendingSpotRequests).has.length(0);
    assume(amiUsage).has.length(0);
  });

  it('should be able to insert a spot request', async () => {
    let result = await db.insertSpotRequest(defaultSR);
    result = await db.listSpotRequests();
    assume(result).has.length(1);
    assume(result[0]).has.property('id', defaultSR.id);
  });
   
  it('should be able to insert an AMI\'s usage', async () => {
    let result = await db.insertAmiUsage({region: defaultSR.region, id: defaultSR.id});
    result = await db.listAmiUsage();
    assume(result).has.length(1);
    assume(result[0]).has.property('id', defaultSR.id);
  });

  it('should be able to filter spot requests', async () => {
    let result = await db.insertSpotRequest(defaultSR);
    result = await db.listSpotRequests({region: 'us-east-1', state: 'open'});
    assume(result).has.length(0);
    result = await db.listSpotRequests({region: 'us-west-1', state: 'open'});
    assume(result).has.length(1);
  });
   
  it('should be able to filter AMI usages', async () => {
    let result = await db.insertAmiUsage({region: defaultSR.region, id: defaultSR.id});
    result = await db.listAmiUsage({region: 'us-east-1', id: 'r-1'});
    assume(result).has.length(0);
    result = await db.listAmiUsage({region: 'us-west-1', id: 'r-1'});
    assume(result).has.length(1);
  });

  it('should be able to insert an on-demand instance', async () => {
    let result = await db.insertInstance(defaultInst);
    let instances = await db.listInstances();
    assume(instances).has.length(1);
    assume(instances[0]).has.property('id', defaultInst.id);
  });

  it('should be able to insert a spot instance, removing the spot request', async () => {
    // We only delete a spot request if the instance has a corresponding spot request
    defaultInst.srid = defaultSR.id;

    await db.insertSpotRequest(defaultSR);
    assume(await db.listSpotRequests()).has.length(1);
    assume(await db.listInstances()).has.length(0);

    let result = await db.insertInstance(defaultInst);
    assume(await db.listSpotRequests()).has.length(0);
    assume(await db.listInstances()).has.length(1);
  });

  it('should be able to upsert a spot instance, removing the spot request', async () => {
    defaultInst.srid = defaultSR.id;

    await db.insertSpotRequest(defaultSR);
    assume(await db.listSpotRequests()).has.length(1);
    assume(await db.listInstances()).has.length(0);

    await db.upsertInstance(defaultInst);
    //await db.upsertInstance({workerType, region, instanceType, id, state, srid});
    assume(await db.listSpotRequests()).has.length(0);
    assume(await db.listInstances()).has.length(1);
  });


  it('should be able to update an instance', async () => {
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
      lastevent: new Date(),
    });
    instances = await db.listInstances(); 
    assume(instances).has.length(1);
    assume(instances[0]).has.property('state', secondState);
  });

  it('should be able to update a spot request', async () => {
    let firstState = 'open';
    let secondState = 'closed';
    defaultSR.state = firstState;

    await db.insertSpotRequest(defaultSR);
    let spotRequests = await db.listSpotRequests(); 
    assume(spotRequests).has.length(1);
    assume(spotRequests[0]).has.property('state', firstState);

    await db.updateSpotRequestState({
      region: defaultSR.region,
      id: defaultSR.id,
      state: secondState,
      status: defaultSR.status,
    });
    spotRequests = await db.listSpotRequests(); 
    assume(spotRequests).has.length(1);
    assume(spotRequests[0]).has.property('state', secondState);
  });
   
  it('should be able to update an AMI\'s usage', async () => {
    await db.insertAmiUsage({region: defaultSR.region, id: defaultSR.id});
    let amiUsage = await db.listAmiUsage(); 
    assume(amiUsage).has.length(1);
    
    await db.updateAmiUsage({
      region: 'us-west-1',
      id: defaultSR.id,
    });
     
    amiUsage = await db.listAmiUsage();
    assume(amiUsage).has.length(1);
    assume(amiUsage[0]).has.property('region', 'us-west-1');
  });

  it('should be able to do an AMI usage upsert', async () => {

    await db.upsertAmiUsage({region: defaultSR.region, id: defaultSR.id});
    let amiUsage = await db.listAmiUsage(); 
    assume(amiUsage).has.length(1);
    assume(amiUsage[0]).has.property('region', defaultSR.region);

    let newRegion = 'us-west-1';
    await db.upsertAmiUsage({region: newRegion, id: defaultSR.id});
    amiUsage = await db.listAmiUsage(); 
    assume(amiUsage).has.length(1);
    assume(amiUsage[0]).has.property('region', 'us-west-1');
  });
   
  it('should have list worker types', async () => {
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

    // Insert some spot requests
    await db.insertSpotRequest(Object.assign({}, defaultSR, {
      id: 'r-1',
      workerType: 'b',
    }));
    await db.insertSpotRequest(Object.assign({}, defaultSR, {
      id: 'r-2',
      workerType: 'd',
    }));

    let actual = await db.listWorkerTypes();
    assume(actual).deeply.equals(['a', 'b', 'c', 'd']);

  });

  it('should have valid instance counts', async () => {
    // Insert some instances
    await db.insertInstance(Object.assign({}, defaultInst, {
      id: 'i-1', region: 'us-east-1', instanceType: 'm3.medium', state: 'running'
    }));
    await db.insertInstance(Object.assign({}, defaultInst, {
      id: 'i-2', region: 'us-east-1', instanceType: 'm3.xlarge', state: 'running'
    }));
    await db.insertInstance(Object.assign({}, defaultInst, {
      id: 'i-3',region: 'us-west-1', instanceType: 'm3.medium', state: 'running'
    }));
    await db.insertInstance(Object.assign({}, defaultInst, {
      id: 'i-4', region: 'us-east-1', instanceType: 'm3.medium', state: 'pending'
    }));
    await db.insertInstance(Object.assign({}, defaultInst, {
      id: 'i-5', region: 'us-east-1', instanceType: 'm3.xlarge', state: 'pending'
    }));
    await db.insertInstance(Object.assign({}, defaultInst, {
      id: 'i-6', region: 'us-west-1', instanceType: 'm3.medium', state: 'pending'
    }));
    // Let's ensure an instance in a state which we don't care about is in there
    await db.insertInstance(Object.assign({}, defaultInst, {
      id: 'i-7', region: 'us-east-1', instanceType: 'm3.2xlarge', state: 'terminated'
    }));

    // Insert some spot requests
    await db.insertSpotRequest(Object.assign({}, defaultSR, {
      id: 'r-1', region: 'us-east-1', instanceType: 'c4.medium'
    }));
    await db.insertSpotRequest(Object.assign({}, defaultSR, {
      id: 'r-2', region: 'us-east-1', instanceType: 'c4.xlarge', state: 'open'
    }));
    await db.insertSpotRequest(Object.assign({}, defaultSR, {
      id: 'r-3', region: 'us-west-1', instanceType: 'c4.medium', state: 'open'
    }));
    await db.insertSpotRequest(Object.assign({}, defaultSR, {
      id: 'r-4', region: 'us-east-1', instanceType: 'c4.medium', state: 'open'
    }));
    await db.insertSpotRequest(Object.assign({}, defaultSR, {
      id: 'r-5', region: 'us-east-1', instanceType: 'c4.xlarge', state: 'open'
    }));
    await db.insertSpotRequest(Object.assign({}, defaultSR, {
      id: 'r-6', region: 'us-west-1', instanceType: 'c4.medium', state: 'open'
    }));
    await db.insertSpotRequest(Object.assign({}, defaultSR, {
      id: 'r-7', region: 'us-west-1', instanceType: 'c4.2xlarge', state: 'failed'
    }));

    let result = await db.instanceCounts({workerType: defaultInst.workerType});

    assume(result).has.property('pending');
    assume(result).has.property('running');
    assume(result.pending).has.lengthOf(4);
    assume(result.running).has.lengthOf(2);
  });

  it('should list the pending spot requests', async () => {
    // Insert some spot requests
    await db.insertSpotRequest(Object.assign({}, defaultSR, {id: 'r-1', state: 'open'}));
    await db.insertSpotRequest(Object.assign({}, defaultSR, {id: 'r-2', state: 'closed'}));
    await db.insertSpotRequest(Object.assign({}, defaultSR, {id: 'r-3', state: 'active'}));
    await db.insertSpotRequest(Object.assign({}, defaultSR, {id: 'r-4', state: 'failed'}));
    await db.insertSpotRequest(Object.assign({}, defaultSR, {id: 'r-5', state: 'open', region: 'us-east-1'}));
    await db.insertSpotRequest(Object.assign({}, defaultSR, {id: 'r-6', state: 'open', status: 'price-too-low'}));
    await db.insertSpotRequest(Object.assign({}, defaultSR, {id: 'r-7', state: 'cancelled'}));

    let result = await db.spotRequestsToPoll({region: defaultSR.region});
    result.sort();
    assume(result).deeply.equals(['r-1']);
  });

  it('should be able to remove a spot request', async () => {
    await db.insertSpotRequest(defaultSR);
    assume(await db.listSpotRequests()).has.length(1);
    await db.removeSpotRequest({region: defaultSR.region, id: defaultSR.id});
    assume(await db.listSpotRequests()).has.length(0);

  });

  it('should be able to remove an instance', async () => {
    await db.insertInstance(defaultInst);
    assume(await db.listInstances()).has.length(1);
    await db.removeInstance({region: defaultInst.region, id: defaultInst.id});
    assume(await db.listInstances()).has.length(0);
  });

  it('should be able to list all instance ids and spot requests of a worker type', async () => {
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
      srid: 'r-3',
    }));

    // Insert some spot requests
    await db.insertSpotRequest(Object.assign({}, defaultSR, {id: 'r-1', region: 'us-east-1'}));
    await db.insertSpotRequest(Object.assign({}, defaultSR, {id: 'r-2', region: 'us-west-1'}));
    
    let expected = {
      instanceIds: [
        {region: 'us-east-1', id: 'i-1'},
        {region: 'us-west-1', id: 'i-2'},
        {region: 'us-west-2', id: 'i-3'},
      ],
      requestIds: [
        {region: 'us-east-1', id: 'r-1'},
        {region: 'us-west-1', id: 'r-2'},
        {region: 'us-west-2', id: 'r-3'},
      ],
    }
    let actual = await db.listIdsOfWorkerType({workerType: defaultInst.workerType});
    assume(expected).deeply.equals(actual);
  });

  it('should log cloud watch events (with generated time)', async () => {
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

});
