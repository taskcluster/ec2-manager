const Database = require('../lib/database');
const assume = require('assume');

describe('Database', () => {
  let db;
  let workerType = 'example-workertype';
  let region = 'us-west-1';
  let instanceType = 'm1.medium';

  before(async () => {
    db = await Database.openDB({dburl: process.env.DATABASE_URL}); 
    await db._runScript('drop-db.sql');
    await db._runScript('create-db.sql');
  });

  // I could add these helper functions to the actual database.js class but I'd
  // rather not have that be so easy to call by mistake in real code
  beforeEach(async () => {
    await db._runScript('clear-db.sql');
  });

  it('should be empty at start of tests', async () => {
    let instances = await db.listAllInstances();
    let pendingSpotRequests = await db.listAllSpotRequests();
    assume(instances).has.length(0);
    assume(pendingSpotRequests).has.length(0);
  });

  it('should be able to insert a spot request', async () => {
    let id = 'r-123456789';
    let state = 'open';

    let result = await db.insertSpotRequest({workerType, region, instanceType, id, state});
    result = await db.listAllSpotRequests();
    assume(result).has.length(1);
    assume(result[0]).has.property('id', id);
  });

  it('should be able to insert an on-demand instance', async () => {
    let id = 'i-123456789';
    let state = 'pending';

    let result = await db.insertInstance({workerType, region, instanceType, id, state});
    let instances = await db.listAllInstances();
    assume(instances).has.length(1);
    assume(instances[0]).has.property('id', id);
  });

  it('should be able to insert a spot instance, removing the spot request', async () => {
    let id = 'i-123456789';
    let state = 'pending';
    let srid = 'r-123456789';

    await db.insertSpotRequest({workerType, region, instanceType, id: srid, state: 'open'});
    assume(await db.listAllSpotRequests()).has.length(1);
    assume(await db.listAllInstances()).has.length(0);

    let result = await db.insertInstance({workerType, region, instanceType, id, state, srid});
    assume(await db.listAllSpotRequests()).has.length(0);
    assume(await db.listAllInstances()).has.length(1);
  });

  it('should be able to update a spot request', async () => {
    let id = 'r-123456789';
    let firstState = 'open';
    let secondState = 'closed';
    await db.insertSpotRequest({workerType, region, instanceType, id, state: firstState});
    let spotRequests = await db.listAllSpotRequests(); 
    assume(spotRequests).has.length(1);
    assume(spotRequests[0]).has.property('state', firstState);
    await db.updateSpotRequestState({region, id, state: secondState});
    spotRequests = await db.listAllSpotRequests(); 
    assume(spotRequests).has.length(1);
    assume(spotRequests[0]).has.property('state', secondState);
  });
 
});
