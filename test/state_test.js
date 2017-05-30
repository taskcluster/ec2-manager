'use strict';
const main = require('../lib/main');
const assume = require('assume');

describe('State', () => {
  let db;
  let workerType = 'example-workertype';
  let region = 'us-west-1';
  let instanceType = 'm1.medium';
  let status = 'pending-fulfillment';

  before(async () => {
    db = await main('state', {profile: 'test', process: 'test'});
    await db._runScript('drop-db.sql');
    await db._runScript('create-db.sql');
  });

  // I could add these helper functions to the actual state.js class but I'd
  // rather not have that be so easy to call by mistake in real code
  beforeEach(async () => {
    await db._runScript('clear-db.sql');
  });

  it('should generate valid listing queries', () => {
    let table = 'junk';
    let listAllExpected = {text: 'SELECT * FROM junk;', values: []};
    let listAllActual = db._generateTableListQuery(table);
    assume(listAllActual).deeply.equals(listAllExpected);
    let oneConditionExpected = {
      text: 'SELECT * FROM junk WHERE a = $1;',
      values: ['aye']
    };
    let oneConditionActual = db._generateTableListQuery(table, {a: 'aye'});
    assume(oneConditionActual).deeply.equals(oneConditionExpected);
    let twoCondititwoxpected = {
      text: 'SELECT * FROM junk WHERE a = $1 AND b = $2;',
      values: ['aye', 'bee']
    };
    let twoConditionActual = db._generateTableListQuery(table, {a: 'aye', b: 'bee'});
    assume(twoConditionActual).deeply.equals(twoCondititwoxpected);
  });

  it('should be empty at start of tests', async () => {
    let instances = await db.listInstances();
    let pendingSpotRequests = await db.listSpotRequests();
    assume(instances).has.length(0);
    assume(pendingSpotRequests).has.length(0);
  });

  it('should be able to insert a spot request', async () => {
    let id = 'r-123456789';
    let state = 'open';

    let result = await db.insertSpotRequest({workerType, region, instanceType, id, state, status});
    result = await db.listSpotRequests();
    assume(result).has.length(1);
    assume(result[0]).has.property('id', id);
  });

  it('should be able to filter spot requests', async () => {
    let id = 'r-123456789';
    let state = 'open';

    let result = await db.insertSpotRequest({workerType, region, instanceType, id, state, status});
    result = await db.listSpotRequests({region: 'us-east-1', state: 'open'});
    assume(result).has.length(0);
    result = await db.listSpotRequests({region: 'us-west-1', state: 'open'});
    assume(result).has.length(1);
  });

  it('should be able to insert an on-demand instance', async () => {
    let id = 'i-123456789';
    let state = 'pending';

    let result = await db.insertInstance({workerType, region, instanceType, id, state});
    let instances = await db.listInstances();
    assume(instances).has.length(1);
    assume(instances[0]).has.property('id', id);
  });

  it('should be able to insert a spot instance, removing the spot request', async () => {
    let id = 'i-123456789';
    let state = 'pending';
    let srid = 'r-123456789';

    await db.insertSpotRequest({workerType, region, instanceType, id: srid, state: 'open', status});
    assume(await db.listSpotRequests()).has.length(1);
    assume(await db.listInstances()).has.length(0);

    let result = await db.insertInstance({workerType, region, instanceType, id, state, srid});
    assume(await db.listSpotRequests()).has.length(0);
    assume(await db.listInstances()).has.length(1);
  });

  it('should be able to upsert a spot instance, removing the spot request', async () => {
    let id = 'i-123456789';
    let state = 'pending';
    let srid = 'r-123456789';

    await db.insertSpotRequest({workerType, region, instanceType, id: srid, state: 'open', status});
    assume(await db.listSpotRequests()).has.length(1);
    assume(await db.listInstances()).has.length(0);

    await db.upsertInstance({workerType, region, instanceType, id, state, srid});
    //await db.upsertInstance({workerType, region, instanceType, id, state, srid});
    assume(await db.listSpotRequests()).has.length(0);
    assume(await db.listInstances()).has.length(1);
  });


  it('should be able to update a spot request', async () => {
    let id = 'r-123456789';
    let firstState = 'open';
    let secondState = 'closed';
    await db.insertSpotRequest({workerType, region, instanceType, id, state: firstState, status});
    let spotRequests = await db.listSpotRequests(); 
    assume(spotRequests).has.length(1);
    assume(spotRequests[0]).has.property('state', firstState);
    await db.updateSpotRequestState({region, id, state: secondState, status});
    spotRequests = await db.listSpotRequests(); 
    assume(spotRequests).has.length(1);
    assume(spotRequests[0]).has.property('state', secondState);
  });

  it('should be able to do a spot request upsert', async () => {
    let id = 'r-123456789';
    let firstState = 'open';
    let secondState = 'closed';
    await db.upsertSpotRequest({workerType, region, instanceType, id, state: firstState, status});
    let spotRequests = await db.listSpotRequests(); 
    assume(spotRequests).has.length(1);
    assume(spotRequests[0]).has.property('state', firstState);
    await db.upsertSpotRequest({workerType, region, instanceType, id, state: secondState, status});
    spotRequests = await db.listSpotRequests(); 
    assume(spotRequests).has.length(1);
    assume(spotRequests[0]).has.property('state', secondState);
  });

  it('should have valid instance counts', async () => {
    // Insert some instances
    await db.insertInstance({id: 'i-1', workerType, region: 'us-east-1', instanceType: 'm3.medium', state: 'running'});
    await db.insertInstance({id: 'i-2', workerType, region: 'us-east-1', instanceType: 'm3.xlarge', state: 'running'});
    await db.insertInstance({id: 'i-3', workerType, region: 'us-west-1', instanceType: 'm3.medium', state: 'running'});
    await db.insertInstance({id: 'i-4', workerType, region: 'us-east-1', instanceType: 'm3.medium', state: 'pending'});
    await db.insertInstance({id: 'i-5', workerType, region: 'us-east-1', instanceType: 'm3.xlarge', state: 'pending'});
    await db.insertInstance({id: 'i-6', workerType, region: 'us-west-1', instanceType: 'm3.medium', state: 'pending'});
    // Let's ensure an instance in a state which we don't care about is in there
    await db.insertInstance({id: 'i-7', workerType, region: 'us-east-1', instanceType: 'm3.2xlarge', state: 'terminated'});
    // Insert some spot requests
    await db.insertSpotRequest({id: 'r-1', workerType, region: 'us-east-1', instanceType: 'c4.medium', state: 'open', status});
    await db.insertSpotRequest({id: 'r-2', workerType, region: 'us-east-1', instanceType: 'c4.xlarge', state: 'open', status});
    await db.insertSpotRequest({id: 'r-3', workerType, region: 'us-west-1', instanceType: 'c4.medium', state: 'open', status});
    await db.insertSpotRequest({id: 'r-4', workerType, region: 'us-east-1', instanceType: 'c4.medium', state: 'open', status});
    await db.insertSpotRequest({id: 'r-5', workerType, region: 'us-east-1', instanceType: 'c4.xlarge', state: 'open', status});
    await db.insertSpotRequest({id: 'r-6', workerType, region: 'us-west-1', instanceType: 'c4.medium', state: 'open', status});
    await db.insertSpotRequest({id: 'r-7', workerType, region: 'us-west-1', instanceType: 'c4.2xlarge', state: 'failed', status});
    let result = await db.instanceCounts({workerType});
    assume(result).has.property('pending');
    assume(result).has.property('running');
    assume(result.pending).has.lengthOf(4);
    assume(result.running).has.lengthOf(2);
  });

  it('should list the pending spot requests', async () => {
    await db.insertSpotRequest({id: 'r-1', workerType, region: 'us-east-1', instanceType: 'c4.medium', state: 'open', status});
    await db.insertSpotRequest({id: 'r-2', workerType, region: 'us-east-1', instanceType: 'c4.xlarge', state: 'open', status});
    await db.insertSpotRequest({id: 'r-3', workerType, region: 'us-east-1', instanceType: 'c4.medium', state: 'open', status});
    await db.insertSpotRequest({id: 'r-4', workerType, region: 'us-east-1', instanceType: 'c4.medium', state: 'open', status});
    await db.insertSpotRequest({id: 'r-5', workerType, region: 'us-west-1', instanceType: 'c4.xlarge', state: 'open', status});
    await db.insertSpotRequest({id: 'r-6', workerType, region: 'us-west-1', instanceType: 'c4.medium', state: 'closed', status});
    await db.insertSpotRequest({id: 'r-7', workerType, region: 'us-west-1', instanceType: 'c4.2xlarge', state: 'failed', status});   
    await db.insertSpotRequest({id: 'r-8', workerType, region: 'us-west-1', instanceType: 'c4.medium', state: 'closed', status});
    await db.insertSpotRequest({id: 'r-9', workerType, region: 'us-west-1', instanceType: 'c4.2xlarge', state: 'failed', status});   
    let result = await db.spotRequestsToPoll({region: 'us-east-1'});
    result.sort();
    assume(result).deeply.equals(['r-1', 'r-2', 'r-3', 'r-4']);
  });

  it('should be able to remove a spot request', async () => {
    let id = 'i-123456789';
    let state = 'pending';
    let srid = 'r-123456789';

    await db.insertSpotRequest({workerType, region, instanceType, id: srid, state: 'open', status});
    assume(await db.listSpotRequests()).has.length(1);
    await db.removeSpotRequest({region, id: srid});
    assume(await db.listSpotRequests()).has.length(0);

  });

  it('should be able to remove an instance', async () => {
    let id = 'i-123456789';
    let state = 'pending';
    let srid = 'r-123456789';

    await db.insertInstance({workerType, region, instanceType, id, state, srid});
    assume(await db.listInstances()).has.length(1);
    await db.removeInstance({region, id});
    assume(await db.listInstances()).has.length(0);
  });


});
