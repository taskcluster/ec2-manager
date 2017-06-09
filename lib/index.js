'use strict';

const sqs = require('sqs-simple');
const assert = require('assert');
const State = require('./state');
const urllib = require('url');

// This is for libraries which we import which are reliant upon babel.
require('source-map-support/register');


async function main() {
  try {
    let db = await openDatabase({dburl: process.env.DATABASE_URL});
  
    //await db.insertSpotRequest({workerType: 'testworkertype', region: 'us-east-2', id: 'r-123456', state: 'open'});
    await db.insertInstance({workerType: 'testworkertype', region: 'us-east-2', id: 'i-12345', state: 'pending', srid: 'r-123456'});


    throw new Error();
    let eventStream = new EventStream({regions: ['us-east-1', 'us-east-2', 'us-west-1', 'us-west-2']});
    let state = new State();
    await eventStream.setupQueues();
    await eventStream.setupRules();
    await eventStream.setupQueueListeners(state);
    eventStream.startListeners();
  } catch (err) {
    process.nextTick(() => { throw err });
  }
}

main().then(() => {}, err => { throw err });

