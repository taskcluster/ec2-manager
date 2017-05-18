let database = require('./lib/database');
let uuid = require('uuid');

async function main() {
  let db = new database._Database({dburl: process.env.DATABASE_URL});
  await db.connect();
  await db.insertSpotRequest({workerType: 'test', region: 'us-east-1', id: uuid.v4(), state: 'open'});
  await db.listPendingSpotRequests();
}

main().catch(console.error);
