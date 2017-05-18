'use strict';
const assert = require('assert');
const EventEmitter = require('events').EventEmitter;
const urllib = require('url');
const _pg = require('pg');

_pg.defaults.ssl = true;

let pg = _pg.native ? _pg.native : _pg;


/**
 * The Database is a storage place where we track the instances and spot
 * requests which are currently interesting.  This means spot requests which
 * are awaiting adjudication and instances which are pending or running.  When
 * a spot request is denied or an instance is shutdown, we insert that
 * information into our metrics and stop tracking it in this database.
 * 
 * Errors from the underlying Postgres connection will be re-emitted by this
 * class, so it's important to catch the 'error' event.
 * 
 * NOTE: This constructor should not be called directly, rather the factory
 * async function 'openDB' ought to be called.
 */
class Database extends EventEmitter{

  constructor({dburl}) {
    super();
    // For some reasons, the URL format from Heroku doesn't seem to agree with
    // the pg module, so we're going to parse it ourselves.
    let parsedUrl = urllib.parse(dburl);

    let [user, password] = parsedUrl.auth.split(':');

    this._client = new pg.Client({
      user,
      password,
      database: parsedUrl.pathname.replace(/^\//, ''),
      port: Number.parseInt(parsedUrl.port, 10),
      host: parsedUrl.hostname,
      ssl: true,
      application_name: 'ec2_manager'
    });

    this._client.on('error', err => {
      this.emit('error', err);
    });
  }

  /**
   * Asynchronous initialization code required to set up a Database.
   */
  async connect() {
    return new Promise((resolve, reject) => {
      this._client.connect(err => {
        if (err) {
          return reject(err);
        }
        return resolve();
      });
    });
  }

  async insertSpotRequest({workerType, region, id, state}) {
    return new Promise((resolve, reject) => {
      this._client.query({
        text: "INSERT INTO spotrequests (id, workerType, region, state) VALUES ($1, $2, $3, $4)",
        values: [id, workerType, region, state]
      }, (err, result) => {
        if (err) {
          return reject(err);
        }

        if (result.command !== 'INSERT' || result.rowCount != 1) {
          return reject(new Error('Query succeeded but did the wrong thing'));
        }
        console.dir(result);
        resolve();
      });
    });
  }

  async updateSpotRequestState({region, id, state}) {

  }


  async removeSpotRequest({region, id}) {

  }

  async insertInstance({workerType, region, id, state, srid}) {
    let text;
    let values;
    // TODO: When there's a spot request id, an instance creation implies that
    // the spot request was adjudicated fully and either resulted in an
    // instance starting or not.  We should consider doing something like a
    // delete * from spotrequests where id = $srid and run the associated metrics
    return new Promise((resolve, reject) => {
      this._client.query({
        text: "INSERT INTO instances (id, workerType, region, state, srid) VALUES ($1, $2, $3, $4, $5)",
        values: [id, workerType, region, state, srid ? srid : 'NULL']
      }, (err, result) => {
        if (err) {
          return reject(err);
        }
        if (result.command !== 'INSERT' || result.rowCount != 1) {
          return reject(new Error('Query succeeded but did the wrong thing'));
        }
        console.dir(result);
        resolve();
      });
    });    
  }

  async updateInstanceState({region, id, state}) {

  }


  async removeInstance({region, id}) {

  }

  async listAllPendingSpotRequests() {
    let query = "SELECT id FROM spotrequests";
    return new Promise((resolve, reject) => {
      this._client.query(query, (err, result) => {
        if (err) {
          return reject(err);
        }
        let ids = result.rows.map(x => x.id);
        console.dir(ids);
        resolve(ids);
      });
    });
  }

  async listSpotRequestsByRegion(region) {
    let query = "SELECT id FROM spotrequests WHERE region = $1";
    return new Promise((resolve, reject) => {
      this._client.query(query, [region], (err, result) => {
        if (err) {
          return reject(err);
        }
        let ids = result.rows.map(x => x.id);
        console.dir(ids);
        resolve(ids);
      });
    });
  }

  /**
   * This method runs any of the regular cleanup we need.  This includes doing
   *  1. All instances which have an SRID which is a valid SRID in the spotrequests
   *     table should result in marking the SR as fulfilled
   *  2. All instances which are not pending or running are deleted
   *  3. All spot requests which are not awaiting fulfillment are deleted
   *  4. Instances or spot requests which have been pending for a really
   *     long time should be polled with the describe EC2 api and deleted if needed
   */
  async cleanup() {

  }
}

async function openDB(opts) {
  let db = new Database(opts);
  await db.connect();
  console.log('DB Initialized');
  return db;
}

module.exports = {
  openDB: openDB,
  _Database: Database
};
