'use strict';
const assert = require('assert');
const EventEmitter = require('events').EventEmitter;
const url = require('url');
const which = require('which');
const path = require('path');
const _pg = require('pg');
const {spawnSync} = require('child_process');

_pg.defaults.ssl = true;

const pg = _pg.native ? _pg.native : _pg;

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

    // We should store the Database's URL for future use
    this._dburl = dburl;

    // For some reasons, the URL format from Heroku doesn't seem to agree with
    // the pg module, so we're going to parse it ourselves.
    let parsedUrl = url.parse(dburl);

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
   * Run a SQL script (e.g. a file) against this database.  Note that this function
   * does *not* run the queries internally, rather uses the DB Url value that this Database
   * was configured with to run the script using the command line 'psql' program.  It chooses
   * the first 'psql' program in the system's path to run the script.
   */
  async _runScript(script) {
    // We need to parse the URL that Heroku and friends parse to us
    let dburlparts = url.parse(this._dburl)
    let [user, password] = dburlparts.auth.split(':');

    // Instead of parsing SQL scripts and figuring out how to run those queries
    // individually, we'll instead take the URL we're given and parse it to
    // figure out the correct values for the command like psql program so that we
    // can automatically run the sql scripts.  The PGPASSWORD environment
    // variable is deprecated (for good reason!) but it saves us from having to
    // write out a .pgpass file to acheive the same result.
    let result = spawnSync(which.sync('psql'), [
      '--host=' + dburlparts.hostname,
      '--port=' + dburlparts.port,
      '--dbname=' + dburlparts.pathname.slice(1),
      '--username=' + user,
      '--file=' + path.normalize(path.join(__dirname, '..', 'sql', script)),
    ], {
      env: {'PGPASSWORD': password},
      stdio: 'inherit'
    });
    if (result.status !== 0) {
      let err = new Error('Failed to initialize database');
      err.failingSpawnReturnValue = result;
      throw err;
    }
  }

  async _runQuery({text, values, expectedCommand, expectedRowCount}) {
    let result = await new Promise((resolve, reject) => {
      this._client.query({text, values}, (err, result) => {
        if (err) return reject(err);
        resolve(result);
      });
    });
    if (expectedCommand && result.command !== expectedCommand) {
      console.dir(result);
      throw new Error('Query succeeded but ran wrong command');
    }
    if (expectedRowCount && result.rowCount !== expectedRowCount) {
      console.dir(result);
      throw new Error('Query returned unexpected number of results');
    }
    return result;
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

  async insertSpotRequest({workerType, region, instanceType, id, state}) {
    await this._runQuery({
      text: "INSERT INTO spotrequests (id, workerType, region, instanceType, state) VALUES ($1, $2, $3, $4, $5)",
      values: [id, workerType, region, instanceType, state],
      expectedCommand: 'INSERT',
      expectedRowCount: 1
    });
  }

  async updateSpotRequestState({region, id, state}) {
    console.log(region);
    console.log(id);
    console.log(state);
    let result = await this._runQuery({
      text: "UPDATE spotrequests SET state = $1 WHERE region = $2 AND id = $3",
      values: [state, region, id],
      expectedRowCount: 1
    });
  }

  async updateInstanceState({region, id, state}) {

  }

  async insertInstance({workerType, region, id, instanceType, state, srid}) {
    await this._runQuery({text: "BEGIN"});

    if (srid) {
      try {
        await this.removeSpotRequest({region, id: srid});
      } catch (err) {
        await this._runQuery({text: "ROLLBACK"});
        throw err;
      }
    }
    
    try {
      await this._runQuery({
        text: "INSERT INTO instances (id, workerType, region, instanceType, state, srid) VALUES ($1, $2, $3, $4, $5, $6)",
        values: [id, workerType, region, instanceType, state, srid ? srid : 'NULL'],
        expectedCommand: 'INSERT',
        expectedRowCount: 1
      });
      await this._runQuery({text: "COMMIT"});
    } catch (err) {
      await this._runQuery({text: "ROLLBACK"});
      throw err;
    }
  }

  async removeSpotRequest({region, id}) {
    let result = await this._runQuery({
      text: "DELETE FROM spotrequests WHERE id = $1 AND region = $2",
      values: [id, region],
      expectedCommand: 'DELETE'
    });
    return result.rowCount;
  }

  async removeInstance({region, id}) {
    let result = await this._runQuery({
      text: "DELETE FROM instances WHERE id = $1 AND region = $2",
      values: [id, region]
    });
    return result.rowCount;
  }

  async listAllInstances() {
    let result = await this._runQuery({text: "SELECT * FROM instances"});
    return result.rows;
  }

  async listAllSpotRequests() {
    let result = await this._runQuery({
      text: "SELECT * FROM spotrequests",
      values: []
    });
    return result.rows;
  }

  async listInstancesByWorkerType(workerType) {
    let result = await this._runQuery({
      text: "SELECT * FROM instances WHERE workerType = $1",
      values: [workerType]
    });
    return result.rows;
  }

  async listSpotRequestByWorkerType(workerType) {
    let result = await this._runQuery({
      text: "SELECT * FROM spotrequests WHERE workerType = $1",
      values: [workerType]
    });
    return result.rows;
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
