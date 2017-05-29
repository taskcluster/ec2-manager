'use strict';
const assert = require('assert');
const EventEmitter = require('events').EventEmitter;
const url = require('url');
const which = require('which');
const path = require('path');
const {spawnSync} = require('child_process');

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

  constructor({pgpool}) {
    super();
    this._pgpool = pgpool;
  }

  /**
   * Run a SQL script (e.g. a file) against this database.  Note that this function
   * does *not* run the queries internally, rather uses the DB Url value that this Database
   * was configured with to run the script using the command line 'psql' program.  It chooses
   * the first 'psql' program in the system's path to run the script.
   */
  async _runScript(script) {

    // Instead of parsing SQL scripts and figuring out how to run those queries
    // individually, we'll instead take the URL we're given and parse it to
    // figure out the correct values for the command like psql program so that we
    // can automatically run the sql scripts.  The PGPASSWORD environment
    // variable is deprecated (for good reason!) but it saves us from having to
    // write out a .pgpass file to acheive the same result.
    let args = [];
    let o = this._pgpool.options; // cuts down on verbosity
    if (o.host) {
      args.push(`--host=${o.host}`);
    }
    if (o.port) {
      args.push(`--port=${o.port}`);
    }
    if (o.database) {
      args.push(`--dbname=${o.database}`);
    }
    if (o.user) {
      args.push(`--username=${o.user}`);
    }
    args.push('--file=' + path.normalize(path.join(__dirname, '..', 'sql', script)));

    let env = {};
    if (o.password) {
      env.PGPASSWORD = o.password;
    }

    let result = spawnSync(which.sync('psql'), args, {env, stdio: 'inherit'});

    if (result.status !== 0) {
      let err = new Error('Failed to initialize database');
      err.failingSpawnReturnValue = result;
      throw err;
    }
  }

  async insertSpotRequest({workerType, region, instanceType, id, state}) {
    let result = await this._pgpool.query({
      text: "INSERT INTO spotrequests (id, workerType, region, instanceType, state) VALUES ($1, $2, $3, $4, $5)",
      values: [id, workerType, region, instanceType, state]
    });
    assert(result.rowCount === 1, 'inserting spot request had incorrect rowCount');
  }

  async updateSpotRequestState({region, id, state}) {
    let result = await this._pgpool.query({
      text: "UPDATE spotrequests SET state = $1 WHERE region = $2 AND id = $3",
      values: [state, region, id],
    });
    assert(result.rowCount === 1, 'updating spot request state had incorrect rowCount');
  }

  async updateInstanceState({region, id, state}) {
    let result = await this._pgpool.query({
      text: "UPDATE instances SET state = $1 WHERE region = $2 AND id = $3",
      values: [state, region, id],
    });
    assert(result.rowCount === 1, 'updating instance state had incorrect rowCount');
  }

  async insertInstance({workerType, region, id, instanceType, state, srid}) {
    let client = await this._pgpool.connect();

    try {
      await client.query({text: "BEGIN"});

      if (srid) {
        try {
          await this.removeSpotRequest({region, id: srid, client});
          // METRICS NOTE: we probably want this to be the place where we mark
          // a spot request as fulfilled, since it was
        } catch (err) {
          await client.query({text: "ROLLBACK"});
          throw err;
        }
      }
      
      try {
        let result = await client.query({
          text: "INSERT INTO instances (id, workerType, region, instanceType, state, srid) VALUES ($1, $2, $3, $4, $5, $6)",
          values: [id, workerType, region, instanceType, state, srid ? srid : 'NULL']
        });
        assert(result.rowCount === 1, 'inserting instance had incorrect rowCount');
        await client.query({text: "COMMIT"});
      } catch (err) {
        await client.query({text: "ROLLBACK"});
        throw err;
      }

    } finally {
      client.release();
    }
  }

  // Note that this method is sort of special because it's the only one which
  // we need to potentially call from a transaction.  In order to support this
  // transaction, we need to use a specific client instead of just grabbing one
  // from the pool.
  async removeSpotRequest({region, id, client}) {
    let text = "DELETE FROM spotrequests WHERE id = $1 AND region = $2";
    let values = [id, region];
    let result;
    if (client) {
      result = await client.query({text, values});
    } else {
      result = await this._pgpool.query({text, values});
    }
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
    let result = await this._pgpool.query({text: "SELECT * FROM instances"});
    return result.rows;
  }

  async listAllSpotRequests() {
    let result = await this._pgpool.query({
      text: "SELECT * FROM spotrequests",
      values: []
    });
    return result.rows;
  }

  async listInstancesByWorkerType(workerType) {
    let result = await this._pgpool.query({
      text: "SELECT * FROM instances WHERE workerType = $1",
      values: [workerType]
    });
    return result.rows;
  }

  async listSpotRequestByWorkerType(workerType) {
    let result = await this._pgpool.query({
      text: "SELECT * FROM spotrequests WHERE workerType = $1",
      values: [workerType]
    });
    return result.rows;
  }

  /**
   * This method runs any of the regular cleanup we need.  This includes doing
   *  1. <strike>All instances which have an SRID which is a valid SRID in the spotrequests
   *     table should result in marking the SR as fulfilled</strike> This is taken care of
   *     by using transactions to delete the SRID if it's there when we create an instance
   *  2. All instances which are not pending or running are deleted
   *  3. All spot requests which are not awaiting fulfillment are deleted
   *  4. Instances or spot requests which have been pending for a really
   *     long time should be polled with the describe EC2 api and deleted if needed
   */
  async cleanup() {

  }
}

module.exports = {Database};
