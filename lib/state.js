'use strict';
const assert = require('assert');
const url = require('url');
const which = require('which');
const path = require('path');
const {spawnSync} = require('child_process');

/**
 * The State class tracks the in-flight status of the instances and spot
 * requests which we care about.  We only track in-flight requests and we do
 * not log the state transitions.  This is to keep the database design simple
 * and avoid issues around duplicate IDs, since AWS will happily reuse.  State
 * is stored in a Posgresql database and does not support alternate backends.
 * The database is accessed through a PG-Pool.
 *
 * Some queries use a transaction internally to ensure data consistency
*/
class State {

  constructor({pgpool, monitor}) {
    this._pgpool = pgpool;
    this._monitor = monitor;
  }

  /**
   * Run a SQL script (e.g. a file) against the database.  Note that this
   * function does *not* run the queries internally, rather uses the database
   * connection parameters value that this State's pg-pool was configured with
   * to run the script using the command line 'psql' program.  It chooses the
   * first 'psql' program in the system's path to run the script.
   * 
   * This is done because I'd like to keep the database definition queries as
   * standard sql scripts, while allowing their use in the setup and teardown
   * sections of the tests
   */
  async _runScript(script) {
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

  /**
   * Either insert or update a spot request.  This ensures an atomic insert or
   * update, but we never learn which one happened.
   */
  async upsertSpotRequest({workerType, region, instanceType, id, state, status}) {
    assert(typeof workerType === 'string');
    assert(typeof region === 'string');
    assert(typeof instanceType === 'string');
    assert(typeof id === 'string');
    assert(typeof state === 'string');
    assert(typeof status === 'string');

    let text = [
      "INSERT INTO spotrequests (id, workerType, region, instanceType, state, status)",
      "VALUES ($1, $2, $3, $4, $5, $6)",
      "ON CONFLICT (region, id) DO UPDATE SET state = $5, status = $6;"
    ].join(' ');

    let values = [id, workerType, region, instanceType, state, status];

    let result = await this._pgpool.query({text, values, name: 'upsert-spot-request'});

    assert(result.rowCount === 1, 'upserting spot request had incorrect rowCount');
  }

  /**
   * Insert a spot request.
   */
  async insertSpotRequest({workerType, region, instanceType, id, state, status}) {
    assert(typeof workerType === 'string');
    assert(typeof region === 'string');
    assert(typeof instanceType === 'string');
    assert(typeof id === 'string');
    assert(typeof state === 'string');
    assert(typeof status === 'string');

    let text = [
      "INSERT INTO spotrequests (id, workerType, region, instanceType, state, status)",
      "VALUES ($1, $2, $3, $4, $5, $6)"
    ].join(' ');

    let values = [id, workerType, region, instanceType, state, status];

    let result = await this._pgpool.query({text, values, name: 'insert-spot-request'});
    assert(result.rowCount === 1, 'inserting spot request had incorrect rowCount');
  }

  /**
   * Update a spot request's state.
   */
  async updateSpotRequestState({region, id, state, status}) {
    assert(typeof region === 'string');
    assert(typeof id === 'string');
    assert(typeof state === 'string');
    assert(typeof status === 'string');

    let result = await this._pgpool.query({
      text: "UPDATE spotrequests SET state = $1, status = $2 WHERE region = $3 AND id = $4",
      values: [state, status, region, id],
      name: 'update-spot-request-state'
    });
    assert(result.rowCount === 1, 'updating spot request state had incorrect rowCount');
  }

  /**
   * Update an instance's state.
   */
  async updateInstanceState({region, id, state}) {
    assert(typeof region === 'string');
    assert(typeof id === 'string');
    assert(typeof state === 'string');

    let result = await this._pgpool.query({
      text: "UPDATE instances SET state = $1 WHERE region = $2 AND id = $3",
      values: [state, region, id],
      name: 'update-instance-state'
    });
    assert(result.rowCount === 1, 'updating instance state had incorrect rowCount');
  }

  /**
   * Insert an instance.  If provided, this function will ensure that any spot
   * requests which have an id of `srid` are removed safely.  The implication
   * here is that any spot request which has an associated instance must have
   * been fulfilled
   */
  async insertInstance({workerType, region, id, instanceType, state, srid}) {
    assert(typeof workerType === 'string');
    assert(typeof region === 'string');
    assert(typeof instanceType === 'string');
    assert(typeof id === 'string');
    assert(typeof state === 'string');
    if (srid) {
      assert(typeof srid === 'string');
    }

    let client = await this._pgpool.connect();

    try {
      await client.query({text: "BEGIN"});

      if (srid) {
        try {
          await this.removeSpotRequest({region, id: srid, client});
        } catch (err) {
          await client.query({text: "ROLLBACK"});
          throw err;
        }
      }
      
      try {
        let text = [
          "INSERT INTO instances (id, workerType, region, instanceType, state, srid)",
          "VALUES ($1, $2, $3, $4, $5, $6)"
        ].join(' ');

        let values = [id, workerType, region, instanceType, state, srid ? srid : 'NULL'];

        let result = await client.query({text, values, name: 'insert-instance'});

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

  /**
   * Insert an instance, or update it if there's a conflict.  If provided, this
   * function will ensure that any spot requests which have an id of `srid` are
   * removed safely.  The implication here is that any spot request which has
   * an associated instance must have been fulfilled
   */
  async upsertInstance({workerType, region, instanceType, id, state, srid}) {
    assert(typeof workerType === 'string');
    assert(typeof region === 'string');
    assert(typeof instanceType === 'string');
    assert(typeof id === 'string');
    assert(typeof state === 'string');
    if (srid) {
      assert(typeof srid === 'string');
    }

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
        let text = [
          "INSERT INTO instances (id, workerType, region, instanceType, state, srid)",
          "VALUES ($1, $2, $3, $4, $5, $6)",
          "ON CONFLICT (region, id) DO UPDATE SET state = $5;"
        ].join(' ');
        let values = [id, workerType, region, instanceType, state, srid ? srid : 'NULL'];
        let result = await client.query({text, values, name: 'upsert-instance'});
        assert(result.rowCount === 1, 'upserting instance had incorrect rowCount');
        await client.query({text: "COMMIT"});
      } catch (err) {
        await client.query({text: "ROLLBACK"});
        throw err;
      }

    } finally {
      client.release();
    }    
  }

  /**
   * Stop tracking a spot request.
   */
  async removeSpotRequest({region, id, client}) {
    assert(typeof region === 'string');
    assert(typeof id === 'string');

    let text = "DELETE FROM spotrequests WHERE id = $1 AND region = $2";
    let values = [id, region];
    let result;
    if (client) {
      result = await client.query({text, values});
    } else {
      result = await this._pgpool.query({text, values, name: 'remove-spot-request'});
    }
    return result.rowCount;
  }

  /**
   * Stop tracking an instance
   */
  async removeInstance({region, id}) {
    assert(typeof region === 'string');
    assert(typeof id === 'string');

    let result = await this._pgpool.query({
      text: "DELETE FROM instances WHERE id = $1 AND region = $2",
      values: [id, region]
    });
    return result.rowCount;
  }

  /**
   * Internal method used to generate queries which have simple and condition
   * checking.  This let's us do very specific queries while not using an ORM
   */
  _generateTableListQuery(table, conditions = {}) {
    assert(typeof table === 'string', 'must provide table');
    assert(typeof conditions === 'object', 'conditions must be object');

    let query = `SELECT * FROM ${table}`;
    let values = [];

    let k = Object.keys(conditions);
    for (let i = 0 ; i < k.length ; i++) {
      // The first condition means we need a WHERE
      if (i === 0) {
        query += ' WHERE';
      }

      // Add the conditional
      query += ` ${k[i]} = $${i + 1}`;
      values.push(conditions[k[i]]);

      // All but the last one need an AND
      if (i < k.length - 1) {
        query += ' AND';
      }
    }
    query += ';';

    return {text: query, values: values};
  }

  /**
   * Internal method which runs the table queries
   */
  async _listTable(table, conditions = {}, client = undefined) {
    assert(typeof table === 'string');
    assert(typeof conditions === 'object');

    let {text, values} = this._generateTableListQuery(table, conditions);
    let p;
    if (!client) {
      p = this._pgpool.query({text, values});
    } else {
      p = client.query({text, values});
    }
    let result = await p;
    return result.rows;   
  }

  /**
   * Get a list of specific instances
   */
  async listInstances(conditions = {}, client = undefined) {
    assert(typeof conditions === 'object');
    return this._listTable('instances', conditions, client);
  }

  /**
   * Get a list of specific spot requests
   */
  async listSpotRequests(conditions = {}, client = undefined) {
    assert(typeof conditions === 'object');
    return this._listTable('spotrequests', conditions, client);
  }

  /**
   * We want to be able to get a simple count of how many worker types there
   * are in a given region.  basically we want something which groups instances
   * first by state (our domain's pending vs. running, not aws's states) and
   * then gives a count for each instance type
   */
  async instanceCounts({workerType}) {
    assert(typeof workerType === 'string');

    let counts = {pending: [], running: []};

    let client = await this._pgpool.connect();
    await client.query("BEGIN");
    try {
      let instanceQuery = [
        "SELECT state, instanceType, count(instanceType) FROM instances",
        "WHERE workerType = $1 AND (state = 'pending' OR state = 'running')",
        "GROUP BY state, instanceType",
      ].join(' ');
      let instancesResult = await client.query({text: instanceQuery, values: [workerType]});

      let spotRequestQuery = [
        "SELECT state, instanceType, count(instanceType) FROM spotrequests",
        "WHERE workerType = $1 AND state = 'open'",
        "GROUP BY state, instanceType",
      ].join(' ');
      let spotRequestsResult = await client.query({text: spotRequestQuery, values: [workerType]});

      await client.query("COMMIT");

      for (let row of instancesResult.rows) {
        let list;
        switch(row.state) {
          case 'running':
            list = counts.running;
            break;
          default:
            list = counts.pending;
            break;
        }
        list.push({instanceType: row.instancetype, count: row.count, type: 'instances'});
      }


      for (let row of spotRequestsResult.rows) {
        counts.pending.push({instanceType: row.instancetype, count: row.count, type: 'spot-request'});
      }     
      return counts;
    } catch (err) {
      client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  }

  /**
   * We want to be able to get a list of those spot requests which need to be
   * checked.  This is done per region only because the list doesn't really
   * make sense to be pan-ec2.  Basically, this is a list of spot requests which
   * we're going to check in on.
   */
  async spotRequestsToPoll({region}) {
    let result = await this.listSpotRequests({region});
    return result.map(x => x.id);
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

module.exports = {State};
