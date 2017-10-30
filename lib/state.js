const assert = require('assert');
const url = require('url');
const which = require('which');
const path = require('path');
const {spawn} = require('child_process');
const log = require('./log');

/* eslint taskcluster/no-for-in: "off" */

function assertValidSpotRequest(spotRequest) {
  let requiredKeys = [
    'id',
    'workerType',
    'region',
    'az',
    'instanceType',
    'state',
    'status',
    'imageId',
    'created',
  ];
  let errors = [];
  for (let key of Object.keys(spotRequest)) {
    if (!requiredKeys.includes(key)) {
      errors.push(`Extraneous Key "${key}" found`);
    }
  }

  for (let key of requiredKeys) {
    if (!spotRequest[key]) {
      errors.push(`Value not found for required key "${key}"`);
      continue;
    } else if (key === 'created') {
      if (typeof spotRequest[key] !== 'object') {
        errors.push(`Key "${key}" is incorrect type "${typeof spotRequest[key]}"`);
      }
    } else if (typeof spotRequest[key] !== 'string') {
      errors.push(`Key "${key}" is incorrect type "${typeof spotRequest[key]}"`);
    }
  }

  if (errors.length > 0) {
    throw new Error(`Invalid Spot Request:\n\n${errors.join('\n')}`);
  }

  return spotRequest;
}

function assertValidInstance(instance) {
  let requiredKeys = [
    'id',
    'workerType',
    'region',
    'az',
    'instanceType',
    'state',
    'imageId',
    'launched',
    'lastevent',
  ];

  let allowedKeys = [
    'srid',
  ];

  let dateKeys = [
    'launched',
    'lastevent',
  ];

  let errors = [];

  for (let key of Object.keys(instance)) {
    if (!requiredKeys.includes(key) && !allowedKeys.includes(key)) {
      errors.push(`Extraneous Key "${key}" found`);
    }
  }

  for (let key of requiredKeys) {
    let value = instance[key];
    if (!value) {
      errors.push(`Value not found for required key "${key}"`);
      continue;
    } else if (dateKeys.includes(key)) {
      if (typeof value !== 'object' || value.constructor.name !== 'Date') {
        errors.push(`Key "${key}" is incorrect type "${typeof value} or not a Date"`);
      }
    } else if (typeof value !== 'string') {
      errors.push(`Key "${key}" is incorrect type "${typeof value}"`);
    }
  }

  for (let key of allowedKeys) {
    let value = instance[key];
    if (value && typeof value !== 'string') {
      errors.push(`Optional key "${key}" is incorrect type "${typeof value}"`);
    }
  }

  if (errors.length > 0) {
    throw new Error(`Invalid Instance:\n\n${errors.join('\n')}`);
  }

  return instance;
}

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

    let psql = which.sync('psql');

    log.debug({cmd: psql, args, env}, 'Running PSQL Command');

    return new Promise(async(resolve, reject) => {
      let psql = await new Promise((res2, rej2) => {
        which('psql', (err, value) => {
          if (err) {
            return rej2(err);
          }
          return res2(value);
        });
      });

      let output = [];

      let proc = spawn(psql, args, {env});

      proc.stdout.on('data', data => {
        output.push(data);
      });

      proc.stderr.on('data', data => {
        output.push(data);
      });

      proc.on('close', code => {
        if (code === 0) {
          resolve();
        } else {
          output = Buffer.concat(output);
          
          // Let's build a string representation of the command so that it's
          // easy to figure out what we were trying to do
          let redoCmd = [];
          if (env) {
            for (let e in env) {
              if (e === 'PGPASSWORD') {
                redoCmd.push(`${e}='<scrubbed>'`);
              } else {
                redoCmd.push(`${e}='${env[e]}'`);
              }
            }
          }
          redoCmd.push(psql);
          for (let arg of args) {
            // Maybe make this a regex for all whitespace
            if (arg.includes(' ')) {
              redoCmd.push(`'${arg.replace(/"/g, '\\"')}'`);
            } else {
              redoCmd.push(arg);
            }
          }

          let err = new Error('PSQL Exited with ' + code + '\nretry: ' + redoCmd.join(' ') + '\n\n' + output);
          err.cmd = psql;
          err.args = args;
          err.env = env;
          reject(err);
        }
      });
    });
  }

  /**
   * Return a client which an external user of this class can use to string
   * commands together
   *
   * NOTE: It's *very* important that this object's .release() method gets
   * called.  Otherwise, the database client pool will be starved
   */
  async getClient() {
    return this._pgpool.connect();
  }

  /**
   * Begin a transaction
   */
  async beginTransaction() {
    let client = await this.getClient();
    try {
      await client.query('BEGIN;');
      return client;
    } catch (err) {
      client.release();
      throw err;
    }
  }

  /**
   * Commit a transaction
   */
  async commitTransaction(client) {
    try {
      await client.query('COMMIT;');
    } catch (err) {
      throw err;
    } finally {
      client.release();
    }
  }

  /**
   * Rollback a transaction
   */
  async rollbackTransaction(client) {
    try {
      await client.query('ROLLBACK;');
    } catch (err) {
      throw err;
    } finally {
      client.release();
    }
  }

  /**
   * Insert a spot request.
   */
  async insertSpotRequest(spotRequest, client) {
    let {
      id,
      workerType,
      region,
      az,
      instanceType,
      state,
      status,
      imageId,
      created,
    } = assertValidSpotRequest(spotRequest);

    let text = [
      'INSERT INTO spotrequests (id, workerType, region, az, instanceType, state, status, imageid, created)',
      'VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)',
    ].join(' ');
    let values = [id, workerType, region, az, instanceType, state, status, imageId, created];
    let name = 'insert-spot-request';

    let result;
    if (client) {
      result = await client.query({text, values, name});
    } else {
      result = await this._pgpool.query({text, values, name});
    }
    assert(result.rowCount === 1, 'inserting spot request had incorrect rowCount');
  }

  /**
   * Either insert or update a spot request.  This ensures an atomic insert or
   * update, but we never learn which one happened.
   */
  async upsertSpotRequest(spotRequest, client) {
    let {
      id,
      workerType,
      region,
      az,
      instanceType,
      state,
      status,
      imageId,
      created,
    } = assertValidSpotRequest(spotRequest);

    let text = [
      'INSERT INTO spotrequests (id, workerType, region, az, instanceType, state, status, imageid, created)',
      'VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)',
      'ON CONFLICT (region, id) DO UPDATE SET state = EXCLUDED.state, status = EXCLUDED.status;',
    ].join(' ');

    let values = [id, workerType, region, az, instanceType, state, status, imageId, created];

    let result;
    if (client) {
      result = await client.query({text, values, name: 'upsert-spot-request'});
    } else {
      result = await this._pgpool.query({text, values, name: 'upsert-spot-request'});
    }

    assert(result.rowCount === 1, 'upserting spot request had incorrect rowCount');
  }

  /**
   * Update a spot request's state.
   */
  async updateSpotRequestState({region, id, state, status}, client) {
    assert(typeof region === 'string');
    assert(typeof id === 'string');
    assert(typeof state === 'string');
    assert(typeof status === 'string');

    let text = 'UPDATE spotrequests SET state = $1, status = $2 WHERE region = $3 AND id = $4';
    let values = [state, status, region, id];
    let name = 'update-spot-request-state';

    let result;
    if (client) {
      result = await client.query({text, values, name});
    } else {
      result = await this._pgpool.query({text, values, name});
    }

    assert(result.rowCount === 1, 'updating spot request state had incorrect rowCount');
  }

  /**
   * Stop tracking a spot request.
   */
  async removeSpotRequest({region, id}, client) {
    assert(typeof region === 'string');
    assert(typeof id === 'string');

    let text = 'DELETE FROM spotrequests WHERE id = $1 AND region = $2';
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
   * Insert an instance.  If provided, this function will ensure that any spot
   * requests which have an id of `srid` are removed safely.  The implication
   * here is that any spot request which has an associated instance must have
   * been fulfilled
   */
  async insertInstance(instance, client) {
    let {
      workerType,
      region,
      az,
      id,
      instanceType,
      state,
      srid,
      imageId,
      launched,
      lastevent,
    } = assertValidInstance(instance);

    let shouldReleaseClient = false;
    if (!client) {
      shouldReleaseClient = true;
      client = await this._pgpool.connect();
    } 

    try {
      await client.query({text: 'BEGIN'});

      if (srid) {
        try {
          await this.removeSpotRequest({region, id: srid}, client);
        } catch (err) {
          await client.query({text: 'ROLLBACK'});
          throw err;
        }
      }
      
      try {
        let text = [
          'INSERT INTO instances',
          '(id, workerType, region, az, instanceType, state, srid, imageid, launched, lastevent)',
          'VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)',
        ].join(' ');

        let values = [
          id,
          workerType,
          region,
          az,
          instanceType,
          state,
          srid,
          imageId,
          launched,
          lastevent,
        ];

        let result = await client.query({text, values, name: 'insert-instance'});

        assert(result.rowCount === 1, 'inserting instance had incorrect rowCount');
        await client.query({text: 'COMMIT'});
      } catch (err) {
        await client.query({text: 'ROLLBACK'});
        throw err;
      }
    } finally {
      if (shouldReleaseClient) {
        client.release();
      }
    }
  }

  /**
   * Insert an instance, or update it if there's a conflict.  If provided, this
   * function will ensure that any spot requests which have an id of `srid` are
   * removed safely.  The implication here is that any spot request which has
   * an associated instance must have been fulfilled
   */
  async upsertInstance(instance, client) {
    let {
      workerType,
      region,
      az,
      id,
      instanceType,
      state,
      srid,
      imageId,
      launched,
      lastevent,
    } = assertValidInstance(instance);

    let shouldReleaseClient = false;
    if (!client) {
      shouldReleaseClient = true;
      client = await this._pgpool.connect();
    }

    try {
      await client.query({text: 'BEGIN'});

      if (srid) {
        try {
          await this.removeSpotRequest({region, id: srid}, client);
          // METRICS NOTE: we probably want this to be the place where we mark
          // a spot request as fulfilled, since it was
        } catch (err) {
          await client.query({text: 'ROLLBACK'});
          throw err;
        }
      }
      
      try {
        let text = [
          'INSERT INTO instances',
          '(id, workerType, region, az, instanceType, state, srid, imageid, launched, lastevent)',
          'VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)',
          'ON CONFLICT (region, id) DO UPDATE SET state = EXCLUDED.state;',
        ].join(' ');
        let values = [
          id,
          workerType,
          region,
          az,
          instanceType,
          state,
          srid,
          imageId,
          launched,
          lastevent,
        ];
        let result = await client.query({text, values, name: 'upsert-instance'});
        assert(result.rowCount === 1, 'upserting instance had incorrect rowCount');
        await client.query({text: 'COMMIT'});
      } catch (err) {
        await client.query({text: 'ROLLBACK'});
        throw err;
      }
    } finally {
      if (shouldReleaseClient) {
        client.release();
      }
    }    
  }

  /**
   * Update an instance's state.
   */
  async updateInstanceState({region, id, state, lastevent}, client) {
    assert(typeof region === 'string');
    assert(typeof id === 'string');
    assert(typeof state === 'string');
    assert(typeof lastevent === 'object');
    assert(lastevent.constructor.name === 'Date');

    let text = 'UPDATE instances SET state = $1, lastevent = $2 WHERE region = $3 AND id = $4';
    let values = [state, lastevent, region, id];
    let name = 'update-instance-state';
    
    let result;
    if (client) {
      result = await client.query({text, values, name});
    } else {
      result = await this._pgpool.query({text, values, name});
    }

    assert(result.rowCount === 1, 'updating instance state had incorrect rowCount');
  }

  /**
   * Stop tracking an instance
   */
  async removeInstance({region, id, srid}, client) {
    assert(typeof region === 'string');
    assert(typeof id === 'string');

    let text = 'DELETE FROM instances WHERE id = $1 AND region = $2';
    let values =[id, region];
    let name = 'remove-instance';

    let result;
    if (client) {
      result = await client.query({text, values, name});
    } else {
      result = await this._pgpool.query({values, text, name});
    }

    return result.rowCount;
  }

  /**
   * Internal method used to generate queries which have simple and condition
   * checking.  This let's us do very specific queries while not using an ORM
   */
  _generateTableListQuery(table, conditions = {}, forUpdate = false) {
    assert(typeof table === 'string', 'must provide table');
    assert(typeof conditions === 'object', 'conditions must be object');

    let query = `SELECT * FROM ${table}`;
    let values = [];

    let k = Object.keys(conditions);

    if (k.length > 0) {
      let whereClauses = [];
      let paramIndex = 1;
      for (let condition in conditions) {
        if (Array.isArray(conditions[condition])) {
          let conditionClauses = [];
          for (let value of conditions[condition]) {
            conditionClauses.push(`${table}.${condition} = $${paramIndex}`);
            values.push(value);
            paramIndex++;
          }
          conditionClauses = conditionClauses.join(' OR ');
          if (k.length === 1) {
            whereClauses.push(conditionClauses);
          } else {
            whereClauses.push(`(${conditionClauses})`);
          }
        } else {
          whereClauses.push(`${table}.${condition} = $${paramIndex}`);
          values.push(conditions[condition]);
          paramIndex++;
        }
      }

      query += ' WHERE ';
      query += whereClauses.join(' AND ');
    }

    if (forUpdate) {
      query += ' FOR UPDATE';
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

    // We want a FOR UPDATE query if a client is passed in because the only
    // reason to pass in a client for a query is to lock the rows as part of a
    // transaction!
    let {text, values} = this._generateTableListQuery(table, conditions, client ? true : false);

    let result;
    if (client) {
      result = await client.query({text, values});
    } else {
      result = await this._pgpool.query({text, values});
    }

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
  async instanceCounts({workerType}, client) {
    assert(typeof workerType === 'string');

    let shouldReleaseClient = false;
    if (!client) {
      shouldReleaseClient = true;
      client = await this._pgpool.connect();
    }

    let counts = {pending: [], running: []};

    await client.query('BEGIN');
    try {
      let instanceQuery = [
        'SELECT state, instanceType, count(instanceType) FROM instances',
        'WHERE workerType = $1 AND (state = \'pending\' OR state = \'running\')',
        'GROUP BY state, instanceType',
      ].join(' ');
      let instancesResult = await client.query({text: instanceQuery, values: [workerType]});

      let spotRequestQuery = [
        'SELECT state, instanceType, count(instanceType) FROM spotrequests',
        'WHERE workerType = $1 AND state = \'open\'',
        'GROUP BY state, instanceType',
      ].join(' ');
      let spotRequestsResult = await client.query({text: spotRequestQuery, values: [workerType]});

      await client.query('COMMIT');

      for (let row of instancesResult.rows) {
        let list;
        switch (row.state) {
          case 'running':
            list = counts.running;
            break;
          default:
            list = counts.pending;
            break;
        }
        list.push({instanceType: row.instancetype, count: row.count, type: 'instance'});
      }

      for (let row of spotRequestsResult.rows) {
        counts.pending.push({instanceType: row.instancetype, count: row.count, type: 'spot-request'});
      }     
      return counts;
    } catch (err) {
      client.query('ROLLBACK');
      throw err;
    } finally {
      if (shouldReleaseClient) {
        client.release();
      }
    }
  }

  /**
   * We want to be able to get a list of those spot requests which need to be
   * checked.  This is done per region only because the list doesn't really
   * make sense to be pan-ec2.  Basically, this is a list of spot requests which
   * we're going to check in on.
   */
  async spotRequestsToPoll({region}, client) {
    let states = ['open'];
    let statuses = [
      'pending-evaluation',
      'pending-fulfillment',
    ];
    let result = await this.listSpotRequests({region, state: states, status: statuses}, client);
    // This is slightly inefficient because we're selecting * from the table
    // instead of just the id.  I think that's really not a huge deal
    // considering we're not even using a cursor or anything...  Let's optimize
    // that when we can show it's actuall important enough to make the query
    // generator complex enough to understand it
    return result.map(x => x.id);
  }

  async listWorkerTypes(client) {
    let text = [
      'SELECT workertype FROM instances',
      'UNION',
      'SELECT workertype FROM spotrequests',
      'ORDER BY workertype;',
    ].join(' ');
    let result;
    if (client) {
      result = await client.query(text);
    } else {
      result = await this._pgpool.query(text);
    }
    return result.rows.map(x => x.workertype);
  }

  /**
   * List all the instance ids and request ids by region
   * so that we can kill them
   */
  async listIdsOfWorkerType({workerType}, client) {

    let shouldReleaseClient = false;
    if (!client) {
      shouldReleaseClient = true;
      client = await this._pgpool.connect();
    } 

    await client.query('BEGIN');

    try {
      let instances = await this.listInstances({workertype: workerType}, client);
      let requests = await this.listSpotRequests({workertype: workerType}, client);
      // Let's find *all* the instance-ids and spot-instance-request-ids.  Remember that
      // there are spot requests for the instances which we're not tracking.  Let's
      // make sure to cancel those just to be on the safer side of life
      let instanceIds = instances.map(x => {
        return {region: x.region, id: x.id};
      });
      let instanceSrids = instances.map(x => {
        return {region: x.region, id: x.srid}; 
      }).filter(x => x.id !== 'NULL' && x.id); // I'm sure we could register the NULL
      // type to be parsed as a JS falsy...

      let requestIds = requests.map(x => {
        return {region: x.region, id: x.id}; 
      }).concat(instanceSrids);

      await client.query('COMMIT');
      return {instanceIds, requestIds};
    } catch (err) {
      await client.query('ROLLBACK');
    } finally {
      if (shouldReleaseClient) {
        client.release();
      }
    }
  }

  async logCloudWatchEvent({region, id, state, generated}, client) {
    assert(typeof region === 'string');
    assert(typeof id === 'string');
    assert(typeof state === 'string');
    assert(typeof generated === 'object');
    assert(generated.constructor.name === 'Date');

    let text = [
      'INSERT INTO cloudwatchlog (id, region, state, generated)',
      'VALUES ($1, $2, $3, $4);',
    ].join(' ');
    let values = [id, region, state, generated];
    let name = 'insert-cloud-watch-log';

    let result;
    try {
      if (client) {
        result = await client.query({text, values, name});
      } else {
        result = await this._pgpool.query({text, values, name});
      }
    } catch (err) {
      // We're going to ignore this primary key violation because it's a sign
      // that a message has already been handled by this system.  Since this is
      // a second receiption of the message we don't really want to log it
      if (err.sqlState !== '23505') {
        throw err;
      }
    }
    assert(result.rowCount === 1, 'inserting spot request had incorrect rowCount');
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
