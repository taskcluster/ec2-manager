const assert = require('assert');
const url = require('url');
const which = require('which');
const path = require('path');
const {spawn} = require('child_process');
const log = require('./log');
const crypto = require('crypto');

/* eslint taskcluster/no-for-in: "off" */

const TERMINATION_CODES = {
  clean_shutdown: ['Client.InstanceInitiatedShutdown', 'Client.UserInitiatedShutdown'],
  spot_kill: ['Server.SpotInstanceTermination'],
  insufficient_capacity: ['Server.InsufficientInstanceCapacity'],
  volume_limit_exceeded: ['Client.VolumeLimitExceeded'],
  missing_ami: ['Client.InvalidSnapshot.NotFound'],
  startup_failed: ['Server.InternalError', 'Client.InternalError'],
};

function _checker(thing, {requiredKeys, allowedKeys, dateKeys}) {
  let allAllowedKeys = [].concat(requiredKeys, allowedKeys);

  let errors = [];

  for (let key of Object.keys(thing)) {
    if (!allAllowedKeys.includes(key)) {
      errors.push(`Extraneous Key "${key}" found`);
    }
  }

  for (let key of allAllowedKeys) {
    let value = thing[key];
    let isreq = requiredKeys.includes(key);

    if (!value) {
      if (isreq) {
        errors.push(`Value not found for required key "${key}"`);
      }
      continue;
    }
    
    // Now, let's check in detail
    if (dateKeys.includes(key)) {
      if (typeof value !== 'object' || value.constructor.name !== 'Date') {
        errors.push(`Key "${key}" is incorrect type "${typeof value} or not a Date"`);
      }
    } else if (typeof value !== 'string') {
      errors.push(`Key "${key}" is incorrect type "${typeof value}"`);
    }
  }

  if (errors.length > 0) {
    console.dir(errors);
    throw new Error(`Invalid Instance:\n\n${errors.join('\n')}`);
  }

  return thing;
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
    'lastEvent',
  ];

  let dateKeys = [
    'launched',
    'lastEvent',
  ];

  return _checker(instance, {requiredKeys, dateKeys});
} 

function assertValidTermination(termination) {
  let requiredKeys = [
    'id',
    'workerType',
    'region',
    'az',
    'instanceType',
    'imageId',
    'launched',
    'lastEvent',
  ];

  let allowedKeys = [
    'code',
    'reason',
    'terminated',
  ];

  let dateKeys = [
    'launched',
    'lastEvent',
    'terminated',
  ];

  return _checker(termination, {requiredKeys, dateKeys, allowedKeys});
}

/**
 * The State class tracks the in-flight status of the instances which we care about.
*/
class State {

  constructor({pgpool, monitor}) {
    this._pgpool = pgpool;
    this._monitor = monitor;
    this._queryHash = new Map();
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

    return new Promise(async (resolve, reject) => {
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

  // Run a query.  Query should be a parameterized string.  Values is a list of
  // parameters for the query.  rollbackOnError (default: true) will cause a
  // rollback to happen if there's an error in the query execution, so that we
  // don't need to catch and rethrow everywhere that calls this
  async runQuery({query, values, client, rollbackOnError = true}) {

    assert(typeof query === 'string', 'query must be string');

    if (typeof values !== 'undefined') {
      assert(typeof values === 'object', 'values must be an object (array)');
      assert(Array.isArray(values), 'values must be an array');
    }

    if (client) {
      assert(typeof client === 'object');
    }
    
    let name = crypto.createHash('sha1').update(query).digest('hex');
    if (!this._queryHash.has(name)) {
      this._queryHash.set(name, query);
    }

    log.debug({query, values, inTransaction: !!client}, 'running query');

    if (client) {
      try {
        return await client.query({text: query, values, name});
      } catch (err) {
        if (rollbackOnError) {
          await this.rollbackTransaction(client);
        }
        log.warn({err, query, values}, 'query in transaction failed');
        throw err;
      }
    } else {
      try {
        return await this._pgpool.query({text: query, values, name});
      } catch (err) {
        log.warn({err, query, values}, 'direct query failed');
      }
    }
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
   * Insert an instance.
   */
  async insertInstance(instance, client) {
    let {
      workerType,
      region,
      az,
      id,
      instanceType,
      state,
      imageId,
      launched,
      lastEvent,
    } = assertValidInstance(instance);

    let query = [
      'INSERT INTO instances',
      '(id, "workerType", region, az, "instanceType", state, "imageId", launched, "lastEvent")',
      'VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)',
    ].join(' ');

    let values = [
      id,
      workerType,
      region,
      az,
      instanceType,
      state,
      imageId,
      launched,
      lastEvent,
    ];

    let result = await this.runQuery({query, values, client});
  }

  /**
   * Insert an instance, or update it if there's a conflict.
   */
  async upsertInstance(instance, client) {
    let {
      workerType,
      region,
      az,
      id,
      instanceType,
      state,
      imageId,
      launched,
      lastEvent,
    } = assertValidInstance(instance);

    let query = [
      'INSERT INTO instances',
      '(id, "workerType", region, az, "instanceType", state, "imageId", launched, "lastEvent")',
      'VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)',
      'ON CONFLICT (region, id) DO',
      'UPDATE SET state = EXCLUDED.state',
      'WHERE instances."lastEvent" IS NULL OR instances."lastEvent" < EXCLUDED."lastEvent";',
    ].join(' ');

    let values = [
      id,
      workerType,
      region,
      az,
      instanceType,
      state,
      imageId,
      launched,
      lastEvent,
    ];

    await this.runQuery({query, values, client});
  } 

  /**
   * Update an instance's state.  Return true if an update occured
   */
  async updateInstanceState({region, id, state, lastEvent}, client) {
    assert(typeof region === 'string');
    assert(typeof id === 'string');
    assert(typeof state === 'string');
    assert(typeof lastEvent === 'object');
    assert(lastEvent.constructor.name === 'Date');

    let query = [
      'UPDATE instances',
      'SET state = $1, "lastEvent" = $2',
      'WHERE region = $3 AND id = $4 AND "lastEvent" < $2',
    ].join(' ');

    let values = [state, lastEvent, region, id];
    
    let result = await this.runQuery({query, values, client});
    return result.rowCount === 1;
  }

  /**
   * Stop tracking an instance
   */
  async removeInstance({region, id}, client) {
    assert(typeof region === 'string');
    assert(typeof id === 'string');

    let query = 'DELETE FROM instances WHERE id = $1 AND region = $2';
    let values =[id, region];
    
    await this.runQuery({query, values, client});
  }
  
  /**
   * Insert an termination.
   */
  async insertTermination(termination, client) {
    let {
      id,
      workerType,
      region,
      az,
      instanceType,
      imageId,
      code,
      reason,
      launched,
      terminated,
      lastEvent,
    } = assertValidTermination(termination);

    let query = [
      'INSERT INTO terminations',
      '(id, "workerType", region, az, "instanceType", "imageId", code, reason, launched, terminated, "lastEvent")',
      'VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)',
    ].join(' ');

    let values = [
      id,
      workerType,
      region,
      az,
      instanceType,
      imageId,
      code,
      reason,
      launched,
      terminated,
      lastEvent,
    ];

    await this.runQuery({query, values, client});
  }

  /**
   * Insert an termination, or update it if there's a conflict.
   */
  async upsertTermination(termination, client) {
    let {
      id,
      workerType,
      region,
      az,
      instanceType,
      imageId,
      code,
      reason,
      launched,
      terminated,
      lastEvent,
    } = assertValidTermination(termination);

    let query = [
      'INSERT INTO terminations',
      '(id, "workerType", region, az, "instanceType", "imageId", code, reason, launched, terminated, "lastEvent")',
      'VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)',
      'ON CONFLICT (region, id) DO UPDATE SET ',
      'code = EXCLUDED.code, ',
      'reason = EXCLUDED.reason, ',
      'terminated = EXCLUDED.terminated',
      'WHERE terminations."lastEvent" IS NULL OR terminations."lastEvent" < EXCLUDED."lastEvent";',
    ].join(' ');

    let values = [
      id,
      workerType,
      region,
      az,
      instanceType,
      imageId,
      code,
      reason,
      launched,
      terminated,
      lastEvent,
    ];

    await this.runQuery({query, values, client});
  } 

  /**
   * Update an termination's state.
   */
  async updateTerminationState({region, id, code, reason, terminated, lastEvent}, client) {
    assert(typeof region === 'string', 'incorrect region type');
    assert(typeof id === 'string', 'incorrect id type');
    assert(typeof code === 'string', 'incorrect code type');
    assert(typeof reason === 'string', 'incorrect reason type');
    if (terminated) {
      assert(typeof terminated === 'object', 'incorrect terminated type');
      assert(terminated.constructor.name === 'Date', 'terminated is not a Date');
    }
    assert(typeof lastEvent === 'object', 'incorrect lastEvent type');
    assert(lastEvent.constructor.name === 'Date', 'lastEvent is not a Date');

    let query;
    let values;
    let name;

    if (terminated) { 
      query = 'UPDATE terminations SET code = $1, reason = $2, terminated = $3, "lastEvent" = $4';
      query += ' WHERE region = $5 AND id = $6;';
      values = [code, reason, terminated, lastEvent, region, id];
      name = 'update-termination-state-with-termination-time';
    } else {
      query = 'UPDATE terminations SET code = $1, reason = $2, "lastEvent" = $3';
      query += ' WHERE region = $4 AND id = $5;';
      values = [code, reason, lastEvent, region, id];
      name = 'update-termination-state-without-termination-time';
    }

    await this.runQuery({query, values, client});
  }

  /**
   * Stop tracking an termination
   */
  async removeTermination({region, id}, client) {
    assert(typeof region === 'string');
    assert(typeof id === 'string');

    let query = 'DELETE FROM terminations WHERE id = $1 AND region = $2';
    let values =[id, region];

    await this.runQuery({query, values, client});
  }
  
  /**
   * Either insert or update an AMI's usage.  This ensures an atomic insert or
   * update, but we never learn which one happened.
   */
  async reportAmiUsage({region, id}, client) {
    assert(typeof region === 'string');
    assert(typeof id === 'string');
    let query = [
      'INSERT INTO amiusage (region, id, "lastUsed")',
      'VALUES ($1, $2, now())',
      'ON CONFLICT (region, id) DO UPDATE SET "lastUsed" = EXCLUDED."lastUsed"',
    ].join(' ');
    
    let values = [region, id];
    
    await this.runQuery({query, values, client});
  }

  /**
   * Internal method used to generate queries which have simple and condition
   * checking.  This let's us do very specific queries while not using an ORM
   *
   * NOTE: NOT SAFE FOR USE WITH USER PROVIDED CONDITION OR TABLE NAMES
   */
  _generateTableListQuery(table, conditions = {}, forUpdate = false, limit) {
    assert(typeof table === 'string', 'must provide table');
    assert(typeof conditions === 'object', 'conditions must be object');

    if (limit) {
      assert(typeof limit === 'number');
      assert(limit % 1 === 0);
      assert(limit > 0);
    }

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
            if (value === null) {
              conditionClauses.push(`${table}."${condition}" IS NULL`);
            } else {
              values.push(value);
              conditionClauses.push(`${table}."${condition}" = $${paramIndex}`);
              paramIndex++;
            }
          }
          conditionClauses = conditionClauses.join(' OR ');
          if (k.length === 1) {
            whereClauses.push(conditionClauses);
          } else {
            whereClauses.push(`(${conditionClauses})`);
          }
        } else if (conditions[condition] === null) {
          whereClauses.push(`${table}."${condition}" IS NULL`);
        } else {
          whereClauses.push(`${table}."${condition}" = $${paramIndex}`);
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

    if (limit) {
      query += ' LIMIT ' + limit; 
    }

    query += ';';

    return {query, values: values};
  }

  /**
   * Internal method which runs the table queries
   *
   * NOTE: NOT SAFE FOR USE WITH USER PROVIDED CONDITION OR TABLE NAMES
   */
  async _listTable({table, conditions = {}, limit}, client) {
    assert(typeof table === 'string');
    assert(typeof conditions === 'object');
    if (limit) {
      assert(typeof limit === 'number');
      assert(limit % 1 === 0);
      assert(limit > 0);
    }

    // We want a FOR UPDATE query if a client is passed in because the only
    // reason to pass in a client for a query is to lock the rows as part of a
    // transaction!
    let {query, values} = this._generateTableListQuery(table, conditions, client ? true : false, limit);

    let result = await this.runQuery({query, values, client});

    return result.rows || [];
  }

  /**
   * Get a list of specific instances.
   *
   * NOTE: NOT SAFE FOR USE WITH USER PROVIDED CONDITION NAMES
   */
  async listInstances(conditions = {}, client) {
    assert(typeof conditions === 'object');
    return this._listTable({table: 'instances', conditions}, client);
  }

  /**
   * Get a list of specific terminations
   *
   * NOTE: NOT SAFE FOR USE WITH USER PROVIDED CONDITION NAMES
   */
  async listTerminations(conditions = {}, client) {
    assert(typeof conditions === 'object');
    return this._listTable({table: 'terminations', conditions}, client);
  }

  /**
   * Get a list of terminations which need to be polled.  Since this will likely
   * involve table-level locking, we'll get terminations for all regions
   */
  async findTerminationsToPoll(count, client) {
    assert(typeof count === 'number');

    let query = [
      'SELECT id, region FROM terminations',
      'WHERE code IS NULL AND reason IS NULL AND terminated > now() - interval \'1h\'',
      'LIMIT $1',
    ].join(' ');
    let values = [count];

    let result = await this.runQuery({query, values, client});

    return result.rows || [];
  }
   
  /**
   * Get a list of AMIs and their usage
   *
   * NOTE: NOT SAFE FOR USE WITH USER PROVIDED CONDITION NAMES
   */
  async listAmiUsage(conditions = {}, client) {
    assert(typeof conditions === 'object');
    return this._listTable({table: 'amiusage', conditions}, client);
  }
  
  /**
  * Get a list of the current EBS volume usage
  */
  async listEbsUsage(conditions = {}, client) {
    assert(typeof conditions === 'object');
    return this._listTable('ebsusage', conditions, client);
  }

  /**
   * We want to be able to get a simple count of how many worker types there
   * are in a given region.  basically we want something which groups instances
   * first by state (our domain's pending vs. running, not aws's states) and
   * then gives a count for each instance type
   */
  async instanceCounts({workerType}, client) {
    assert(typeof workerType === 'string');

    let query = [
      'SELECT state, "instanceType", count("instanceType") FROM instances',
      'WHERE "workerType" = $1 AND (state = \'pending\' OR state = \'running\')',
      'GROUP BY state, "instanceType"',
    ].join(' ');

    let values = [workerType];

    let result = await this.runQuery({query, values, client});

    let counts = {pending: [], running: []};

    for (let row of result.rows) {
      let list;
      if (row.state === 'running') {
        list = counts.running;
      } else {
        list = counts.pending;
      }
      list.push({instanceType: row.instanceType, count: row.count, type: 'instance'});
    }

    return counts;
  }

  async listWorkerTypes(client) {
    let query = [
      'SELECT DISTINCT "workerType" FROM instances',
      'ORDER BY "workerType";',
    ].join(' ');

    let result = await this.runQuery({query, client});
    return result.rows.map(x => x.workerType);
  }

  /**
   * List all the instance ids by region
   * so that we can kill them
   */
  async listIdsOfWorkerType({workerType}, client) {

    let result = await this.listInstances({workerType}, client);

    return {instanceIds: result.map(x => {
      return {region: x.region, id: x.id};
    })};

  }
    
  /**
   * Log a request to AWS services
   */
  async logAWSRequest({
    region,
    requestId,
    duration,
    method,
    service,
    error,
    called,
    code,
    message,
    workerType,
    az,
    instanceType,
    imageId,
  }) {
    assert(typeof region === 'string');
    assert(typeof requestId === 'string');
    assert(typeof duration === 'number');
    assert(typeof method === 'string');
    assert(typeof service === 'string');
    assert(typeof error === 'boolean');
    assert(typeof called === 'object');
    assert(called.constructor.name === 'Date');

    let columns = ['region', 'requestId', 'duration', 'method', 'service', 'error', 'called'];

    let values = [
      region,
      requestId,
      duration,
      method,
      service,
      error,
      called,
    ];

    if (code || message || error) {
      assert(typeof code === 'string');
      assert(typeof message === 'string');
      assert(error);
      columns.push('code');
      columns.push('message');
      values.push(code);
      values.push(message);
    }

    if (workerType) {
      assert(typeof workerType === 'string');
      columns.push('workerType');
      values.push(workerType);
    }

    if (az) {
      assert(typeof az === 'string');
      columns.push('az');
      values.push(az);
    }

    if (instanceType) {
      assert(typeof instanceType === 'string');
      columns.push('instanceType');
      values.push(instanceType);
    }

    if (imageId) {
      assert(typeof imageId === 'string');
      columns.push('imageId');
      values.push(imageId);
    }

    // Sanity check
    assert(columns.length === values.length, 'differing number of columns and values');

    let vals = [];
    for (let i = 1 ; i <= values.length; i++) {
      vals.push('$' + i);
    }

    // Fix the duration.  We want to store an interval, which requires a
    // special values entry
    let durIdx = columns.indexOf('duration');
    values[durIdx] = duration;
    vals[durIdx] = `$${durIdx + 1} * interval '1 us'`;

    let query = [
      `INSERT INTO awsrequests (${columns.map(x => `"${x}"`).join(', ')})`,
      `VALUES (${vals.join(', ')})`,
      'ON CONFLICT DO NOTHING;',
    ].join(' ');
    
    return this.runQuery({query, values});
  }

  /**
   * Get a list of AMIs and their usage
   *
   * NOTE: NOT SAFE FOR USE WITH USER PROVIDED CONDITION NAMES
   */
  async listAWSRequests(conditions = {}, client) {
    assert(typeof conditions === 'object');
    return this._listTable({table: 'awsrequests', conditions}, client);
  }

  /**
   * Determine the health of aws requests in each region, az and instanceType
   * configuration, grouped by those properties but only in the date range
   * specified
   */
  async getRequestHealth({end, start, workerType}) {
    assert(typeof end === 'object');
    assert(typeof start === 'object');
    assert(end.constructor.name === 'Date');
    assert(start.constructor.name === 'Date');
    if (workerType) {
      assert(typeof workerType === 'string');
    }

    let query = [
      'SELECT region, az, "instanceType",',
      'count(nullif(error=TRUE, TRUE)) AS successful,',
      'count(nullif(error=TRUE, FALSE)) AS failed,',
      'count(nullif(code LIKE \'Invalid%\', FALSE)) AS "configuration_issue",',
      'count(nullif(code = \'RequestLimitExceeded\', FALSE)) AS "throttled_calls",',
      'count(nullif(code LIKE \'Insufficient%\', FALSE)) AS "insufficient_capacity",',
      'count(nullif(code LIKE \'%LimitExceeded\' AND code != \'RequestLimitExceeded\', FALSE)) as "limit_exceeded"',
      'FROM awsrequests',
      'WHERE method = \'runInstances\'',
      'AND called > $1 AND called <= $2',
    ];
    let values = [start, end];

    if (workerType) {
      query.push('AND "workerType" = $3');
      values.push(workerType);
    }

    query.push('GROUP BY region, az, "instanceType";');

    query = query.join(' ');

    let result = await this.runQuery({query, values});

    return result.rows || [];
  }

  /**
   * Determine the health of instance terminations in each region, az and
   * instanceType configuration, grouped by those properties but only in the
   * date range specified
   */
  async getTerminationHealth({end, start, workerType}) {
    assert(typeof end === 'object');
    assert(typeof start === 'object');
    assert(end.constructor.name === 'Date');
    assert(start.constructor.name === 'Date');
    if (workerType) {
      assert(typeof workerType === 'string');
    }

    // We're going to grade the terminations for each configuration.  This
    // query is somewhat complicated to generate, but we're doing it this way
    // to make sure that we're not missing any codes
    //
    // First, select the rows which we'll use to label the groups
    let query = [
      'SELECT region, az, "instanceType"',
    ];

    let values = [];

    // Next figure out all the counts we need.  First, we'll count the groups
    // of termination codes which we care about
    let knownCodes = [];
    for (let key of Object.keys(TERMINATION_CODES)) {
      query.push(`, count(nullif(code = \'${TERMINATION_CODES[key].join('\' OR code = \'')}\', FALSE)) AS "${key}"`);
      Array.prototype.push.apply(knownCodes, TERMINATION_CODES[key]);
    }

    // Now that we know all the termination codes which we do care about,
    // let's make sure that we have something to highlight unknown error codes
    query.push(`, count(nullif(code = \'${knownCodes.join('\' OR code = \'')}\', TRUE)) AS "unknown_codes"`);

    // We also want to show how many terminations had no code, meaning they weren't found
    // in the database
    query.push(', count(nullif(code is null, FALSE)) as "no_code"');
    query.push('FROM terminations');
    
    // Now we need to specify the date ranges for the queries
    query.push('WHERE terminated > $1 AND terminated <= $2');
    Array.prototype.push.apply(values, [start, end]);

    if (workerType) {
      query.push('AND "workerType" = $3');
      values.push(workerType);
    }

    Array.prototype.push.apply(query, [
      'GROUP BY region, az, "instanceType" ',
      'ORDER BY region, az, "instanceType";',
    ]);

    query = query.join(' ');

    let result = await this.runQuery({query, values});

    return result.rows || [];
  }

  /**
   * Determine the statistics about the instances which are currently running.
   * These instances are assumed to be in good standing, and so they should be
   * added to those which are in a known-good state
   */
  async getRunningInformation({workerType}) {
    if (workerType) {
      assert(typeof workerType === 'string');
    }

    // We'll list the running instances.  This is not filtered by the number of
    // minutes, since everything which is currently running is...  running and
    // should be included in the statistics
    let query = [
      'SELECT region, az, "instanceType", count(*) as "running" FROM instances',
    ];

    let values = [];

    if (workerType) {
      query.push('WHERE "workerType" = $1'); 
      values.push(workerType);
    }

    Array.prototype.push.apply(query, [
      'GROUP BY region, az, "instanceType"',
      'ORDER BY region, az, "instanceType";',
    ]);

    query = query.join(' ');

    let result = await this.runQuery({query, values});

    return result.rows || [];
  }

  async getHealth(opts) {
    let {minutes, workerType} = opts || {};
    minutes = minutes || 24 * 60;
    assert(typeof minutes === 'number', 'minutes must be a number');
    if (workerType) {
      assert(typeof workerType === 'string');
    }

    // Rather than doing locking on the DB, which isn't even possible for
    // queries which have aggregating functions, we'll instead timebox our
    // queries.  This is safe because the only time we change the tables involved,
    // we are certain to use transactions.  E.g. an instance wouldn't be counted
    // as a termination *and* a running instance because the conversion from
    // running instance to termination is done in a transaction.
    let end = (await this._pgpool.query('SELECT now();')).rows[0].now;

    let start = new Date(end);
    start.setMinutes(start.getMinutes() - minutes);

    return {
      requestHealth: await this.getRequestHealth({start, end, workerType}),
      terminationHealth: await this.getTerminationHealth({start, end, workerType}),
      running: await this.getRunningInformation({workerType}),
    };
  }

  /** 
   * Get the most recent errors.  workerType parameter limits to a specific worker type,
   * minutes gives that many minutes into the past, count gives the upper bounds to how many
   */
  async getRecentErrors(opts) {
    let {count, minutes, workerType} = opts || {};
    minutes = minutes || 24 * 60;
    assert(typeof minutes === 'number', 'minutes must be a number');
    if (workerType) {
      assert(typeof workerType === 'string');
    }
    count = count || 1000;
    assert(typeof count === 'number', 'count must be a number');
  
    let end = (await this._pgpool.query('SELECT now();')).rows[0].now;

    let start = new Date(end);
    start.setMinutes(start.getMinutes() - minutes);

    let query = [
      'SELECT \'termination\' AS type, t.region, t.az, t."instanceType", t."workerType", t.code, t.time, t.message',
      /*** START SUBQUERY 1 ***/
      'FROM (',
      'select region, az, "instanceType", "workerType", code, terminated AS time, reason AS message',
      'FROM terminations',
      `WHERE code <> \'${TERMINATION_CODES.clean_shutdown.join('\' AND code <> \'')}\'`,
      'AND code IS NOT NULL',
      'AND terminated > $1 AND terminated <= $2',
      workerType ? 'AND "workerType" = $4' : '',
      'ORDER BY terminated',
      'LIMIT $3',
      ') AS t',
      /*** END SUBQUERY 1 ***/
      'UNION',
      /*** START SUBQUERY 2 ***/
      'SELECT \'instance-request\' AS type, a.region, a.az, a."instanceType", a."workerType",',
      'a.code, a.time, a.message',
      'FROM (',
      'SELECT region, az, "instanceType", "workerType", code, called AS time, message',
      'FROM awsrequests',
      'WHERE error = true AND method = \'runInstances\'',
      'AND called > $1 AND called <= $2',
      workerType ? 'AND "workerType" = $4' : '',
      'ORDER BY called LIMIT $3',
      ') AS a',
      /*** END SUBQUERY 2 ***/
      'ORDER BY time',
      'LIMIT $3;',
    ];

    let values = [start, end, count];
    if (workerType) {
      values.push(workerType);
    }

    query = query.join(' ');

    let result = await this.runQuery({query, values});

    let recentErrors = result.rows || [];

    return recentErrors.map(x => {
      if (x.code === 'UnauthorizedOperation') {
        x.message = '----- HIDDEN -----';
      }
      return x;
    });
  }

  async logCloudWatchEvent({region, id, state, generated}) {
    assert(typeof region === 'string');
    assert(typeof id === 'string');
    assert(typeof state === 'string');
    assert(typeof generated === 'object');
    assert(generated.constructor.name === 'Date');

    let query = [
      'INSERT INTO cloudwatchlog (id, region, state, generated)',
      'VALUES ($1, $2, $3, $4)',
      'ON CONFLICT DO NOTHING;',
    ].join(' ');

    let values = [id, region, state, generated];

    await this.runQuery({query, values});
  }
}

module.exports = {State, TERMINATION_CODES};
