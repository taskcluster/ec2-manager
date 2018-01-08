const assert = require('assert');
const url = require('url');
const which = require('which');
const path = require('path');
const {spawn} = require('child_process');
const log = require('./log');

/* eslint taskcluster/no-for-in: "off" */

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

  let errors = [];

  for (let key of Object.keys(instance)) {
    if (!requiredKeys.includes(key)) {
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

  if (errors.length > 0) {
    throw new Error(`Invalid Instance:\n\n${errors.join('\n')}`);
  }

  return instance;
}

/**
 * The State class tracks the in-flight status of the instances which we care about.
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

    let text = [
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

    let name = 'insert-instance';

    let result;
    if (client) {
      result = await client.query({text, values, name});
    } else {
      result = await this._pgpool.query({text, values, name});
    }

    assert(result.rowCount === 1, 'inserting instance had incorrect rowCount');
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


    let text = [
      'INSERT INTO instances',
      '(id, "workerType", region, az, "instanceType", state, "imageId", launched, "lastEvent")',
      'VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)',
      'ON CONFLICT (region, id) DO UPDATE SET state = EXCLUDED.state;',
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
    let result;

    let name = 'upsert-instance';

    let result;
    if (client) {
      result = await client.query({text, values, name});
    } else {
      result = await this._pgpool.query({text, values, name});
    }
  } 

  /**
   * Update an instance's state.
   */
  async updateInstanceState({region, id, state, lastEvent}, client) {
    assert(typeof region === 'string');
    assert(typeof id === 'string');
    assert(typeof state === 'string');
    assert(typeof lastEvent === 'object');
    assert(lastEvent.constructor.name === 'Date');

    let text = 'UPDATE instances SET state = $1, "lastEvent" = $2 WHERE region = $3 AND id = $4';
    let values = [state, lastEvent, region, id];
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
  async removeInstance({region, id}, client) {
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
   * Either insert or update an AMI's usage.  This ensures an atomic insert or
   * update, but we never learn which one happened.
   */
  async reportAmiUsage({region, id}, client) {
    assert(typeof region === 'string');
    assert(typeof id === 'string');
    let text = [
      'INSERT INTO amiusage (region, id, "lastUsed")',
      'VALUES ($1, $2, now())',
      'ON CONFLICT (region, id) DO UPDATE SET "lastUsed" = EXCLUDED."lastUsed"',
    ].join(' ');
    
    let values = [region, id];
    
    let result;
    if (client) {
      result = await client.query({text, values, name: 'upsert-amiusage'});
    } else {
      result = await this._pgpool.query({text, values, name: 'upsert-amiusage'});
    }
    assert(result.rowCount === 1, 'upserting AMI usage had incorrect rowCount');
  }

  /**
   * Internal method used to generate queries which have simple and condition
   * checking.  This let's us do very specific queries while not using an ORM
   *
   * NOTE: NOT SAFE FOR USE WITH USER PROVIDED INPUT
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
            conditionClauses.push(`${table}."${condition}" = $${paramIndex}`);
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

    query += ';';

    return {text: query, values: values};
  }

  /**
   * Internal method which runs the table queries
   *
   * NOTE: NOT SAFE FOR USE WITH USER PROVIDED CONDITIONS
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
   *
   * NOTE: NOT SAFE FOR USE WITH USER PROVIDED CONDITIONS
   */
  async listInstances(conditions = {}, client = undefined) {
    assert(typeof conditions === 'object');
    return this._listTable('instances', conditions, client);
  }
   
  /**
   * Get a list of AMIs and their usage
   *
   * NOTE: NOT SAFE FOR USE WITH USER PROVIDED CONDITIONS
   */
  async listAmiUsage(conditions = {}, client = undefined) {
    assert(typeof conditions === 'object');
    return this._listTable('amiusage', conditions, client);
  }
  
  /**
  * Get a list of the current EBS volume usage
  */
  async listEbsUsage(conditions = {}, client = undefined) {
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

    let shouldReleaseClient = false;
    if (!client) {
      shouldReleaseClient = true;
      client = await this._pgpool.connect();
    }

    let counts = {pending: [], running: []};

    await client.query('BEGIN');
    try {
      let instanceQuery = [
        'SELECT state, "instanceType", count("instanceType") FROM instances',
        'WHERE "workerType" = $1 AND (state = \'pending\' OR state = \'running\')',
        'GROUP BY state, "instanceType"',
      ].join(' ');
      let instancesResult = await client.query({text: instanceQuery, values: [workerType]});

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
        list.push({instanceType: row.instanceType, count: row.count, type: 'instance'});
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

  async listWorkerTypes(client) {
    let text = [
      'SELECT DISTINCT "workerType" FROM instances',
      'ORDER BY "workerType";',
    ].join(' ');
    let result;
    if (client) {
      result = await client.query(text);
    } else {
      result = await this._pgpool.query(text);
    }
    return result.rows.map(x => x.workerType);
  }

  /**
   * List all the instance ids by region
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
      let instances = await this.listInstances({workerType}, client);
      // Let's find *all* the instance-ids
      let instanceIds = instances.map(x => {
        return {region: x.region, id: x.id};
      });
      await client.query('COMMIT');
      return {instanceIds};
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

    assert(result.rowCount === 1, 'logging cloud watch event had incorrect rowCount');
  }
}

module.exports = {State};
