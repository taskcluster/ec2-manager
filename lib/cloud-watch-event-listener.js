const aws = require('aws-sdk');
const sqslib = require('sqs-simple');
const _ = require('lodash');
const events = require('events');
const assert = require('assert');
const {runAWSRequest} = require('./aws-request');
const {tagResources} = require('./tag-resources');
const log = require('./log');

function missingTags(obj) {
  let hasTag = false;
  if (obj.Tags) {
    for (let tag of obj.Tags) {
      if (tag.Key === 'Owner') {
        hasTag = true;
      }
    }
  }
  return !hasTag;
};

class CloudWatchEventListener extends events.EventEmitter {

  constructor({
    state,
    sqs,
    ec2,
    queueName = 'ec2-events',
    monitor,
    region,
    keyPrefix,
    runaws = runAWSRequest,
  }) {
    super();
    
    // Store the reference to the State we're using
    assert(typeof state === 'object');
    this.state = state;

    // Store the list of regions in which we're operating
    assert(typeof region === 'string');
    this.region = region;

    // Store some basic configuration values
    assert(typeof queueName === 'string');
    this.queueName = queueName;

    // We want to ensure that the keyPrefix is in the correct trailing colon
    // format passed in to avoid confusion, but we only internally use it as
    // the actual value without the colon, so we store that here
    assert(typeof keyPrefix === 'string');
    assert(keyPrefix[keyPrefix.length - 1] === ':');
    this.provisionerId = keyPrefix.slice(0, keyPrefix.length - 1);
    this.keyPrefix = keyPrefix;

    // Set up all the AWS clients that we'll possibly need
    assert(sqs);
    this.sqs = sqs;

    assert(ec2);
    this.ec2 = ec2;

    // We should always be using a
    assert(this.ec2.config.region === region);
    assert(this.sqs.config.region === region);

    // Store the reference to the monitor instance
    this.monitor = monitor.prefix('cloud-watch-events');

    this.queueUrl = undefined;

    this.sqsQueue = undefined;

    this.runaws = runaws;
  }

  async init() {
    this.queueUrl = await sqslib.getQueueUrl({sqs: this.sqs, queueName: this.queueName});

    this.sqsQueue = new sqslib.QueueListener({
      sqs: this.sqs,
      queueUrl: this.queueUrl,
      decodeMessage: false,
      maxNumberOfMessages: 10,
      handler: async msg => {
        let doodad = this.monitor.timeKeeper('message-handler-time');
        await this.__handler(msg);
        doodad.measure();
        this.monitor.count('handled-messages', 1);
      },
    });

    this.sqsQueue.on('error', (err, errType) => {
      // We probably want to bubble this up... maybe?
      //this.emit('error', err, errType);
      this.monitor.count('handler-errors', 1);
      log.error({err, errType}, 'SQS Handler Error');
      this.monitor.reportError(err, {errType});
    });
  }

  async __handler(msg) {
    let body = JSON.parse(msg);
    let region = body.region;
    let id = body.detail['instance-id'];
    let state = body.detail.state;

    try {
      await this.state.logCloudWatch({region, id, state});
    } catch (err) {
      // We don't want to block things
      this.monitor.reportError(err);
    }

    if (state === 'pending' || state === 'running') {
      let apiResponse;
      try {
        apiResponse = await this.runaws(this.ec2, 'describeInstances', {
          InstanceIds: [id]
        });
      } catch (err) {
        // We're ignoring this error because it might happen that it is only
        // delay in internal EC2 updates.  Given that, we're going to wait
        // until we've exhausted all redeliveries, which we do in the dead
        // letter queue handler
        if (err.code !== 'InvalidInstanceID.NotFound') {
          this.monitor.reportError(err);
        } else {
          this.monitor.count('global.api-lag', 1);
          this.monitor.count(`${region}.api-lag`, 1);
        }
        throw err;
      }

      // TODO: CRITICAL Skip things which have a keyname which does not match ours

      assert(Array.isArray(apiResponse.Reservations));
      assert(apiResponse.Reservations.length === 1);
      assert(Array.isArray(apiResponse.Reservations[0].Instances));
      assert(apiResponse.Reservations[0].Instances.length === 1);
      let instance = apiResponse.Reservations[0].Instances[0];

      let [provisionerId, workerType] = instance.KeyName.split(':');
      let instanceType = instance.InstanceType;
      let srid = instance.SpotInstanceRequestId;

      // We check for workertype being truthy because it's always possible that
      // this instance is one which is not in the provisioner/ec2-manager sphere
      // of knowledge at all and as such has no colons in its name.
      if (workerType && missingTags(instance)) {
        await tagResources({
          runaws: this.runaws,
          ec2: this.ec2,
          ids: [id],
          keyPrefix: this.keyPrefix,
          workerType: workerType,
        });
      }

      if (workerType && provisionerId === this.provisionerId) {
        await this.state.upsertInstance({workerType, region, instanceType, id, state, srid});
        log.info({
          workerType,
          region,
          instanceType,
          id,
          state,
          srid: srid ? srid : 'undefined',
        }, 'CloudWatch Event resulting in insertion');
      } else {
        log.info({
          provisionerId,
          workerType,
          region,
          instanceType,
          id,
          state,
          srid: srid ? srid : 'undefined',
        }, 'Ignoring instance because it does not belong to this manager');
      }
    } else {
      await this.state.removeInstance({region, id});
      log.info({region, id}, 'CloudWatch Event resulting in deletion');
    }
  }

  start() {
    assert(this.sqsQueue);
    this.sqsQueue.start();
  }
  
  stop() {
    assert(this.sqsQueue);
    this.sqsQueue.stop();
  }
}

class DeadCloudWatchEventListener extends events.EventEmitter {

  constructor({
    sqs,
    queueName = 'ec2-events',
    monitor,
    region,
  }) {
    super();

    // Store the list of regions in which we're operating
    assert(typeof region === 'string');
    this.region = region;

    // Store some basic configuration values
    assert(typeof queueName === 'string');
    this.queueName = queueName;

    // Set up all the AWS clients that we'll possibly need
    assert(sqs);
    this.sqs = sqs;

    assert(this.sqs.config.region === region);

    // Store the reference to the monitor instance
    this.monitor = monitor.prefix('cloud-watch-events');

    this.queueUrl = undefined;

    this.sqsQueue = undefined;
  }

  async init() {
    this.queueUrl = await sqslib.getQueueUrl({sqs: this.sqs, queueName: this.queueName});

    this.sqsQueue = new sqslib.QueueListener({
      sqs: this.sqs,
      queueUrl: this.queueUrl,
      decodeMessage: false,
      maxNumberOfMessages: 10,
      maxReceiveCount: 20,
      handler: async msg => {
        await this.__handler(msg);
      },
    });

    this.sqsQueue.on('error', (err, errType) => {
      // We probably want to bubble this up... maybe?
      //this.emit('error', err, errType);
      log.error({err, errType}, 'SQS Handler Error');
    });
  }

  // TODO: Maybe what we should do is store these instance ids in a table and
  // poll them to see when they do become available and insert them into the
  // database *then*
  async __handler(msg) {
    let errorMsg = [
      'UNTRACKED INSTANCE\n\n',
      'A CloudWatch Event message has failed.  This is likely because the',
      'EC2 API call to DescribeInstances did not return information.  While',
      'we do retry this a number of times, we eventually give up.  This instance',
      'should probably be killed or else deleted.'
    ].join(' ');

    errorMsg += '\nFailing message follows:\n\n';
    errorMsg += msg;

    this.monitor.reportError(new Error(errorMsg), 'info');
  }

  start() {
    assert(this.sqsQueue);
    this.sqsQueue.start();
  }
  
  stop() {
    assert(this.sqsQueue);
    this.sqsQueue.stop();
  }
}

async function initCloudWatchEventListener(opts) {
  let obj = new CloudWatchEventListener(opts);
  await obj.init();
  return obj;
}

async function initDeadCloudWatchEventListener(opts) {
  let obj = new DeadCloudWatchEventListener(opts);
  await obj.init();
  return obj;
}

module.exports = {
  initCloudWatchEventListener,
  initDeadCloudWatchEventListener,
  CloudWatchEventListener
};
