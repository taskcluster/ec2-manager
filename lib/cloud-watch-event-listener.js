const aws = require('aws-sdk');
const sqslib = require('sqs-simple');
const _ = require('lodash');
const events = require('events');
const assert = require('assert');
const runaws = require('./aws-request').runAWSRequest;

class CloudWatchEventListener extends events.EventEmitter {

  constructor({
    state,
    sqs,
    ec2,
    queueName = 'ec2-events',
    monitor = null,
    region = 'us-east-1',
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

    // Set up all the AWS clients that we'll possibly need
    assert(sqs);
    this.sqs = sqs;

    assert(ec2);
    this.ec2 = ec2;

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
      console.log(errType);
      console.error(err);
    });
  }

  async __handler(msg) {
    let body = JSON.parse(msg);
    let region = body.region;
    let id = body.detail['instance-id'];
    let state = body.detail.state;

    if (state === 'pending' || state === 'running') {
      let apiResponse = await runaws(this.ec2, 'describeInstances', {
        InstanceIds: [id]
      });

      assert(Array.isArray(apiResponse.Reservations));
      assert(apiResponse.Reservations.length === 1);
      assert(Array.isArray(apiResponse.Reservations[0].Instances));
      assert(apiResponse.Reservations[0].Instances.length === 1);
      let instance = apiResponse.Reservations[0].Instances[0];

      let workerType = instance.KeyName.split(':').slice(1, 2)[0];
      let instanceType = instance.InstanceType;
      let srid = instance.SpotInstanceRequestId;

      await this.state.upsertInstance({workerType, region, instanceType, id, state, srid});
      console.log(`CLOUD WATCH EVENT: ${workerType} ${region}:${id} change to ${state}`);
    } else {
      await this.state.removeInstance({region, id});
      console.log(`CLOUD WATCH EVENT: removing ${region}:${id} because of change to ${state}`);
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

async function initCloudWatchEventListener(opts) {
  let obj = new CloudWatchEventListener(opts);
  await obj.init();
  return obj;
}

module.exports = {initCloudWatchEventListener};
