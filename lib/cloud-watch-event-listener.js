const aws = require('aws-sdk');
const sqslib = require('sqs-simple');
const _ = require('lodash');
const events = require('events');
const assert = require('assert');

class CloudWatchEventListener extends events.EventEmitter {

  constructor({
    region = 'us-east-1',
    sqs = {},
    queueName = 'ec2-events',
    monitor = null,
  }) {
    super();

    // Store the list of regions in which we're operating
    this.region = region;

    // Store some basic configuration values
    this.queueName = queueName;

    // Set up all the AWS clients that we'll possibly need
    this.sqs = sqs;

    // Store the reference to the monitor instance
    this.monitor = monitor.prefix('cloud-watch-events');

    this.queueUrl = undefined;

    this.sqsQueue = undefined
  }

  async init() {
    this.queueUrl = await sqslib.getQueueUrl({sqs: this.sqs, queueName: this.queueName});

    this.sqsQueue = new sqslib.QueueListener({
      sqs: this.sqs,
      queueUrl: this.queueUrl,
      decodeMessage: false,
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
    console.log('HERE IS WHERE ID HANDLE A STATE CHANGE');
    console.log(this.region);
    console.dir(body);
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
