const aws = require('aws-sdk');
const sqs = require('sqs-simple');
const _ = require('lodash');

class CloudWatchEventListener {

  constructor({
    regions = ['us-east-1'],
    awscfg = {},
    queueName = 'ec2-events'
  }) {

    // Store some basic configuration values
    this._queueName = queueName;

    // Store the list of regions in which we're operating
    this._regions = regions;

    // Set up all the AWS clients that we'll possibly need
    let _clients = {ec2: {}, sqs: {}};
    for (let region of regions) {
      let _awscfg = _.defaultsDeep({}, {region}, awscfg);
      _clients.ec2[region] = new aws.EC2(_awscfg);
      _clients.sqs[region] = new aws.SQS(_awscfg);
    }
    this._clients = _clients;

    // Hold references to the queue listeners
    this.sqsQueues = {};

    for (let region of this._regions) {
      let sqs = this._clients.sqs[region];
      let ec2 = this._clients.ec2[region];

      let queueUrl = await sqs.getQueueUrl({sqs, queueName: queueName});

      this.sqsQueues[region] = new sqs.QueueListener({
        sqs,
        queueUrl,
        decodeMessage: false,
        handler: async msg => {
          await this.handleStateChange(msg, region);
        },
      });

      this.sqsQueues[region].on('error', (err, errType) => {
        console.log(errType);
        console.error(err);
      });
    }

  }

  async handleStateChange(msg, region) {
    body = JSON.parse(body);
    console.log('HERE IS WHERE ID HANDLE A STATE CHANGE');
    console.log(region);
    console.dir(body);
  }

  startListeners() {
    for (let region of this._regions) {
      assert(this.sqsQueues[region]);
      this.sqsQueues[region].start();
    }
  }
  
  stopListeners() {
    for (let region of this._regions) {
      assert(this.sqsQueues[region]);
      this.sqsQueues[region].stop();
    }
  }
}



