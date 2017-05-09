'use strict';

const aws = require('aws-sdk');
const sqs = require('sqs-simple');
const assert = require('assert');
const State = require('./state');
const urllib = require('url');

// This is for libraries which we import which are reliant upon babel.
require('source-map-support/register');

const EVENT_QUEUE_NAME = 'ec2-event-queue';


class EventStream {
  // TODO:
  //   * allow passing in credentials instead of reading from env
  //   * allow passing in ec2 client options
  constructor({regions}) {
    assert(regions, 'must provide a list of regions to operate in');
    assert(Array.isArray(regions), 'regions must be an array');

    this._regions = regions;

    // Have API clients for all the AWS services
    this._clients = {
      ec2: {},
      sqs: {},
      cwe: {}
    };
    for (let region of regions) {
      this._clients.ec2[region] = new aws.EC2({region: region});
      this._clients.sqs[region] = new aws.SQS({region: region});
      this._clients.cwe[region] = new aws.CloudWatchEvents({region: region});
    }

    // Hold references to all the sqsQueue details
    this.sqsQueuesInfo = {};

    // Hold references to the queue listeners
    this.sqsQueues = {};

    // Hardcode for now
    this._ruleName = 'ec2-event-state-transitions';
  }

  async setupQueueListeners(state) {
    for (let region of this._regions) {
      this.sqsQueues[region] = new sqs.QueueListener({
        sqs: this._clients.sqs[region],
        queueUrl: this.sqsQueuesInfo[region].queueUrl,
        decodeMessage: false,
        handler: async body => {
          body = JSON.parse(body);
          await state.setInstanceState(body.detail['instance-id'], body.detail.state);
        }
      });

      this.sqsQueues[region].on('error', (err, errType) => {
        console.log(errType);
        console.error(err);
      });
    }
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

  async setupQueues() {
    for (let region of this._regions) {
      let queueInfo = await sqs.initQueue({
        queueName: EVENT_QUEUE_NAME,
        sqs: this._clients.sqs[region]
      });
      
      this.sqsQueuesInfo[region] = queueInfo;
    }
  }

  /**
   * Set up the CloudWatch Events rules needed to get EC2 state change events
   * sent to the appropriate SQS queues
   */
  async setupRules() {
    for (let region of this._regions) {
      let cwe = this._clients.cwe[region];
      let hasRuleAlready = false;
      let rulesList;
      do {
        let request = {
          NamePrefix: this._ruleName
        };
        if (rulesList && rulesList.NextToken) {
          request.NextToken = rulesList.NextToken;
        }
        rulesList = await cwe.listRules(request).promise();
        for (let rule of rulesList.Rules) {
          if (rule.Name === this._ruleName) {
            console.log('Rule already exists with this name');
            hasRuleAlready = true;
            break;
          }
        }
      } while (rulesList.NextToken);
      
      if (!hasRuleAlready) {
        let response = await cwe.putRule({
          Name: this._ruleName,
          Description: "This rule sends all EC2 instance state change notifications to an SQS queue",
          EventPattern: JSON.stringify({
            "source": [
              "aws.ec2"
            ],
            "detail-type": [
              "EC2 Instance State-change Notification"
            ]
          }),
          State: 'ENABLED'
        }).promise();
        console.log('Rule ARN: ' + response.RuleArn);
      }

      let hasTargetAlready = false;
      let targetsList;
      do {
        let request = {
          Rule: this._ruleName,
        }
        if (targetsList && targetsList.NextToken) {
          request.NextToken = targetsList.NextToken;
        }
        targetsList = await cwe.listTargetsByRule(request).promise();
        for (let target of targetsList.Targets) {
          if (target.Arn === this.sqsQueuesInfo[region].queueArn) {
            console.log('Already have a target');
            hasTargetAlready = true;
            break;
          }
        }
      } while(targetsList.NextToken);

      if (!hasTargetAlready) {
        console.log('Setting up target for ' + JSON.stringify(this.sqsQueuesInfo[region].queueArn));
        let response = await cwe.putTargets({
          Rule: this._ruleName,
          Targets: [{
            Arn: this.sqsQueuesInfo[region].queueArn,
            Id: this._ruleName + '-target',
          }]
        }).promise();

        if (response.FailedEntryCount > 0) {
          throw new Error('Failed to set up target');
        }
      }
    }
  }
}


async function main() {
  try {
    let db = await openDatabase({dburl: process.env.DATABASE_URL});
  
    //await db.insertSpotRequest({workerType: 'testworkertype', region: 'us-east-2', id: 'r-123456', state: 'open'});
    await db.insertInstance({workerType: 'testworkertype', region: 'us-east-2', id: 'i-12345', state: 'pending', srid: 'r-123456'});


    throw new Error();
    let eventStream = new EventStream({regions: ['us-east-1', 'us-east-2', 'us-west-1', 'us-west-2']});
    let state = new State();
    await eventStream.setupQueues();
    await eventStream.setupRules();
    await eventStream.setupQueueListeners(state);
    eventStream.startListeners();
  } catch (err) {
    process.nextTick(() => { throw err });
  }
}

main().then(() => {}, err => { throw err });

