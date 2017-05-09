const aws = require('aws-sdk');
const sqs = require('sqs-simple');
const _ = require('lodash');


/**
 * Create and link the CloudWatch Events rules and targets as well as the SQS
 * queues to send ec2 instance state change messages
 */
async function setupCloudWatchEvents({
  regions = ['us-east-1'],
  awscfg = {},
  ruleName = 'ec2-instance-state-transitions',
  queueName = 'ec2-events'
}) {
  // Set up all the AWS clients that we'll possibly need
  let clients = {ec2: {}, sqs: {}, cwe: {}};
  for (let region of regions) {
    let _awscfg = _.defaultsDeep({}, {region}, awscfg);
    let ec2 = new aws.EC2(_awscfg);
    let sqs = new aws.SQS(_awscfg);
    let cwe = new aws.CloudWatchEvents(_awscfg);

    let queueInfo = await sqs.initQueue({queueName: _queueName, sqs});

    // Let's list the rules and determine if the rule already exists
    let hasRuleAlready = false;
    let rulesList;
    do {
      let request = {
        NamePrefix: _ruleName
      };
      if (rulesList && rulesList.NextToken) {
        request.NextToken = rulesList.NextToken;
      }
      rulesList = await cwe.listRules(request).promise();
      for (let rule of rulesList.Rules) {
        if (rule.Name === _ruleName) {
          console.log('Rule already exists with this name');
          hasRuleAlready = true;
          break;
        }
      }
    } while (rulesList.NextToken);
        
    // Create the rule if it doesn't already exist
    if (!hasRuleAlready) {
      let response = await cwe.putRule({
        Name: _ruleName,
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
        Rule: _ruleName,
      }
      if (targetsList && targetsList.NextToken) {
        request.NextToken = targetsList.NextToken;
      }
      targetsList = await cwe.listTargetsByRule(request).promise();
      for (let target of targetsList.Targets) {
        if (target.Arn === queueInfo.queueArn) {
          console.log('Already have a target');
          hasTargetAlready = true;
          break;
        }
      }
    } while(targetsList.NextToken);

    if (!hasTargetAlready) {
      console.log('Setting up target for ' + JSON.stringify(queueInfo.queueArn));
      let response = await cwe.putTargets({
        Rule: _ruleName,
        Targets: [{
          Arn: queueInfo.queueArn,
          Id: _ruleName + '-target',
        }]
      }).promise();

      if (response.FailedEntryCount > 0) {
        throw new Error('Failed to set up target');
      }
    }
  } 
}

module.exports = setupCloudWatch;
