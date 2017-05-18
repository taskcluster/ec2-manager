const aws = require('aws-sdk');
const sqslib = require('sqs-simple');
const _ = require('lodash');
const _log = require('./log');


/**
 * Create and link the CloudWatch Events rules and targets as well as the SQS
 * queues to send ec2 instance state change messages
 */
async function setupCloudWatchEvents({
  sqs, cwe,
  regions = ['us-east-1'],
  ruleName = 'ec2-instance-state-transitions',
  queueName = 'ec2-events'
}) {
  for (let region of regions) {
    let log = _log.child({region, ruleName, queueName});
    let _sqs = sqs[region];
    let _cwe = cwe[region];

    let queueInfo = await sqslib.initQueue({queueName: queueName, sqs: _sqs});

    // Let's list the rules and determine if the rule already exists
    let hasRuleAlready = false;
    let rulesList;
    do {
      let request = {
        NamePrefix: ruleName
      };
      if (rulesList && rulesList.NextToken) {
        request.NextToken = rulesList.NextToken;
      }
      rulesList = await _cwe.listRules(request).promise();
      for (let rule of rulesList.Rules) {
        if (rule.Name === ruleName) {
          log.info({ruleName}, 'Rule already exists with this name');
          hasRuleAlready = true;
          break;
        }
      }
    } while (rulesList.NextToken);
        
    // Create the rule if it doesn't already exist
    if (!hasRuleAlready) {
      let response = await _cwe.putRule({
        Name: ruleName,
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
      log.info({ruleArn: response.RuleArn}, 'Rule created');
    }

    let hasTargetAlready = false;
    let targetsList;
    do {
      let request = {
        Rule: ruleName,
      }
      if (targetsList && targetsList.NextToken) {
        request.NextToken = targetsList.NextToken;
      }
      targetsList = await _cwe.listTargetsByRule(request).promise();
      for (let target of targetsList.Targets) {
        if (target.Arn === queueInfo.queueArn) {
          log.info({targetArn: target.Arn}, 'Already have a target');
          hasTargetAlready = true;
          break;
        }
      }
    } while(targetsList.NextToken);

    if (!hasTargetAlready) {
      log.info({queueInfo}, 'Setting up target');
      let response = await _cwe.putTargets({
        Rule: ruleName,
        Targets: [{
          Arn: queueInfo.queueArn,
          Id: ruleName + '-target',
        }]
      }).promise();

      if (response.FailedEntryCount > 0) {
        throw new Error('Failed to set up target');
      }
    }
  } 
}

module.exports = setupCloudWatchEvents;
