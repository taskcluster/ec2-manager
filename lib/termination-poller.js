const assert = require('assert');
const {runAWSRequest} = require('./aws-request');
const Iterate = require('taskcluster-lib-iterate');
const log = require('./log');

const {TERMINATION_CODES} = require('./state');

const INSTANCE_LIMIT = 200;
const POLL_DELAY = 30;

class TerminationPoller {
  constructor({ec2, regions, runaws = runAWSRequest, pollDelay = POLL_DELAY, state, monitor}) {
    assert(typeof ec2 === 'object');
    assert(Array.isArray(regions));
    regions.forEach(region => assert(typeof region === 'string'));
    assert(typeof state === 'object');
    assert(typeof monitor === 'object');

    this.ec2 = ec2;
    this.regions = regions;
    this.state = state;
    this.runaws = runaws;
    this.monitor = monitor;

    this.iterator = new Iterate({
      maxIterationTime: 60 * 15,
      watchDog: 60 * 15,
      maxFailures: 10,
      waitTime: pollDelay,
      handler: async(watchdog) => {
        try {
          await this.poll(watchdog);
        } catch (err) {
          log.error({err}, 'iteration error');
        }
      },
    });

  }

  start() {
    return this.iterator.start();
  }

  stop() {
    return this.iterator.stop();
  }

  async poll(watchdog, sharedState) {
    let toPoll = await this.state.findTerminationsToPoll(INSTANCE_LIMIT);

    if (toPoll.length === 0) {
      return;
    }

    let resolutions = [];

    await Promise.all(this.regions.map(async region => {

      let instanceIds = toPoll.filter(x => x.region === region).map(x => x.id);

      if (instanceIds.length === 0) {
        log.debug({region}, 'found no terminations, skipping this region');
        return;
      }

      log.info({instanceIds, region}, 'polling terminations');

      // Now let's ask AWS about these instances.  If the describeInstances call
      // fails we'll give up and try again
      let result;
      try {
        result = await this.runaws(this.ec2[region], 'describeInstances', {
          InstanceIds: instanceIds,
        });
      } catch (err) {
        log.warn({err}, 'failed to describe instances for termination');
        return;
      }

      watchdog.touch();

      let lastEvent = new Date();

      // Now let's figure out what the code and reason are for these instances.
      // We're going to add these to a list of resolutions so that we can do a
      // single transaction later using UPDATE...ON CONFLICT... logic to
      // minimize DB locking
      for (let reservation of result.Reservations || []) {
        for (let instance of reservation.Instances || []) {
          if (instance.StateReason && instance.StateReason.Code && instance.StateReason.Message) {
            let msg = instance.StateReason.Message;
            let code = instance.StateReason.Code;
            if (msg.slice(0, code.length + 2) === code + ': ') {
              msg = msg.slice(code.length + 2);
            }
            resolutions.push({
              region,
              id: instance.InstanceId,
              code: code,
              reason: msg,
              lastEvent,
            });

            // We want to measure each termination code's regularity

            // NOTE: Ideally we'd be using the database to get these values,
            // but that would require an otherwise unneeded query to the DB to
            // get these values.  Since these values are set in the
            // ec2-manager, we know.  As a result, we'll just grab the data out
            // of the underlying instance object returned by the EC2 api.  Be
            // forewarned, those who change how these values are set up
            let workerType = 'unknown-worker-type';
            for (let tag of instance.Tags || []) {
              if (tag.Key === 'Name') {
                workerType = tag.Value;
              }
            }
            let instanceType = instance.InstanceType;
            let codeType;
            if (TERMINATION_CODES.clean_shutdown.includes(code)) {
              codeType = 'clean';
            } else {
              codeType = 'exceptional';
              let errMsg = [
                `THIS IS A WORKER TYPE CONFIGURATION ISSUE OF ${workerType} OR `,
                `AN EC2 SERVICE DISRUPTION IN ${region}/${instanceType}!!!  `,
                'This report is part of the normal operation of EC2-Manager ',
                `and is not a bug: ${code}: ${msg}`,
              ];
              this.monitor.reportError(new Error(errMsg.join('')), 'info', {
                workerType,
                instanceType,
                region,
              });
            }
            let groupings = [
              'overall',
              'instance-type.' + instance.InstanceType,
              'worker-type.' + workerType,
              'region.' + region,
            ].map(x => `termination-code.${codeType}.${code}.${x}`);
            for (let grouping of groupings) {
              this.monitor.count(grouping);
            }
          } else {
            log.info({instance, region}, 'found terminated instance without code or reason');
            // We want to report *something* to say that we weren't able to find a code or reason
            // for the shutdown
            let errMsg = [
              `THIS IS A WORKER TYPE CONFIGURATION ISSUE OF ${workerType} OR `,
              `AN EC2 SERVICE DISRUPTION IN ${region}/${instanceType}!!!  `,
              'This report is part of the normal operation of EC2-Manager ',
              'and is not a bug: unable to determine error code or reason for termination',
            ];
            this.monitor.reportError(new Error(errMsg.join('')), 'info', {
              workerType,
              instanceType,
              region,
            });
          }
        }
      }
      watchdog.touch();

    }));

    // Now let's update them.  We're not using any transactions here because we
    // know that each instance's resolution should attempt update regardless of
    // whether others fail.
    for (let resolution of resolutions) {
      try {
        await this.state.updateTerminationState(resolution);
      } catch (err) {
        log.error({err, resolution}, 'error setting resolution reason');
      }
    }

    if (resolutions.length > 0) {
      log.info({
        resolutions: resolutions.map(x => {
          return {
            id: x.id,
            region: x.region,
            code: x.code,
          };
        }),
      }, 'found termination reasons');
    } else {
      log.info('no termination reasons found');
    }

    watchdog.touch();
  }

}

module.exports = {
  TerminationPoller,
};

