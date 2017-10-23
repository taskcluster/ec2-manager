const assert = require('assert');
const {runAWSRequest} = require('./aws-request');
const Iterate = require('taskcluster-lib-iterate');

const log = require('./log');

const BATCH_SIZE = 100;

class AmiExistencePoller {

  constructor({ec2, runaws = runAWSRequest, amiPollDelay = 25, state}) {
    assert(typeof ec2 === 'object');
    assert(typeof state === 'object');

    this.ec2 = ec2;
    this.state = state;
    this.runaws = runaws;

    this.iterator = new Iterate({
      maxIterationTime: 1000 * 60 * 15,
      watchDog: 1000 * 60 * 15,
      maxFailures: 10,
      waitTime: amiPollDelay,
      handler: async (watchdog) => {
        try {
          await this.poll();
        } catch (err) {
          console.dir(err.stack || err);
          monitor.reportError(err, 'Error polling pricing data');
        }
      }
    });
  };

  start() {
    return this.iterator.start();
  }

  stop() {
    return this.iterator.stop();
  }

  async poll(amiId) {
    assert(typeof amiId === 'string', 'ami is not string, rather a ' + typeof ami);
    const amisInDatabase = await this.state.listAmiUsage({ id: amiId });
    assert(amisInDatabase.length, 'ami not in database');

    const {Images} = await this.runaws(this.ec2, 'describeImages', {
      ImageIds: [amiId],
    });

    return (Images.length === 1 && Images[0].ImageId === amiId && Images[0].State === 'available');
  }
}

module.exports = {AmiExistencePoller};
