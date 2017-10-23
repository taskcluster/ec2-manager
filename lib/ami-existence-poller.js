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
    this.amis = [];

    this.iterator = new Iterate({
      maxIterationTime: 1000 * 60 * 15,
      watchDog: 1000 * 60 * 15,
      maxFailures: 10,
      waitTime: amiPollDelay,
      handler: async(watchdog) => {
        try {
          await this.poll();
        } catch (err) {
          console.dir(err.stack || err);
          monitor.reportError(err, 'Error polling pricing data');
        }
      },
    });
  };

  start() {
    return this.iterator.start();
  }

  stop() {
    return this.iterator.stop();
  }

  async loadAmis(amis) {
    this.amis = amis ? amis : (await this.state.listAmiUsage()).map(o => o.id);
  }

  async poll() {
    assert(Array.isArray(this.amis), 'this.amis is not an array');
    let result = [];

    let i = 0;
    while (i < this.amis.length) {
      result = [...result, ...await this.pollBatch(this.amis.slice(i, BATCH_SIZE))];
      i += BATCH_SIZE;
    }

    return result;
  }

  /**
   * Return a list of amiIds not available in EC2
   */
  async pollBatch(amiIds) {
    assert(Array.isArray(amiIds), 'amiIds is not an array');
    amiIds.forEach(amiId => {
      assert(typeof amiId === 'string', `ami ${amiId} is not a string, rather type ${typeof amiId}`);
    });

    const {Images} = await this.runaws(this.ec2, 'describeImages', {
      ImageIds: [amiIds],
    });
    assert(Array.isArray(Images), 'Invalid aws response');

    const imagesMap = new Map(Images.map(i => [i.ImageId, i.State]));

    return amiIds.filter(ami => !(imagesMap.has(ami) && imagesMap.get(ami) === 'available'));
  }
}

module.exports = {AmiExistencePoller};
