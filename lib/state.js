const redis = require('redis');
const bluebird = require('bluebird');

bluebird.promisifyAll(redis.RedisClient.prototype);
bluebird.promisifyAll(redis.Multi.prototype);

class State {
  constructor() {
    this.redis = new redis.createClient();
    this.redis.on('error', err => {
      console.dir(err);
      process.nextTick(() => { throw err });
    });
  }

  async setInstanceState(instance, state) {
    await this.redis.setAsync('instance_state_' + instance, state);
  }

  async getInstanceState(instance) {
    return await this.redis.getAsync('instance_state_' + instance);
  }
}

module.exports = State;
