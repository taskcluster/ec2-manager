const assert = require('assert');
const {runAWSRequest} = require('./aws-request');

const log = require('./log');

class ZombieKiller {

  constructor({ec2, state, regions, keyPrefix, maxInstanceLifeHours = 96, monitor, runaws = runAWSRequest}) {
    assert(typeof ec2 === 'object');
    assert(typeof state === 'object');
    assert(typeof regions === 'object');
    assert(Array.isArray(regions));
    assert(typeof keyPrefix === 'string');
    assert(typeof runaws === 'function');
    assert(typeof maxInstanceLifeHours === 'number');
    assert(maxInstanceLifeHours > 0);
    assert(typeof monitor === 'object');
    this.ec2 = ec2;
    this.state = state;
    this.regions = regions;
    this.keyPrefix = keyPrefix;
    this.runaws = runaws;
    this.maxInstanceLifeHours = maxInstanceLifeHours;
    this.monitor = monitor;
  }

  async kill() {
    log.info('starting zombie killer');
    let killIfOlder = new Date();
    killIfOlder.setHours(killIfOlder.getHours() - this.maxInstanceLifeHours);

    let zombieInfo = {};
    await Promise.all(this.regions.map(async region => {
      let ids = [];

      let instances = await this.runaws(this.ec2[region], 'describeInstances', {
        Filters: [{
          Name: 'key-name',
          Values: [this.keyPrefix + '*'],
        }, {
          Name: 'instance-state-name',
          Values: ['running', 'pending'],
        }],
      });

      for (let reservation of instances.Reservations) {
        for (let instance of reservation.Instances) {
          if (instance.LaunchTime) {
            let launchTime = new Date(instance.LaunchTime);
            if (launchTime < killIfOlder) {
              ids.push(instance.InstanceId);
            }
          }
        }
      }

      zombieInfo[region] = ids;
      if (ids.length > 0) {
        await this.runaws(this.ec2[region], 'terminateInstances', {
          InstanceIds: ids,
        });
        await Promise.all(ids.map(id => this.state.removeInstance({region, id})));

        log.info({ids}, 'killed zombie instances');

        this.monitor.count('found-zombie.global', ids.length);
        this.monitor.count(`found-zombie.${region}`, ids.length);
      }
    }));

    log.info(zombieInfo, 'killed zombie instances');
  }

}

module.exports = {ZombieKiller};
