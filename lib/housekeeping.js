const assert = require('assert');
const {runAWSRequest} = require('./aws-request');
const log = require('./log');

class HouseKeeper {

  constructor({ec2, state, regions, managerId, maxInstanceLifeHours = 96, monitor, runaws = runAWSRequest}) {
    assert(typeof ec2 === 'object');
    assert(typeof state === 'object');
    assert(typeof regions === 'object');
    assert(Array.isArray(regions));
    assert(typeof managerId === 'string');
    assert(typeof runaws === 'function');
    assert(typeof maxInstanceLifeHours === 'number');
    assert(maxInstanceLifeHours > 0);
    assert(typeof monitor === 'object');
    this.ec2 = ec2;
    this.state = state;
    this.regions = regions;
    this.managerId = managerId;
    this.runaws = runaws;
    this.maxInstanceLifeHours = maxInstanceLifeHours;
    this.monitor = monitor;
  }

  async sweep() {
    log.info('starting housekeeping');

    /* A Zombie is considered to be an instance which has lived for too long.
     * These instances are often long living because they've forgotten to shut
     * themselves down.  When that happens, we could end up spending a lot of
     * money on instances which aren't doing anything productive.
     *
     * This is usually 96 Hours, but is configurable.  Instances should still
     * shut themselves down well before this time, ideally at or before 72
     * hours
     */
    let killIfOlder = new Date();
    killIfOlder.setHours(killIfOlder.getHours() - this.maxInstanceLifeHours);

    let outcomeInfo = {};

    // We want to do housekeeping for each region independently.
    await Promise.all(this.regions.map(async region => {
      let zombieIds = [];
      let apiInstanceIds = [];

      let stateInstances = await this.state.listInstances({region});
      let stateInstanceIds = stateInstances.map(x => x.id);

      let instances = await this.runaws(this.ec2[region], 'describeInstances', {
        Filters: [{
          Name: 'tag:Owner',
          Values: [this.managerId],
        }, {
          Name: 'instance-state-name',
          Values: ['running', 'pending'],
        }],
      }, 4 * 60 * 1000);

      let missingInstances = 0;
      let extraneousInstances = 0;

      // Iterate over the API's view of our account.  Check if each instance in
      // the region is a zombie (been online too long).  If it's a zombie, add
      // it to the list of instances to kill.  If the instance is not a zombie
      // but is not in our internal state, add it to our state.
      for (let reservation of instances.Reservations) {
        for (let instance of reservation.Instances) {
          if (instance.LaunchTime) {
            let id = instance.InstanceId;

            // We'll try to find the managerId (Owner) and workerType (Name)
            // tags from EC2.  
            let managerId;
            let workerType;
            for (let tag of instance.Tags || []) {
              if (tag.Key === 'Owner') {
                managerId = tag.Value;
              } else if (tag.Key === 'Name') {
                workerType = tag.Value;
              }
            }

            // If it doesn't have a manager id, we know it isn't owned by us
            // and we skip it
            if (!managerId) {
              log.error({instanceId: id}, 'missing Owner tag');
              continue;
            }

            // If it does have a manager id that isn't ours, we skip it
            if (managerId !== this.managerId) {
              continue;
            }

            // But if it has our managerId and no workerType, then we know it's
            // bad and needs to be terminated
            if (!workerType) {
              log.error({instanceId: id, region}, 'missing Name tag');
              zombieIds.push(id);
              continue;
            }

            let instanceType = instance.InstanceType;
            let state = instance.State.Name;
            let imageId = instance.ImageId;
            let launched = new Date(instance.LaunchTime);
            let az = instance.Placement.AvailabilityZone;

            let launchTime = new Date(instance.LaunchTime);
            if (launchTime < killIfOlder) {
              zombieIds.push(id);
            } else {
              apiInstanceIds.push(id);
              if (!stateInstanceIds.includes(id)) {
                await this.state.upsertInstance({
                  workerType,
                  region,
                  instanceType,
                  id,
                  state,
                  imageId,
                  az,
                  launched: launched,
                  lastEvent: new Date(),
                });
                this.monitor.count('state.instance-missing-from-state.global', 1);
                this.monitor.count(`state.instance-missing-from-state.${region}`, 1);
                missingInstances++;
              }
            }
          }
        }
      }

      // Kill all the zombies that we've found in as few calls to the API as we can
      if (zombieIds.length > 0) {
        await this.runaws(this.ec2[region], 'terminateInstances', {
          InstanceIds: zombieIds,
        });
        await Promise.all(zombieIds.map(id => this.state.removeInstance({region, id})));

        log.info({zombieIds}, 'killed zombie instances');

        this.monitor.count('found-zombie.global', zombieIds.length);
        this.monitor.count(`found-zombie.${region}`, zombieIds.length);
        // We're going to remove these instances from those in state rather
        // than refetching the state after the removals from the state object.
        stateInstanceIds = stateInstanceIds.filter(x => !zombieIds.includes(x));
      }

      // We want to remove any instances which are in our internal state but
      // which are not in the API view of state
      for (let stateId of stateInstanceIds) {
        if (!apiInstanceIds.includes(stateId)) {
          await this.state.removeInstance({region, id: stateId});
          this.monitor.count('state.extraneous-instance.global', 1);
          this.monitor.count(`state.extraneous-instance.${region}`, 1);
          extraneousInstances++;
        }
      }

      outcomeInfo[region] = {
        state: {
          missingInstances,
          extraneousInstances,
        },
        zombies: zombieIds,
      };

    }));

    log.info(outcomeInfo, 'Housekeeping is finished');

    // Only returning for unit testing
    return outcomeInfo;
  }

}

module.exports = {HouseKeeper};
