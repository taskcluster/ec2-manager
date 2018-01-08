const assert = require('assert');
const {runAWSRequest} = require('./aws-request');
const log = require('./log');

function missingTags(obj) {
  let hasTag = false;
  if (obj.Tags) {
    for (let tag of obj.Tags) {
      if (tag.Key === 'Owner') {
        hasTag = true;
      }
    }
  }
  return !hasTag;
};

class HouseKeeper {

  constructor({ec2, state, regions, keyPrefix, maxInstanceLifeHours = 96, monitor, runaws = runAWSRequest, tagger}) {
    assert(typeof ec2 === 'object');
    assert(typeof state === 'object');
    assert(typeof regions === 'object');
    assert(Array.isArray(regions));
    assert(typeof keyPrefix === 'string');
    assert(typeof runaws === 'function');
    assert(typeof maxInstanceLifeHours === 'number');
    assert(maxInstanceLifeHours > 0);
    assert(typeof monitor === 'object');
    assert(typeof tagger === 'object');
    this.ec2 = ec2;
    this.state = state;
    this.regions = regions;
    this.keyPrefix = keyPrefix;
    this.runaws = runaws;
    this.maxInstanceLifeHours = maxInstanceLifeHours;
    this.monitor = monitor;
    this.tagger = tagger;
  }
  
  _calculateVolumeTotals(tabulatedVolumes) {
    let totalVolumes = {};
    
    Object.keys(tabulatedVolumes).forEach(region => {
      // Initialize totals for each region
      totalVolumes[region] = {};
      
      tabulatedVolumes[region].forEach(volume => {
        let type = volume.VolumeType;
        
        if (!(type in totalVolumes[region])) {
          totalVolumes[region][type] = {};
          totalVolumes[region][type]['active'] = {};
          totalVolumes[region][type]['unused'] = {};
          
          totalVolumes[region][type]['active']['count'] = 0;
          totalVolumes[region][type]['active']['gb'] = 0;
          totalVolumes[region][type]['unused']['count'] = 0;
          totalVolumes[region][type]['unused']['gb'] = 0;
        }
        
        if (volume.State === 'available') {
          totalVolumes[region][type]['unused']['count'] += 1;
          totalVolumes[region][type]['unused']['gb'] += volume.Size;
        } else if (volume.State === 'in-use') {
          totalVolumes[region][type]['active']['count'] += 1;
          totalVolumes[region][type]['active']['gb'] += volume.Size;
        }
      });
    });
    
    return totalVolumes;
  }

  _submitVolumeTotals(totalsByCategory) {
    // Assumption:
    // totalsByCategory is organized as follows:
    // { 
    //   ${region}:  
    //     {
    //       ${vol-type}: {
    //         active: {
    //           gb: value,
    //           count: value
    //         },
    //         unused: {
    //           gb: value,
    //           count: value
    //         }
    //       },
    //       ...
    //     },
    //   ${region}: 
    //     {
    //     ...
    //     },
    //   ...
    // }

    let volumesMonitor = this.monitor.prefix('ebs-volumes');

    assert(typeof totalsByCategory === 'object');

    // Keep track of overall totals, over all regions
    let totalGbUsage = 0;
    let totalCount = 0;
    
    Object.keys(totalsByCategory).forEach(region => {
  
      let volumeTypes = totalsByCategory[region];
      assert(typeof volumeTypes === 'object');

      Object.keys(volumeTypes).forEach(type => {
        assert(typeof type === 'string');

        let statuses = volumeTypes[type];
        assert(statuses.hasOwnProperty('active'));
        assert(statuses.hasOwnProperty('unused'));
        let {active, unused} = statuses;
        
        assert(typeof active === 'object');
        assert(typeof unused === 'object');
        assert(active.hasOwnProperty('gb'));
        assert(active.hasOwnProperty('count'));
        assert(unused.hasOwnProperty('gb'));
        assert(unused.hasOwnProperty('count'));

        volumesMonitor.measure('${region}.unused-gb-usage', unused.gb);
        volumesMonitor.measure('${region}.unused-count', unused.count);
        
        let combinedGbUsage = active.gb + unused.gb;
        let combinedCount = active.count + unused.count;

        volumesMonitor.measure('${region}.gb-usage', combinedGbUsage);
        volumesMonitor.measure('${region}.count', combinedCount);
        volumesMonitor.measure('${region}.type:${type}.gb-usage', combinedGbUsage);
        volumesMonitor.measure('${region}.type:${type}.count', combinedCount);
        totalGbUsage += combinedGbUsage;
        totalCount += combinedCount;
      });
    });
    
    // Update global totals after all regions have been processed
    volumesMonitor.measure('global.gb-usage', totalGbUsage);
    volumesMonitor.measure('global.count', totalCount);
  }

  _handleVolumeData(volumes) {
    // First, calculate total GBs and volumes by region and category
    let totals = this._calculateVolumeTotals(volumes);

    // Now, send an update to each corresponding statsum series with current totals.
    this._submitVolumeTotals(totals);
  }

  async _sweepVolumes(region) {
    // Collect all volume objects returned from AWS in a single array
    let volumes = [];

    // We need to keep track of the NextToken value returned by AWS. Its value is non-null
    // if the number of entries in AWS exceeds MaxResults and the next page can be retrieved
    // by specifying NextToken in a subsequent request.
    let nextToken = null;
    do {
      let awsResults = await this.runaws(this.ec2[region], 'describeVolumes', {
        Filters: [{
          Name: 'status',
          Values: ['available', 'in-use'],
        }],
        MaxResults: 500, // This is the maximum page size allowed by the describeVolumes endpoint.
        NextToken: nextToken,
      });

      volumes = volumes.concat(awsResults.Volumes);
      nextToken = awsResults.NextToken;
    } while (nextToken !== null);

    return volumes;
    
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
    let volumesByRegion = {};

    // We want to do housekeeping for each region independently.
    await Promise.all(this.regions.map(async region => {
      let zombieIds = [];
      let apiInstanceIds = [];
      let resourcesToTag = [];

      let stateInstances = await this.state.listInstances({region});
      let stateInstanceIds = stateInstances.map(x => x.id);

      //let volumes = await this._sweepVolumes(region);
      let volumes = [];
      // After accumlating all of the volumes in this region returned by AWS, add them to
      // the volumesByRegion dictionary. However, we should only bother processing if this
      // region actually has any volumes.
      if (volumes.length > 0) {
        volumesByRegion[region] = volumes;
      }

      let instances = await this.runaws(this.ec2[region], 'describeInstances', {
        Filters: [{
          Name: 'key-name',
          Values: [this.keyPrefix + '*'],
        }, {
          Name: 'instance-state-name',
          Values: ['running', 'pending'],
        }],
      });

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
            let [provisionerId, workerType] = instance.KeyName.split(':');
            let instanceType = instance.InstanceType;
            let state = instance.State.Name;
            let imageId = instance.ImageId;
            let launched = new Date(instance.LaunchTime);
            let az = instance.Placement.AvailabilityZone;

            // NOTE: We want to ensure Zombies were tagged too, since they're part of the
            // total bill!
            if (missingTags(instance)) {
              resourcesToTag.push({id, workerType});
            }

            let launchTime = new Date(instance.LaunchTime);
            if (launchTime < killIfOlder) {
              zombieIds.push(id);
            } else {
              apiInstanceIds.push(id);
              if (!stateInstanceIds.includes(id)) {

                // Note that unlike the cloud watch event listener, we don't
                // need to double check the provisioner id field here since we
                // only get the correct provisioner ids based on the filter
                // that we're giving to the API
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

      // Now we need to categorize our resources which need tags into the
      // appropriate bucket
      this.monitor.count('state.missing-tags.global', resourcesToTag.length);
      this.monitor.count(`state.missing-tags.${region}`, resourcesToTag.length);

      let workerTypes = [];
      for (let resource of resourcesToTag) {
        if (workerTypes[resource.workerType]) {
          workerTypes[resource.workerType].push(resource.id);
        } else {
          workerTypes[resource.workerType] = [resource.id];
        }
      }

      // Now do all the tagging
      for (let workerType of Object.keys(workerTypes)) {
        await this.tagger.tagResources({
          ids: workerTypes[workerType],
          workerType: workerType,
          region: region,
        });
      }

      outcomeInfo[region] = {
        state: {
          missingInstances,
          extraneousInstances,
        },
        zombies: zombieIds,
      };

    }));

    // Calculate totals and update statum series after all regions' volume data
    // has been accumulated.
    this._handleVolumeData(volumesByRegion);
    
    log.info(outcomeInfo, 'Housekeeping is finished');

    // Only returning for unit testing
    return outcomeInfo;
  }

}

module.exports = {HouseKeeper};
