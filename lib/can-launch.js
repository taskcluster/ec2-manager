const assert = require('assert');

const MANDATORY_KEYS = [
  'ImageId',
  'InstanceType',
  'KeyName',
];

const ALLOWED_KEYS = [
  'SecurityGroups',
  'AddressingType',
  'SecurityGroups',
  'AddressingType',
  'BlockDeviceMappings',
  'EbsOptimized',
  'IamInstanceProfile',
  'KernelId',
  'MonitoringEnabled',
  'NetworkInterfaces',
  'Placement',
  'RamdiskId',
  'SubnetId',
  'UserData',
].concat(MANDATORY_KEYS);

class LaunchSpecificationChecker {

  constructor({regions, ec2, runaws}) {
    assert(regions);
    assert(Array.isArray(regions));
    assert(typeof ec2 === 'object');
    this.regions = regions;
    this.ec2 = ec2;
    this.runaws = runaws;
  }

  async checkImageId({imageId, region}) {
    return true; // or false
  }

  async checkKernelId({kernelId, region}) {
    return true; // or false
  } 

  async checkSecurityGroups({securityGroups, region}) {
    return true; // or false
  }

  // TODO: Verify what's actually checked, maybe we need to be a little better
  // about this.  Maybe we should submit a real spot request at a the absolute
  // lowest price the api will allowed, knowing it'll be rejected (or basically
  // freee!)
  async checkDryRun({launchSpecification, region}) {
    try {
      let result = await this.runaws(this.ec2[region], 'requestSpotInstances', {
        InstanceCount: 1,
        LaunchSpecification: launchSpecification,
        SpotPrice: '0.01',
        Type: 'one-time',
        DryRun: true
      });
    } catch (err) {
      if (err.code === 'DryRunOperation') {
        return true;
      } else {
        throw err;
      }
    }
  }

  async checkAllRegions({launchSpecifications, regions}) {
    let outcomes = await Promise.all(regions.map(async region => {
      let outcome = await this.check({
        launchSpecification: launchSpecs[region],
        region: region
      });
      outcome.region = region;
      return outcome
    }));

    let success = outcomes.filter(x => !x.outcome).length === 0;
    return {
      outcome: success,
      outcomesDetail: outcomes
    };
  }

  async check({launchSpecification, region}) {
    let reasons = [];
    if (!this.regions.includes(region)) {
      reasons.push('Not in an allowed region');
    }
    let lsKeys = Object.keys(launchSpecification);
    for (let key of MANDATORY_KEYS) {
      if (!lsKeys.includes(key)) {
        reasons.push('Mandatory key ' + key + ' not present');
      }
    }
    for (let key of lsKeys) {
      if (!ALLOWED_KEYS.includes(key)) {
        reasons.push('Key ' + key + ' is not allowed');
      }
    }
    let imageId = launchSpec.ImageId;
    let kernelId = launchSpec.KernelId;
    let securityGroups = launchSpec.SecurityGroups;

    if (!checkImageId({imageId, region})){
      reasons.push('Image Id ' + imageId + ' not found');
    }

    if (!checkSecurityGroups({securityGroups, region})) {
      reasons.push('One or more security groups missing');
    }

    if (kernelId) {
      if (!checkKernelId({kernelId, region})){
        reasons.push('Kernel Id ' + kernelId + ' not found');
      }
    }
    
    return {
      outcome: reasons.length === 0,
      reasons: reasons,
    };
  }
}
