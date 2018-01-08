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

  async checkAllRegions({launchSpecifications, regions}) {
    let outcomes = await Promise.all(regions.map(async region => {
      let outcome = await this.check({
        launchSpecification: launchSpecs[region],
        region: region,
      });
      outcome.region = region;
      return outcome;
    }));

    let success = outcomes.filter(x => !x.outcome).length === 0;
    return {
      outcome: success,
      outcomesDetail: outcomes,
    };
  }

  async check({launchSpecification, region}) {
    assert(typeof launchSpecification === 'object');
    assert(typeof region === 'string');
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
    let imageId = launchSpecification.ImageId;
    let kernelId = launchSpecification.KernelId;
    let securityGroups = launchSpecification.SecurityGroups;

    if (!this.checkImageId({imageId, region})) {
      reasons.push('Image Id ' + imageId + ' not found');
    }

    if (!this.checkSecurityGroups({securityGroups, region})) {
      reasons.push('One or more security groups missing');
    }

    if (kernelId) {
      if (!this.checkKernelId({kernelId, region})) {
        reasons.push('Kernel Id ' + kernelId + ' not found');
      }
    }
    
    return {
      outcome: reasons.length === 0,
      reasons: reasons,
    };
  }
}

module.exports = {LaunchSpecificationChecker};
