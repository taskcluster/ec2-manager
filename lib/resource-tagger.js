const assert = require('assert');

class ResourceTagger {

  constructor({runaws, ec2, keyPrefix}) {
    assert(typeof runaws === 'function');
    this.runaws = runaws;

    assert(typeof ec2 === 'object');
    this.ec2 = ec2;    
 
    assert(typeof keyPrefix === 'string');
    assert(keyPrefix[keyPrefix.length - 1] === ':');
    this.keyPrefix = keyPrefix;
    this.managerId = keyPrefix.slice(0, keyPrefix.length - 1);

  } 
   
  async tagResources({ids, workerType, region}) {
    assert(typeof workerType === 'string');
 
    await this.runaws(this.ec2[region], 'createTags', {
      Tags: [{
        Key: 'Name', Value: workerType,
      }, {
        Key: 'Owner', Value: this.managerId,
      }, {
        Key: 'WorkerType', Value: `${this.managerId}/${workerType}`,
      }],
      Resources: ids,    
    });
  }
}

module.exports = {ResourceTagger};
