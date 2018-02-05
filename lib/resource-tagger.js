const assert = require('assert');

class ResourceTagger {

  constructor({runaws, ec2, managerId}) {
    assert(typeof runaws === 'function');
    assert(typeof ec2 === 'object');
    assert(typeof managerId === 'string');

    this.runaws = runaws;
    this.ec2 = ec2;    
    this.managerId = managerId;

  } 
  
  generateTags({workerType}) {
    return [{
      Key: 'Name', Value: workerType,
    }, {
      Key: 'Owner', Value: this.managerId,
    }, {
      Key: 'WorkerType', Value: `${this.managerId}/${workerType}`,
    }];
  }

  async tagResources({ids, workerType, region}) {
    assert(Array.isArray(ids));
    assert(typeof workerType === 'string');
    assert(typeof region === 'string');

    await this.runaws(this.ec2[region], 'createTags', {
      Tags: this.generateTags({workerType}),
      Resources: ids,    
    });
  }
}

module.exports = {ResourceTagger};
