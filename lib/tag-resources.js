const assert = require('assert');

async function tagResources({runaws, ec2, ids, keyPrefix, workerType}) {
  assert(typeof runaws === 'function');
  assert(typeof ec2 === 'object');
  assert(typeof keyPrefix === 'string');
  assert(typeof workerType === 'string');
  assert(keyPrefix[keyPrefix.length - 1] === ':');
  let managerId = keyPrefix.slice(0, keyPrefix.length - 1);
  await runaws(ec2, 'createTags', {
    Tags: [{
      Key: 'Name', Value: workerType,
    }, {
      Key: 'Owner', Value: managerId,
    }, {
      Key: 'WorkerType', Value: `${managerId}/${workerType}`,
    }],
    Resources: ids,
  });
}

module.exports = {tagResources};
