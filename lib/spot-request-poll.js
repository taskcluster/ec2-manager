const assert = require('assert');
const {runAWSRequest} = require('./aws-request');

const BATCH_SIZE = 100;

async function pollSpotRequests({ec2, region, state}) {
  assert(typeof ec2 === 'object');
  assert(typeof region === 'string');
  assert(typeof state === 'object');

  let idsToPoll = await state.spotRequestsToPoll({region});

  console.log('Polling for changes in ' + idsToPoll.join(', '));

  let nBatch = Math.ceil(idsToPoll.length, BATCH_SIZE);
  for (let i = 0 ; i < nBatch ; i++) {
    let ids = idsToPoll.slice(i, i + BATCH_SIZE);
    let result = await runAWSRequest(ec2, 'describeSpotInstanceRequests', {
      SpotInstanceRequestIds: ids
    });

    for (let spotRequest of result.SpotInstanceRequests) {
      let id = spotRequest.SpotInstanceRequestId;
      let requestState = spotRequest.State;
      let status = spotRequest.Status.Code;
      await state.updateSpotRequestState({id, region, state: requestState, status});
    }
  }
}

module.exports = {pollSpotRequests};
