const assert = require('assert');
const {runAWSRequest} = require('./aws-request');

const log = require('./log');

const BATCH_SIZE = 100;

async function pollSpotRequests({ec2, region, state, runaws = runAWSRequest}) {
  assert(typeof ec2 === 'object');
  assert(typeof region === 'string');
  assert(typeof state === 'object');

  let idsToPoll = await state.spotRequestsToPoll({region});

  log.info({ids: idsToPoll}, 'Polling spot requests');

  let nBatch = Math.ceil(idsToPoll.length, BATCH_SIZE);
  for (let i = 0 ; i < nBatch ; i++) {
    let ids = idsToPoll.slice(i, i + BATCH_SIZE);
    let result = await runaws(ec2, 'describeSpotInstanceRequests', {
      SpotInstanceRequestIds: ids
    });

    let idsToKill = [];

    for (let spotRequest of result.SpotInstanceRequests) {
      let id = spotRequest.SpotInstanceRequestId;
      let requestState = spotRequest.State;
      let status = spotRequest.Status.Code;
      // NOTE:
      // The states which we should continue to poll are 'pending-evaluation' and
      // 'pending-fulfillment'.  For the other states, we should delete them.  For
      // those states which are not open and pending-* and not active, we should
      // also request those instances be killed by the EC2 api because we don't
      // want to wait around for them.
      // I guess this could've been a switch but I'd rather not nest those
      if (requestState === 'open') {
        if (status === 'pending-evaluation' || status === 'pending-fulfillment') {
          await state.updateSpotRequestState({id, region, state: requestState, status});
        } else {
          // NOTE: we'll delete from the database only after the killing has been done
          // this is done here so that we can avoid having zombies
          idsToKill.push(id);
        }
      } else {
        await state.removeSpotRequest({region, id});
      }
    }

    if (idsToKill.length > 0) {
      let killed = await runaws(ec2, 'cancelSpotInstanceRequests', {
        SpotInstanceRequestIds: idsToKill,
      });
      let idsKilled = killed.CancelledSpotInstanceRequests.map(x => {
        return x.SpotInstanceRequestId
      });
      await Promise.all(idsKilled.map(id => {
        console.dir({region, id});
        return state.removeSpotRequest({region, id})
      }));
    }
  }
}

module.exports = {pollSpotRequests};
