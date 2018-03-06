const assert = require('assert');
const delayer = require('./delayer');
const log = require('./log');

const NS_PER_SEC = 1e9;
const MAX_REQUEST_DURATION = 10 * 1000; // 25s

// Return duration in us based on a start
function duration(start) {
  let diff = process.hrtime(start);
  return (diff[0] * NS_PER_SEC + diff[1]) / 1000;
}

/**
 * This method will run an EC2 operation as a promise.  This promise has a
 * built-in timeout feature, which ensures that even when the AWS-SDK library
 * timeout itself doesn't work, requests complete in finite time.
 */
async function runAWSRequest(service, method, body, state) {
  assert(typeof service === 'object');
  assert(typeof method === 'string');
  assert(typeof body === 'object');
  if (state) {
    assert(typeof state === 'object');
  }

  // This is the information about this request that we're going to log to
  // terminal and also into the awsrequest table
  let requestInfo = {
    region: service.config.region || 'unknown-region',
    service: service.serviceIdentifier || 'unknown-service',
    method,
    called: new Date(),
  };

  // Special service.method's which should get extra metadata
  if (requestInfo.service === 'ec2') {
    if (method === 'runInstances' && body) {
      if (body.TagSpecifications) {
        loop1:
        for (let spec of body.TagSpecifications || []) {
          for (let tag of spec.Tags || []) {
            if (tag.Key === 'Name') {
              requestInfo.workerType = tag.Value;
              break loop1;
            }
          }
        }
      }
      if (body.Placement && body.Placement.AvailabilityZone) {
        requestInfo.az = body.Placement.AvailabilityZone;
      }
      requestInfo.instanceType = body.InstanceType;
      requestInfo.imageId = body.ImageId;
    }
  }

  return new Promise(async(resolve, reject) => {

    // We need to make sure that this promise, if awaited on a second
    // time will resolve or reject exactly the same way, per the promise spec
    let rejectionValue;
    let resolutionValue;
    if (rejectionValue) {
      return reject(rejectionValue);
    }
    if (resolutionValue) {
      return resolve(resolutionValue);
    }

    // Prepare the request
    let request = service[method](body);
    let start;

    // We want to have requests take no longer than this number of
    // milliseconds.  We've found that any timeouts built into the aws-sdk
    // client are unreliable
    let timeout = setTimeout(() => request.abort(), MAX_REQUEST_DURATION);

    // Handle success
    request.once('success', async response => {
      try {
        clearTimeout(timeout);
        requestInfo.duration = duration(start);

        requestInfo.requestId = response.requestId;
        if (!requestInfo.requestId) {
          // Probably paranoia, but better to know and not needed it than the
          // alternative.  We don't want to kill the service though
          let msg = `${requestInfo.service}:${requestInfo.region}.${requestInfo.method} missing requestId`;
          log.warn(new Error(msg), 'missing requestId on succesful request');
          requestInfo.requestId = '----- MISSING REQUEST ID -----';
        }

        requestInfo.error = false;
        log.debug(requestInfo, 'aws request successful');
        await state.logAWSRequest(requestInfo);
        return resolve(resolutionValue = response.data);
      } catch (err) {
        return reject(rejectionValue = err);
      }
    });

    // Handle failure
    request.once('error', async(error, response) => {
      try {
        clearTimeout(timeout);
        requestInfo.duration = duration(start);
        
        requestInfo.error = true;
        requestInfo.code = error.code;
        requestInfo.message = error.message;

        // Try *really* hard to get the requestId
        requestInfo.requestId = error.requestId || response.requestId || request.response.requestId;

        // If we abort the request before we get a requestId, we should still do something to avoid
        // this service from being killed
        if (!requestInfo.requestId && error.code === 'RequestAbortedError') {
          requestInfo.requestId = '----- ABORTED REQUEST -----';
        }
        
        if (!requestInfo.requestId) {
          let msg = `${requestInfo.service}:${requestInfo.region}.${requestInfo.method} missing requestId`;
          log.error(new Error(msg), 'missing requestId on failed request');
          requestInfo.requestId = '----- MISSING REQUEST ID -----';
        }

        await state.logAWSRequest(requestInfo);

        return reject(rejectionValue = error);
      } catch (err) {
        return reject(rejectionValue = err);
      }
    });

    try {
      start = process.hrtime();
      request.send();
    } catch (err) {
      return reject(rejectionValue = err);
    }
  });
}

module.exports = {runAWSRequest};
