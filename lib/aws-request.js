const assert = require('assert');
const delayer = require('./delayer');
const log = require('./log');

const NS_PER_SEC = 1e9;

// Return duration in us based on a start
function duration(start) {
  let diff = process.hrtime(start);
  return (diff[0] * NS_PER_SEC + diff[1]) / 1000;
}

/**
 * This method will run an EC2 operation.  Because the AWS-SDK client is so
 * much fun to work with, we need to do the following things above what it does
 * to get useful information out of it.
 *
 *   1. We want to have exceptions that always have region, method and service
 *      name if available
 *   2. Any requests which would have a requestId should include it in their
 *      exceptions
 *   3. Sometimes promises from AWS-SDK just magically never return and never
 *      timeout even though we've set those options.  We have our own timeout
 *   4. Useful logging that includes useful debugging information
 *   5. Because we're catching the exceptions here, there's a *chance* that
 *      we might get useful stack traces.  AWS-SDK exceptions which aren't
 *      caught seem to have the most utterly useless stacks, which only
 *      have frames in their own state machine and never include the call
 *      site
 *
 * I am halfway tempted to rewrite this file using aws4 because it is a more
 * sensible library.
 *
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

  let request;

  let start = process.hrtime();

  try {
    request = service[method](body);

    let response = await Promise.race([
      request.promise(),
      delayer(240 * 1000)().then(() => {
        let err = new Error(`Timeout in ${region} ${serviceId}.${method}`);
        err.region = region;
        err.service = serviceId;
        err.body = body;

        requestInfo.error = true;
        requestInfo.code = 'TC.Timeout';
        requestInfo.messsage = 'Custom Taskcluster derived timeout -- client froze!';
        requestInfo.duration = duration(start);

        throw err;
      }),
    ]);

    requestInfo.duration = duration(start);

    if (request.response.requestId) {
      requestInfo.requestId = request.response.requestId;
    }

    requestInfo.error = false;

    log.debug(requestInfo, 'aws request success');

    return response;
  } catch (err) {
    // We want to have properties we think might be in the error and
    // are relevant right here
    requestInfo.error = true;
    requestInfo.code = err.code;
    requestInfo.message = err.message;

    if (!requestInfo.duration) {
      requestInfo.duration = duration(start);
    }

    if (!requestInfo.requestId) {
      if (request && request.response && request.response.error) {
        requestInfo.requestId = request.response.error.requestId;
      } else if (request && request.response) {
        requestInfo.requestId = request.response.requestId;
      }
    }

    let newErr = new Error('Failure to run AWS Request');
    Object.assign(newErr, requestInfo);
    newErr.originalErr = err;

    let logObj = Object.assign({}, requestInfo, {body});
    log.error(requestInfo, 'aws request failure');

    throw newErr;
  } finally {
    try {
      await state.logAWSRequest(requestInfo);
    } catch (err2) {
      log.error({err: err2}, 'error reporting aws request');
    }
  }
}

module.exports = {runAWSRequest};
