const assert = require('assert');
const delayer = require('./delayer');
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
async function runAWSRequest(service, method, body) {
  assert(typeof service === 'object');
  assert(typeof method === 'string');
  assert(typeof body === 'object');

  let region = service.config.region || 'unknown-region';
  let serviceId = service.serviceIdentifier || 'unknown-service';

  let request;

  try {
    // We have to have a reference to the AWS.Request object
    // because we'll later need to refer to its .response property
    // to find out the requestId. part 1/2 of the hack
    request = service[method](body);
    let response = await Promise.race([
      request.promise(),
      delayer(240 * 1000)().then(() => {
        let err = new Error(`Timeout in ${region} ${serviceId}.${method}`);
        err.region = region;
        err.service = serviceId;
        err.body = body;
        throw err;
      }),
    ]);
    return response;
  } catch (err) {
    let logObj = {
      //err,
      method,
    };

    // We want to have properties we think might be in the error and
    // are relevant right here
    for (let prop of ['code', 'region', 'service', 'requestId']) {
      if (err[prop]) { logObj[prop] = err[prop]; }
    }

    // Grab the request id if it's there. part 2/2 of the hack
    if (request.response && request.response.requestId) {
      if (logObj.requestId) {
        logObj.requestIdFromHack = request.response.requestId;
      } else {
        logObj.requestId = request.response.requestId;
      }
    }

    // For the region and service, if they don't already exist, we'll
    // set it to the values here.
    if (!logObj.region) {
      logObj.region = region;
    }
    if (!logObj.service) {
      logObj.service = serviceId;
    }

    // We're going to add these in because they're handy to have
    if (!err.region) {
      err.region = region;
    }
    if (!err.service) {
      err.service = serviceId;
    }
    if (!err.method) {
      err.method = method;
    }

    logObj.msg = 'aws request failure';
    console.log(JSON.stringify(logObj));

    // We're just logging here so rethrow
    throw err;
  }
}

module.exports = {runAWSRequest};
