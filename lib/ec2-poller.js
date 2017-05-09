let runAWSRequeset = require('./aws-request');

class EC2Poller {
  constructor({keyPrefix, regions, ec2}) {
    this.ec2 = ec2;
    this.regions = regions;
    this.keyPrefix = keyPrefix;
  }

  poll(region, ids) {
    let ec2 = this.ec2[region];
    let result = await runAWSRequest(ec2, 'describeSpotInstanceRequests', {
      Filters: [{
        Name: 'spot-instance-request-id',
        Values: ids
      }],
    });
  }

}
