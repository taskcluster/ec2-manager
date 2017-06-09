let runAWSRequeset = require('./aws-request');

class SpotChecker {
  constructor({keyPrefix, region, ec2, db}) {
    this.ec2 = ec2;
    this.region = region;
    this.keyPrefix = keyPrefix;
    this.db = db;
  }

  async getAPIState() {
    let pendingIds = await this.db.listSpotRequestsByRegion(this.region);

    let result = await runAWSRequest(this.ec2, 'describeSpotInstanceRequests', {
      Filters: [{
        Name: 'spot-instance-request-id',
        Values: ids
      }],
    });

    states = 


  }

  updateState() {

  }

}
