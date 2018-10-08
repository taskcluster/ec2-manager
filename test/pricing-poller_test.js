const {PricingPoller} = require('../lib/pricing-poller');
const sinon = require('sinon');
const assume = require('assume');

describe('Pricing', () => {
  const sandbox = sinon.sandbox.create();
  const region = 'us-east-1';
  const zones = ['a', 'b', 'c'].map(x => region + x);
  let describeAZStub;
  let describeSPHStub;

  beforeEach(() => {
    describeAZStub = sandbox.stub();
    describeSPHStub = sandbox.stub();
  });

  afterEach(() => {
    sandbox.restore();
  });

  it('should calculate the correct price for a type', () => {
    let poller = new PricingPoller({
      ec2: {},
      regions: [region],
      runaws: () => {},
      timePeriod: 30,
      pollDelay: 3600,
      monitor: {},
    });

    let result = poller._findSpotPriceForInstanceType([
      {price: 1, time: new Date()},
      {price: 2, time: new Date()},
      {price: 3, time: new Date()},
      {price: 3.5, time: new Date()},
    ]);

    assume(result).equals(3.5);

  });

  it('should find spot prices for a specific region', () => {
    let poller = new PricingPoller({
      ec2: {},
      regions: [region],
      runaws: () => {},
      timePeriod: 30,
      pollDelay: 3600,
      monitor: {},
    });

    let result = poller._findSpotPricesForRegion({pricePoints:[{
      InstanceType: 'm3.medium',
      Timestamp: new Date().toString(),
      SpotPrice: '0.1',
      AvailabilityZone: zones[0],
    }, {
      InstanceType: 'm3.medium',
      Timestamp: new Date().toString(),
      SpotPrice: '0.2',
      AvailabilityZone: zones[1],
    }, {
      InstanceType: 'm3.large',
      Timestamp: new Date().toString(),
      SpotPrice: '0.3',
      AvailabilityZone: zones[0],
    }]});

    let expected = {};
    assume(result).deeply.equals([
      {price: 0.1, zone: zones[0], instanceType: 'm3.medium'}, 
      {price: 0.3, zone: zones[0], instanceType: 'm3.large'}, 
      {price: 0.2, zone: zones[1], instanceType: 'm3.medium'}, 
    ]);

  });

  it('should be able to do a full poll', async () => {
    let poller = new PricingPoller({
      ec2: {},
      regions: [region],
      runaws: (ec2, method, params) => {
        switch (method) {
          case 'describeSpotPriceHistory':
            return describeSPHStub(ec2, method, params);
            break;
          case 'describeAvailabilityZones':
            return describeAZStub(ec2, method, params);
            break;
          default:
            throw new Error('Unknown method');
        }
      },
      timePeriod: 30,
      pollDelay: 3600,
      monitor: {},
    });

    describeSPHStub.onFirstCall().returns({
      NextToken: 'abc123',
      SpotPriceHistory: [{
        Timestamp: '2017-07-05T13:11:21.000Z', 
        AvailabilityZone: zones[0], 
        InstanceType: 'm3.medium', 
        ProductDescription: 'Linux/UNIX', 
        SpotPrice: '0.1',
      }, {
        Timestamp: '2017-07-05T13:14:21.000Z', 
        AvailabilityZone: zones[0], 
        InstanceType: 'm3.medium', 
        ProductDescription: 'Linux/UNIX', 
        SpotPrice: '0.2',
      }, {
        Timestamp: '2017-07-05T13:14:21.000Z', 
        AvailabilityZone: zones[0], 
        InstanceType: 'm3.xlarge', 
        ProductDescription: 'Linux/UNIX', 
        SpotPrice: '0.7',
      }, {
        Timestamp: '2017-07-05T13:14:21.000Z', 
        AvailabilityZone: zones[0], 
        InstanceType: 'm3.xlarge', 
        ProductDescription: 'Linux/UNIX', 
        SpotPrice: '0.1',
      }, {
        Timestamp: '2017-07-05T13:14:21.000Z', 
        AvailabilityZone: zones[1], 
        InstanceType: 'm3.medium', 
        ProductDescription: 'Linux/UNIX', 
        SpotPrice: '0.1',
      }],
    });

    describeSPHStub.onSecondCall().returns({
      SpotPriceHistory: [{
        Timestamp: '2017-08-05T13:11:21.000Z',
        AvailabilityZone: 'd',
        InstanceType: 'm3.medium',
        ProductDescription: 'Linux/UNIX',
        SpotPrice: '1.1',
      }, {
        Timestamp: '2017-08-05T13:14:21.000Z',
        AvailabilityZone: 'd',
        InstanceType: 'm3.medium',
        ProductDescription: 'Linux/UNIX',
        SpotPrice: '1.2',
      }, {
        Timestamp: '2017-08-05T13:14:21.000Z',
        AvailabilityZone: 'd',
        InstanceType: 'm3.xlarge',
        ProductDescription: 'Linux/UNIX',
        SpotPrice: '1.7',
      }, {
        Timestamp: '2017-08-05T13:14:21.000Z',
        AvailabilityZone: 'd',
        InstanceType: 'm3.xlarge',
        ProductDescription: 'Linux/UNIX',
        SpotPrice: '1.1',
      }, {
        Timestamp: '2017-08-05T13:14:21.000Z',
        AvailabilityZone: zones[2],
        InstanceType: 'm3.medium',
        ProductDescription: 'Linux/UNIX',
        SpotPrice: '1.2',
      }],
    });

    describeAZStub.onFirstCall().returns({
      AvailabilityZones: [{
        State: 'available', 
        ZoneName: zones[0], 
        Messages: [], 
        RegionName: region,
      }, {
        State: 'available', 
        ZoneName: zones[1], 
        Messages: [], 
        RegionName: region,
      }, {
        State: 'available', 
        ZoneName: zones[2], 
        Messages: [], 
        RegionName: region,
      }],
    });

    //describeAZStub.onSecondCall().throws();

    await poller.poll();

    let expected = [
      {instanceType: 'm3.medium', price: 0.1, region, type: 'spot', zone: zones[1]},
      {instanceType: 'm3.medium', price: 0.2, region, type: 'spot', zone: zones[0]},
      {instanceType: 'm3.xlarge', price: 0.7, region, type: 'spot', zone: zones[0]},
      {instanceType: 'm3.medium', price: 1.2, region, type: 'spot', zone: zones[2]},
    ];

    assume(describeSPHStub.callCount).is.equal(2);
    assume(describeAZStub.callCount).is.equal(1);
    assume(describeSPHStub.secondCall.args[2].NextToken).equals('abc123');
    assume(poller.prices).deeply.equals(expected);
  });

  describe('Getting prices', () => {
    let poller;

    beforeEach(() => {
      poller = new PricingPoller({
        ec2: {},
        regions: [region],
        runaws:() => {},
        timePeriod: 30,
        pollDelay: 3600,
        monitor: {},
      });
    });

    it('should get a price without restrictions', () => {
      let expected = [
        {instanceType: 'm3.medium', price: 0.1, region, type: 'spot', zone: zones[1]}, 
      ];

      poller.prices = expected;
      assume(poller.getPrices()).deeply.equals(poller.prices);
    });

    it('should get a price with a single string restriction', () => {
      let expected = [
        {instanceType: 'm3.medium', price: 0.1, region, type: 'spot', zone: zones[1]}, 
      ];

      poller.prices = expected.slice();
      poller.prices.push({
        instanceType: 'm3.large',
        price: 0.1,
        region,
        type: 'spot',
        zone: zones[1],
      });
      let actual = poller.getPrices([{key: 'instanceType', restriction: 'm3.medium'}]);
      assume(actual).deeply.equals(expected);
    });
    
    it('should get a price with multiple string restrictions', () => {
      let expected = [
        {instanceType: 'm3.medium', price: 0.1, region, type: 'spot', zone: zones[1]}, 
        {instanceType: 'm3.large', price: 0.1, region, type: 'spot', zone: zones[1]}, 
      ];

      poller.prices = expected.slice();
      poller.prices.push({
        instanceType: 'm3.xlarge',
        price: 0.1,
        region,
        type: 'spot',
        zone: zones[1],
      });

      let actual = poller.getPrices([
        {key: 'instanceType', restriction: ['m3.medium', 'm3.large']},
      ]);
      assume(actual).deeply.equals(expected);
    });
    
    it('should get a price with a string and numeric price restriction', () => {
      let expected = [
        {instanceType: 'm3.medium', price: 0.2, region, type: 'spot', zone: zones[1]}, 
      ];

      poller.prices = expected.slice();
      poller.prices.push({
        instanceType: 'm3.xlarge',
        price: 0.1,
        region,
        type: 'spot',
        zone: zones[1],
      });
      poller.prices.push({
        instanceType: 'm3.xlarge',
        price: 0.3,
        region,
        type: 'spot',
        zone: zones[1],
      });

      let actual = poller.getPrices([
        {key: 'price', restriction: '0.2'},
      ]);
      assume(actual).deeply.equals(expected);

      actual = poller.getPrices([
        {key: 'price', restriction: 0.2},
      ]);
      assume(actual).deeply.equals(expected);
    });    

    it('should get a price with a string and numeric min and max price restriction', () => {
      poller.prices = [{
        instanceType: 'm3.xlarge',
        price: 0.1,
        region,
        type: 'spot',
        zone: zones[1],
      }, {
        instanceType: 'm3.xlarge',
        price: 0.2,
        region,
        type: 'spot',
        zone: zones[1],
      }, {
        instanceType: 'm3.xlarge',
        price: 0.3,
        region,
        type: 'spot',
        zone: zones[1],
      }];

      assume(poller.getPrices([
        {key: 'minPrice', restriction: 0.2},
      ])).deeply.equals([
        {instanceType: 'm3.xlarge', price: 0.2, region, type: 'spot', zone: zones[1]}, 
        {instanceType: 'm3.xlarge', price: 0.3, region, type: 'spot', zone: zones[1]}, 
      ]);
      
      assume(poller.getPrices([
        {key: 'minPrice', restriction: '0.2'},
      ])).deeply.equals([
        {instanceType: 'm3.xlarge', price: 0.2, region, type: 'spot', zone: zones[1]}, 
        {instanceType: 'm3.xlarge', price: 0.3, region, type: 'spot', zone: zones[1]}, 
      ]);
      
      assume(poller.getPrices([
        {key: 'maxPrice', restriction: 0.2},
      ])).deeply.equals([
        {instanceType: 'm3.xlarge', price: 0.1, region, type: 'spot', zone: zones[1]}, 
        {instanceType: 'm3.xlarge', price: 0.2, region, type: 'spot', zone: zones[1]}, 
      ]);

      assume(poller.getPrices([
        {key: 'maxPrice', restriction: '0.2'},
      ])).deeply.equals([
        {instanceType: 'm3.xlarge', price: 0.1, region, type: 'spot', zone: zones[1]}, 
        {instanceType: 'm3.xlarge', price: 0.2, region, type: 'spot', zone: zones[1]}, 
      ]);

      assume(poller.getPrices([
        {key: 'minPrice', restriction: 0.2},
        {key: 'maxPrice', restriction: 0.2},
      ])).deeply.equals([
        {instanceType: 'm3.xlarge', price: 0.2, region, type: 'spot', zone: zones[1]}, 
      ]);

      assume(poller.getPrices([
        {key: 'minPrice', restriction: '0.2'},
        {key: 'maxPrice', restriction: '0.2'},
      ])).deeply.equals([
        {instanceType: 'm3.xlarge', price: 0.2, region, type: 'spot', zone: zones[1]}, 
      ]);
    });
  });
  
});

