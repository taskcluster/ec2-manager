const log = require('./log');
const assert = require('assert');
const Iterate = require('taskcluster-lib-iterate');
/* eslint taskcluster/no-for-in: "off" */

function toFloat(v) {
  if (typeof v === 'number') {
    return v;
  } else if (typeof v === 'string') {
    return parseFloat(v, 10);    
  } else {
    throw new Error('Number is not and cannot be a float');
  }
}

class PricingPoller {

  constructor({ec2, timePeriod, regions, runaws, pollDelay, monitor}) {
    assert(typeof ec2 === 'object');
    this.ec2 = ec2;

    assert(typeof runaws === 'function');
    this.runaws = runaws;

    assert(typeof regions === 'object');
    assert(Array.isArray(regions));
    this.regions = regions;

    // Number of minutes to consider for pricing
    assert(typeof timePeriod === 'number');
    this.timePeriod = timePeriod;

    // Number of seconds to wait before polling for pricing again
    assert(typeof pollDelay === 'number');
    this.pollDelay = pollDelay;

    // Monitor instance
    assert(typeof monitor === 'object');
    this.monitor = monitor;

    this.iterator = new Iterate({
      maxIterationTime: 1000 * 60 * 15,
      watchDog: 1000 * 60 * 15,
      maxFailures: 10,
      waitTime: pollDelay,
      handler: async (watchdog) => {
        try {
          await this.poll();
        } catch (err) {
          console.dir(err.stack || err);
          monitor.reportError(err, 'Error polling pricing data');
        }
      },
    });

    // It is important for getPrices() behaviour that this variable be set to
    // undefined before prices are found so we can throw an error appropriately
    this.prices = undefined;
  }

  start() {
    return this.iterator.start();
  }

  stop() {
    return this.iterator.stop();
  }

  /**
   * Take a list of {price, time} and return a price that we should use
   */
  _findSpotPriceForInstanceType(series) {
    let now = new Date();
    let maxPrice = 0;
    for (let {price, time} of series) {
      if (price > maxPrice) {
        maxPrice = price;
      }
    }
    return maxPrice;
  }

  /**
   * Take a list of prices by region and a zone and return an object which maps
   * the instanceType in that region to the price of that instanceType in that
   * region
   */
  _findSpotPricesForRegion({pricePoints}) {
    debugger;
    let zones = {};

    for (let pricePoint of pricePoints) {
      let instanceType = pricePoint.InstanceType;
      let time = new Date(pricePoint.Timestamp);
      let price = toFloat(pricePoint.SpotPrice, 10);
      let zone = pricePoint.AvailabilityZone;

      if (!zones[zone]) {
        zones[zone] = {};
      }
      if (!zones[zone][instanceType]) {
        zones[zone][instanceType] = [];
      }

      zones[zone][instanceType].push({price, time});
    }

    let prices = [];

    for (let zone of Object.keys(zones)) {
      for (let instanceType of Object.keys(zones[zone])) {
        let price = this._findSpotPriceForInstanceType(zones[zone][instanceType]);
        prices.push({zone, instanceType, price});
      }
    }

    return prices;
  }

  /**
   * Stub.  This should return a list of objects in the form:
   * {zone, instanceType, price}
   */
  _findOnDemandPricesForRegion() {
    return [];
  }

  async poll() {
    // Doing this in a for..of loop instead of in a promise because we get way
    // better stacks this way, and since this is an infrequent poller we don't
    // need to worry about speed
    let startTime = new Date();
    startTime.setMinutes(startTime.getMinutes() + this.timePeriod);

    let prices = [];

    for (let region of this.regions) {
      let ec2 = this.ec2[region];
      let [azResult, priceResult] = await Promise.all([
        this.runaws(ec2, 'describeAvailabilityZones', {
          Filters: [{
            Name: 'state',
            Values: ['available'],
          }],
        }),
        this.runaws(ec2, 'describeSpotPriceHistory', {
          StartTime: startTime,
          Filters: [{
            Name: 'product-description',
            Values: ['Linux/UNIX'],
          }],
        }),
      ]);

      // Get the information from the API responses that we care about
      let zones = azResult.AvailabilityZones.map(x => x.ZoneName);
      let pricePoints = priceResult.SpotPriceHistory.filter(x => zones.includes(x.AvailabilityZone));

      // Determine what the prices which the API described.
      let spotPrices = this._findSpotPricesForRegion({pricePoints});
      let onDemandPrices = this._findOnDemandPricesForRegion();

      // Add some information to the spot prices
      spotPrices = spotPrices.map(x => {
        Object.assign(x, {type: 'spot', region});
        return x;
      });

      // Add some information to the on-demand prices
      onDemandPrices = onDemandPrices.map(x => {
        Object.assign(x, {type: 'ondemand', region});
        return x;
      });

      Array.prototype.push.apply(prices, spotPrices);
      Array.prototype.push.apply(prices, onDemandPrices);
    }

    prices.sort((a, b) => {
      return a.price - b.price;
    });
    this.prices = prices;
    log.info({count: prices.length}, 'Found prices');
    log.trace({prices}, 'All Prices');
  }

  /**
   * Return a list of prices filtered by a list of restrictions.  The
   * restrictions are given in the form of a list of objects which each have a
   * 'key' property and a 'restriction' value.  The 'key' property refers to
   * any of the keys on a price object (instanceType, region, zone, type,
   * price).  For non-price restrictions, either a single string can be given
   * or a list of strings.  There is no pattern matching on the strings, only
   * simple equivalence checking.  For price, either a string or numeric value
   * can be given.  The key 'price' checks for an exact price, the 'minPrice'
   * ensures returned prices are at greater than or equal to the 'minPrice'
   * value.  The 'maxPrice' key ensures that returned prices are less than or
   * equal to that price.  Note that this function does not check other
   * restrictions when evaluating a given restriction.  That means if you
   * specify a minimum price which higher than the maximum price, you will get
   * no results
   */
  getPrices(restrictions = []) {
    if (!this.prices) {
      throw new Error('We do not have pricing data yet');
    }
    let prices = this.prices.slice();

    return prices.filter(price => {
      for (let {key, restriction} of restrictions) {
        if (key === 'minPrice' || key === 'maxPrice' || key === 'price') {
          // We need to handle minPrice, maxPrice and price differently because
          // they are numerical comparisons
          restriction = toFloat(restriction);
          if (key === 'minPrice' && price.price < restriction) {
            return false;
          } else if (key === 'maxPrice' && price.price > restriction) {
            return false;
          } else if (key === 'price' && restriction !== price.price) {
            return false;
          }
        } else {
          // For string values, we just want simple comparisons.  When the
          // restriction is a list, we say that any matching value is allowed
          let allowedValues = restriction;

          if (!Array.isArray(restriction)) {
            allowedValues = [restriction];
          }

          if (!allowedValues.includes(price[key])) {
            return false;
          }
        }
      }
      return true;
    });

    return prices;
  }
}

module.exports = {PricingPoller};
