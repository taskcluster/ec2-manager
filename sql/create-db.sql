-- Set up a PostgreSQL Database for use as the backing
-- data store for the EC2 Manager component

-- Here's the SQL to drop your things
--   DROP TABLE IF EXISTS instances;
--   DROP TABLE IF EXISTS spotrequests;
--   DROP FUNCTION IF EXISTS update_touched();

-- This function updates the 'touched' column on the table
-- it is tied to to ensure that any time we update the entry
-- we automatically update the touched column
--
-- Based on http://stackoverflow.com/a/26284695
CREATE OR REPLACE FUNCTION update_touched()
RETURNS TRIGGER AS $$
BEGIN
  IF row(NEW.*) IS DISTINCT FROM row(OLD.*) THEN
    NEW.touched = now();
    RETURN NEW;
  ELSE
    RETURN OLD;
  END IF;
END;
$$ language 'plpgsql';


-- spotrequests table contains minimal information on
-- any spot requests owned by this ec2 manager
CREATE TABLE IF NOT EXISTS spotrequests (
  id VARCHAR(128) NOT NULL, -- opaque ID per Amazon
  workerType VARCHAR(128) NOT NULL, -- taskcluster worker type
  region VARCHAR(128) NOT NULL, -- ec2 region
  az VARCHAR(128) NOT NULL, -- availability zone
  instanceType VARCHAR(128) NOT NULL, -- ec2 instance type
  state VARCHAR(128) NOT NULL, -- e.g. open, closed, failed
  status VARCHAR(128) NOT NULL, -- e.g. pending-fulfillment
  imageid VARCHAR(128) NOT NULL, -- AMI/ImageId value
  created TIMESTAMPTZ NOT NULL, -- Time spot request was created
  touched TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY(id, region)
);

-- instances table contains minimal information on
-- any instances owned by this ec2 manager.  We don't reference
-- the spotrequest table because the application logic will be
-- required to delete any spotrequests which are outstanding
CREATE TABLE IF NOT EXISTS instances (
  id VARCHAR(128) NOT NULL, -- opaque ID per Amazon
  workerType VARCHAR(128) NOT NULL, -- taskcluster worker type
  region VARCHAR(128) NOT NULL, -- ec2 region
  az VARCHAR(128) NOT NULL, -- availability zone
  instanceType VARCHAR(128) NOT NULL, -- ec2 instance type
  state VARCHAR(128) NOT NULL, -- e.g. running, pending, terminated
  srid VARCHAR(128), -- spot request id if applicable
  imageid VARCHAR(128) NOT NULL, -- AMI/ImageId value
  launched TIMESTAMPTZ NOT NULL, -- Time instance launched
  lastevent TIMESTAMPTZ NOT NULL, -- Time that the last event happened in the api. Used
                                  -- to ensure that we have correct ordering of cloud watch
                                  -- events
  touched TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY(id, region)
);

-- Constraints that I want but don't know how to write
-- 1. When inserting an instance, if there's a spot request with the matching srid use the
-- values from it

-- Automatically keep spotrequests touched parameter up to date
CREATE TRIGGER update_spotrequests_touched
BEFORE UPDATE ON spotrequests
FOR EACH ROW EXECUTE PROCEDURE update_touched();

-- Automatically keep instances touched parameter up to date
CREATE TRIGGER update_instances_touched
BEFORE UPDATE ON instances
FOR EACH ROW EXECUTE PROCEDURE update_touched();

-- Cloudwatch Events Log
-- We want to keep a log of when every cloud watch event was generated
CREATE TABLE IF NOT EXISTS cloudwatchlog (
  region VARCHAR(128), -- ec2 region
  id VARCHAR(128), -- opaque ID per amazon
  state VARCHAR(128), -- state from message
  generated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id, region, state, generated)
);
