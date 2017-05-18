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
  id VARCHAR(128) NOT NULL,
  workerType VARCHAR(128) NOT NULL,
  region VARCHAR(128) NOT NULL,
  instanceType VARCHAR(128) NOT NULL,
  state VARCHAR(128) NOT NULL,
  touched TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY(id, region)
);

-- instances table contains minimal information on
-- any instances owned by this ec2 manager.  We don't reference
-- the spotrequest table because the application logic will be
-- required to delete any spotrequests which are outstanding
CREATE TABLE IF NOT EXISTS instances (
  id VARCHAR(128) NOT NULL,
  workerType VARCHAR(128) NOT NULL,
  region VARCHAR(128) NOT NULL,
  instanceType VARCHAR(128) NOT NULL,
  state VARCHAR(128) NOT NULL,
  srid VARCHAR(128),
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


-- Here's a couple of inserts which will work with
-- these queries:
--INSERT INTO spotrequests (id, workerType, region, instanceType, state)
--VALUES ('r-1234', 'test-workertype', 'us-east-1', 'm3.xlarge', 'open');

--INSERT INTO instances (id, workerType, region, instanceType, state, srid)
--VALUES ('i-1235', 'test-workertype', 'us-east-1', 'm3.xlarge', 'running', 'r-1234');
