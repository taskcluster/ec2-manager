-- Set up a PostgreSQL Database for use as the backing
-- data store for the EC2 Manager component

-- Here's the SQL to drop your things
--   DROP TABLE IF EXISTS instances;
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

-- instances table contains minimal information on
-- any instances owned by this ec2 manager
CREATE TABLE IF NOT EXISTS instances (
  id VARCHAR(128) NOT NULL, -- opaque ID per Amazon
  "workerType" VARCHAR(128) NOT NULL, -- taskcluster worker type
  region VARCHAR(128) NOT NULL, -- ec2 region
  az VARCHAR(128) NOT NULL, -- availability zone
  "instanceType" VARCHAR(128) NOT NULL, -- ec2 instance type
  state VARCHAR(128) NOT NULL, -- e.g. running, pending, terminated
  "imageId" VARCHAR(128) NOT NULL, -- AMI/ImageId value
  launched TIMESTAMPTZ NOT NULL, -- Time instance launched
  "lastEvent" TIMESTAMPTZ NOT NULL, -- Time that the last event happened in the api. Used
                                    -- to ensure that we have correct ordering of cloud watch
                                    -- events
  touched TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY(id, region)
);

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
  received TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id, region, state, generated)
);

-- Amazon Machine Image (ami) usage
CREATE TABLE IF NOT EXISTS amiusage (
  region VARCHAR(128) NOT NULL, -- ec2 region
  id VARCHAR(128) NOT NULL, -- opaque ID per Amazon
  "lastUsed" TIMESTAMPTZ NOT NULL, -- most recent usage
  PRIMARY KEY(id, region)
);

-- Elastic Block Store (EBS) Volumes uasge
-- We are only interested in storing the total number of volumes and the total number
-- of GBs per type/region from the most recent housekeeping sweep.
CREATE TABLE IF NOT EXISTS ebsusage (
  region VARCHAR(128) NOT NULL, -- ec2 region
  volumetype VARCHAR(128) NOT NULL,  -- volume type (e.g gp2, st1)
  state VARCHAR(6) NOT NULL CHECK (state in ('active', 'unused')), -- state of aggregated volumes
  totalcount INTEGER NOT NULL, -- number of volumes in region, of volumetype and state
  totalgb INTEGER NOT NULL, -- number of GBs among volumes in region, of volumetype and state
  touched TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY(region, volumetype, state)
);
